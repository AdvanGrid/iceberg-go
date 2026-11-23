// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

var errDataFileCreate = errors.New("simulated data file create failure")

// failingCreateFS fails every file it is asked to create, as an object store
// that refuses uploads does.
type failingCreateFS struct {
	iceio.LocalFS
}

func (failingCreateFS) Create(string) (iceio.FileWriter, error) {
	return nil, errDataFileCreate
}

// blockingSourceReader yields its one batch and then blocks until its source
// context ends, as a throttled database stream waits for its throttle.
type blockingSourceReader struct {
	ctx     context.Context
	schema  *arrow.Schema
	batch   arrow.RecordBatch
	current arrow.RecordBatch
	err     error
}

func (r *blockingSourceReader) Retain()                        {}
func (r *blockingSourceReader) Release()                       {}
func (r *blockingSourceReader) Schema() *arrow.Schema          { return r.schema }
func (r *blockingSourceReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *blockingSourceReader) Record() arrow.RecordBatch      { return r.current }
func (r *blockingSourceReader) Err() error                     { return r.err }

func (r *blockingSourceReader) Next() bool {
	if r.batch != nil {
		r.current, r.batch = r.batch, nil

		return true
	}
	r.current = nil
	<-r.ctx.Done()
	r.err = context.Cause(r.ctx)

	return false
}

// TestOverwriteCancelsSourceWhenWriteFails verifies that a partitioned or
// unpartitioned overwrite whose data file write fails calls the source cancel given with
// WithOverwriteSourceCancel, so a reader blocked on its source context stops
// and the overwrite returns the write error instead of waiting for that
// context to end on its own.
func TestOverwriteCancelsSourceWhenWriteFails(t *testing.T) {
	t.Run("partitioned", func(t *testing.T) {
		testOverwriteCancelsSourceWhenWriteFails(t, iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
		}))
	})
	t.Run("unpartitioned", func(t *testing.T) {
		testOverwriteCancelsSourceWhenWriteFails(t, *iceberg.UnpartitionedSpec)
	})
}

func testOverwriteCancelsSourceWhenWriteFails(t *testing.T, spec iceberg.PartitionSpec) {
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "part", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"db", "overwrite_source_cancel"}, meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return failingCreateFS{}, nil },
		&rowDeltaCatalog{metadata: meta})

	arrowSc, err := table.SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSc)
	bldr.Field(0).(*array.Int64Builder).Append(1)
	bldr.Field(1).(*array.StringBuilder).Append("a")
	batch := bldr.NewRecordBatch()
	bldr.Release()
	defer batch.Release()

	waitLimit, stopWaiting := context.WithTimeoutCause(t.Context(), 10*time.Second, errors.New("source never cancelled"))
	defer stopWaiting()
	sourceCtx, cancelSource := context.WithCancel(waitLimit)
	defer cancelSource()
	rdr := &blockingSourceReader{ctx: sourceCtx, schema: arrowSc, batch: batch}

	err = tbl.NewTransaction().Overwrite(t.Context(), rdr, nil, table.WithOverwriteSourceCancel(cancelSource))

	require.ErrorIs(t, err, errDataFileCreate)
	require.NoError(t, waitLimit.Err(), "the overwrite returned only when the source context timed out")
	require.ErrorIs(t, rdr.err, context.Canceled)
}
