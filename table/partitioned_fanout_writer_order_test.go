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
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestAppendKeepsBatchOrderWithinPartition verifies that the partitioned
// fanout writer adds the batches of one partition to its data files in input
// order, so sorted input stays sorted even though several workers partition
// the batches concurrently.
func TestAppendKeepsBatchOrderWithinPartition(t *testing.T) {
	const (
		numBatches   = 200
		rowsPerBatch = 64
	)

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "part", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"db", "fanout_order"}, meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&rowDeltaCatalog{metadata: meta})

	arrowSc, err := table.SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)
	batches := make([]arrow.RecordBatch, numBatches)
	for i := range batches {
		bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSc)
		for r := range rowsPerBatch {
			bldr.Field(0).(*array.Int64Builder).Append(int64(i*rowsPerBatch + r))
			bldr.Field(1).(*array.StringBuilder).Append("a")
		}
		batches[i] = bldr.NewRecordBatch()
		bldr.Release()
	}
	rdr, err := array.NewRecordReader(arrowSc, batches)
	require.NoError(t, err)
	for _, b := range batches {
		b.Release()
	}
	defer rdr.Release()

	tbl, err = tbl.Append(t.Context(), rdr, nil)
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, tasks)

	var rows int64
	for _, task := range tasks {
		ids := readInt64Column(t, task.File.FilePath(), 0)
		for i := 1; i < len(ids); i++ {
			require.Less(t, ids[i-1], ids[i], "row %d of %s is out of input order", i, task.File.FilePath())
		}
		rows += int64(len(ids))
	}
	require.EqualValues(t, numBatches*rowsPerBatch, rows)
}

func readInt64Column(t *testing.T, path string, col int) []int64 {
	t.Helper()

	pf, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	defer pf.Close()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := fr.ReadTable(t.Context())
	require.NoError(t, err)
	defer tbl.Release()

	var out []int64
	for _, chunk := range tbl.Column(col).Data().Chunks() {
		out = append(out, chunk.(*array.Int64).Int64Values()...)
	}

	return out
}
