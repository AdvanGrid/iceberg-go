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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCopyOnWriteRewriteKeepsMergeOnReadDeletes checks that a copy-on-write
// rewrite of a data file applies the delete files that already target it. A
// merge-on-read DELETE leaves position deletes against the file; a later
// Overwrite that matches part of the file rewrites it, and rows deleted
// earlier must stay deleted.
func TestCopyOnWriteRewriteKeepsMergeOnReadDeletes(t *testing.T) {
	for _, version := range []string{"2", "3"} {
		t.Run("v"+version, func(t *testing.T) {
			ctx := context.Background()
			tbl := newMergeOnReadTestTableVersion(t, version)
			tbl = appendTenRows(t, tbl)

			tbl, err := tbl.Delete(ctx, iceberg.LessThanEqual(iceberg.Reference("id"), int64(3)), nil)
			require.NoError(t, err)
			require.Equal(t, []int64{4, 5, 6, 7, 8, 9, 10}, idsInTable(t, tbl))

			tbl, err = tbl.Overwrite(ctx, emptyIDDataReader(t), nil,
				table.WithOverwriteFilter(iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(9))))
			require.NoError(t, err)

			assert.Equal(t, []int64{4, 5, 6, 7, 8}, idsInTable(t, tbl),
				"rows 1 to 3 were deleted before the overwrite and must not come back")
		})
	}
}

func emptyIDDataReader(t *testing.T) array.RecordReader {
	t.Helper()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer b.Release()
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	t.Cleanup(rdr.Release)

	return rdr
}
