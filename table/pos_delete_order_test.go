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
	"cmp"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMergeOnReadDeleteWritesSortedPositionDeletes checks that every position
// delete file a merge-on-read DELETE writes is sorted by (file_path, pos), as
// the spec requires. Iceberg Java stops applying a delete file's rows for a
// data file once file_path moves past it, so an unsorted file silently leaves
// deleted rows visible to Java readers.
func TestMergeOnReadDeleteWritesSortedPositionDeletes(t *testing.T) {
	for _, partitioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("partitioned=%t", partitioned), func(t *testing.T) {
			ctx := context.Background()
			tbl := newSortedPosDeleteTestTable(t, partitioned)
			for batch := range 12 {
				tbl = appendCategoryRows(t, tbl, int64(batch*10), 10)
			}

			tbl, err := tbl.Delete(ctx, iceberg.LessThan(iceberg.Reference("v"), int64(5)), nil)
			require.NoError(t, err)

			deleteFiles := positionDeleteFiles(t, tbl)
			require.NotEmpty(t, deleteFiles)
			total := 0
			for _, path := range deleteFiles {
				rows := readPositionDeletes(t, path)
				total += len(rows)
				assert.True(t, slices.IsSortedFunc(rows, comparePositionDelete),
					"position delete file %s is not sorted by (file_path, pos)", path)
			}
			assert.Equal(t, 12*5, total)
			assert.Len(t, idsInTable(t, tbl), 12*5)
		})
	}
}

type positionDelete struct {
	path string
	pos  int64
}

func comparePositionDelete(a, b positionDelete) int {
	return cmp.Or(strings.Compare(a.path, b.path), cmp.Compare(a.pos, b.pos))
}

func newSortedPosDeleteTestTable(t *testing.T, partitioned bool) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "v", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.UnpartitionedSpec
	if partitioned {
		s := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "category",
		})
		spec = &s
	}
	meta, err := table.NewMetadata(schema, spec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: "2",
			table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "sorted_pos_deletes"}, meta, metaLoc, fsF, cat)
}

// appendCategoryRows appends n rows with ids from firstID, v from 0 to n-1,
// and category "x" for even ids, "y" for odd ones.
func appendCategoryRows(t *testing.T, tbl *table.Table, firstID int64, n int) *table.Table {
	t.Helper()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "v", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer b.Release()
	for i := range int64(n) {
		id := firstID + i
		b.Field(0).(*array.Int64Builder).Append(id)
		b.Field(1).(*array.StringBuilder).Append(map[bool]string{true: "x", false: "y"}[id%2 == 0])
		b.Field(2).(*array.Int64Builder).Append(i)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rdr.Release()

	tbl, err = tbl.Append(context.Background(), rdr, nil)
	require.NoError(t, err)

	return tbl
}

func positionDeleteFiles(t *testing.T, tbl *table.Table) []string {
	t.Helper()

	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)

	var paths []string
	for _, m := range manifests {
		for entry, err := range m.Entries(fs, true) {
			require.NoError(t, err)
			if entry.DataFile().ContentType() == iceberg.EntryContentPosDeletes {
				paths = append(paths, entry.DataFile().FilePath())
			}
		}
	}

	return paths
}

func readPositionDeletes(t *testing.T, path string) []positionDelete {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	pf, err := file.NewParquetReader(f)
	require.NoError(t, err)
	defer pf.Close()
	rdr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := rdr.ReadTable(t.Context())
	require.NoError(t, err)
	defer tbl.Release()

	var rows []positionDelete
	tr := array.NewTableReader(tbl, -1)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		paths := rec.Column(0).(*array.String)
		positions := rec.Column(1).(*array.Int64)
		for i := range int(rec.NumRows()) {
			rows = append(rows, positionDelete{path: paths.Value(i), pos: positions.Value(i)})
		}
	}

	return rows
}
