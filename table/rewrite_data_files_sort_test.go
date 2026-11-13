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
	"fmt"
	"path/filepath"
	"strings"
	"testing"

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

func idAscending(t *testing.T) table.SortOrder {
	t.Helper()

	order, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
		Direction: table.SortASC, NullOrder: table.NullsFirst,
	}})
	require.NoError(t, err)

	return order
}

func newSortRewriteTestTable(t *testing.T, order table.SortOrder) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, order, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "sort_rewrite_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&rowDeltaCatalog{metadata: meta},
	)
}

// addIDFiles adds one data file per id list, rows in the given order.
func addIDFiles(t *testing.T, tbl *table.Table, files ...[]int64) *table.Table {
	t.Helper()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	paths := make([]string, len(files))
	for i, ids := range files {
		rows := make([]string, len(ids))
		for j, id := range ids {
			rows[j] = fmt.Sprintf(`{"id": %d, "data": "row-%d"}`, id, id)
		}
		paths[i] = tbl.Location() + fmt.Sprintf("/data/ids-%d.parquet", i)
		writeParquetFile(t, paths[i], arrowSc, "["+strings.Join(rows, ",")+"]")
	}

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), paths, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

func wholeTableGroup(t *testing.T, tbl *table.Table) table.CompactionTaskGroup {
	t.Helper()

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	group := table.CompactionTaskGroup{PartitionKey: "all", Tasks: tasks}
	for _, task := range tasks {
		group.TotalSizeBytes += task.File.FileSizeBytes()
	}

	return group
}

// fileIDs reads the id column of a data file in file order.
func fileIDs(t *testing.T, df iceberg.DataFile) []int64 {
	t.Helper()

	rdr, err := file.OpenParquetFile(strings.TrimPrefix(df.FilePath(), "file://"), false)
	require.NoError(t, err)
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := arrowRdr.ReadTable(t.Context())
	require.NoError(t, err)
	defer tbl.Release()

	var ids []int64
	for _, chunk := range tbl.Column(0).Data().Chunks() {
		ids = append(ids, chunk.(*array.Int64).Int64Values()...)
	}

	return ids
}

func compactedIDs(t *testing.T, result table.CompactionGroupResult) []int64 {
	t.Helper()

	var ids []int64
	for _, df := range result.NewDataFiles {
		ids = append(ids, fileIDs(t, df)...)
	}

	return ids
}

func sequence(from, to int64) []int64 {
	ids := make([]int64, 0, to-from+1)
	for id := from; id <= to; id++ {
		ids = append(ids, id)
	}

	return ids
}

func TestExecuteCompactionGroup_SortStrategyMergesSortedFiles(t *testing.T) {
	tbl := newSortRewriteTestTable(t, idAscending(t))
	tbl = addIDFiles(t, tbl,
		[]int64{1, 4, 7, 10, 13},
		[]int64{2, 5, 8, 11, 14},
		[]int64{3, 6, 9, 12, 15},
		[]int64{16, 17, 18},
	)

	result, err := table.ExecuteCompactionGroup(t.Context(), tbl, wholeTableGroup(t, tbl),
		table.WithCompactionStrategy(table.SortStrategy),
		table.WithCompactionArrowBatchSize(2))
	require.NoError(t, err)

	assert.Equal(t, sequence(1, 18), compactedIDs(t, result))
	assert.Len(t, result.OldDataFiles, 4)
	requireSortOrderID(t, result, tbl.SortOrder().OrderID())
}

func requireSortOrderID(t *testing.T, result table.CompactionGroupResult, want int) {
	t.Helper()

	require.NotEmpty(t, result.NewDataFiles)
	for _, df := range result.NewDataFiles {
		require.NotNil(t, df.SortOrderID(), df.FilePath())
		assert.Equal(t, want, *df.SortOrderID(), df.FilePath())
	}
}

func TestExecuteCompactionGroup_SortStrategySortsOutOfOrderFile(t *testing.T) {
	tbl := newSortRewriteTestTable(t, idAscending(t))
	tbl = addIDFiles(t, tbl,
		[]int64{1, 3, 5, 9},
		[]int64{8, 6, 4, 2},
		[]int64{7, 10},
	)

	result, err := table.ExecuteCompactionGroup(t.Context(), tbl, wholeTableGroup(t, tbl),
		table.WithCompactionStrategy(table.SortStrategy),
		table.WithCompactionArrowBatchSize(3))
	require.NoError(t, err)

	assert.Equal(t, sequence(1, 10), compactedIDs(t, result))
	requireSortOrderID(t, result, tbl.SortOrder().OrderID())
}

func TestExecuteCompactionGroup_SortStrategyRefusesUnsortedTable(t *testing.T) {
	tbl := newSortRewriteTestTable(t, table.UnsortedSortOrder)
	tbl = addIDFiles(t, tbl, []int64{1}, []int64{2})

	_, err := table.ExecuteCompactionGroup(t.Context(), tbl, wholeTableGroup(t, tbl),
		table.WithCompactionStrategy(table.SortStrategy))
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.ErrorContains(t, err, "sort order")
}

func TestExecuteCompactionGroup_SortStrategyRefusesTransformBeforeLastField(t *testing.T) {
	order, err := table.NewSortOrder(1, []table.SortField{
		{SourceIDs: []int{2}, Transform: iceberg.TruncateTransform{Width: 2}, Direction: table.SortASC, NullOrder: table.NullsFirst},
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst},
	})
	require.NoError(t, err)
	tbl := newSortRewriteTestTable(t, order)
	tbl = addIDFiles(t, tbl, []int64{1}, []int64{2})

	_, err = table.ExecuteCompactionGroup(t.Context(), tbl, wholeTableGroup(t, tbl),
		table.WithCompactionStrategy(table.SortStrategy))
	require.ErrorIs(t, err, table.ErrInvalidOperation)
}

func TestExecuteCompactionGroup_SortStrategyRefusesDeleteFiles(t *testing.T) {
	tbl := newSortRewriteTestTable(t, idAscending(t))
	tbl = addIDFiles(t, tbl, []int64{1, 2}, []int64{3, 4})

	posDelPath := tbl.Location() + "/data/pos-del.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": "%s", "pos": 0}]`, tbl.Location()+"/data/ids-0.parquet"))
	posDel, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(posDel.Build())
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	group := wholeTableGroup(t, tbl)
	_, err = table.ExecuteCompactionGroup(t.Context(), tbl, group,
		table.WithCompactionStrategy(table.SortStrategy))
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.ErrorContains(t, err, "delete files")

	_, err = tbl.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{group}, table.RewriteDataFilesOptions{
		GroupOptions: []table.CompactionGroupOption{table.WithCompactionStrategy(table.SortStrategy)},
	})
	require.ErrorIs(t, err, table.ErrInvalidOperation)
}
