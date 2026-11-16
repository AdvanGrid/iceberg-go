package cases

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

var floatSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Float64},
)

func sortedByX() catalog.CreateTableOpt {
	order, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst,
	}})
	if err != nil {
		panic(err)
	}

	return catalog.WithSortOrder(order)
}

// appendFloats appends xs as one batch and returns the data file it added.
func appendFloats(ctx context.Context, t *testing.T, tbl *table.Table, xs ...float64) (*table.Table, string) {
	t.Helper()
	before, err := lane.LiveDataFiles(ctx, tbl)
	require.NoError(t, err)
	sc := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, nil)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	bldr.Field(0).(*array.Float64Builder).AppendValues(xs, nil)
	rec := bldr.NewRecordBatch()
	bldr.Release()
	rdr, err := array.NewRecordReader(sc, []arrow.RecordBatch{rec})
	rec.Release()
	require.NoError(t, err)
	defer rdr.Release()
	tbl, err = tbl.Append(ctx, rdr, nil)
	require.NoError(t, err)
	after, err := lane.LiveDataFiles(ctx, tbl)
	require.NoError(t, err)
	require.Len(t, after, len(before)+1)

	return tbl, after[len(after)-1]
}

// fileFloats reads column x of a data file with DuckDB in physical row order.
func fileFloats(ctx context.Context, t *testing.T, lk *lane.Lake, path string) []float64 {
	t.Helper()
	vals, err := lk.Duck.Strings(ctx, fmt.Sprintf(
		"SELECT x::VARCHAR FROM read_parquet('%s', file_row_number = true) ORDER BY file_row_number", path))
	require.NoError(t, err)
	xs := make([]float64, len(vals))
	for i, v := range vals {
		xs[i], err = strconv.ParseFloat(v, 64)
		require.NoError(t, err)
	}

	return xs
}

// TestNaNSortKeyClaim checks with Spark and DuckDB that a file whose float
// sort key holds a NaN does not claim the sort order: the writer places NaN
// by the null order, before the numbers here, where the declared ascending
// order places it after them. Sort compaction of such a file with one in
// order must still put the numbers in order.
func TestNaNSortKeyClaim(t *testing.T) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lk.CreateTable(t, floatSchema, sortedByX(),
		catalog.WithProperties(iceberg.Properties{table.ParquetBatchSizeKey: "2"}))

	tbl, nanFile := appendFloats(ctx, t, tbl, 2, math.NaN(), 1)
	tbl, _ = appendFloats(ctx, t, tbl, 0, 3)
	ids := sparkSortOrderIDs(ctx, t, lk, tbl)
	require.Equal(t, "null", ids[nanFile], "append with a NaN sort key")

	tbl = compactForOrder(ctx, t, tbl)
	ids = sparkSortOrderIDs(ctx, t, lk, tbl)
	require.Len(t, ids, 1)
	var numbers []float64
	for f, id := range ids {
		require.Equal(t, "null", id, "sort compaction output %s holds a NaN", f)
		for _, x := range fileFloats(ctx, t, lk, f) {
			if !math.IsNaN(x) {
				numbers = append(numbers, x)
			}
		}
	}
	require.Equal(t, []float64{0, 1, 2, 3}, numbers, "numbers after sort compaction")
}
