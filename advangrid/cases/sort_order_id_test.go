package cases

import (
	"context"
	"fmt"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// sparkSortOrderIDs returns sort_order_id per live data file as Spark's
// files metadata table reads it, "null" when unset.
func sparkSortOrderIDs(ctx context.Context, t *testing.T, lk *lane.Lake, tbl *table.Table) map[string]string {
	t.Helper()
	id := tbl.Identifier()
	rows, err := lk.Spark.Query(ctx, fmt.Sprintf("SELECT file_path, sort_order_id FROM lake.%s.%s.files",
		id[len(id)-2], id[len(id)-1]))
	require.NoError(t, err)
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		out[*r[0]] = "null"
		if r[1] != nil {
			out[*r[0]] = *r[1]
		}
	}

	return out
}

// TestSortOrderIDOnFilesInOrder checks with Spark that data files carry the
// table's sort order id when their rows are in the declared order: after a
// sorted append and after sort compaction, and not after an unsorted append.
func TestSortOrderIDOnFilesInOrder(t *testing.T) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lk.CreateTable(t, lane.Schema, monthPartitioned(), sortedByGK())
	gen := lane.NewGen(5)
	model := lane.Model{}
	appendRows := func(rows []lane.Row, batchRows int) []string {
		before, err := lane.LiveDataFiles(ctx, tbl)
		require.NoError(t, err)
		rdr := lane.Reader(rows, batchRows)
		defer rdr.Release()
		tbl, err = tbl.Append(ctx, rdr, nil)
		require.NoError(t, err)
		model.Put(rows)
		after, err := lane.LiveDataFiles(ctx, tbl)
		require.NoError(t, err)
		seen := map[string]bool{}
		for _, f := range before {
			seen[f] = true
		}
		var added []string
		for _, f := range after {
			if !seen[f] {
				added = append(added, f)
			}
		}
		require.Len(t, added, 1)

		return added
	}

	from, to := lane.Day(0)
	sorted := gen.Rows(400, from, to)
	sortByGK(sorted)
	sortedFile := appendRows(sorted, len(sorted))[0]
	from, to = lane.Day(3)
	unsortedFile := appendRows(gen.Rows(400, from, to), 50)[0]

	ids := sparkSortOrderIDs(ctx, t, lk, tbl)
	require.Equal(t, "1", ids[sortedFile], "sorted append")
	require.Equal(t, "null", ids[unsortedFile], "unsorted append")

	tbl = compactForOrder(ctx, t, tbl)
	lk.RequireChecksum(t, tbl, 0, model.Checksum())
	ids = sparkSortOrderIDs(ctx, t, lk, tbl)
	require.Len(t, ids, 1)
	for f, id := range ids {
		require.Equal(t, "1", id, "sort compaction output %s", f)
	}
}
