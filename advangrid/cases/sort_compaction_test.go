package cases

import (
	"cmp"
	"context"
	"slices"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

func sortedByGK() catalog.CreateTableOpt {
	order, err := table.NewSortOrder(1, []table.SortField{
		{SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst},
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst},
	})
	if err != nil {
		panic(err)
	}

	return catalog.WithSortOrder(order)
}

func sortByGK(rows []lane.Row) {
	slices.SortFunc(rows, func(a, b lane.Row) int {
		return cmp.Or(cmp.Compare(a.G, b.G), cmp.Compare(a.K, b.K))
	})
}

// compactForOrder compacts every month partition of tbl in one rewrite with
// the sort strategy.
func compactForOrder(ctx context.Context, t *testing.T, tbl *table.Table) *table.Table {
	t.Helper()
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 2
	cfg.Strategy = table.SortStrategy
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}
	res, err := tbl.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{
		GroupOptions: []table.CompactionGroupOption{table.WithCompactionStrategy(table.SortStrategy)},
	})
	require.NoError(t, err)

	return res.Table
}

// TestSortCompactionKeepsOrder is the twin of the spike's sortcompact: a
// month-partitioned table sorted by (g, k) gets many small sorted appends and
// one unsorted one, then compaction. Every compacted file must be in (g, k)
// order, and iceberg-go, DuckDB and Spark must read the rows from before.
func TestSortCompactionKeepsOrder(t *testing.T) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lk.CreateTable(t, lane.Schema, monthPartitioned(), sortedByGK())
	gen := lane.NewGen(2628)
	model := lane.Model{}

	appendRows := func(rows []lane.Row, batchRows int) {
		rdr := lane.Reader(rows, batchRows)
		defer rdr.Release()
		var err error
		tbl, err = tbl.Append(ctx, rdr, nil)
		require.NoError(t, err)
		model.Put(rows)
	}
	for day := 0; day < 56; day += 4 {
		from, to := lane.Day(day)
		rows := gen.Rows(400, from, to)
		sortByGK(rows)
		appendRows(rows, len(rows))
	}
	from, to := lane.Day(9)
	appendRows(gen.Rows(400, from, to), 50)

	before := model.Checksum()
	lk.RequireChecksum(t, tbl, 0, before)

	tbl = compactForOrder(ctx, t, tbl)

	files, err := lane.LiveDataFiles(ctx, tbl)
	require.NoError(t, err)
	require.Len(t, files, 2, "one file per month")
	for _, f := range files {
		breaks, err := lk.Duck.OrderBreaks(ctx, f, "g", "k")
		require.NoError(t, err)
		require.Zero(t, breaks, "order breaks in %s", f)
	}
	lk.RequireChecksum(t, tbl, 0, before)
}
