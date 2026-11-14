package cases

import (
	"context"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/stretchr/testify/require"
)

// TestAppendKeepsSortedBatchOrder appends input sorted by k in many batches to
// a month-partitioned table sorted by k. Every data file keeps the input order,
// and all three engines read the model.
func TestAppendKeepsSortedBatchOrder(t *testing.T) {
	for _, mode := range lane.DeleteModes {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			lk := lane.Open(t)
			tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, monthPartitioned(), sortedByK()), mode)
			gen := lane.NewGen(4)
			model := lane.Model{}

			from, _ := lane.Day(0)
			_, to := lane.Day(58)
			rows := gen.Rows(30*2048, from, to)
			model.Put(rows)
			rdr := lane.Reader(rows, 2048)
			tbl, err := tbl.Append(ctx, rdr, nil)
			rdr.Release()
			require.NoError(t, err)

			files, err := lane.LiveDataFiles(ctx, tbl)
			require.NoError(t, err)
			require.Len(t, files, 2, "one data file per month")
			for _, path := range files {
				breaks, err := lk.Duck.DataFileOrderBreaks(ctx, path, "k")
				require.NoError(t, err)
				require.Zero(t, breaks, "order breaks in %s", path)
			}
			lk.RequireChecksum(t, tbl, 0, model.Checksum())
		})
	}
}
