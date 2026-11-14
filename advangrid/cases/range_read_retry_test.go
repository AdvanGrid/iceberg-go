package cases

import (
	"context"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/stretchr/testify/require"
)

// TestScanSurvivesCutRangeReads scans a lane table through a proxy that cuts
// the first two ranged data file reads mid-body, as a dropped object store
// connection does. iceberg-go must retry and read the model, as DuckDB and
// Spark do.
func TestScanSurvivesCutRangeReads(t *testing.T) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lk.CreateTable(t, lane.Schema, monthPartitioned())
	gen := lane.NewGen(12)
	model := lane.Model{}

	for day := range 3 {
		from, to := lane.Day(day * 40)
		rows := gen.Rows(2000, from, to)
		rdr := lane.Reader(rows, 500)
		var err error
		tbl, err = tbl.Append(ctx, rdr, nil)
		rdr.Release()
		require.NoError(t, err)
		model.Put(rows)
	}

	viaProxy, err := lane.CatalogVia(t, lane.CutProxy(t, 2)).LoadTable(ctx, tbl.Identifier())
	require.NoError(t, err)
	records, err := viaProxy.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer records.Release()
	got, err := lane.ChecksumOfRecords(viaProxy.Schema(), records)
	require.NoError(t, err)
	require.Equal(t, model.Checksum(), got)

	lk.RequireChecksum(t, tbl, 0, model.Checksum())
}
