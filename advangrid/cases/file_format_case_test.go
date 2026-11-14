package cases

import (
	"context"
	"fmt"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/stretchr/testify/require"
)

// TestCompactsDuckDBDataFiles appends with iceberg-go and inserts with
// DuckDB, which records file_format in lower case, then bin-pack compacts the
// table with iceberg-go. All three engines must read the model afterwards.
func TestCompactsDuckDBDataFiles(t *testing.T) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lk.CreateTable(t, lane.Schema)
	gen := lane.NewGen(9)
	model := lane.Model{}

	from, to := lane.Day(0)
	rows := gen.Rows(500, from, to)
	rdr := lane.Reader(rows, 256)
	tbl, err := tbl.Append(ctx, rdr, nil)
	rdr.Release()
	require.NoError(t, err)
	model.Put(rows)

	for range 2 {
		rows := gen.Rows(200, from, to)
		_, err := lk.Duck.Row(ctx, fmt.Sprintf("INSERT INTO lk.%s %s", lane.QualifiedName(tbl), lane.SQLValues(rows)))
		require.NoError(t, err)
		model.Put(rows)
	}
	tbl = lk.Reload(t, tbl)
	lk.RequireChecksum(t, tbl, 0, model.Checksum())

	require.NoError(t, binPackCompact(ctx, tbl))
	tbl = lk.Reload(t, tbl)
	lk.RequireChecksum(t, tbl, 0, model.Checksum())
}
