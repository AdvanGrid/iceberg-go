package cases

import (
	"context"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestOverwriteKeepsMergeOnReadDeletes deletes rows with merge-on-read
// deletes, then overwrites one day of the same data file. The copy-on-write
// rewrite of the file must keep the earlier deletes.
func TestOverwriteKeepsMergeOnReadDeletes(t *testing.T) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema), table.WriteModeMergeOnRead)
	gen := lane.NewGen(4)
	model := lane.Model{}

	from, _ := lane.Day(0)
	_, to := lane.Day(1)
	rows := gen.Rows(400, from, to)
	rdr := lane.Reader(rows, 4096)
	tbl, err := tbl.Append(ctx, rdr, nil)
	rdr.Release()
	require.NoError(t, err)
	model.Put(rows)

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("g"), int32(1)), nil)
	require.NoError(t, err)
	model.DeleteWhere(func(r lane.Row) bool { return r.G == 1 })
	lk.RequireChecksum(t, tbl, 0, model.Checksum())

	empty := lane.Reader(nil, 1)
	tbl, err = tbl.Overwrite(ctx, empty, nil, table.WithOverwriteFilter(dayFilter(0)))
	empty.Release()
	require.NoError(t, err)
	model.DeleteWhere(inDay(0))

	lk.RequireChecksum(t, tbl, 0, model.Checksum())
}
