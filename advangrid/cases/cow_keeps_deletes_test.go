package cases

import (
	"context"
	"slices"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

// TestOverwriteKeepsEqualityDeletes deletes keys with an equality delete,
// then overwrites the rows below those keys. The equality delete's bounds
// do not overlap the overwrite filter, yet the rewrite keeps exactly the
// rows it deletes, so it must still apply them.
func TestOverwriteKeepsEqualityDeletes(t *testing.T) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lk.CreateTable(t, lane.Schema)
	gen := lane.NewGen(7)
	model := lane.Model{}

	from, _ := lane.Day(0)
	_, to := lane.Day(1)
	rows := gen.Rows(400, from, to)
	rdr := lane.Reader(rows, 4096)
	tbl, err := tbl.Append(ctx, rdr, nil)
	rdr.Release()
	require.NoError(t, err)
	model.Put(rows)

	deleted := []int64{301, 302, 350}
	keySchema, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Int64}),
		nil, true, false)
	require.NoError(t, err)
	b := array.NewRecordBuilder(memory.DefaultAllocator, keySchema)
	b.Field(0).(*array.Int64Builder).AppendValues(deleted, nil)
	keys := b.NewRecordBatch()
	b.Release()
	defer keys.Release()
	txn := tbl.NewTransaction()
	require.NoError(t, txn.CommitEqualityDeletes(ctx, []int{1}, func(yield func(arrow.RecordBatch, error) bool) {
		yield(keys, nil)
	}, nil))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)
	model.DeleteWhere(func(r lane.Row) bool { return slices.Contains(deleted, r.K) })
	lk.RequireChecksum(t, tbl, 0, model.Checksum())

	empty := lane.Reader(nil, 1)
	tbl, err = tbl.Overwrite(ctx, empty, nil,
		table.WithOverwriteFilter(iceberg.LessThanEqual(iceberg.Reference("k"), int64(100))))
	empty.Release()
	require.NoError(t, err)
	model.DeleteWhere(func(r lane.Row) bool { return r.K <= 100 })

	lk.RequireChecksum(t, tbl, 0, model.Checksum())
}
