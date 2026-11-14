package cases

import (
	"context"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestPositionDeletesAcrossFilesAreSorted deletes rows from many data files
// of one partition with merge-on-read deletes. Every position delete file
// must be sorted by (file_path, pos), as the spec requires and as readers
// that stop at the first file_path past their data file rely on, and the
// three engines must read the model.
func TestPositionDeletesAcrossFilesAreSorted(t *testing.T) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, monthPartitioned()), table.WriteModeMergeOnRead)
	gen := lane.NewGen(6)
	model := lane.Model{}

	for day := range 12 {
		from, to := lane.Day(day)
		rows := gen.Rows(200, from, to)
		rdr := lane.Reader(rows, 4096)
		var err error
		tbl, err = tbl.Append(ctx, rdr, nil)
		rdr.Release()
		require.NoError(t, err)
		model.Put(rows)
	}

	tbl, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("g"), int32(3)), nil)
	require.NoError(t, err)
	model.DeleteWhere(func(r lane.Row) bool { return r.G == 3 })

	lk.RequireChecksum(t, tbl, 0, model.Checksum())

	deleteFiles, err := lane.PositionDeleteFiles(ctx, tbl)
	require.NoError(t, err)
	require.NotEmpty(t, deleteFiles)
	for _, path := range deleteFiles {
		breaks, err := lk.Duck.PositionDeleteOrderBreaks(ctx, path)
		require.NoError(t, err)
		require.Zero(t, breaks, "position delete file %s is not sorted by (file_path, pos)", path)
	}
}
