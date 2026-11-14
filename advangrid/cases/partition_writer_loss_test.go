package cases

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

var errInjectedCreate = errors.New("injected transient create failure")

// failFirstDataCreateIO fails the first data file it is asked to create, as
// a transient object store error would, and passes everything else through.
type failFirstDataCreateIO struct {
	iceio.WriteFileIO
	failed *atomic.Bool
}

func (f failFirstDataCreateIO) Create(name string) (iceio.FileWriter, error) {
	if strings.Contains(name, "/data/") && f.failed.CompareAndSwap(false, true) {
		return nil, errInjectedCreate
	}

	return f.WriteFileIO.Create(name)
}

// pausingReader waits before every batch after the first, so the writer of a
// failed file has stopped before the next batch of its partition arrives.
type pausingReader struct {
	array.RecordReader
	batches int
}

func (r *pausingReader) Next() bool {
	if r.batches > 0 {
		time.Sleep(2 * time.Second)
	}
	r.batches++

	return r.RecordReader.Next()
}

// TestAppendReportsPartitionWriterFailure appends two batches of one month
// partition to a lane table whose IO fails the first data file create. The
// append must fail with that error and leave the table as it was; before the
// fix the second batch went to a fresh writer and the append committed
// without the first batch's rows.
func TestAppendReportsPartitionWriterFailure(t *testing.T) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lk.CreateTable(t, lane.Schema, monthPartitioned())
	gen := lane.NewGen(11)
	model := lane.Model{}

	from, to := lane.Day(0)
	rows := gen.Rows(200, from, to)
	rdr := lane.Reader(rows, 256)
	tbl, err := tbl.Append(ctx, rdr, nil)
	rdr.Release()
	require.NoError(t, err)
	model.Put(rows)

	fs, err := tbl.FS(ctx)
	require.NoError(t, err)
	writeFS, ok := fs.(iceio.WriteFileIO)
	require.True(t, ok)
	failing := failFirstDataCreateIO{WriteFileIO: writeFS, failed: &atomic.Bool{}}
	cat, ok := lk.Catalog.(table.CatalogIO)
	require.True(t, ok)
	flaky := table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(),
		func(context.Context) (iceio.IO, error) { return failing, nil }, cat)

	rows = gen.Rows(500, from, to)
	inner := lane.Reader(rows, 250)
	_, err = flaky.Append(ctx, &pausingReader{RecordReader: inner}, nil)
	inner.Release()
	require.ErrorIs(t, err, errInjectedCreate)

	lk.RequireChecksum(t, lk.Reload(t, tbl), 0, model.Checksum())
}
