package cases

import (
	"context"
	"testing"
	"time"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestWriteRecordsReportsWriteCause writes a batch the writer cannot convert,
// waits until the writer goroutine has stopped on it, and then writes a valid
// batch. WriteRecords must report the conversion error, not ErrWriterClosed.
// The zero-column batch is what a released duckdb-go batch looked like in the
// spike, where only ErrWriterClosed reached the caller.
func TestWriteRecordsReportsWriteCause(t *testing.T) {
	lk := lane.Open(t)
	tbl := lk.CreateTable(t, lane.Schema)
	from, to := lane.Day(0)
	rows := lane.NewGen(1).Rows(10, from, to)

	records := func(yield func(arrow.RecordBatch, error) bool) {
		broken := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, int64(len(rows)))
		if !yield(broken, nil) {
			return
		}
		time.Sleep(2 * time.Second)
		yield(lane.Batch(rows), nil)
	}

	var err error
	for _, err = range table.WriteRecords(context.Background(), tbl, lane.ArrowSchema, records) {
		if err != nil {
			break
		}
	}
	require.Error(t, err)
	require.NotErrorIs(t, err, table.ErrWriterClosed)
	require.ErrorContains(t, err, "cannot find k")
}
