package cases

import (
	"context"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestWriteRecordsKeepsCallerBatches writes batches through WriteRecords,
// reads the same batches again afterwards, then commits the files with
// AddDataFiles. The batches still read the model, and so do all three engines.
func TestWriteRecordsKeepsCallerBatches(t *testing.T) {
	for _, mode := range lane.DeleteModes {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			lk := lane.Open(t)
			tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, monthPartitioned()), mode)
			gen := lane.NewGen(7)
			model := lane.Model{}

			var batches []arrow.RecordBatch
			defer func() {
				for _, b := range batches {
					b.Release()
				}
			}()
			for day := range 3 {
				from, to := lane.Day(day * 20)
				rows := gen.Rows(500, from, to)
				batches = append(batches, lane.Batch(rows))
				model.Put(rows)
			}

			records := func(yield func(arrow.RecordBatch, error) bool) {
				for _, b := range batches {
					if !yield(b, nil) {
						return
					}
				}
			}
			var files []iceberg.DataFile
			for df, err := range table.WriteRecords(ctx, tbl, lane.ArrowSchema, records) {
				require.NoError(t, err)
				files = append(files, df)
			}

			got, err := lane.ChecksumOfBatches(lane.Schema, batches)
			require.NoError(t, err, "caller's batches after WriteRecords")
			require.Equal(t, model.Checksum(), got)

			txn := tbl.NewTransaction()
			require.NoError(t, txn.AddDataFiles(ctx, files, nil))
			tbl, err = txn.Commit(ctx)
			require.NoError(t, err)
			lk.RequireChecksum(t, tbl, 0, model.Checksum())
		})
	}
}
