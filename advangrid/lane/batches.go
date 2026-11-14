package lane

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// ChecksumOfBatches is the checksum of batches the caller still owns. It
// fails on a batch that lost its columns, which a released batch does.
func ChecksumOfBatches(sc *iceberg.Schema, batches []arrow.RecordBatch) (Checksum, error) {
	for i, b := range batches {
		if int(b.NumCols()) != ArrowSchema.NumFields() {
			return "", fmt.Errorf("ChecksumOfBatches - batch %d has %d columns, want %d", i, b.NumCols(), ArrowSchema.NumFields())
		}
	}
	tbl := array.NewTableFromRecords(ArrowSchema, batches)
	defer tbl.Release()

	return ChecksumOfRecords(sc, tbl)
}
