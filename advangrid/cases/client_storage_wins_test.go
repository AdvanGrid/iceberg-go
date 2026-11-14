package cases

import (
	"context"
	"testing"
	"time"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

const preferClientConfigKey = "rest.prefer-client-config"

// TestClientStorageEndpointWins writes from the host to the split warehouse,
// whose catalog-side storage endpoint the host cannot resolve. The catalog's
// table config wins by default, so the write fails; with the client config
// preferred it goes to the client's endpoint, and all engines read it.
func TestClientStorageEndpointWins(t *testing.T) {
	ctx := context.Background()
	from, to := lane.Day(0)
	rows := lane.NewGen(8).Rows(1000, from, to)

	t.Run("table config wins by default", func(t *testing.T) {
		lk := lane.OpenSplit(t, nil)
		tbl := lk.CreateTable(t, lane.Schema)
		rdr := lane.Reader(rows, 256)
		defer rdr.Release()
		writeCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		_, err := tbl.Append(writeCtx, rdr, nil)
		require.Error(t, err)
	})

	t.Run("client config wins when preferred", func(t *testing.T) {
		lk := lane.OpenSplit(t, iceberg.Properties{preferClientConfigKey: "true"})
		tbl := lk.CreateTable(t, lane.Schema)
		rdr := lane.Reader(rows, 256)
		defer rdr.Release()
		tbl, err := tbl.Append(ctx, rdr, nil)
		require.NoError(t, err)
		tbl = lk.Reload(t, tbl)

		model := lane.Model{}
		model.Put(rows)
		want := model.Checksum()
		require.Equal(t, lane.Checksums{IcebergGo: want, DuckDB: want, Spark: want}, lk.SplitChecksums(t, tbl))
	})
}
