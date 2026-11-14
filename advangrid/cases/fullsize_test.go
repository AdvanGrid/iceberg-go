//go:build fullsize && duckdb_arrow

package cases

import (
	"context"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

// The archived gridlink_line table: row count and sum(id) from the archive
// README, checked when the rows were exported.
const (
	fullsizeRows  = 239746731
	fullsizeSumID = "6279228451513748320"
	fullsizeOrder = "device_id, device_time, phase, id"
)

// TestFullSize runs the archived 240M-row table through backfill, a daily
// overwrite, sort compaction, manifest rewrite, expiry and orphan removal,
// and checks after every step that iceberg-go, DuckDB and Spark read the same
// checksum. Run on a workstation with the archive:
//
//	LANE_FULLSIZE_DIR=/home/tomsj/.local/share/lake-spike-archive/parquet/gridlink_line \
//	  go test -tags fullsize,duckdb_arrow -count=1 -v -timeout 24h -run TestFullSize ./cases/
func TestFullSize(t *testing.T) {
	dir := os.Getenv("LANE_FULLSIZE_DIR")
	require.NotEmpty(t, dir, "LANE_FULLSIZE_DIR must point at the gridlink_line parquet directory")
	ctx := context.Background()
	lk := lane.Open(t)
	src := fmt.Sprintf("read_parquet('%s')", filepath.Join(dir, "*.parquet"))

	tbl, sc := createFullSizeTable(t, lk, src)
	months, err := lk.Duck.Strings(ctx, fmt.Sprintf(
		"SELECT DISTINCT strftime(date_trunc('month', device_time), '%%Y-%%m-%%d') FROM %s ORDER BY 1", src))
	require.NoError(t, err)

	step := func(name string, started time.Time) lane.Checksums {
		tbl = lk.Reload(t, tbl)
		sums := lk.Checksums(t, tbl, 0)
		require.Equal(t, sums.IcebergGo, sums.DuckDB, name)
		require.Equal(t, sums.IcebergGo, sums.Spark, name)
		t.Logf("%s: %s in %s", name, sums.IcebergGo, time.Since(started).Round(time.Second))

		return sums
	}

	started := time.Now()
	byYear := map[string][]string{}
	var years []string
	for _, m := range months {
		if _, ok := byYear[m[:4]]; !ok {
			years = append(years, m[:4])
		}
		byYear[m[:4]] = append(byYear[m[:4]], monthQuery(src, m))
	}
	for _, y := range years {
		tbl = writeSorted(t, lk, tbl, sc, byYear[y])
		t.Logf("backfill %s committed after %s", y, time.Since(started).Round(time.Second))
	}
	backfilled := step("backfill", started)
	idRow, err := lk.Duck.Row(ctx, "SELECT CAST(count(*) AS VARCHAR), CAST(sum(id) AS VARCHAR) FROM lk."+lane.QualifiedName(tbl))
	require.NoError(t, err)
	require.Equal(t, fmt.Sprint(fullsizeRows), *idRow[0])
	require.Equal(t, fullsizeSumID, *idRow[1])
	requireNoOrderBreaks(t, lk, tbl)

	started = time.Now()
	day := time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)
	tbl = overwriteDay(t, lk, tbl, src, day)
	require.Equal(t, backfilled, step("daily overwrite", started))

	started = time.Now()
	tbl = sortCompact(t, tbl)
	require.Equal(t, backfilled, step("sort compaction", started))
	requireNoOrderBreaks(t, lk, tbl)

	started = time.Now()
	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx, table.WithRewriteManifestClusterBy(func(df iceberg.DataFile) any {
		return fmt.Sprint(df.Partition())
	}))
	require.NoError(t, err)
	if !res.IsNoOp() {
		_, err = txn.Commit(ctx)
		require.NoError(t, err)
	}
	require.Equal(t, backfilled, step("manifest rewrite", started))

	started = time.Now()
	txn = lk.Reload(t, tbl).NewTransaction()
	require.NoError(t, txn.ExpireSnapshots(table.WithRetainLast(1), table.WithOlderThan(0)))
	_, err = txn.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, backfilled, step("expiry", started))

	started = time.Now()
	orphans, err := lk.Reload(t, tbl).DeleteOrphanFiles(ctx, table.WithFilesOlderThan(0))
	require.NoError(t, err)
	t.Logf("orphan removal deleted %d files", len(orphans.DeletedFiles))
	require.Equal(t, backfilled, step("orphan removal", started))
	requireFileSets(t, lk, tbl)
}

func monthQuery(src, month string) string {
	from, err := time.Parse(time.DateOnly, month)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("SELECT * FROM %s WHERE device_time >= TIMESTAMP '%s' AND device_time < TIMESTAMP '%s' ORDER BY %s",
		src, from.Format(time.DateTime), from.AddDate(0, 1, 0).Format(time.DateTime), fullsizeOrder)
}

func createFullSizeTable(t *testing.T, lk *lane.Lake, src string) (*table.Table, *arrow.Schema) {
	t.Helper()
	var sc *arrow.Schema
	require.NoError(t, lk.Duck.Arrow(context.Background(), "SELECT * FROM "+src+" LIMIT 0", func(rr array.RecordReader) error {
		sc = rr.Schema()

		return nil
	}))
	isc, err := table.ArrowSchemaToIcebergWithFreshIDs(sc, false)
	require.NoError(t, err)
	timeField, ok := isc.FindFieldByName("device_time")
	require.True(t, ok)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{timeField.ID}, FieldID: 1000, Name: "device_time_month", Transform: iceberg.MonthTransform{},
	})
	var fields []table.SortField
	for _, name := range []string{"device_id", "device_time"} {
		f, ok := isc.FindFieldByName(name)
		require.True(t, ok)
		fields = append(fields, table.SortField{SourceIDs: []int{f.ID}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst})
	}
	order, err := table.NewSortOrder(1, fields)
	require.NoError(t, err)

	tbl := lk.CreateTable(t, isc, catalog.WithPartitionSpec(&spec), catalog.WithSortOrder(order),
		catalog.WithProperties(iceberg.Properties{
			table.ParquetCompressionKey:      "zstd",
			table.ParquetCompressionLevelKey: "3",
			table.ParquetRowGroupLimitKey:    "262144",
		}))

	return tbl, sc
}

// duckBatches chains the results of queries into one stream. The batches
// belong to DuckDB's reader; WriteRecords does not release them.
func duckBatches(ctx context.Context, lk *lane.Lake, queries []string) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		for _, q := range queries {
			stopped := false
			err := lk.Duck.Arrow(ctx, q, func(rr array.RecordReader) error {
				for rr.Next() {
					if !yield(rr.RecordBatch(), nil) {
						stopped = true

						return nil
					}
				}

				return rr.Err()
			})
			if err != nil {
				yield(nil, err)

				return
			}
			if stopped {
				return
			}
		}
	}
}

// writeSorted writes the partition-clustered, sorted results of queries with
// the clustered writer and commits them as one append.
func writeSorted(t *testing.T, lk *lane.Lake, tbl *table.Table, sc *arrow.Schema, queries []string) *table.Table {
	t.Helper()
	ctx := context.Background()
	var files []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, tbl, sc, duckBatches(ctx, lk, queries), table.WithClusteredWrite()) {
		require.NoError(t, err)
		files = append(files, df)
	}
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFiles(ctx, files, nil))
	tbl, err := txn.Commit(ctx)
	require.NoError(t, err)

	return tbl
}

// overwriteDay replaces one UTC day with the same rows read again from the
// source, in one overwrite snapshot.
func overwriteDay(t *testing.T, lk *lane.Lake, tbl *table.Table, src string, day time.Time) *table.Table {
	t.Helper()
	ctx := context.Background()
	next := day.AddDate(0, 0, 1)
	q := fmt.Sprintf("SELECT * FROM %s WHERE device_time >= TIMESTAMP '%s' AND device_time < TIMESTAMP '%s' ORDER BY %s",
		src, day.Format(time.DateTime), next.Format(time.DateTime), fullsizeOrder)
	filter := iceberg.NewAnd(
		iceberg.GreaterThanEqual(iceberg.Reference("device_time"), iceberg.Timestamp(day.UnixMicro())),
		iceberg.LessThan(iceberg.Reference("device_time"), iceberg.Timestamp(next.UnixMicro())))
	var out *table.Table
	require.NoError(t, lk.Duck.Arrow(ctx, q, func(rr array.RecordReader) error {
		var err error
		out, err = tbl.Overwrite(ctx, rr, nil, table.WithOverwriteFilter(filter))

		return err
	}))

	return out
}

func sortCompact(t *testing.T, tbl *table.Table) *table.Table {
	t.Helper()
	ctx := context.Background()
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 2
	cfg.Strategy = table.SortStrategy
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	if len(plan.Groups) == 0 {
		return tbl
	}
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}
	res, err := tbl.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{
		GroupOptions: []table.CompactionGroupOption{table.WithCompactionStrategy(table.SortStrategy)},
	})
	require.NoError(t, err)
	t.Logf("sort compaction rewrote %d groups", len(groups))

	return res.Table
}

func requireNoOrderBreaks(t *testing.T, lk *lane.Lake, tbl *table.Table) {
	t.Helper()
	files, err := lane.LiveDataFiles(context.Background(), tbl)
	require.NoError(t, err)
	for _, f := range files {
		breaks, err := lk.Duck.OrderBreaks(context.Background(), f, "device_id", "device_time")
		require.NoError(t, err)
		require.Zero(t, breaks, "order breaks in %s", f)
	}
	t.Logf("%d live data files, 0 order breaks", len(files))
}
