package cases

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

// twins are two tables that receive the same commits from iceberg-go. Go is
// maintained by iceberg-go, Java by Spark's procedures.
type twins struct {
	lk   *lane.Lake
	Go   *table.Table
	Java *table.Table
}

func (tw *twins) commit(t *testing.T, write func(*table.Table) (*table.Table, error)) {
	t.Helper()
	var err error
	tw.Go, err = write(tw.Go)
	require.NoError(t, err, "iceberg-go twin")
	tw.Java, err = write(tw.Java)
	require.NoError(t, err, "spark twin")
}

func (tw *twins) reload(t *testing.T) {
	tw.Go = tw.lk.Reload(t, tw.Go)
	tw.Java = tw.lk.Reload(t, tw.Java)
}

func (tw *twins) files(t *testing.T) (lane.FileSet, lane.FileSet) {
	return tw.lk.Files(t, tw.Go), tw.lk.Files(t, tw.Java)
}

// requireSameContent checks that all three engines read the model from both
// tables and that both have the same live data files.
func (tw *twins) requireSameContent(t *testing.T, model lane.Model, step string) {
	t.Helper()
	tw.lk.RequireChecksum(t, tw.Go, 0, model.Checksum())
	tw.lk.RequireChecksum(t, tw.Java, 0, model.Checksum())
	require.Equal(t, lane.LiveDataIDs(t, tw.Java), lane.LiveDataIDs(t, tw.Go), "live data files after %s", step)
}

func (tw *twins) spark(t *testing.T, call string, args ...any) []*string {
	t.Helper()
	rows, err := tw.lk.Spark.Query(context.Background(), fmt.Sprintf(call, args...))
	require.NoError(t, err)
	require.Len(t, rows, 1, call)

	return rows[0]
}

func sparkName(tbl *table.Table) string { return strings.Join(tbl.Identifier(), ".") }

func sparkTimestamp(ts time.Time) string {
	return "TIMESTAMP '" + ts.UTC().Format("2006-01-02 15:04:05.000") + "'"
}

// TestTwinMaintenance sends the same appends and day overwrites to two
// month-partitioned tables sorted by (g, k), then maintains one with
// iceberg-go and the other with Spark's procedures: compaction, manifest
// rewrite, snapshot expiry and orphan removal. After each step both tables
// read the model in all three engines and have the same live data files,
// compacted files have no order breaks, and expiry and orphan removal delete
// the same files.
func TestTwinMaintenance(t *testing.T) {
	for _, mode := range lane.DeleteModes {
		t.Run(mode, func(t *testing.T) { runTwinMaintenance(t, mode) })
	}
}

func runTwinMaintenance(t *testing.T, mode string) {
	ctx := context.Background()
	lk := lane.Open(t)
	tw := &twins{
		lk:   lk,
		Go:   lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, monthPartitioned(), sortedByGK()), mode),
		Java: lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, monthPartitioned(), sortedByGK()), mode),
	}
	model := writeTwinHistory(t, tw)
	tw.reload(t)
	tw.requireSameContent(t, model, "commits")
	months := 3

	compactTwins(t, tw, mode)
	tw.reload(t)
	tw.requireSameContent(t, model, "compaction")
	require.Len(t, lane.LiveDataIDs(t, tw.Go), months, "one file per month")

	goManifests, javaManifests := lane.CurrentManifests(t, tw.Go), lane.CurrentManifests(t, tw.Java)
	res := rewriteManifestsByPartition(t, tw.Go)
	java := tw.spark(t, "CALL lake.system.rewrite_manifests(table => '%s')", sparkName(tw.Java))
	tw.reload(t)
	tw.requireSameContent(t, model, "manifest rewrite")
	// Both rewrite every current manifest, but the twins enter with different
	// manifests: iceberg-go's compaction leaves fewer than Java's, which keeps
	// every manifest it touched. iceberg-go clusters by partition and writes
	// one manifest per month; Java's rewrite packs the entries by target size,
	// 8 MB by default, so this small table ends with one manifest.
	require.Len(t, res.RewrittenManifests, goManifests, "iceberg-go rewritten manifests")
	require.Equal(t, fmt.Sprint(javaManifests), *java[0], "spark rewritten manifests")
	require.Equal(t, months, len(res.AddedManifests))
	require.Equal(t, "1", *java[1])
	require.Equal(t, months, lane.CurrentManifests(t, tw.Go))
	require.Equal(t, 1, lane.CurrentManifests(t, tw.Java))

	goBefore, javaBefore := tw.files(t)
	txn := tw.Go.NewTransaction()
	require.NoError(t, txn.ExpireSnapshots(table.WithRetainLast(1), table.WithOlderThan(0)))
	_, err := txn.Commit(ctx)
	require.NoError(t, err)
	tw.spark(t, "CALL lake.system.expire_snapshots(table => '%s', older_than => %s, retain_last => 1)",
		sparkName(tw.Java), sparkTimestamp(time.Now()))
	tw.reload(t)
	tw.requireSameContent(t, model, "expiry")
	goAfter, javaAfter := tw.files(t)
	goExpired, javaExpired := lane.DeletedBetween(goBefore, goAfter), lane.DeletedBetween(javaBefore, javaAfter)
	require.NotEmpty(t, goExpired.Data, "expiry must delete replaced data files")
	// Manifest counts differ by design (see the manifest rewrite above), so
	// each twin must delete exactly the manifests expiry left unreferenced;
	// everything else must match.
	require.Equal(t, droppedManifests(goBefore, goAfter), goExpired.Manifests, "iceberg-go manifests")
	require.Equal(t, droppedManifests(javaBefore, javaAfter), javaExpired.Manifests, "spark manifests")
	goExpired.Manifests, javaExpired.Manifests = 0, 0
	require.Equal(t, javaExpired, goExpired, "files deleted by expiry")
	require.Len(t, tw.Go.Metadata().Snapshots(), 1)
	require.Len(t, tw.Java.Metadata().Snapshots(), 1)

	orphans := []string{"data/twin-orphan.parquet", "metadata/twin-orphan.avro"}
	lane.WriteObjects(t, tw.Go, orphans...)
	lane.WriteObjects(t, tw.Java, orphans...)
	goBefore, javaBefore = tw.files(t)
	require.ElementsMatch(t, orphans, goBefore.Unreferenced(), "iceberg-go twin")
	require.ElementsMatch(t, orphans, javaBefore.Unreferenced(), "spark twin")
	_, err = tw.Go.DeleteOrphanFiles(ctx, table.WithFilesOlderThan(0))
	require.NoError(t, err)
	removeOrphansWithSpark(t, tw, javaBefore)
	tw.reload(t)
	tw.requireSameContent(t, model, "orphan removal")
	goAfter, javaAfter = tw.files(t)
	goRemoved, javaRemoved := lane.DeletedBetween(goBefore, goAfter), lane.DeletedBetween(javaBefore, javaAfter)
	require.Equal(t, []string{"unreferenced " + orphans[0], "unreferenced " + orphans[1]}, goRemoved.Other)
	require.Equal(t, javaRemoved, goRemoved, "files deleted by orphan removal")
}

// removeOrphansWithSpark runs Spark's orphan removal on the twin with the
// listing in files, every object dated 2000-01-01. The procedure refuses an
// age under 24 hours, and the oracle cannot list S3 itself: Java lists through
// Hadoop, which has no s3 file system there, and Iceberg 1.8 has no prefix
// listing in the procedure. The old dates make the 24-hour threshold act as
// age 0, the threshold iceberg-go's twin runs with.
func removeOrphansWithSpark(t *testing.T, tw *twins, files lane.FileSet) {
	t.Helper()
	view := "objects_" + tw.Java.Identifier()[len(tw.Java.Identifier())-1]
	rows := make([]string, len(files.Objects))
	for i, o := range files.Objects {
		rows[i] = fmt.Sprintf("('%s', TIMESTAMP '2000-01-01 00:00:00')", o)
	}
	ctx := context.Background()
	_, err := tw.lk.Spark.Query(ctx, fmt.Sprintf(
		"CREATE OR REPLACE TEMPORARY VIEW %s AS SELECT * FROM VALUES %s AS t(file_path, last_modified)",
		view, strings.Join(rows, ", ")))
	require.NoError(t, err)
	_, err = tw.lk.Spark.Query(ctx, fmt.Sprintf(
		"CALL lake.system.remove_orphan_files(table => '%s', older_than => %s, file_list_view => '%s')",
		sparkName(tw.Java), sparkTimestamp(time.Now().Add(-24*time.Hour)), view))
	require.NoError(t, err)
}

// writeTwinHistory appends 200 rows for every third day of January to March
// 2025 to both twins and overwrites an earlier day after every fourth append:
// 24 appends and 6 overwrites.
func writeTwinHistory(t *testing.T, tw *twins) lane.Model {
	t.Helper()
	ctx := context.Background()
	gen := lane.NewGen(2628)
	model := lane.Model{}
	for i := range 24 {
		from, to := lane.Day(3 * i)
		rows := gen.Rows(200, from, to)
		sortByGK(rows)
		tw.commit(t, func(tbl *table.Table) (*table.Table, error) {
			rdr := lane.Reader(rows, 64)
			defer rdr.Release()

			return tbl.Append(ctx, rdr, nil)
		})
		model.Put(rows)

		if i%4 != 3 {
			continue
		}
		day := 3 * (i - 2)
		from, to = lane.Day(day)
		rows = gen.Rows(50, from, to)
		sortByGK(rows)
		tw.commit(t, func(tbl *table.Table) (*table.Table, error) {
			rdr := lane.Reader(rows, 64)
			defer rdr.Release()

			return tbl.Overwrite(ctx, rdr, nil, table.WithOverwriteFilter(dayFilter(day)))
		})
		model.DeleteWhere(inDay(day))
		model.Put(rows)
	}

	return model
}

func rewriteManifestsByPartition(t *testing.T, tbl *table.Table) *table.RewriteManifestsResult {
	t.Helper()
	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(context.Background(), table.WithRewriteManifestClusterBy(func(df iceberg.DataFile) any {
		return fmt.Sprint(df.Partition())
	}))
	require.NoError(t, err)
	require.False(t, res.IsNoOp())
	_, err = txn.Commit(context.Background())
	require.NoError(t, err)

	return res
}

// compactTwins compacts every month with two or more files: with the sort
// strategy on copy-on-write tables, and with bin-pack on merge-on-read
// tables, because iceberg-go's sort strategy refuses files with deletes.
// Sorted files must keep the (g, k) order in both twins.
func compactTwins(t *testing.T, tw *twins, mode string) {
	t.Helper()
	ctx := context.Background()
	strategy, sparkStrategy := table.SortStrategy, "sort"
	if mode == table.WriteModeMergeOnRead {
		strategy, sparkStrategy = table.BinPackStrategy, "binpack"
	}

	tasks, err := tw.Go.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 2
	cfg.Strategy = strategy
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}
	_, err = tw.Go.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{
		GroupOptions: []table.CompactionGroupOption{table.WithCompactionStrategy(strategy)},
	})
	require.NoError(t, err)

	tw.spark(t, "CALL lake.system.rewrite_data_files(table => '%s', strategy => '%s', options => map('min-input-files', '2'))",
		sparkName(tw.Java), sparkStrategy)

	if strategy != table.SortStrategy {
		return
	}
	tw.reload(t)
	for _, tbl := range []*table.Table{tw.Go, tw.Java} {
		files, err := lane.LiveDataFiles(ctx, tbl)
		require.NoError(t, err)
		for _, f := range files {
			breaks, err := tw.lk.Duck.OrderBreaks(ctx, f, "g", "k")
			require.NoError(t, err)
			require.Zero(t, breaks, "order breaks in %s", f)
		}
	}
}

func droppedManifests(before, after lane.FileSet) int {
	n := 0
	for path, kind := range before.Referenced {
		if _, still := after.Referenced[path]; kind == "manifest" && !still {
			n++
		}
	}

	return n
}
