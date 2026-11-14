package cases

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

// tableLayout is one combination of the table properties the lake sets
// (#2632, #2629). Each field switches one property between iceberg-go's
// default and the lake's value. The row group cap, target file size and
// manifest merge count are scaled down so the lane's small tables reach them.
type tableLayout struct {
	dayPartitioned bool
	sorted         bool
	zstd3          bool
	rowGroupCap    bool
	smallFiles     bool
	manifestMerge  bool
}

const layoutFactors = 6

func layoutOf(bits [layoutFactors]bool) tableLayout {
	return tableLayout{
		dayPartitioned: bits[0], sorted: bits[1], zstd3: bits[2],
		rowGroupCap: bits[3], smallFiles: bits[4], manifestMerge: bits[5],
	}
}

func (l tableLayout) String() string {
	parts := []string{"month"}
	if l.dayPartitioned {
		parts[0] = "day"
	}
	for _, f := range []struct {
		on   bool
		name string
	}{
		{l.sorted, "sorted"}, {l.zstd3, "zstd3"}, {l.rowGroupCap, "rgcap"},
		{l.smallFiles, "smallfiles"}, {l.manifestMerge, "merge"},
	} {
		if f.on {
			parts = append(parts, f.name)
		}
	}

	return strings.Join(parts, "_")
}

func (l tableLayout) createOpts() []catalog.CreateTableOpt {
	props := iceberg.Properties{
		table.MetadataDeleteAfterCommitEnabledKey: "true",
		table.MetadataPreviousVersionsMaxKey:      "10",
	}
	if l.zstd3 {
		props["write.parquet.compression-codec"] = "zstd"
		props["write.parquet.compression-level"] = "3"
	}
	if l.rowGroupCap {
		props["write.parquet.row-group-limit"] = "1000"
	}
	if l.smallFiles {
		props[table.WriteTargetFileSizeBytesKey] = "16384"
	}
	if l.manifestMerge {
		props[table.ManifestMergeEnabledKey] = "true"
		props[table.ManifestMinMergeCountKey] = "4"
	}
	opts := []catalog.CreateTableOpt{monthPartitioned(), catalog.WithProperties(props)}
	if l.dayPartitioned {
		opts[0] = dayPartitioned()
	}
	if l.sorted {
		opts = append(opts, sortedByGT())
	}

	return opts
}

func sortedByGT() catalog.CreateTableOpt {
	order, err := table.NewSortOrder(1, []table.SortField{
		{SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst},
		{SourceIDs: []int{3}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst},
	})
	if err != nil {
		panic(err)
	}

	return catalog.WithSortOrder(order)
}

// allLayouts is the full cross product of the layout properties.
func allLayouts() []tableLayout {
	layouts := make([]tableLayout, 0, 1<<layoutFactors)
	for i := range 1 << layoutFactors {
		var bits [layoutFactors]bool
		for f := range bits {
			bits[f] = i&(1<<f) != 0
		}
		layouts = append(layouts, layoutOf(bits))
	}

	return layouts
}

// pairwiseLayouts are eight layouts in which every pair of values of any
// two properties occurs: an L8 orthogonal array whose columns are the bits
// a, b, c of the row and their pairwise sums.
func pairwiseLayouts() []tableLayout {
	layouts := make([]tableLayout, 0, 8)
	for i := range 8 {
		a, b, c := i&1 != 0, i&2 != 0, i&4 != 0
		layouts = append(layouts, layoutOf([layoutFactors]bool{a, b, c, a != b, a != c, b != c}))
	}

	return layouts
}

type randomizedRun struct {
	layout tableLayout
	seed   uint64
	steps  int
}

// randomizedRuns are the fixed per-PR set, the pairwise layouts, and in the
// nightly run every layout. Each layout runs under its own seed.
func randomizedRuns() []randomizedRun {
	layouts, steps := pairwiseLayouts(), 12
	if os.Getenv("LANE_NIGHTLY") != "" {
		layouts, steps = allLayouts(), 40
	}
	var runs []randomizedRun
	for i, l := range layouts {
		if inShard(i) {
			runs = append(runs, randomizedRun{layout: l, seed: uint64(i + 1), steps: steps})
		}
	}

	return runs
}

// inShard reports whether the i-th run belongs to this process's shard.
// LANE_SHARD is "k/n" (shard k of n); without it every run belongs.
func inShard(i int) bool {
	shard := os.Getenv("LANE_SHARD")
	if shard == "" {
		return true
	}
	var k, n int
	if _, err := fmt.Sscanf(shard, "%d/%d", &k, &n); err != nil || n <= 0 {
		panic(fmt.Sprintf("LANE_SHARD %q is not k/n", shard))
	}

	return i%n == k
}

// appendDays is the span of days one append covers, like an ingest run
// that writes the latest days. Spanning all 90 days would give every append
// of a day-partitioned table 90 tiny files.
const appendDays = 7

var randomOps = []string{"append", "append", "overwrite", "delete", "compact", "expire", "orphans", "rewrite_manifests"}

// TestRandomized runs random sequences of writes and maintenance and checks
// after every step that the three engines read the model, every retained
// snapshot still reads what it read when it was current, no referenced file
// is missing, and iceberg-go's orphan plan is exactly the unreferenced files.
func TestRandomized(t *testing.T) {
	for _, mode := range lane.DeleteModes {
		for _, run := range randomizedRuns() {
			t.Run(fmt.Sprintf("%s/%s/seed%d", mode, run.layout, run.seed), func(t *testing.T) {
				runRandomized(t, mode, run)
			})
		}
	}
}

func runRandomized(t *testing.T, mode string, run randomizedRun) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, run.layout.createOpts()...), mode)
	gen := lane.NewGen(run.seed)
	model := lane.Model{}
	snapshotChecksums := map[int64]lane.Checksum{}
	var history []string

	for step := range run.steps {
		started := time.Now()
		op := randomOps[gen.Rand.IntN(len(randomOps))]
		if step < 3 {
			op = "append"
		}
		tbl = lk.Reload(t, tbl)
		desc, err := applyRandomOp(ctx, tbl, op, gen, model, run.layout.sorted)
		history = append(history, fmt.Sprintf("%d %s", step, desc))
		require.NoError(t, err, "history: %s", strings.Join(history, "; "))

		tbl = lk.Reload(t, tbl)
		lk.RequireChecksum(t, tbl, 0, model.Checksum())
		if cur := tbl.CurrentSnapshot(); cur != nil {
			snapshotChecksums[cur.SnapshotID] = model.Checksum()
		}
		requireRetainedSnapshots(t, lk, tbl, snapshotChecksums)
		requireFileSets(t, lk, tbl)
		if op == "compact" && run.layout.sorted {
			requireSortedFilesInOrder(t, lk, tbl)
		}
		t.Logf("step %d %s rows=%d %s", step, desc, len(model), time.Since(started).Round(time.Millisecond))
	}
}

func applyRandomOp(ctx context.Context, tbl *table.Table, op string, gen *lane.Gen, model lane.Model, sorted bool) (string, error) {
	switch op {
	case "append":
		first := gen.Rand.IntN(90 - appendDays + 1)
		from, _ := lane.Day(first)
		_, to := lane.Day(first + appendDays - 1)
		rows := gen.Rows(500+gen.Rand.IntN(1500), from, to)
		rdr := lane.Reader(rows, 512)
		defer rdr.Release()
		if _, err := tbl.Append(ctx, rdr, nil); err != nil {
			return op, err
		}
		model.Put(rows)

		return fmt.Sprintf("append %d to days %d-%d", len(rows), first, first+appendDays-1), nil
	case "overwrite":
		day := gen.Rand.IntN(90)
		from, to := lane.Day(day)
		rows := gen.Rows(gen.Rand.IntN(300), from, to)
		rdr := lane.Reader(rows, 512)
		defer rdr.Release()
		desc := fmt.Sprintf("overwrite day %d with %d", day, len(rows))
		if _, err := tbl.Overwrite(ctx, rdr, nil, table.WithOverwriteFilter(dayFilter(day))); err != nil {
			return desc, err
		}
		model.DeleteWhere(inDay(day))
		model.Put(rows)

		return desc, nil
	case "delete":
		g := int32(gen.Rand.IntN(10))
		desc := fmt.Sprintf("delete g=%d", g)
		if _, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("g"), g), nil); err != nil {
			return desc, err
		}
		model.DeleteWhere(func(r lane.Row) bool { return r.G == g })

		return desc, nil
	case "compact":
		if !sorted {
			return op, compact(ctx, tbl, table.BinPackStrategy)
		}
		err := compact(ctx, tbl, table.SortStrategy)
		if errors.Is(err, table.ErrInvalidOperation) {
			return "compact bin-pack, files have deletes", compact(ctx, tbl, table.BinPackStrategy)
		}

		return "compact sort", err
	case "expire":
		keep := 1 + gen.Rand.IntN(3)
		txn := tbl.NewTransaction()
		if err := txn.ExpireSnapshots(table.WithRetainLast(keep), table.WithOlderThan(0)); err != nil {
			return op, err
		}
		_, err := txn.Commit(ctx)

		return fmt.Sprintf("expire retain %d", keep), err
	case "orphans":
		res, err := tbl.DeleteOrphanFiles(ctx, table.WithFilesOlderThan(0))

		return fmt.Sprintf("orphans deleted %d", len(res.DeletedFiles)), err
	case "rewrite_manifests":
		txn := tbl.NewTransaction()
		res, err := txn.RewriteManifests(ctx, table.WithRewriteManifestClusterBy(func(df iceberg.DataFile) any {
			return fmt.Sprint(df.Partition())
		}))
		if err != nil || res.IsNoOp() {
			return op, err
		}
		_, err = txn.Commit(ctx)

		return fmt.Sprintf("rewrite manifests %d -> %d", len(res.RewrittenManifests), len(res.AddedManifests)), err
	}

	return op, fmt.Errorf("unknown op %s", op)
}

// compact rewrites every partition with two or more files. The sort
// strategy refuses files with deletes with table.ErrInvalidOperation.
func compact(ctx context.Context, tbl *table.Table, strategy table.CompactionStrategy) error {
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return err
	}
	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 2
	cfg.Strategy = strategy
	plan, err := cfg.PlanCompaction(tasks)
	if err != nil || len(plan.Groups) == 0 {
		return err
	}
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}
	txn := tbl.NewTransaction()
	opts := table.RewriteDataFilesOptions{
		GroupOptions: []table.CompactionGroupOption{table.WithCompactionStrategy(strategy)},
	}
	if _, err := txn.RewriteDataFiles(ctx, groups, opts); err != nil {
		return err
	}
	_, err = txn.Commit(ctx)

	return err
}

func requireRetainedSnapshots(t *testing.T, lk *lane.Lake, tbl *table.Table, checksums map[int64]lane.Checksum) {
	t.Helper()
	retained := map[int64]bool{}
	for _, s := range tbl.Metadata().Snapshots() {
		retained[s.SnapshotID] = true
		want, ok := checksums[s.SnapshotID]
		if !ok {
			continue
		}
		records, err := tbl.Scan(table.WithSnapshotID(s.SnapshotID)).ToArrowTable(context.Background())
		require.NoError(t, err)
		got, err := lane.ChecksumOfRecords(tbl.Schema(), records)
		records.Release()
		require.NoError(t, err)
		require.Equal(t, want, got, "snapshot %d", s.SnapshotID)
	}
	for id := range checksums {
		if !retained[id] {
			delete(checksums, id)
		}
	}
}

func requireFileSets(t *testing.T, lk *lane.Lake, tbl *table.Table) {
	t.Helper()
	ctx := context.Background()
	ref, err := lane.ReferencedFiles(ctx, tbl)
	require.NoError(t, err)
	objects, err := lk.Duck.ListObjects(ctx, tbl.Location())
	require.NoError(t, err)

	for path, kind := range ref {
		if kind != "metadata" {
			require.True(t, slices.Contains(objects, path), "referenced %s %s is missing", kind, path)
		}
	}

	plan, err := tbl.PlanOrphanFiles(ctx, table.WithFilesOlderThan(0))
	require.NoError(t, err)
	var unreferenced []string
	for _, o := range objects {
		if _, used := ref[o]; !used {
			unreferenced = append(unreferenced, o)
		}
	}
	require.ElementsMatch(t, unreferenced, plan.Files(), "orphan plan must be exactly the unreferenced objects")
}

// requireSortedFilesInOrder checks that every live data file that declares
// the table's sort order has its rows in (g, t) order.
func requireSortedFilesInOrder(t *testing.T, lk *lane.Lake, tbl *table.Table) {
	t.Helper()
	ctx := context.Background()
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	for _, task := range tasks {
		id := task.File.SortOrderID()
		if id == nil || *id != tbl.SortOrder().OrderID() {
			continue
		}
		breaks, err := lk.Duck.OrderBreaks(ctx, task.File.FilePath(), "g", "t")
		require.NoError(t, err)
		require.Zero(t, breaks, "order breaks in %s", task.File.FilePath())
	}
}
