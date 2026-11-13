package cases

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

// randomizedRuns are the seeds and steps per seed: a fixed set on every
// run, more of both in the nightly run.
func randomizedRuns() (seeds []uint64, steps int) {
	if os.Getenv("LANE_NIGHTLY") != "" {
		for s := uint64(1); s <= 20; s++ {
			seeds = append(seeds, s)
		}

		return seeds, 100
	}

	return []uint64{1, 2, 3}, 40
}

var randomOps = []string{"append", "append", "overwrite", "delete", "compact", "expire", "orphans", "rewrite_manifests"}

// TestRandomized runs random sequences of writes and maintenance and checks
// after every step that the three engines read the model, every retained
// snapshot still reads what it read when it was current, no referenced file
// is missing, and iceberg-go's orphan plan is exactly the unreferenced files.
func TestRandomized(t *testing.T) {
	seeds, steps := randomizedRuns()
	for _, mode := range lane.DeleteModes {
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s/seed%d", mode, seed), func(t *testing.T) {
				runRandomized(t, mode, seed, steps)
			})
		}
	}
}

func runRandomized(t *testing.T, mode string, seed uint64, steps int) {
	ctx := context.Background()
	lk := lane.Open(t)
	tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, monthPartitioned(), sortedByK()), mode)
	gen := lane.NewGen(seed)
	model := lane.Model{}
	snapshotChecksums := map[int64]lane.Checksum{}
	var history []string

	for step := range steps {
		started := time.Now()
		op := randomOps[gen.Rand.IntN(len(randomOps))]
		if step < 3 {
			op = "append"
		}
		tbl = lk.Reload(t, tbl)
		desc, err := applyRandomOp(ctx, tbl, op, gen, model)
		history = append(history, fmt.Sprintf("%d %s", step, desc))
		require.NoError(t, err, "history: %s", strings.Join(history, "; "))

		tbl = lk.Reload(t, tbl)
		lk.RequireChecksum(t, tbl, 0, model.Checksum())
		if cur := tbl.CurrentSnapshot(); cur != nil {
			snapshotChecksums[cur.SnapshotID] = model.Checksum()
		}
		requireRetainedSnapshots(t, lk, tbl, snapshotChecksums)
		requireFileSets(t, lk, tbl)
		t.Logf("step %d %s rows=%d %s", step, desc, len(model), time.Since(started).Round(time.Millisecond))
	}
}

func applyRandomOp(ctx context.Context, tbl *table.Table, op string, gen *lane.Gen, model lane.Model) (string, error) {
	switch op {
	case "append":
		from, _ := lane.Day(0)
		_, to := lane.Day(89)
		rows := gen.Rows(500+gen.Rand.IntN(1500), from, to)
		rdr := lane.Reader(rows, 4096)
		defer rdr.Release()
		if _, err := tbl.Append(ctx, rdr, nil); err != nil {
			return op, err
		}
		model.Put(rows)

		return fmt.Sprintf("append %d", len(rows)), nil
	case "overwrite":
		day := gen.Rand.IntN(90)
		from, to := lane.Day(day)
		rows := gen.Rows(gen.Rand.IntN(300), from, to)
		rdr := lane.Reader(rows, 4096)
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
		return op, binPackCompact(ctx, tbl)
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

func binPackCompact(ctx context.Context, tbl *table.Table) error {
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return err
	}
	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 2
	plan, err := cfg.PlanCompaction(tasks)
	if err != nil || len(plan.Groups) == 0 {
		return err
	}
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}
	txn := tbl.NewTransaction()
	if _, err := txn.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{}); err != nil {
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
