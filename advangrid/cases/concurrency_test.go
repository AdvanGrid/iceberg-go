package cases

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

const (
	writeFor       = 90 * time.Second
	snapshotAge    = time.Minute
	orphanAge      = 2 * time.Minute
	maintenanceGap = 2 * time.Second
)

func dayPartitioned() catalog.CreateTableOpt {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{3}, FieldID: 1000, Name: "t_day", Transform: iceberg.DayTransform{},
	})

	return catalog.WithPartitionSpec(&spec)
}

func commitRetries() catalog.CreateTableOpt {
	return catalog.WithProperties(iceberg.Properties{
		table.CommitNumRetriesKey:          "50",
		table.CommitMinRetryWaitMsKey:      "50",
		table.CommitMaxRetryWaitMsKey:      "2000",
		table.CommitTotalRetryTimeoutMsKey: strconv.Itoa(int((10 * time.Minute).Milliseconds())),
	})
}

func records(rows []lane.Row) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		batch := lane.Batch(rows)
		defer batch.Release()
		yield(batch, nil)
	}
}

// writeDataFiles writes rows as data files of tbl without committing them.
func writeDataFiles(ctx context.Context, tbl *table.Table, rows []lane.Row) ([]iceberg.DataFile, error) {
	var files []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, tbl, lane.ArrowSchema, records(rows)) {
		if err != nil {
			return nil, err
		}
		files = append(files, df)
	}

	return files, nil
}

// concurrentWriter owns a set of days and a key range, so its model is the
// table's content on those days whatever the other writer commits.
type concurrentWriter struct {
	cat   catalog.Catalog
	ident table.Identifier
	days  []int
	gen   *lane.Gen
	keyOf int64
	model lane.Model
}

func (w *concurrentWriter) rows(n, day int) []lane.Row {
	from, to := lane.Day(day)
	rows := w.gen.Rows(n, from, to)
	for i := range rows {
		rows[i].K += w.keyOf
	}

	return rows
}

// run appends to and overwrites its days for at least d, so that the first
// snapshots outlive snapshotAge while the writers still run.
func (w *concurrentWriter) run(ctx context.Context, d time.Duration) error {
	start := time.Now()
	for round := 0; round < len(w.days) || time.Since(start) < d; round++ {
		// Each writer commits every few seconds: an overwrite retry re-reads
		// every manifest of the fresh head (AdvanGrid/advangrid#2654), so a
		// peer committing faster than that starves it.
		time.Sleep(time.Duration(2000+w.gen.Rand.IntN(3000)) * time.Millisecond)
		tbl, err := w.cat.LoadTable(ctx, w.ident)
		if err != nil {
			return fmt.Errorf("round %d load: %w", round, err)
		}
		day := w.days[round%len(w.days)]
		if round < len(w.days) || w.gen.Rand.IntN(2) == 0 {
			rows := w.rows(200+w.gen.Rand.IntN(300), day)
			rdr := lane.Reader(rows, 4096)
			_, err = tbl.Append(ctx, rdr, nil)
			rdr.Release()
			if err != nil {
				return fmt.Errorf("round %d append day %d: %w", round, day, err)
			}
			w.model.Put(rows)

			continue
		}
		rows := w.rows(w.gen.Rand.IntN(200), day)
		rdr := lane.Reader(rows, 4096)
		_, err = tbl.Overwrite(ctx, rdr, nil, table.WithOverwriteFilter(dayFilter(day)))
		rdr.Release()
		if err != nil {
			return fmt.Errorf("round %d overwrite day %d: %w", round, day, err)
		}
		w.model.DeleteWhere(inDay(day))
		w.model.Put(rows)
	}

	return nil
}

// pausedAppend writes data files for day, hands their paths to written,
// waits for resume, and only then commits them.
func (w *concurrentWriter) pausedAppend(ctx context.Context, day int, written chan<- []string, resume <-chan struct{}) error {
	tbl, err := w.cat.LoadTable(ctx, w.ident)
	if err != nil {
		return fmt.Errorf("paused load: %w", err)
	}
	rows := w.rows(300, day)
	files, err := writeDataFiles(ctx, tbl, rows)
	if err != nil {
		return fmt.Errorf("paused write: %w", err)
	}
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.FilePath()
	}
	written <- paths
	<-resume

	txn := tbl.NewTransaction()
	if err := txn.AddDataFiles(ctx, files, nil); err != nil {
		return fmt.Errorf("paused add: %w", err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("paused commit: %w", err)
	}
	w.model.Put(rows)

	return nil
}

// maintenanceRun is what a maintenance loop did: how many passes it made, how
// many snapshots its expiry removed, and the files its orphan removal deleted.
type maintenanceRun struct {
	passes  int
	expired int
	deleted []string
}

// maintain expires snapshots older than snapshotAge and removes orphans
// older than orphanAge until stop is closed. The age thresholds keep the
// base snapshots and files of in-flight writes.
func maintain(ctx context.Context, cat catalog.Catalog, ident table.Identifier, stop <-chan struct{}) (maintenanceRun, error) {
	var run maintenanceRun
	for {
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			return run, fmt.Errorf("maintain load: %w", err)
		}
		before := len(tbl.Metadata().Snapshots())
		txn := tbl.NewTransaction()
		if err := txn.ExpireSnapshots(table.WithRetainLast(1), table.WithOlderThan(snapshotAge)); err != nil {
			return run, fmt.Errorf("maintain expire: %w", err)
		}
		if tbl, err = txn.Commit(ctx); err != nil {
			return run, fmt.Errorf("maintain expire commit: %w", err)
		}
		run.expired += max(0, before-len(tbl.Metadata().Snapshots()))
		res, err := tbl.DeleteOrphanFiles(ctx, table.WithFilesOlderThan(orphanAge))
		if err != nil {
			return run, fmt.Errorf("maintain orphans: %w", err)
		}
		run.deleted = append(run.deleted, res.DeletedFiles...)
		run.passes++
		select {
		case <-stop:
			return run, nil
		case <-time.After(maintenanceGap):
		}
	}
}

// TestConcurrentWritersAndMaintenance runs two writers on disjoint days and
// a maintenance loop of expiry and orphan removal against one table at the
// same time. Commit conflicts must retry and leave the union of the writers'
// models, and orphan removal must keep the data files of a write that is in
// flight and younger than the age threshold.
func TestConcurrentWritersAndMaintenance(t *testing.T) {
	for _, mode := range lane.DeleteModes {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			lk := lane.Open(t)
			tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, dayPartitioned(), commitRetries()), mode)
			ident := tbl.Identifier()

			writers := make([]*concurrentWriter, 2)
			for i := range writers {
				writers[i] = &concurrentWriter{
					cat:   lane.Open(t).Catalog,
					ident: ident,
					days:  []int{i, i + 2, i + 4, i + 6},
					gen:   lane.NewGen(uint64(10 + i)),
					keyOf: int64(i) * 1_000_000_000,
					model: lane.Model{},
				}
			}
			maintCat := lane.Open(t).Catalog

			written := make(chan []string, 1)
			resume := make(chan struct{})
			stopMaintenance := make(chan struct{})
			var wg sync.WaitGroup
			writerErrs := make([]error, len(writers))
			for i, w := range writers {
				wg.Go(func() {
					writerErrs[i] = w.run(ctx, writeFor)
					if writerErrs[i] == nil && i == 0 {
						writerErrs[i] = w.pausedAppend(ctx, 8, written, resume)
					}
					if writerErrs[i] != nil && i == 0 {
						close(written)
					}
				})
			}
			var maint maintenanceRun
			var maintErr error
			maintDone := make(chan struct{})
			go func() {
				defer close(maintDone)
				maint, maintErr = maintain(ctx, maintCat, ident, stopMaintenance)
			}()

			inFlight, ok := <-written
			var candidates, pausedDeleted []string
			var pausedErr error
			if ok {
				candidates, pausedDeleted, pausedErr = orphanPassWhilePaused(ctx, lk.Catalog, ident)
				close(resume)
			}
			wg.Wait()
			close(stopMaintenance)
			<-maintDone
			require.NoError(t, errors.Join(append(writerErrs, maintErr, pausedErr)...))
			require.Subset(t, candidates, inFlight, "in-flight files must be orphan candidates apart from their age")
			require.Empty(t, intersect(pausedDeleted, inFlight), "orphan removal deleted in-flight files")
			require.Empty(t, intersect(maint.deleted, inFlight), "maintenance deleted in-flight files")
			require.Positive(t, maint.expired, "expiry must remove snapshots while the writers run")
			t.Logf("maintenance passes %d, snapshots expired %d, orphans deleted %d", maint.passes, maint.expired, len(maint.deleted))

			tbl = lk.Reload(t, tbl)
			model := lane.Model{}
			for _, w := range writers {
				for _, r := range w.model {
					model[r.K] = r
				}
			}
			requireReferencedFilesExist(t, lk, tbl)
			lk.RequireChecksum(t, tbl, 0, model.Checksum())
		})
	}
}

// orphanPassWhilePaused returns the orphan candidates regardless of age and
// the files an orphan removal with the age threshold deleted.
func orphanPassWhilePaused(ctx context.Context, cat catalog.Catalog, ident table.Identifier) ([]string, []string, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, nil, fmt.Errorf("paused load: %w", err)
	}
	plan, err := tbl.PlanOrphanFiles(ctx, table.WithFilesOlderThan(0))
	if err != nil {
		return nil, nil, fmt.Errorf("paused plan: %w", err)
	}
	res, err := tbl.DeleteOrphanFiles(ctx, table.WithFilesOlderThan(orphanAge))
	if err != nil {
		return nil, nil, fmt.Errorf("paused orphans: %w", err)
	}

	return plan.Files(), res.DeletedFiles, nil
}

func requireReferencedFilesExist(t *testing.T, lk *lane.Lake, tbl *table.Table) {
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
}

func intersect(a, b []string) []string {
	var out []string
	for _, x := range a {
		if slices.Contains(b, x) {
			out = append(out, x)
		}
	}

	return out
}
