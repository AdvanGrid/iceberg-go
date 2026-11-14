package cases

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

const (
	crashTableEnv = "LANE_CRASH_TABLE"
	crashFileLine = "LANE_CRASH_FILE "
	crashReady    = "LANE_CRASH_READY"
)

// TestCrashBeforeCommit kills a process with SIGKILL after it wrote data
// files and before it committed them. The table must still read the model,
// and orphan removal with age 0 must delete exactly the files it wrote.
func TestCrashBeforeCommit(t *testing.T) {
	for _, mode := range lane.DeleteModes {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			lk := lane.Open(t)
			tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, dayPartitioned()), mode)
			gen := lane.NewGen(7)
			model := lane.Model{}
			for day := range 3 {
				from, to := lane.Day(day)
				rows := gen.Rows(500, from, to)
				rdr := lane.Reader(rows, 4096)
				var err error
				tbl, err = tbl.Append(ctx, rdr, nil)
				rdr.Release()
				require.NoError(t, err)
				model.Put(rows)
			}
			from, to := lane.Day(1)
			rows := gen.Rows(100, from, to)
			rdr := lane.Reader(rows, 4096)
			tbl, err := tbl.Overwrite(ctx, rdr, nil, table.WithOverwriteFilter(dayFilter(1)))
			rdr.Release()
			require.NoError(t, err)
			model.DeleteWhere(inDay(1))
			model.Put(rows)

			written := runKilledWriter(t, strings.Join(tbl.Identifier(), "."))
			require.NotEmpty(t, written)

			tbl = lk.Reload(t, tbl)
			lk.RequireChecksum(t, tbl, 0, model.Checksum())
			res, err := tbl.DeleteOrphanFiles(ctx, table.WithFilesOlderThan(0))
			require.NoError(t, err)
			t.Logf("killed writer wrote %v, orphan removal deleted %v", written, res.DeletedFiles)
			require.ElementsMatch(t, written, res.DeletedFiles, "orphan removal must delete exactly the killed writer's files")
			lk.RequireChecksum(t, lk.Reload(t, tbl), 0, model.Checksum())
		})
	}
}

// runKilledWriter runs TestCrashWriter in a subprocess, waits until it has
// written its data files, kills it with SIGKILL, and returns the paths it
// reported.
func runKilledWriter(t *testing.T, ident string) []string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestCrashWriter$", "-test.count", "1")
	cmd.Env = append(os.Environ(), crashTableEnv+"="+ident)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	var written, output []string
	ready := false
	scanner := bufio.NewScanner(stdout)
	for !ready && scanner.Scan() {
		line := scanner.Text()
		output = append(output, line)
		switch {
		case strings.HasPrefix(line, crashFileLine):
			written = append(written, strings.TrimPrefix(line, crashFileLine))
		case line == crashReady:
			ready = true
		}
	}
	require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
	err = cmd.Wait()
	require.True(t, ready, "writer exited before it was ready (%v): %s", err, strings.Join(output, "\n"))
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, status.Signal())

	return written
}

// TestCrashWriter is the subprocess of TestCrashBeforeCommit: it writes data
// files for the table named in LANE_CRASH_TABLE, reports their paths, and
// blocks until it is killed.
func TestCrashWriter(t *testing.T) {
	ident := os.Getenv(crashTableEnv)
	if ident == "" {
		t.Skip("subprocess of TestCrashBeforeCommit")
	}
	ctx := context.Background()
	cat, err := catalog.Load(ctx, "lane", lane.CatalogProperties(lane.Warehouse))
	require.NoError(t, err)
	tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(strings.Split(ident, ".")...))
	require.NoError(t, err)

	gen := lane.NewGen(8)
	var rows []lane.Row
	for _, day := range []int{1, 3, 4} {
		from, to := lane.Day(day)
		rows = append(rows, gen.Rows(200, from, to)...)
	}
	files, err := writeDataFiles(ctx, tbl, rows)
	require.NoError(t, err)
	for _, f := range files {
		fmt.Println(crashFileLine + f.FilePath())
	}
	fmt.Println(crashReady)
	time.Sleep(time.Hour)
}
