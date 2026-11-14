package lane

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// FileSet is a table's files at one point: what its metadata references,
// the content identity of each referenced data file, and the objects under
// its location.
type FileSet struct {
	Location   string
	Referenced map[string]string
	DataIDs    map[string]string
	Objects    []string
}

// Files reads the table's current FileSet.
func (l *Lake) Files(t testing.TB, tbl *table.Table) FileSet {
	t.Helper()
	ctx := context.Background()
	ref, err := ReferencedFiles(ctx, tbl)
	require.NoError(t, err)
	ids, err := DataFileIDs(ctx, tbl)
	require.NoError(t, err)
	objects, err := l.Duck.ListObjects(ctx, tbl.Location())
	require.NoError(t, err)

	return FileSet{Location: strings.TrimSuffix(tbl.Location(), "/"), Referenced: ref, DataIDs: ids, Objects: objects}
}

// DataFileIDs names every data and delete file the table's retained
// snapshots reference by its content: content type, partition, row count and
// the bounds of column k. Twin tables that received the same rows hold files
// with the same identities under different paths, so their file sets compare
// by identity.
func DataFileIDs(ctx context.Context, tbl *table.Table) (map[string]string, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("DataFileIDs - fs: %w", err)
	}
	ids := map[string]string{}
	for _, snap := range tbl.Metadata().Snapshots() {
		manifests, err := snap.Manifests(fs)
		if err != nil {
			return nil, fmt.Errorf("DataFileIDs - manifests of %d: %w", snap.SnapshotID, err)
		}
		for _, m := range manifests {
			entries, err := m.FetchEntries(fs, true)
			if err != nil {
				return nil, fmt.Errorf("DataFileIDs - entries of %s: %w", m.FilePath(), err)
			}
			for _, e := range entries {
				ids[e.DataFile().FilePath()] = dataFileID(e.DataFile())
			}
		}
	}

	return ids, nil
}

func dataFileID(df iceberg.DataFile) string {
	const keyField = 1
	bound := func(b map[int][]byte) string {
		if len(b[keyField]) != 8 {
			return "?"
		}

		return fmt.Sprint(int64(binary.LittleEndian.Uint64(b[keyField])))
	}

	return fmt.Sprintf("%s %v rows=%d k=%s..%s", df.ContentType(), df.Partition(), df.Count(),
		bound(df.LowerBoundValues()), bound(df.UpperBoundValues()))
}

// LiveDataIDs returns the sorted identities of the current snapshot's data files.
func LiveDataIDs(t testing.TB, tbl *table.Table) []string {
	t.Helper()
	ctx := context.Background()
	paths, err := LiveDataFiles(ctx, tbl)
	require.NoError(t, err)
	ids, err := DataFileIDs(ctx, tbl)
	require.NoError(t, err)
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = ids[p]
	}
	slices.Sort(out)

	return out
}

// Deleted is what a maintenance step removed from a table's location, by the
// kind each object had before the step. Data files are named by identity so
// twins compare; manifests and manifest lists have no identity across tables
// and are counted. Other holds deleted metadata files and objects that were
// not referenced before, by their path under the table location.
type Deleted struct {
	Data          []string
	Deletes       []string
	Manifests     int
	ManifestLists int
	Other         []string
}

// DeletedBetween is the objects listed before and not after, by kind.
func DeletedBetween(before, after FileSet) Deleted {
	var d Deleted
	for _, obj := range before.Objects {
		if slices.Contains(after.Objects, obj) {
			continue
		}
		switch kind := before.Referenced[obj]; kind {
		case "data":
			d.Data = append(d.Data, before.DataIDs[obj])
		case "delete":
			d.Deletes = append(d.Deletes, before.DataIDs[obj])
		case "manifest":
			d.Manifests++
		case "manifest-list":
			d.ManifestLists++
		case "":
			d.Other = append(d.Other, "unreferenced "+before.relative(obj))
		default:
			d.Other = append(d.Other, kind+" "+before.relative(obj))
		}
	}
	slices.Sort(d.Data)
	slices.Sort(d.Deletes)
	slices.Sort(d.Other)

	return d
}

// CurrentManifests returns the number of manifests of the table's current snapshot.
func CurrentManifests(t testing.TB, tbl *table.Table) int {
	t.Helper()
	ctx := context.Background()
	fs, err := tbl.FS(ctx)
	require.NoError(t, err)
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)

	return len(manifests)
}

func (fs FileSet) relative(path string) string {
	return strings.TrimPrefix(path, fs.Location+"/")
}

// Unreferenced returns the listed objects the metadata does not reference,
// by their path under the table location.
func (fs FileSet) Unreferenced() []string {
	var out []string
	for _, o := range fs.Objects {
		if _, ok := fs.Referenced[o]; !ok {
			out = append(out, fs.relative(o))
		}
	}

	return out
}

// WriteObjects writes a small object at each path under the table location.
func WriteObjects(t testing.TB, tbl *table.Table, paths ...string) {
	t.Helper()
	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)
	wfs, ok := fs.(icebergio.WriteFileIO)
	require.True(t, ok, "table file IO cannot write")
	for _, p := range paths {
		require.NoError(t, wfs.WriteFile(strings.TrimSuffix(tbl.Location(), "/")+"/"+p, []byte("orphan")))
	}
}
