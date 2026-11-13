package lane

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go/table"
)

// ReferencedFiles returns every file the table's retained snapshots and
// metadata log point at, with its kind: metadata, manifest-list, manifest,
// data or delete.
func ReferencedFiles(ctx context.Context, tbl *table.Table) (map[string]string, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("ReferencedFiles - fs: %w", err)
	}
	ref := map[string]string{tbl.MetadataLocation(): "metadata"}
	for entry := range tbl.Metadata().PreviousFiles() {
		ref[entry.MetadataFile] = "metadata"
	}
	for _, snap := range tbl.Metadata().Snapshots() {
		ref[snap.ManifestList] = "manifest-list"
		manifests, err := snap.Manifests(fs)
		if err != nil {
			return nil, fmt.Errorf("ReferencedFiles - manifests of %d: %w", snap.SnapshotID, err)
		}
		for _, m := range manifests {
			if _, seen := ref[m.FilePath()]; seen {
				continue
			}
			ref[m.FilePath()] = "manifest"
			entries, err := m.FetchEntries(fs, true)
			if err != nil {
				return nil, fmt.Errorf("ReferencedFiles - entries of %s: %w", m.FilePath(), err)
			}
			for _, e := range entries {
				kind := "data"
				if e.DataFile().ContentType() != 0 {
					kind = "delete"
				}
				ref[e.DataFile().FilePath()] = kind
			}
		}
	}

	return ref, nil
}

// ListObjects lists every object under the table location.
func (d *Duck) ListObjects(ctx context.Context, location string) ([]string, error) {
	return d.Strings(ctx, fmt.Sprintf("SELECT file FROM glob('%s/**') ORDER BY 1", strings.TrimSuffix(location, "/")))
}

// LiveDataFiles returns the paths of the current snapshot's data files.
func LiveDataFiles(ctx context.Context, tbl *table.Table) ([]string, error) {
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("LiveDataFiles - plan: %w", err)
	}
	paths := make([]string, len(tasks))
	for i, t := range tasks {
		paths[i] = t.File.FilePath()
	}

	return paths, nil
}
