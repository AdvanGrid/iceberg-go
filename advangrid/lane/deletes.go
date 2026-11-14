package lane

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// PositionDeleteFiles returns the paths of the current snapshot's position
// delete files.
func PositionDeleteFiles(ctx context.Context, tbl *table.Table) ([]string, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("PositionDeleteFiles - fs: %w", err)
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil, nil
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("PositionDeleteFiles - manifests: %w", err)
	}
	var paths []string
	for _, m := range manifests {
		entries, err := m.FetchEntries(fs, true)
		if err != nil {
			return nil, fmt.Errorf("PositionDeleteFiles - entries: %w", err)
		}
		for _, e := range entries {
			if e.DataFile().ContentType() == iceberg.EntryContentPosDeletes {
				paths = append(paths, e.DataFile().FilePath())
			}
		}
	}

	return paths, nil
}

// PositionDeleteOrderBreaks counts the rows of a position delete file that
// come before their predecessor in (file_path, pos) order, as DuckDB reads
// the file.
func (d *Duck) PositionDeleteOrderBreaks(ctx context.Context, path string) (int64, error) {
	row, err := d.Row(ctx, fmt.Sprintf(`SELECT CAST(count(*) AS VARCHAR) FROM (
  SELECT file_path, pos,
    lag(file_path) OVER (ORDER BY file_row_number) AS prev_path,
    lag(pos) OVER (ORDER BY file_row_number) AS prev_pos
  FROM read_parquet('%s', file_row_number = true))
WHERE file_path < prev_path OR (file_path = prev_path AND pos < prev_pos)`, path))
	if err != nil {
		return 0, fmt.Errorf("PositionDeleteOrderBreaks - %s: %w", path, err)
	}
	var n int64
	_, err = fmt.Sscan(*row[0], &n)

	return n, err
}
