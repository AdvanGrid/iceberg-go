package lane

import (
	"context"
	"fmt"
	"strconv"
)

// DataFileOrderBreaks counts the rows of a data file whose column value is
// lower than the previous row's, in the file's physical row order.
func (d *Duck) DataFileOrderBreaks(ctx context.Context, path, column string) (int64, error) {
	row, err := d.Row(ctx, fmt.Sprintf(`SELECT count(*) FILTER (WHERE %[1]s < prev) FROM (
  SELECT %[1]s, lag(%[1]s) OVER (ORDER BY file_row_number) AS prev
  FROM read_parquet('%[2]s', file_row_number = true))`, column, path))
	if err != nil {
		return 0, fmt.Errorf("DataFileOrderBreaks - %s: %w", path, err)
	}

	return strconv.ParseInt(*row[0], 10, 64)
}
