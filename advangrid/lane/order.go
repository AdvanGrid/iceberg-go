package lane

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// OrderBreaks counts the rows of a Parquet file, in file order, that sort
// before their predecessor by the given ascending columns.
func (d *Duck) OrderBreaks(ctx context.Context, path string, cols ...string) (int64, error) {
	lags := make([]string, len(cols))
	for i, c := range cols {
		lags[i] = fmt.Sprintf("%s, lag(%s) OVER (ORDER BY file_row_number) AS prev_%s", c, c, c)
	}
	before := make([]string, len(cols))
	for i, c := range cols {
		eq := make([]string, 0, i+1)
		for _, p := range cols[:i] {
			eq = append(eq, fmt.Sprintf("%s = prev_%s", p, p))
		}
		eq = append(eq, fmt.Sprintf("%s < prev_%s", c, c))
		before[i] = "(" + strings.Join(eq, " AND ") + ")"
	}
	q := fmt.Sprintf("SELECT count(*) FILTER (WHERE %s) FROM (SELECT %s FROM read_parquet('%s', file_row_number = true))",
		strings.Join(before, " OR "), strings.Join(lags, ", "), path)
	row, err := d.Row(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("OrderBreaks - %s: %w", path, err)
	}
	breaks, err := strconv.ParseInt(*row[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("OrderBreaks - parse: %w", err)
	}

	return breaks, nil
}
