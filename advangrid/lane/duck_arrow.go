//go:build duckdb_arrow

package lane

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// Arrow streams the result of query as Arrow record batches into fn. The
// batches belong to the reader.
func (d *Duck) Arrow(ctx context.Context, query string, fn func(array.RecordReader) error) error {
	return d.conn.Raw(func(dc any) error {
		ar, err := duckdb.NewArrowFromConn(dc.(driver.Conn))
		if err != nil {
			return fmt.Errorf("Duck.Arrow - conn: %w", err)
		}
		rr, err := ar.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("Duck.Arrow - query: %w", err)
		}
		defer rr.Release()

		return fn(rr)
	})
}
