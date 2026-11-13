package lane

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// Duck is an embedded DuckDB with the pinned extensions, attached to the
// lake warehouse as catalog lk.
type Duck struct {
	conn *sql.Conn
}

func extensionDir() string {
	_, file, _, _ := runtime.Caller(0)

	return filepath.Join(filepath.Dir(file), "..", ".duckdb", "extensions")
}

func OpenDuck(t testing.TB) *Duck {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = db.Close()
	})
	_, err = os.Stat(extensionDir())
	require.NoError(t, err, "pinned DuckDB extensions missing, run make advangrid-setup")

	setup := []string{
		"SET autoinstall_known_extensions = false",
		"SET autoload_known_extensions = false",
		fmt.Sprintf("SET extension_directory = '%s'", extensionDir()),
		"LOAD httpfs",
		"LOAD avro",
		"LOAD iceberg",
		"SET TimeZone = 'UTC'",
		fmt.Sprintf("CREATE SECRET lane_s3 (TYPE s3, KEY_ID '%s', SECRET '%s', ENDPOINT 'localhost:3900', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')", S3KeyID, S3Secret),
		fmt.Sprintf("ATTACH '%s' AS lk (TYPE iceberg, ENDPOINT '%s', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')", Warehouse, CatalogURI),
	}
	for _, stmt := range setup {
		_, err := conn.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	return &Duck{conn: conn}
}

// Row runs a query that returns one row and scans it as strings.
func (d *Duck) Row(ctx context.Context, query string) ([]*string, error) {
	rows, err := d.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("Duck.Row - query: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("Duck.Row - columns: %w", err)
	}
	if !rows.Next() {
		return nil, fmt.Errorf("Duck.Row - no row: %w", rows.Err())
	}
	out := make([]*string, len(cols))
	dest := make([]any, len(cols))
	for i := range out {
		dest[i] = &out[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, fmt.Errorf("Duck.Row - scan: %w", err)
	}

	return out, nil
}

// Strings runs a query that returns one string column.
func (d *Duck) Strings(ctx context.Context, query string) ([]string, error) {
	rows, err := d.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("Duck.Strings - query: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("Duck.Strings - scan: %w", err)
		}
		out = append(out, s)
	}

	return out, rows.Err()
}
