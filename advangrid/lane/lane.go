// Package lane connects tests to the differential lane started by
// make advangrid-setup: Lakekeeper and Garage as the catalog and store,
// iceberg-go as the writer, and DuckDB and Spark (Iceberg Java) as readers.
package lane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

const (
	CatalogURI = "http://localhost:8282/catalog"
	S3Endpoint = "http://localhost:3900"
	S3KeyID    = "GK26280000000000000000000a"
	S3Secret   = "2628000000000000000000000000000000000000000000000000000000000000"
	Warehouse  = "lake"
	Namespace  = "lane"
)

// Lake is one test's view of the lane: the iceberg-go catalog and both
// reference readers.
type Lake struct {
	Catalog catalog.Catalog
	Duck    *Duck
	Spark   *Spark
}

// CatalogProperties are the iceberg-go REST catalog properties for a warehouse.
func CatalogProperties(warehouse string) iceberg.Properties {
	return iceberg.Properties{
		"type":                        "rest",
		"uri":                         CatalogURI,
		"warehouse":                   warehouse,
		"s3.region":                   "us-east-1",
		"s3.access-key-id":            S3KeyID,
		"s3.secret-access-key":        S3Secret,
		"s3.force-virtual-addressing": "false",
	}
}

func Open(t testing.TB) *Lake {
	t.Helper()
	ctx := context.Background()
	cat, err := catalog.Load(ctx, "lane", CatalogProperties(Warehouse))
	require.NoError(t, err)
	ensureNamespace(t, cat)

	return &Lake{Catalog: cat, Duck: OpenDuck(t), Spark: &Spark{}}
}

func ensureNamespace(t testing.TB, cat catalog.Catalog) {
	t.Helper()
	ctx := context.Background()
	exists, err := cat.CheckNamespaceExists(ctx, catalog.ToIdentifier(Namespace))
	require.NoError(t, err)
	if !exists {
		err = cat.CreateNamespace(ctx, catalog.ToIdentifier(Namespace), nil)
		if err != nil && !strings.Contains(err.Error(), "exists") {
			require.NoError(t, err)
		}
	}
}

var nonIdentifier = regexp.MustCompile(`[^a-z0-9]+`)

// TableName is a fresh table name derived from the test name.
func TableName(t testing.TB) string {
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	base := nonIdentifier.ReplaceAllString(strings.ToLower(t.Name()), "_")
	if len(base) > 40 {
		base = base[len(base)-40:]
	}

	return strings.Trim(base, "_") + "_" + hex.EncodeToString(suffix)
}

func Ident(name string) table.Identifier { return catalog.ToIdentifier(Namespace, name) }

// CreateTable creates a fresh table and drops it, with its files, when the
// test ends.
func (l *Lake) CreateTable(t testing.TB, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) *table.Table {
	t.Helper()
	name := TableName(t)
	tbl, err := l.Catalog.CreateTable(context.Background(), Ident(name), sc, opts...)
	require.NoError(t, err, "create %s", name)
	t.Cleanup(func() {
		_ = l.Catalog.DropTable(context.Background(), Ident(name))
	})

	return tbl
}

// Reload returns the table's current state from the catalog.
func (l *Lake) Reload(t testing.TB, tbl *table.Table) *table.Table {
	t.Helper()
	fresh, err := l.Catalog.LoadTable(context.Background(), tbl.Identifier())
	require.NoError(t, err)

	return fresh
}

// SetDeleteMode sets write.delete.mode on the table.
func SetDeleteMode(t testing.TB, tbl *table.Table, mode string) *table.Table {
	t.Helper()
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: mode}))
	tbl, err := txn.Commit(context.Background())
	require.NoError(t, err)

	return tbl
}

// DeleteModes are the delete modes every case runs under.
var DeleteModes = []string{table.WriteModeCopyOnWrite}

func tableName(tbl *table.Table) string {
	id := tbl.Identifier()

	return fmt.Sprintf("%s.%s", id[len(id)-2], id[len(id)-1])
}
