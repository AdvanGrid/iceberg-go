package lane

import (
	"context"
	"fmt"
	"maps"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// SplitWarehouse is the lane warehouse whose storage endpoint,
// http://spark-iceberg:3900, resolves inside the stack but not on the host.
const SplitWarehouse = "split"

// OpenSplit opens the split warehouse from the host, with the host's S3
// endpoint and extra as client properties. DuckDB attaches it as sp and Spark
// registers it as catalog split; read it with SplitChecksums, not Checksums.
func OpenSplit(t testing.TB, extra iceberg.Properties) *Lake {
	t.Helper()
	ctx := context.Background()
	props := CatalogProperties(SplitWarehouse)
	props[iceio.S3EndpointURL] = S3Endpoint
	maps.Copy(props, extra)
	cat, err := catalog.Load(ctx, "split", props)
	require.NoError(t, err)
	ensureNamespace(t, cat)

	duck := OpenDuck(t)
	attach := fmt.Sprintf("ATTACH '%s' AS sp (TYPE iceberg, ENDPOINT '%s', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')",
		SplitWarehouse, CatalogURI)
	_, err = duck.conn.ExecContext(ctx, attach)
	require.NoError(t, err, attach)

	spark := &Spark{}
	for key, value := range sparkCatalogConf(SplitWarehouse) {
		_, err := spark.Query(ctx, fmt.Sprintf("SET `spark.sql.catalog.%s%s`=`%s`", SplitWarehouse, key, value))
		require.NoError(t, err)
	}

	return &Lake{Catalog: cat, Duck: duck, Spark: spark}
}

func sparkCatalogConf(warehouse string) map[string]string {
	return map[string]string{
		"":                      "org.apache.iceberg.spark.SparkCatalog",
		".type":                 "rest",
		".uri":                  CatalogURI,
		".warehouse":            warehouse,
		".io-impl":              "org.apache.iceberg.aws.s3.S3FileIO",
		".s3.access-key-id":     S3KeyID,
		".s3.secret-access-key": S3Secret,
		".s3.path-style-access": "true",
		".client.region":        "us-east-1",
		".cache-enabled":        "false",
	}
}

func inCatalog(d dialect, catalogName string) dialect {
	d.tableName = func(name string, _ int64) string { return catalogName + "." + name }

	return d
}

// SplitChecksums reads the current state of a table in the split warehouse
// with iceberg-go, DuckDB and Spark.
func (l *Lake) SplitChecksums(t testing.TB, tbl *table.Table) Checksums {
	t.Helper()
	ctx := context.Background()
	name := QualifiedName(tbl)
	sc := tbl.Schema()

	records, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err, "iceberg-go scan")
	defer records.Release()
	goSum, err := ChecksumOfRecords(sc, records)
	require.NoError(t, err)

	duckRow, err := l.Duck.Row(ctx, inCatalog(duckDialect, "sp").checksumSQL(sc, name, 0))
	require.NoError(t, err, "duckdb checksum")
	sparkRow, err := l.Spark.Row(ctx, inCatalog(sparkDialect, SplitWarehouse).checksumSQL(sc, name, 0))
	require.NoError(t, err, "spark checksum")

	return Checksums{IcebergGo: goSum, DuckDB: joinChecksum(duckRow), Spark: joinChecksum(sparkRow)}
}
