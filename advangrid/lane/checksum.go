package lane

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// Checksum is the engine-neutral checksum of a table state: the row count,
// the exact sum of every integer and decimal column, and the sum of every
// timestamp column as epoch milliseconds. Other column types are not part of
// it. Timestamps before 1970 are not supported, because the engines round
// negative milliseconds differently.
type Checksum string

type column struct {
	name  string
	kind  columnKind
	scale int
}

type columnKind int

const (
	integerColumn columnKind = iota
	decimalColumn
	timestampColumn
)

func checksumColumns(sc *iceberg.Schema) []column {
	var cols []column
	for _, f := range sc.Fields() {
		switch t := f.Type.(type) {
		case iceberg.Int32Type, iceberg.Int64Type:
			cols = append(cols, column{name: f.Name, kind: integerColumn})
		case iceberg.DecimalType:
			cols = append(cols, column{name: f.Name, kind: decimalColumn, scale: t.Scale()})
		case iceberg.TimestampType, iceberg.TimestampTzType:
			cols = append(cols, column{name: f.Name, kind: timestampColumn})
		}
	}

	return cols
}

type dialect struct {
	quote     func(string) string
	str       string
	epochMs   func(string) string
	tableName func(name string, snapshotID int64) string
}

var duckDialect = dialect{
	quote:   func(c string) string { return `"` + c + `"` },
	str:     "VARCHAR",
	epochMs: func(c string) string { return "(epoch_us(" + c + ") // 1000)" },
	tableName: func(name string, snapshotID int64) string {
		if snapshotID == 0 {
			return "lk." + name
		}

		return fmt.Sprintf("lk.%s AT (VERSION => %d)", name, snapshotID)
	},
}

var sparkDialect = dialect{
	quote:   func(c string) string { return "`" + c + "`" },
	str:     "STRING",
	epochMs: func(c string) string { return "unix_millis(CAST(" + c + " AS TIMESTAMP))" },
	tableName: func(name string, snapshotID int64) string {
		if snapshotID == 0 {
			return "lake." + name
		}

		return fmt.Sprintf("lake.%s VERSION AS OF %d", name, snapshotID)
	},
}

func (d dialect) checksumSQL(sc *iceberg.Schema, name string, snapshotID int64) string {
	parts := []string{fmt.Sprintf("CAST(count(*) AS %s)", d.str)}
	for _, c := range checksumColumns(sc) {
		value := d.quote(c.name)
		if c.kind == timestampColumn {
			value = d.epochMs(value)
		}
		parts = append(parts, fmt.Sprintf("CAST(coalesce(sum(CAST(%[1]s AS DECIMAL(38,%[2]d))), CAST(0 AS DECIMAL(38,%[2]d))) AS %[3]s)",
			value, c.scale, d.str))
	}

	return fmt.Sprintf("SELECT %s FROM %s", strings.Join(parts, ", "), d.tableName(name, snapshotID))
}

func joinChecksum(values []*string) Checksum {
	parts := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			parts[i] = "null"
		} else {
			parts[i] = *v
		}
	}

	return Checksum(strings.Join(parts, "|"))
}

// checksumAccumulator sums the checksum columns batch by batch.
type checksumAccumulator struct {
	cols []column
	sums []*big.Int
	rows int64
}

func newChecksumAccumulator(sc *iceberg.Schema) *checksumAccumulator {
	cols := checksumColumns(sc)
	sums := make([]*big.Int, len(cols))
	for i := range sums {
		sums[i] = new(big.Int)
	}

	return &checksumAccumulator{cols: cols, sums: sums}
}

func (a *checksumAccumulator) add(rec arrow.RecordBatch) error {
	for i, c := range a.cols {
		idx := rec.Schema().FieldIndices(c.name)
		if len(idx) != 1 {
			return fmt.Errorf("checksumAccumulator.add - column %s not found", c.name)
		}
		if err := addColumn(a.sums[i], rec.Column(idx[0])); err != nil {
			return fmt.Errorf("checksumAccumulator.add - %s: %w", c.name, err)
		}
	}
	a.rows += rec.NumRows()

	return nil
}

func (a *checksumAccumulator) checksum() Checksum {
	parts := []string{fmt.Sprint(a.rows)}
	for i, c := range a.cols {
		parts = append(parts, formatScaled(a.sums[i], c.scale))
	}

	return Checksum(strings.Join(parts, "|"))
}

// ChecksumOfRecords computes the checksum of Arrow data with the table's
// schema, the way the engines compute it in SQL.
func ChecksumOfRecords(sc *iceberg.Schema, tbl arrow.Table) (Checksum, error) {
	acc := newChecksumAccumulator(sc)
	tr := array.NewTableReader(tbl, -1)
	defer tr.Release()
	for tr.Next() {
		if err := acc.add(tr.RecordBatch()); err != nil {
			return "", err
		}
	}

	return acc.checksum(), nil
}

// ChecksumOfScan streams the scan's batches into the checksum, so the table
// never has to fit in memory.
func ChecksumOfScan(ctx context.Context, sc *iceberg.Schema, scan *table.Scan) (Checksum, error) {
	_, records, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return "", fmt.Errorf("ChecksumOfScan - scan: %w", err)
	}
	acc := newChecksumAccumulator(sc)
	for rec, err := range records {
		if err != nil {
			return "", fmt.Errorf("ChecksumOfScan - read: %w", err)
		}
		err = acc.add(rec)
		rec.Release()
		if err != nil {
			return "", err
		}
	}

	return acc.checksum(), nil
}

func addColumn(sum *big.Int, arr arrow.Array) error {
	for j := 0; j < arr.Len(); j++ {
		if arr.IsNull(j) {
			continue
		}
		switch a := arr.(type) {
		case *array.Int32:
			sum.Add(sum, big.NewInt(int64(a.Value(j))))
		case *array.Int64:
			sum.Add(sum, big.NewInt(a.Value(j)))
		case *array.Decimal128:
			sum.Add(sum, a.Value(j).BigInt())
		case *array.Timestamp:
			unit := a.DataType().(*arrow.TimestampType).Unit
			sum.Add(sum, big.NewInt(a.Value(j).ToTime(unit).UnixMilli()))
		default:
			return fmt.Errorf("addColumn - unsupported %s", arr.DataType())
		}
	}

	return nil
}

func formatScaled(v *big.Int, scale int) string {
	if scale == 0 {
		return v.String()
	}
	neg := v.Sign() < 0
	digits := new(big.Int).Abs(v).String()
	for len(digits) <= scale {
		digits = "0" + digits
	}
	out := digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	if neg {
		out = "-" + out
	}

	return out
}

// Checksums reads a table state with iceberg-go, DuckDB and Spark. A
// snapshotID of 0 reads the current snapshot.
type Checksums struct {
	IcebergGo Checksum
	DuckDB    Checksum
	Spark     Checksum
}

func (l *Lake) Checksums(t testing.TB, tbl *table.Table, snapshotID int64) Checksums {
	t.Helper()
	ctx := context.Background()
	name := QualifiedName(tbl)
	sc := tbl.Schema()

	var opts []table.ScanOption
	if snapshotID != 0 {
		opts = append(opts, table.WithSnapshotID(snapshotID))
	}
	goSum, err := ChecksumOfScan(ctx, sc, tbl.Scan(opts...))
	require.NoError(t, err, "iceberg-go scan")

	duckRow, err := l.Duck.Row(ctx, duckDialect.checksumSQL(sc, name, snapshotID))
	require.NoError(t, err, "duckdb checksum")
	sparkRow, err := l.Spark.Row(ctx, sparkDialect.checksumSQL(sc, name, snapshotID))
	require.NoError(t, err, "spark checksum")

	return Checksums{IcebergGo: goSum, DuckDB: joinChecksum(duckRow), Spark: joinChecksum(sparkRow)}
}

// RequireChecksum fails the test unless all three engines read want.
func (l *Lake) RequireChecksum(t testing.TB, tbl *table.Table, snapshotID int64, want Checksum) {
	t.Helper()
	got := l.Checksums(t, tbl, snapshotID)
	require.Equal(t, Checksums{IcebergGo: want, DuckDB: want, Spark: want}, got,
		"table %s snapshot %d", QualifiedName(tbl), snapshotID)
}
