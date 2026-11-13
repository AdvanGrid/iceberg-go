package lane

import (
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

// Row is one row of the lane's test tables. Amount is in thousandths.
type Row struct {
	K      int64
	G      int32
	T      time.Time
	V      int64
	Amount int64
}

// Schema is the test table schema: k long, g int, t timestamp, v long,
// amount decimal(12,3).
var Schema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "g", Type: iceberg.PrimitiveTypes.Int32},
	iceberg.NestedField{ID: 3, Name: "t", Type: iceberg.PrimitiveTypes.Timestamp},
	iceberg.NestedField{ID: 4, Name: "v", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 5, Name: "amount", Type: iceberg.DecimalTypeOf(12, 3)},
)

var ArrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "g", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "t", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
	{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "amount", Type: &arrow.Decimal128Type{Precision: 12, Scale: 3}, Nullable: true},
}, nil)

// Batch builds one Arrow record batch from rows. The caller releases it.
func Batch(rows []Row) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, ArrowSchema)
	defer b.Release()
	for _, r := range rows {
		b.Field(0).(*array.Int64Builder).Append(r.K)
		b.Field(1).(*array.Int32Builder).Append(r.G)
		b.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(r.T.UnixMicro()))
		b.Field(3).(*array.Int64Builder).Append(r.V)
		b.Field(4).(*array.Decimal128Builder).Append(decimal128.FromI64(r.Amount))
	}

	return b.NewRecordBatch()
}

// Reader wraps rows as a record reader of batches of at most batchRows rows.
// The caller releases it.
func Reader(rows []Row, batchRows int) array.RecordReader {
	batches := []arrow.RecordBatch{}
	for start := 0; start < len(rows); start += batchRows {
		batches = append(batches, Batch(rows[start:min(start+batchRows, len(rows))]))
	}
	if len(batches) == 0 {
		batches = append(batches, Batch(nil))
	}
	rdr, err := array.NewRecordReader(ArrowSchema, batches)
	if err != nil {
		panic(err)
	}
	for _, b := range batches {
		b.Release()
	}

	return rdr
}

// Model is the expected content of a table, keyed by k.
type Model map[int64]Row

func (m Model) Put(rows []Row) {
	for _, r := range rows {
		m[r.K] = r
	}
}

func (m Model) DeleteWhere(match func(Row) bool) {
	for k, r := range m {
		if match(r) {
			delete(m, k)
		}
	}
}

func (m Model) Clone() Model {
	out := make(Model, len(m))
	for k, r := range m {
		out[k] = r
	}

	return out
}

func (m Model) Keys() []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	return keys
}

// Checksum is the model's checksum in the column order of Schema.
func (m Model) Checksum() Checksum {
	var k, g, t, v, amount big.Int
	for _, r := range m {
		k.Add(&k, big.NewInt(r.K))
		g.Add(&g, big.NewInt(int64(r.G)))
		t.Add(&t, big.NewInt(r.T.UnixMilli()))
		v.Add(&v, big.NewInt(r.V))
		amount.Add(&amount, big.NewInt(r.Amount))
	}
	parts := []string{fmt.Sprint(len(m)), k.String(), g.String(), t.String(), v.String(), formatScaled(&amount, 3)}

	return Checksum(strings.Join(parts, "|"))
}
