package cases

import (
	"context"
	"testing"

	"github.com/AdvanGrid/iceberg-go/advangrid/lane"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func dayFilter(day int) iceberg.BooleanExpression {
	from, to := lane.Day(day)

	return iceberg.NewAnd(
		iceberg.GreaterThanEqual(iceberg.Reference("t"), iceberg.Timestamp(from.UnixMicro())),
		iceberg.LessThan(iceberg.Reference("t"), iceberg.Timestamp(to.UnixMicro())))
}

func inDay(day int) func(lane.Row) bool {
	from, to := lane.Day(day)

	return func(r lane.Row) bool { return !r.T.Before(from) && r.T.Before(to) }
}

// TestOperationsAgree runs append, overwrite by filter and delete, and checks
// after each that iceberg-go, DuckDB and Spark read the model.
func TestOperationsAgree(t *testing.T) {
	for _, mode := range lane.DeleteModes {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			lk := lane.Open(t)
			tbl := lane.SetDeleteMode(t, lk.CreateTable(t, lane.Schema, monthPartitioned()), mode)
			gen := lane.NewGen(1)
			model := lane.Model{}

			for day := range 3 {
				from, to := lane.Day(day)
				rows := gen.Rows(1000, from, to)
				rdr := lane.Reader(rows, 256)
				var err error
				tbl, err = tbl.Append(ctx, rdr, nil)
				rdr.Release()
				require.NoError(t, err)
				model.Put(rows)
				lk.RequireChecksum(t, tbl, 0, model.Checksum())
			}

			from, to := lane.Day(1)
			rows := gen.Rows(100, from, to)
			rdr := lane.Reader(rows, 256)
			tbl, err := tbl.Overwrite(ctx, rdr, nil, table.WithOverwriteFilter(dayFilter(1)))
			rdr.Release()
			require.NoError(t, err)
			model.DeleteWhere(inDay(1))
			model.Put(rows)
			lk.RequireChecksum(t, tbl, 0, model.Checksum())

			tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("g"), int32(3)), nil)
			require.NoError(t, err)
			model.DeleteWhere(func(r lane.Row) bool { return r.G == 3 })
			lk.RequireChecksum(t, tbl, 0, model.Checksum())
		})
	}
}
