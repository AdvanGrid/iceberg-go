package lane

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/apache/iceberg-go/table"
)

// SQLValues renders rows as a SQL VALUES list in the column order of Schema,
// for engines to insert the same rows the model holds.
func SQLValues(rows []Row) string {
	values := make([]string, len(rows))
	for i, r := range rows {
		values[i] = fmt.Sprintf("(%d, %d, make_timestamp(%d), %d, CAST('%s' AS DECIMAL(12,3)))",
			r.K, r.G, r.T.UnixMicro(), r.V, formatScaled(big.NewInt(r.Amount), 3))
	}

	return "VALUES " + strings.Join(values, ", ")
}

// QualifiedName is the table's namespace-qualified name as engines attached to
// the lane's catalog address it.
func QualifiedName(tbl *table.Table) string { return tableName(tbl) }
