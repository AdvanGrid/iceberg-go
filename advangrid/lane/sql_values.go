package lane

import (
	"fmt"
	"math/big"
	"strings"
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
