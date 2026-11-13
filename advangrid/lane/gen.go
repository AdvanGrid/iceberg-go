package lane

import (
	"math/rand/v2"
	"time"
)

// Epoch is the first day of the lane's generated data.
var Epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// Gen generates rows with unique, increasing keys from a seed.
type Gen struct {
	Rand  *rand.Rand
	nextK int64
}

func NewGen(seed uint64) *Gen {
	return &Gen{Rand: rand.New(rand.NewPCG(seed, 2628)), nextK: 1}
}

// Rows returns n rows with t in [from, to) and g in [0, 10).
func (g *Gen) Rows(n int, from, to time.Time) []Row {
	rows := make([]Row, n)
	span := to.Sub(from).Microseconds()
	for i := range rows {
		rows[i] = Row{
			K:      g.nextK,
			G:      int32(g.Rand.IntN(10)),
			T:      from.Add(time.Duration(g.Rand.Int64N(span)) * time.Microsecond),
			V:      g.Rand.Int64N(1_000_000),
			Amount: g.Rand.Int64N(10_000_000) - 5_000_000,
		}
		g.nextK++
	}

	return rows
}

// Day returns the UTC day d days after Epoch and the day after it.
func Day(d int) (time.Time, time.Time) {
	from := Epoch.AddDate(0, 0, d)

	return from, from.AddDate(0, 0, 1)
}
