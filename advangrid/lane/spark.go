package lane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const oracleURL = "http://localhost:8899/sql"

// Spark runs SQL in the lane's Spark session, the Iceberg Java reference.
// Its catalog lake is the lake warehouse.
type Spark struct{}

type sparkResult struct {
	Columns []string    `json:"columns"`
	Rows    [][]*string `json:"rows"`
	Error   string      `json:"error"`
}

// Query runs one statement and returns its rows as strings.
func (s *Spark) Query(ctx context.Context, query string) ([][]*string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, oracleURL, strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("Spark.Query - request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Spark.Query - post: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Spark.Query - read: %w", err)
	}
	var res sparkResult
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("Spark.Query - decode %q: %w", body, err)
	}
	if res.Error != "" {
		return nil, fmt.Errorf("Spark.Query - %s: %s", query, res.Error)
	}

	return res.Rows, nil
}

// Row runs a query that returns exactly one row.
func (s *Spark) Row(ctx context.Context, query string) ([]*string, error) {
	rows, err := s.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("Spark.Row - %d rows for %s", len(rows), query)
	}

	return rows[0], nil
}
