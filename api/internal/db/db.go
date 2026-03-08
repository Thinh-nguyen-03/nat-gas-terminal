// Package db provides a read-only DuckDB connection for the API server.
//
// DuckDB supports multiple concurrent readers on the same file when opened
// with access_mode=READ_ONLY. The Python scheduler is the sole writer and
// opens the file separately in read-write mode.
package db

import (
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb"
)

// Open returns a read-only sql.DB connected to the DuckDB file at path.
// The caller is responsible for calling Close when done.
func Open(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s?access_mode=READ_ONLY", path)
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open duckdb %q: %w", path, err)
	}
	// DuckDB embedded — a single connection is sufficient for read-only workload.
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb %q: %w", path, err)
	}
	return db, nil
}
