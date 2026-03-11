// Package db provides a DuckDB connection for the API server.
//
// DuckDB uses exclusive/shared file locks across processes. A READ_ONLY
// connection holds a shared lock that still conflicts with the Python
// scheduler's READ_WRITE (exclusive) lock. To allow Python to write,
// Go must not hold the lock between requests.
//
// Setting SetMaxIdleConns(0) causes sql.DB to close the underlying
// DuckDB connection immediately after each query completes, releasing
// the file lock. Python can write during the idle windows between Go
// requests. On the next Go query, a fresh connection is opened.
package db

import (
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb"
)

// Open returns a read-only sql.DB connected to the DuckDB file at path.
// Connections are not pooled — each query opens and closes the file,
// releasing the lock so the Python scheduler can write between requests.
func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", path+"?access_mode=READ_ONLY")
	if err != nil {
		return nil, fmt.Errorf("open duckdb %q: %w", path, err)
	}
	// Open at most one connection at a time (DuckDB is single-threaded per file).
	db.SetMaxOpenConns(1)
	// Do not keep idle connections — release the file lock between queries
	// so the Python scheduler can acquire a write lock to update data.
	db.SetMaxIdleConns(0)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb %q: %w", path, err)
	}
	return db, nil
}
