// Package db provides per-request DuckDB connections for the API server.
//
// DuckDB holds an exclusive/shared file lock at the database-object level,
// not the connection level. Even with SetMaxIdleConns(0), go-duckdb v1.x
// keeps the database object alive as long as the sql.DB pool exists, so
// the file lock is never released between requests.
//
// The correct fix is to open a FRESH sql.DB (and thus a fresh duckdb_database
// C handle) per request and close it immediately when done. This guarantees
// the file lock is released after every handler, giving the Python scheduler
// a window to acquire a write lock and update the database.
package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// Open returns a fresh read-only *sql.DB for one request, retrying with
// exponential backoff when the file is locked by the Python scheduler.
// sql.Open with go-duckdb is lazy — the file lock is only acquired on the
// first Ping/Query, so we call Ping immediately to detect and retry lock
// conflicts here rather than letting them surface mid-handler.
// The caller MUST call db.Close() when done to release the file lock.
func Open(path string) (*sql.DB, error) {
	const maxAttempts = 10
	wait := 200 * time.Millisecond
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		db, err := sql.Open("duckdb", path+"?access_mode=READ_ONLY")
		if err != nil {
			return nil, fmt.Errorf("open duckdb %q: %w", path, err)
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(0)

		// Force the connection open now so lock errors surface immediately.
		if pingErr := db.Ping(); pingErr == nil {
			return db, nil
		} else {
			db.Close()
			if !strings.Contains(pingErr.Error(), "already open") || attempt == maxAttempts {
				return nil, fmt.Errorf("open duckdb %q: %w", path, pingErr)
			}
		}
		time.Sleep(wait)
		wait *= 2
	}
	return nil, fmt.Errorf("open duckdb %q: exhausted retries", path)
}
