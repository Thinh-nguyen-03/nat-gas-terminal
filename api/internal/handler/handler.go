// Package handler contains all HTTP handlers for the terminal API.
//
// Each handler file owns one endpoint group. All handlers share the Handler
// struct which carries the database connection and SSE broker as dependencies.
package handler

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/nat-gas-terminal/api/internal/sse"
)

// Handler holds shared dependencies injected at startup.
type Handler struct {
	DB     *sql.DB
	Broker *sse.Broker
	// InternalKey is the pre-shared key required on POST /internal/notify.
	// An empty string disables the check (development only).
	InternalKey string
}

// writeJSON serialises v as JSON and writes it with the given status code.
// On marshalling failure it returns 500 and logs the error.
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		slog.Error("json marshal failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(b)
}

// writeError writes a JSON error body: {"error": "message"}.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// nullFloat64 returns nil if the sql.NullFloat64 is not valid, otherwise the
// float value. Used when building JSON responses that must omit missing data
// instead of returning 0.
func nullFloat64(n sql.NullFloat64) *float64 {
	if !n.Valid {
		return nil
	}
	v := n.Float64
	return &v
}

// nullString returns nil for invalid sql.NullString values.
func nullString(n sql.NullString) *string {
	if !n.Valid {
		return nil
	}
	v := n.String
	return &v
}
