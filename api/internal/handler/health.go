package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// CollectorStatus is one row from the collector_health table.
type CollectorStatus struct {
	SourceName          string     `json:"source_name"`
	LastAttempt         time.Time  `json:"last_attempt"`
	LastSuccess         *time.Time `json:"last_success"`
	LastStatus          string     `json:"last_status"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	ErrorMessage        *string    `json:"error_message"`
}

// HealthResponse is the JSON body returned by GET /api/health.
type HealthResponse struct {
	// DBOk is true when the database connection responds to a ping.
	DBOk       bool              `json:"db_ok"`
	Collectors []CollectorStatus `json:"collectors"`
	// ServerTime is the current UTC time on the API server.
	ServerTime time.Time `json:"server_time"`
}

// Health handles GET /api/health.
// Returns database connectivity status and last-known status for every
// registered collector.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	db := h.DB
	dbOk := db.PingContext(r.Context()) == nil

	if !dbOk {
		writeJSON(w, http.StatusServiceUnavailable, HealthResponse{
			DBOk:       false,
			Collectors: []CollectorStatus{},
			ServerTime: time.Now().UTC(),
		})
		return
	}

	rows, err := db.QueryContext(r.Context(), `
		SELECT
		    source_name,
		    last_attempt,
		    last_success,
		    last_status,
		    consecutive_failures,
		    error_message
		FROM collector_health
		ORDER BY source_name
	`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer rows.Close()

	var collectors []CollectorStatus
	for rows.Next() {
		var c CollectorStatus
		var lastSuccess sql.NullTime
		var errMsg sql.NullString
		if err := rows.Scan(
			&c.SourceName,
			&c.LastAttempt,
			&lastSuccess,
			&c.LastStatus,
			&c.ConsecutiveFailures,
			&errMsg,
		); err != nil {
			slog.Warn("health scan failed", "err", err)
			continue
		}
		if lastSuccess.Valid {
			t := lastSuccess.Time
			c.LastSuccess = &t
		}
		c.ErrorMessage = nullString(errMsg)
		collectors = append(collectors, c)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	status := http.StatusOK
	if !dbOk {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, HealthResponse{
		DBOk:       dbOk,
		Collectors: collectors,
		ServerTime: time.Now().UTC(),
	})
}
