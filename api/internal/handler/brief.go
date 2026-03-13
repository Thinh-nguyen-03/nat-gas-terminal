package handler

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// BriefContent is the parsed Gemini output stored in summary_outputs.
type BriefContent struct {
	Outlook     string   `json:"outlook"`
	Drivers     []string `json:"drivers"`
	Risk        string   `json:"risk"`
	Model       string   `json:"model"`
	GeneratedAt string   `json:"generated_at"`
}

// BriefResponse is the JSON body for GET /api/brief.
type BriefResponse struct {
	Date    string       `json:"date"`
	Content BriefContent `json:"content"`
	AsOf    string       `json:"as_of"`
}

// Brief handles GET /api/brief.
// Returns the most recent market_brief from summary_outputs.
func (h *Handler) Brief(w http.ResponseWriter, r *http.Request) {
	db, err := h.openDB()
	if err != nil {
		slog.Error("db open failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer db.Close()

	row := db.QueryRowContext(r.Context(), `
		SELECT summary_date::VARCHAR, content
		FROM summary_outputs
		WHERE summary_type = 'market_brief'
		ORDER BY summary_date DESC
		LIMIT 1
	`)

	var summaryDate, contentJSON string
	if err := row.Scan(&summaryDate, &contentJSON); err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "no brief available yet")
			return
		}
		slog.Error("brief query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	var content BriefContent
	if err := json.Unmarshal([]byte(contentJSON), &content); err != nil {
		slog.Error("brief json unmarshal failed", "err", err)
		writeError(w, http.StatusInternalServerError, "malformed brief data")
		return
	}

	if content.Drivers == nil {
		content.Drivers = []string{}
	}

	writeJSON(w, http.StatusOK, BriefResponse{
		Date:    summaryDate,
		Content: content,
		AsOf:    time.Now().UTC().Format(time.RFC3339),
	})
}
