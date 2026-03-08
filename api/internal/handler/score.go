package handler

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// ScoreHistoryPoint is one daily score observation for charting.
type ScoreHistoryPoint struct {
	Date  string  `json:"date"`
	Score float64 `json:"score"`
	Label string  `json:"label"`
}

// ScoreResponse is the JSON body returned by GET /api/score.
type ScoreResponse struct {
	// SummaryDate is the date the score was computed for (YYYY-MM-DD).
	SummaryDate string `json:"summary_date"`
	// Score is the composite fundamental score in the range [-100, +100].
	Score float64 `json:"score"`
	// Label is the human-readable interpretation (e.g. "Mildly Bullish").
	Label string `json:"label"`
	// Drivers is the ordered list of up to four key driver bullets.
	Drivers []string `json:"drivers"`
	// WhatChanged is the ordered list of feature deltas vs the prior day.
	WhatChanged []map[string]any `json:"what_changed"`
	// GeneratedAt is the UTC timestamp when the summary was last computed.
	GeneratedAt time.Time `json:"generated_at"`
	// History is up to 90 days of composite scores, newest first.
	History []ScoreHistoryPoint `json:"history"`
}

// Score handles GET /api/score.
// It returns the most recent fundamental score and what-changed table from
// summary_outputs, which the Python transform layer writes after each run.
func (h *Handler) Score(w http.ResponseWriter, r *http.Request) {
	row := h.DB.QueryRowContext(r.Context(), `
		SELECT summary_date, content, generated_at
		FROM summary_outputs
		WHERE summary_type = 'fundamental_score'
		ORDER BY summary_date DESC
		LIMIT 1
	`)

	var (
		summaryDate string
		contentJSON string
		generatedAt time.Time
	)
	if err := row.Scan(&summaryDate, &contentJSON, &generatedAt); err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "no score available yet")
			return
		}
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	var scoreData struct {
		Score   float64  `json:"score"`
		Label   string   `json:"label"`
		Drivers []string `json:"drivers"`
	}
	if err := json.Unmarshal([]byte(contentJSON), &scoreData); err != nil {
		writeError(w, http.StatusInternalServerError, "malformed score data")
		return
	}

	// Fetch what_changed for the same date.
	var whatChanged []map[string]any
	wcRow := h.DB.QueryRowContext(r.Context(), `
		SELECT content
		FROM summary_outputs
		WHERE summary_type = 'what_changed'
		  AND summary_date = ?
		LIMIT 1
	`, summaryDate)
	var wcJSON string
	if err := wcRow.Scan(&wcJSON); err == nil {
		if err := json.Unmarshal([]byte(wcJSON), &whatChanged); err != nil {
			slog.Warn("what_changed unmarshal failed", "err", err)
		}
	}

	history, err := h.queryScoreHistory(r)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	writeJSON(w, http.StatusOK, ScoreResponse{
		SummaryDate: summaryDate,
		Score:       scoreData.Score,
		Label:       scoreData.Label,
		Drivers:     scoreData.Drivers,
		WhatChanged: whatChanged,
		GeneratedAt: generatedAt,
		History:     history,
	})
}

// queryScoreHistory returns up to 90 days of composite fundamental scores
// from summary_outputs, newest first.
func (h *Handler) queryScoreHistory(r *http.Request) ([]ScoreHistoryPoint, error) {
	rows, err := h.DB.QueryContext(r.Context(), `
		SELECT summary_date, content
		FROM summary_outputs
		WHERE summary_type = 'fundamental_score'
		ORDER BY summary_date DESC
		LIMIT 90
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ScoreHistoryPoint
	for rows.Next() {
		var date, contentJSON string
		if err := rows.Scan(&date, &contentJSON); err != nil {
			slog.Warn("score history scan failed", "err", err)
			continue
		}
		var parsed struct {
			Score float64 `json:"score"`
			Label string  `json:"label"`
		}
		if err := json.Unmarshal([]byte(contentJSON), &parsed); err != nil {
			slog.Warn("score history unmarshal failed", "date", date, "err", err)
			continue
		}
		out = append(out, ScoreHistoryPoint{
			Date:  date,
			Score: parsed.Score,
			Label: parsed.Label,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
