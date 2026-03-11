package handler

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// AnalogFeatureCompare shows how one feature compares between the analog
// period and the current period.
type AnalogFeatureCompare struct {
	Feature      string   `json:"feature"`
	AnalogValue  *float64 `json:"analog_value"`
	CurrentValue *float64 `json:"current_value"`
	Matched      bool     `json:"matched"`
}

// AnalogPriceOutcome records what the front-month price did after the analog.
type AnalogPriceOutcome struct {
	Return4w  *float64 `json:"return_4w_pct"`
	Return8w  *float64 `json:"return_8w_pct"`
	Return12w *float64 `json:"return_12w_pct"`
}

// Analog is one historical analog period returned by the finder.
type Analog struct {
	Rank            int                    `json:"rank"`
	PeriodDate      string                 `json:"period_date"`
	SimilarityScore float64                `json:"similarity_score"`
	Label           string                 `json:"label"`
	Features        []AnalogFeatureCompare `json:"features"`
	PriceOutcome    AnalogPriceOutcome     `json:"price_outcome"`
}

// AnalogsResponse is the JSON body returned by GET /api/analogs.
type AnalogsResponse struct {
	// ComputedAt is when the analog finder last ran. Nil if no results yet.
	ComputedAt *string  `json:"computed_at"`
	Analogs    []Analog `json:"analogs"`
}

// Analogs handles GET /api/analogs.
// Returns the top historical analog periods from summary_outputs
// (written by transforms/features_analog.py, not yet implemented).
// Returns an empty list until the analog transform is running.
func (h *Handler) Analogs(w http.ResponseWriter, r *http.Request) {
	db := h.DB

	row := db.QueryRowContext(r.Context(), `
		SELECT content, generated_at::VARCHAR
		FROM summary_outputs
		WHERE summary_type = 'analog_finder'
		ORDER BY summary_date DESC
		LIMIT 1
	`)

	var contentJSON, generatedAt string
	err := row.Scan(&contentJSON, &generatedAt)
	if err == sql.ErrNoRows {
		// Transform not running yet — return empty response, not an error.
		writeJSON(w, http.StatusOK, AnalogsResponse{
			Analogs: []Analog{},
		})
		return
	}
	if err != nil {
		slog.Error("analogs query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	var analogs []Analog
	if err := json.Unmarshal([]byte(contentJSON), &analogs); err != nil {
		slog.Error("analogs unmarshal failed", "err", err)
		writeError(w, http.StatusInternalServerError, "malformed analog data")
		return
	}

	if analogs == nil {
		analogs = []Analog{}
	}

	writeJSON(w, http.StatusOK, AnalogsResponse{
		ComputedAt: &generatedAt,
		Analogs:    analogs,
	})
}

// AnalogsUpdatedAt returns the timestamp of the most recent analog computation,
// or the zero time if no results exist yet. Used by the SSE broker.
func (h *Handler) AnalogsUpdatedAt(r *http.Request) time.Time {
	db := h.DB

	row := db.QueryRowContext(r.Context(), `
		SELECT generated_at FROM summary_outputs
		WHERE summary_type = 'analog_finder'
		ORDER BY summary_date DESC LIMIT 1
	`)
	var t time.Time
	_ = row.Scan(&t)
	return t
}
