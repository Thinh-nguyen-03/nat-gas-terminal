package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// COTFeature is one row from features_daily for a COT-related feature.
type COTFeature struct {
	Name           string    `json:"name"`
	Value          *float64  `json:"value"`
	Interpretation string    `json:"interpretation"`
	ComputedAt     time.Time `json:"computed_at"`
}

// COTHistoryPoint is one weekly COT observation from facts_time_series.
type COTHistoryPoint struct {
	ReportDate string   `json:"report_date"`
	MMNet      *float64 `json:"mm_net"`
	MMNetPctOI *float64 `json:"mm_net_pct_oi"`
	OpenInt    *float64 `json:"open_interest"`
}

// COTResponse is the JSON body returned by GET /api/cot.
type COTResponse struct {
	Features []COTFeature `json:"features"`
	// History is up to 52 weeks of MM net and open interest.
	History []COTHistoryPoint `json:"history"`
}

var cotFeatureNames = []string{
	"cot_mm_net_contracts",
	"cot_mm_net_pct_oi",
	"cot_mm_net_wow",
	"cot_open_interest",
}

// COT handles GET /api/cot.
func (h *Handler) COT(w http.ResponseWriter, r *http.Request) {
	db := h.DB

	args := make([]any, len(cotFeatureNames))
	for i, n := range cotFeatureNames {
		args[i] = n
	}

	frows, err := db.QueryContext(r.Context(), `
		SELECT feature_name, value, interpretation, computed_at
		FROM features_daily
		WHERE feature_name IN (`+inClause(len(cotFeatureNames))+`)
		  AND region = 'US'
		ORDER BY feature_date DESC, feature_name
	`, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer frows.Close()

	var features []COTFeature
	seen := make(map[string]bool)
	for frows.Next() {
		var name, interp string
		var val sql.NullFloat64
		var computedAt time.Time
		if err := frows.Scan(&name, &val, &interp, &computedAt); err != nil {
			slog.Warn("cot feature scan failed", "err", err)
			continue
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		features = append(features, COTFeature{
			Name:           name,
			Value:          nullFloat64(val),
			Interpretation: interp,
			ComputedAt:     computedAt,
		})
	}
	if err := frows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	history, err := h.queryCOTHistory(r, db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	writeJSON(w, http.StatusOK, COTResponse{
		Features: features,
		History:  history,
	})
}

func (h *Handler) queryCOTHistory(r *http.Request, db *sql.DB) ([]COTHistoryPoint, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT
		    observation_time::TIMESTAMP::DATE::VARCHAR,
		    MAX(CASE WHEN series_name = 'cot_mm_long'       THEN value END)
		  - MAX(CASE WHEN series_name = 'cot_mm_short'      THEN value END),
		    MAX(CASE WHEN series_name = 'cot_open_interest' THEN value END)
		FROM facts_time_series
		WHERE source_name = 'cftc'
		  AND series_name IN ('cot_mm_long','cot_mm_short','cot_open_interest')
		GROUP BY observation_time::TIMESTAMP::DATE::VARCHAR
		ORDER BY observation_time::TIMESTAMP::DATE::VARCHAR DESC
		LIMIT 52
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []COTHistoryPoint
	for rows.Next() {
		var date string
		var mmNet, openInt sql.NullFloat64
		if err := rows.Scan(&date, &mmNet, &openInt); err != nil {
			slog.Warn("cot history scan failed", "err", err)
			continue
		}
		// Compute mm_net_pct_oi inline when open interest is available.
		var pctPtr *float64
		if mmNet.Valid && openInt.Valid && openInt.Float64 != 0 {
			v := mmNet.Float64 / openInt.Float64 * 100
			pctPtr = &v
		}
		out = append(out, COTHistoryPoint{
			ReportDate: date,
			MMNet:      nullFloat64(mmNet),
			MMNetPctOI: pctPtr,
			OpenInt:    nullFloat64(openInt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
