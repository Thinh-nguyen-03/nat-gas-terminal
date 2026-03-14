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

// FairValuePoint is one daily observation for the fair value history chart.
type FairValuePoint struct {
	Date  string   `json:"date"`
	Mid   *float64 `json:"mid"`
	Low   *float64 `json:"low"`
	High  *float64 `json:"high"`
	Gap   *float64 `json:"gap"`
	Price *float64 `json:"price"`
}

// FairValueData holds the current fair value estimate and 90-day history.
type FairValueData struct {
	Mid            *float64         `json:"mid"`
	Low            *float64         `json:"low"`
	High           *float64         `json:"high"`
	Gap            *float64         `json:"gap"`
	Interpretation *string          `json:"interpretation"`
	Confidence     *string          `json:"confidence"`
	History        []FairValuePoint `json:"history"`
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
	// FairValue is the current fair value model estimate, nil if not yet computed.
	FairValue *FairValueData `json:"fair_value,omitempty"`
}

// Score handles GET /api/score.
// It returns the most recent fundamental score and what-changed table from
// summary_outputs, which the Python transform layer writes after each run.
func (h *Handler) Score(w http.ResponseWriter, r *http.Request) {
	db, err := h.openDB()
	if err != nil {
		slog.Error("db open failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer db.Close()

	row := db.QueryRowContext(r.Context(), `
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

	var whatChanged []map[string]any
	wcRow := db.QueryRowContext(r.Context(), `
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

	history, err := h.queryScoreHistory(r, db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	fairValue, err := h.queryFairValue(r, db)
	if err != nil {
		slog.Warn("fair value query failed", "err", err)
	}

	writeJSON(w, http.StatusOK, ScoreResponse{
		SummaryDate: summaryDate,
		Score:       scoreData.Score,
		Label:       scoreData.Label,
		Drivers:     scoreData.Drivers,
		WhatChanged: whatChanged,
		GeneratedAt: generatedAt,
		History:     history,
		FairValue:   fairValue,
	})
}

// queryFairValue reads the latest fair value model output from features_daily
// and 90 days of history joined with actual prices from facts_time_series.
func (h *Handler) queryFairValue(r *http.Request, db *sql.DB) (*FairValueData, error) {
	row := db.QueryRowContext(r.Context(), `
		SELECT
			MAX(CASE WHEN feature_name='fairvalue_mid'  THEN value END),
			MAX(CASE WHEN feature_name='fairvalue_low'  THEN value END),
			MAX(CASE WHEN feature_name='fairvalue_high' THEN value END),
			MAX(CASE WHEN feature_name='fairvalue_gap'  THEN value END),
			MAX(CASE WHEN feature_name='fairvalue_mid'  THEN interpretation END),
			MAX(CASE WHEN feature_name='fairvalue_mid'  THEN confidence END)
		FROM features_daily
		WHERE feature_name IN ('fairvalue_mid','fairvalue_low','fairvalue_high','fairvalue_gap')
		  AND region = 'US'
		  AND feature_date = (
		      SELECT MAX(feature_date) FROM features_daily
		      WHERE feature_name = 'fairvalue_mid' AND region = 'US'
		  )
	`)

	var (
		mid, low, high, gap sql.NullFloat64
		interp, confidence  sql.NullString
	)
	if err := row.Scan(&mid, &low, &high, &gap, &interp, &confidence); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if !mid.Valid {
		return nil, nil
	}

	rows, err := db.QueryContext(r.Context(), `
		WITH fv AS (
			SELECT feature_date,
				MAX(CASE WHEN feature_name='fairvalue_mid'  THEN value END) AS mid,
				MAX(CASE WHEN feature_name='fairvalue_low'  THEN value END) AS low,
				MAX(CASE WHEN feature_name='fairvalue_high' THEN value END) AS high,
				MAX(CASE WHEN feature_name='fairvalue_gap'  THEN value END) AS gap
			FROM features_daily
			WHERE feature_name IN ('fairvalue_mid','fairvalue_low','fairvalue_high','fairvalue_gap')
			  AND region = 'US'
			  AND feature_date >= CURRENT_DATE - INTERVAL '90 days'
			GROUP BY feature_date
		),
		fred_p AS (
			SELECT observation_time::TIMESTAMP::DATE AS d, value
			FROM facts_time_series
			WHERE source_name = 'fred' AND series_name = 'ng_spot_price'
		),
		yf_p AS (
			SELECT observation_time::TIMESTAMP::DATE AS d, value
			FROM facts_time_series
			WHERE source_name = 'yfinance' AND series_name = 'ng_front_close'
		)
		SELECT fv.feature_date::TEXT, fv.mid, fv.low, fv.high, fv.gap,
		       COALESCE(fp.value, yp.value) AS price
		FROM fv
		LEFT JOIN fred_p fp ON fp.d = fv.feature_date
		LEFT JOIN yf_p   yp ON yp.d = fv.feature_date
		ORDER BY fv.feature_date DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []FairValuePoint
	for rows.Next() {
		var (
			date                         string
			fvMid, fvLow, fvHigh, fvGap sql.NullFloat64
			price                        sql.NullFloat64
		)
		if err := rows.Scan(&date, &fvMid, &fvLow, &fvHigh, &fvGap, &price); err != nil {
			slog.Warn("fair value history scan failed", "err", err)
			continue
		}
		history = append(history, FairValuePoint{
			Date:  date,
			Mid:   nullFloat64(fvMid),
			Low:   nullFloat64(fvLow),
			High:  nullFloat64(fvHigh),
			Gap:   nullFloat64(fvGap),
			Price: nullFloat64(price),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &FairValueData{
		Mid:            nullFloat64(mid),
		Low:            nullFloat64(low),
		High:           nullFloat64(high),
		Gap:            nullFloat64(gap),
		Interpretation: nullString(interp),
		Confidence:     nullString(confidence),
		History:        history,
	}, nil
}

// queryScoreHistory returns up to 90 days of composite fundamental scores
// from summary_outputs, newest first.
func (h *Handler) queryScoreHistory(r *http.Request, db *sql.DB) ([]ScoreHistoryPoint, error) {
	rows, err := db.QueryContext(r.Context(), `
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
