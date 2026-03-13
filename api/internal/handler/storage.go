package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// StorageFeature is one row from features_daily for a storage-related feature.
type StorageFeature struct {
	Name           string    `json:"name"`
	Value          *float64  `json:"value"`
	Interpretation string    `json:"interpretation"`
	Confidence     string    `json:"confidence"`
	ComputedAt     time.Time `json:"computed_at"`
}

// StorageBand holds the EIA official 5-year avg/max/min for the current week.
type StorageBand struct {
	Avg *float64 `json:"avg_bcf"`
	Max *float64 `json:"max_bcf"`
	Min *float64 `json:"min_bcf"`
	// WeekEnding is the observation date of the matching band row.
	WeekEnding string `json:"week_ending"`
}

// StorageHistoryPoint is one weekly observation of total US working gas.
type StorageHistoryPoint struct {
	WeekEnding string   `json:"week_ending"`
	TotalBcf   *float64 `json:"total_bcf"`
	Avg5YrBcf  *float64 `json:"avg_5yr_bcf,omitempty"`
	Max5YrBcf  *float64 `json:"max_5yr_bcf,omitempty"`
	Min5YrBcf  *float64 `json:"min_5yr_bcf,omitempty"`
}

// StorageConsensus holds the market consensus estimate for the next EIA report.
// Populated from the consensus_inputs table (manually entered or scraped).
type StorageConsensus struct {
	ReportDate    string   `json:"report_date"`
	Low           *float64 `json:"low_bcf"`
	Consensus     *float64 `json:"consensus_bcf"`
	High          *float64 `json:"high_bcf"`
	ModelEstimate *float64 `json:"model_estimate_bcf"`
	Source        *string  `json:"source"`
}

// StorageSurprisePoint is one week's actual vs consensus comparison.
type StorageSurprisePoint struct {
	ReportDate string   `json:"report_date"`
	Actual     *float64 `json:"actual_bcf"`
	Consensus  *float64 `json:"consensus_bcf"`
	Surprise   *float64 `json:"surprise_bcf"`
}

// StorageResponse is the JSON body returned by GET /api/storage.
type StorageResponse struct {
	Features         []StorageFeature       `json:"features"`
	Band             *StorageBand           `json:"five_year_band"`
	LatestWeekEnding *string                `json:"latest_week_ending"`
	// Consensus is the current week's market consensus estimate (nil if not yet entered).
	Consensus        *StorageConsensus      `json:"consensus"`
	// SurpriseHistory is up to 52 weeks of actual vs consensus, newest first.
	SurpriseHistory  []StorageSurprisePoint `json:"surprise_history"`
	// History is up to 52 weeks of total storage with 5-year band overlay, newest first.
	History          []StorageHistoryPoint  `json:"history"`
}

var storageFeatureNames = []string{
	"storage_total_bcf",
	"storage_wow_change_bcf",
	"storage_deficit_vs_5yr_bcf",
	"storage_deficit_vs_py_bcf",
	"storage_eos_projection_bcf",
	"storage_eos_deficit_vs_norm_bcf",
	"storage_weeks_remaining",
	"storage_avg_weekly_pace_bcf",
}

// Storage handles GET /api/storage.
// Returns today's computed storage features and the EIA 5-year band values.
func (h *Handler) Storage(w http.ResponseWriter, r *http.Request) {
	db, err := h.openDB()
	if err != nil {
		slog.Error("db open failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer db.Close()

	args := make([]any, len(storageFeatureNames))
	for i, n := range storageFeatureNames {
		args[i] = n
	}

	rows, err := db.QueryContext(r.Context(), `
		SELECT feature_name, value, interpretation, confidence, computed_at
		FROM features_daily
		WHERE feature_name IN (`+inClause(len(storageFeatureNames))+`)
		  AND region = 'US'
		ORDER BY feature_date DESC, feature_name
	`, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer rows.Close()

	var features []StorageFeature
	seen := make(map[string]bool)
	for rows.Next() {
		var name, interp, conf string
		var val sql.NullFloat64
		var computedAt time.Time
		if err := rows.Scan(&name, &val, &interp, &conf, &computedAt); err != nil {
			slog.Warn("storage feature scan failed", "err", err)
			continue
		}
		if seen[name] {
			continue // keep only the most recent row per feature name
		}
		seen[name] = true
		features = append(features, StorageFeature{
			Name:           name,
			Value:          nullFloat64(val),
			Interpretation: interp,
			Confidence:     conf,
			ComputedAt:     computedAt,
		})
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	history, err := h.queryStorageHistory(r, db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	surpHist, err := h.querySurpriseHistory(r, db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	writeJSON(w, http.StatusOK, StorageResponse{
		Features:         features,
		Band:             h.queryStorageBand(r, db),
		LatestWeekEnding: h.queryLatestStorageWeek(r, db),
		Consensus:        h.queryStorageConsensus(r, db),
		SurpriseHistory:  surpHist,
		History:          history,
	})
}

func (h *Handler) queryStorageBand(r *http.Request, db *sql.DB) *StorageBand {
	rows, err := db.QueryContext(r.Context(), `
		SELECT series_name, value, observation_time::TIMESTAMP::DATE::VARCHAR
		FROM facts_time_series
		WHERE source_name = 'eia_storage_stats'
		  AND series_name IN (
		        'storage_5yr_avg_total',
		        'storage_5yr_max_total',
		        'storage_5yr_min_total'
		      )
		ORDER BY observation_time DESC
		LIMIT 3
	`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	band := &StorageBand{}
	for rows.Next() {
		var series, week string
		var val float64
		if err := rows.Scan(&series, &val, &week); err != nil {
			slog.Warn("storage band scan failed", "err", err)
			continue
		}
		band.WeekEnding = week
		v := val
		switch series {
		case "storage_5yr_avg_total":
			band.Avg = &v
		case "storage_5yr_max_total":
			band.Max = &v
		case "storage_5yr_min_total":
			band.Min = &v
		}
	}
	if err := rows.Err(); err != nil {
		slog.Error("storage band iteration failed", "err", err)
		return nil
	}
	if band.Avg == nil && band.Max == nil && band.Min == nil {
		return nil
	}
	return band
}

func (h *Handler) queryLatestStorageWeek(r *http.Request, db *sql.DB) *string {
	row := db.QueryRowContext(r.Context(), `
		SELECT observation_time::TIMESTAMP::DATE::VARCHAR
		FROM facts_time_series
		WHERE source_name = 'eia_storage' AND series_name = 'storage_total'
		ORDER BY observation_time DESC
		LIMIT 1
	`)
	var week string
	if err := row.Scan(&week); err != nil {
		return nil
	}
	return &week
}

// queryStorageHistory returns up to 52 weeks of total US storage inventory
// joined with the EIA 5-year band (avg/max/min) for each matching week.
func (h *Handler) queryStorageHistory(r *http.Request, db *sql.DB) ([]StorageHistoryPoint, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT
		    s.observation_time::TIMESTAMP::DATE::VARCHAR AS week_ending,
		    s.value AS total_bcf,
		    MAX(CASE WHEN b.series_name = 'storage_5yr_avg_total' THEN b.value END) AS avg_5yr,
		    MAX(CASE WHEN b.series_name = 'storage_5yr_max_total' THEN b.value END) AS max_5yr,
		    MAX(CASE WHEN b.series_name = 'storage_5yr_min_total' THEN b.value END) AS min_5yr
		FROM facts_time_series s
		LEFT JOIN facts_time_series b
		    ON b.source_name = 'eia_storage_stats'
		   AND b.series_name IN ('storage_5yr_avg_total','storage_5yr_max_total','storage_5yr_min_total')
		   AND b.observation_time = s.observation_time
		WHERE s.source_name = 'eia_storage'
		  AND s.series_name = 'storage_total'
		GROUP BY s.observation_time, s.value
		ORDER BY s.observation_time DESC
		LIMIT 52
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []StorageHistoryPoint
	for rows.Next() {
		var week string
		var total, avg5, max5, min5 sql.NullFloat64
		if err := rows.Scan(&week, &total, &avg5, &max5, &min5); err != nil {
			slog.Warn("storage history scan failed", "err", err)
			continue
		}
		out = append(out, StorageHistoryPoint{
			WeekEnding: week,
			TotalBcf:   nullFloat64(total),
			Avg5YrBcf:  nullFloat64(avg5),
			Max5YrBcf:  nullFloat64(max5),
			Min5YrBcf:  nullFloat64(min5),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// queryStorageConsensus returns the most recent consensus estimate from
// consensus_inputs, joined with the model estimate from features_daily.
func (h *Handler) queryStorageConsensus(r *http.Request, db *sql.DB) *StorageConsensus {
	row := db.QueryRowContext(r.Context(), `
		SELECT
		    input_date::VARCHAR,
		    MAX(CASE WHEN input_type = 'eia_storage_consensus'     THEN value END),
		    MAX(CASE WHEN input_type = 'eia_storage_consensus_low' THEN value END),
		    MAX(CASE WHEN input_type = 'eia_storage_consensus_high' THEN value END),
		    MAX(source_note)
		FROM consensus_inputs
		WHERE input_type IN (
		        'eia_storage_consensus',
		        'eia_storage_consensus_low',
		        'eia_storage_consensus_high'
		      )
		GROUP BY input_date
		ORDER BY input_date DESC
		LIMIT 1
	`)
	var reportDate string
	var consensus, low, high sql.NullFloat64
	var source sql.NullString
	if err := row.Scan(&reportDate, &consensus, &low, &high, &source); err != nil {
		return nil
	}
	if !consensus.Valid {
		return nil
	}

	// Pull the model estimate from features_daily (written by features_storage.py).
	modelRow := db.QueryRowContext(r.Context(), `
		SELECT value FROM features_daily
		WHERE feature_name = 'storage_consensus_bcf' AND region = 'US'
		ORDER BY feature_date DESC LIMIT 1
	`)
	var model sql.NullFloat64
	_ = modelRow.Scan(&model)

	return &StorageConsensus{
		ReportDate:    reportDate,
		Consensus:     nullFloat64(consensus),
		Low:           nullFloat64(low),
		High:          nullFloat64(high),
		ModelEstimate: nullFloat64(model),
		Source:        nullString(source),
	}
}

// querySurpriseHistory returns up to 52 weeks of EIA storage surprise data
// (actual WoW change vs consensus) from features_daily, newest first.
func (h *Handler) querySurpriseHistory(r *http.Request, db *sql.DB) ([]StorageSurprisePoint, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT
		    feature_date::VARCHAR,
		    MAX(CASE WHEN feature_name = 'storage_wow_change_bcf'   THEN value END) AS actual,
		    MAX(CASE WHEN feature_name = 'storage_consensus_bcf'    THEN value END) AS consensus,
		    MAX(CASE WHEN feature_name = 'storage_eia_surprise_bcf' THEN value END) AS surprise
		FROM features_daily
		WHERE feature_name IN (
		        'storage_wow_change_bcf',
		        'storage_consensus_bcf',
		        'storage_eia_surprise_bcf'
		      )
		  AND region = 'US'
		GROUP BY feature_date
		HAVING MAX(CASE WHEN feature_name = 'storage_eia_surprise_bcf' THEN value END) IS NOT NULL
		ORDER BY feature_date DESC
		LIMIT 52
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []StorageSurprisePoint
	for rows.Next() {
		var date string
		var actual, consensus, surprise sql.NullFloat64
		if err := rows.Scan(&date, &actual, &consensus, &surprise); err != nil {
			slog.Warn("surprise history scan failed", "err", err)
			continue
		}
		out = append(out, StorageSurprisePoint{
			ReportDate: date,
			Actual:     nullFloat64(actual),
			Consensus:  nullFloat64(consensus),
			Surprise:   nullFloat64(surprise),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []StorageSurprisePoint{}
	}
	return out, nil
}

// inClause builds a comma-separated "?,?,?" placeholder string for SQL IN clauses.
func inClause(n int) string {
	if n == 0 {
		return ""
	}
	b := make([]byte, 0, n*2-1)
	for i := range n {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '?')
	}
	return string(b)
}
