package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// ISOPowerData holds the current LMP and stress signal for one ISO.
type ISOPowerData struct {
	ISO            string   `json:"iso"`
	Region         string   `json:"region"`
	HubNode        string   `json:"hub_node"`
	LMPUSDPerMWh   *float64 `json:"lmp_usd_mwh"`
	AvgLMP30d      *float64 `json:"avg_lmp_30d_usd_mwh"`
	ZScore         *float64 `json:"z_score"`
	Signal         string   `json:"signal"`
	UpdatedAt      *string  `json:"updated_at"`
}

// PowerDemandSummary holds the composite stress index across all ISOs.
type PowerDemandSummary struct {
	StressIndex    *float64 `json:"stress_index"`
	Interpretation string   `json:"interpretation"`
	UpdatedAt      *string  `json:"updated_at"`
}

// PowerHistoryPoint is one hourly composite stress index observation.
type PowerHistoryPoint struct {
	Timestamp   string   `json:"ts"`
	StressIndex *float64 `json:"stress_index"`
}

// PowerResponse is the JSON body returned by GET /api/power.
type PowerResponse struct {
	DataAvailable bool                `json:"data_available"`
	Summary       PowerDemandSummary  `json:"summary"`
	ISOs          []ISOPowerData      `json:"isos"`
	History       []PowerHistoryPoint `json:"history"`
}

// knownISOs is the static list of ISOs tracked for nat gas power demand signals.
var knownISOs = []ISOPowerData{
	{ISO: "PJM",    Region: "Mid-Atlantic / Midwest", HubNode: "PJMWH",        Signal: "unknown"},
	{ISO: "ISO-NE", Region: "New England",             HubNode: "Mass Hub 4001", Signal: "unknown"},
	{ISO: "NYISO",  Region: "New York",                HubNode: "Zone J (NYC)", Signal: "unknown"},
	{ISO: "MISO",   Region: "Midwest / South",         HubNode: "Illinois Hub",  Signal: "unknown"},
	{ISO: "ERCOT",  Region: "Texas",                   HubNode: "HB_NORTH",      Signal: "unknown"},
	{ISO: "CAISO",  Region: "California",              HubNode: "NP15",          Signal: "unknown"},
}

// Power handles GET /api/power.
// Returns real-time ISO LMP data and a composite power demand stress index.
// ISOs have signal="unknown" until iso_lmp collector data is available.
func (h *Handler) Power(w http.ResponseWriter, r *http.Request) {
	db, err := h.openDB()
	if err != nil {
		slog.Error("db open failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer db.Close()

	isos, hasLiveData, err := h.queryISOLMPs(r, db)
	if err != nil {
		slog.Error("power iso query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	summary := h.queryPowerSummary(r, db)

	history, err := h.queryPowerHistory(r, db)
	if err != nil {
		slog.Error("power history query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	writeJSON(w, http.StatusOK, PowerResponse{
		DataAvailable: hasLiveData,
		Summary:       summary,
		ISOs:          isos,
		History:       history,
	})
}

func (h *Handler) queryISOLMPs(r *http.Request, db *sql.DB) ([]ISOPowerData, bool, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT region, series_name, value, observation_time::VARCHAR
		FROM (
		    SELECT region, series_name, value, observation_time,
		           ROW_NUMBER() OVER (PARTITION BY region ORDER BY observation_time DESC) AS rn
		    FROM facts_time_series
		    WHERE source_name = 'iso_lmp'
		      AND series_name = 'lmp_hub'
		      AND observation_time >= NOW()::TIMESTAMP - INTERVAL '4 hours'
		) ranked
		WHERE rn = 1
	`)
	if err != nil {
		return knownISOs, false, err
	}
	defer rows.Close()

	type lmpRow struct {
		lmp       *float64
		updatedAt *string
	}
	lmpMap := make(map[string]lmpRow)

	for rows.Next() {
		var region, series, updAt string
		var val sql.NullFloat64
		if err := rows.Scan(&region, &series, &val, &updAt); err != nil {
			slog.Warn("iso lmp scan failed", "err", err)
			continue
		}
		lmpMap[region] = lmpRow{lmp: nullFloat64(val), updatedAt: &updAt}
	}
	if err := rows.Err(); err != nil {
		return knownISOs, false, err
	}

	hasLiveData := len(lmpMap) > 0

	isos := make([]ISOPowerData, len(knownISOs))
	copy(isos, knownISOs)

	for i, iso := range isos {
		d, ok := lmpMap[iso.ISO]
		if !ok {
			continue
		}
		isos[i].LMPUSDPerMWh = d.lmp
		isos[i].UpdatedAt = d.updatedAt

		// Z-score and stress signal come from features_daily once the
		// power demand transform (features_power_demand.py) is running.
		zRow := db.QueryRowContext(r.Context(), `
			SELECT value FROM features_daily
			WHERE feature_name = 'lmp_stress_score'
			  AND region = ?
			  AND feature_date >= CURRENT_DATE - INTERVAL 1 DAYS
			ORDER BY feature_date DESC, computed_at DESC
			LIMIT 1
		`, iso.ISO)
		var z sql.NullFloat64
		if err := zRow.Scan(&z); err == nil && z.Valid {
			isos[i].ZScore = nullFloat64(z)
			isos[i].Signal = classifyZScore(z.Float64)
		}
	}

	return isos, hasLiveData, nil
}

func (h *Handler) queryPowerSummary(r *http.Request, db *sql.DB) PowerDemandSummary {
	var s PowerDemandSummary
	row := db.QueryRowContext(r.Context(), `
		SELECT value, interpretation, computed_at::VARCHAR
		FROM features_daily
		WHERE feature_name = 'power_demand_stress_index'
		  AND region = 'US'
		ORDER BY feature_date DESC, computed_at DESC
		LIMIT 1
	`)
	var val sql.NullFloat64
	var interp, updAt sql.NullString
	if err := row.Scan(&val, &interp, &updAt); err == nil {
		s.StressIndex = nullFloat64(val)
		if interp.Valid {
			s.Interpretation = interp.String
		}
		s.UpdatedAt = nullString(updAt)
	}
	if s.StressIndex == nil {
		t := time.Now().UTC().Format(time.RFC3339)
		s.UpdatedAt = &t
		s.Interpretation = "no_data"
	}
	return s
}

func (h *Handler) queryPowerHistory(r *http.Request, db *sql.DB) ([]PowerHistoryPoint, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT ts::VARCHAR, value
		FROM features_intraday
		WHERE feature_name = 'power_demand_stress_index'
		  AND region = 'US'
		ORDER BY ts DESC
		LIMIT 72
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PowerHistoryPoint
	for rows.Next() {
		var ts string
		var val sql.NullFloat64
		if err := rows.Scan(&ts, &val); err != nil {
			slog.Warn("power history scan failed", "err", err)
			continue
		}
		out = append(out, PowerHistoryPoint{
			Timestamp:   ts,
			StressIndex: nullFloat64(val),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []PowerHistoryPoint{}
	}
	return out, nil
}

func classifyZScore(z float64) string {
	switch {
	case z > 2.0:
		return "high"
	case z > 0.5:
		return "elevated"
	case z < -1.0:
		return "suppressed"
	default:
		return "normal"
	}
}
