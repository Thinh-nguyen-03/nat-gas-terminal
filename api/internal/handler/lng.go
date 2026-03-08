package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// LNGTerminal holds the current berth status for one US LNG export terminal.
type LNGTerminal struct {
	Name              string   `json:"name"`
	Location          string   `json:"location"`
	CapacityBcfd      float64  `json:"capacity_bcfd"`
	ShipsLoading      *int     `json:"ships_loading"`
	ShipsAnchored     *int     `json:"ships_anchored"`
	Status            string   `json:"status"`
	UpdatedAt         *string  `json:"updated_at"`
}

// LNGSummary aggregates across all terminals.
type LNGSummary struct {
	ImpliedExportsBcfd    *float64 `json:"implied_exports_bcfd"`
	TerminalUtilizationPct *float64 `json:"terminal_utilization_pct"`
	TotalCapacityBcfd     float64  `json:"total_capacity_bcfd"`
}

// LNGHistoryPoint is one daily implied export observation for charting.
type LNGHistoryPoint struct {
	Date               string   `json:"date"`
	ImpliedExportsBcfd *float64 `json:"implied_exports_bcfd"`
}

// LNGResponse is the JSON body returned by GET /api/lng.
type LNGResponse struct {
	// DataAvailable is false until collectors/lng_vessels.py (Feature 2) is running.
	DataAvailable bool              `json:"data_available"`
	UpdatedAt     *string           `json:"updated_at"`
	Summary       LNGSummary        `json:"summary"`
	Terminals     []LNGTerminal     `json:"terminals"`
	History       []LNGHistoryPoint `json:"history"`
}

// knownTerminals is the static list of US LNG export terminals with their
// coordinates and design capacities. The AIS collector will add live berth
// status once operational; until then the status is returned as "unknown".
var knownTerminals = []LNGTerminal{
	{Name: "Sabine Pass",    Location: "Cameron Parish, LA", CapacityBcfd: 5.00, Status: "unknown"},
	{Name: "Corpus Christi", Location: "Corpus Christi, TX", CapacityBcfd: 2.40, Status: "unknown"},
	{Name: "Freeport LNG",   Location: "Freeport, TX",       CapacityBcfd: 2.40, Status: "unknown"},
	{Name: "Cameron LNG",    Location: "Hackberry, LA",       CapacityBcfd: 2.10, Status: "unknown"},
	{Name: "Calcasieu Pass", Location: "Calcasieu, LA",       CapacityBcfd: 1.40, Status: "unknown"},
	{Name: "Cove Point",     Location: "Lusby, MD",           CapacityBcfd: 0.75, Status: "unknown"},
	{Name: "Elba Island",    Location: "Savannah, GA",        CapacityBcfd: 0.35, Status: "unknown"},
}

// LNG handles GET /api/lng.
//
// Returns AIS-derived LNG terminal berth status and implied export rate.
// Returns the terminal list with status="unknown" until collectors/lng_vessels.py
// (Feature 2) is running and populating facts_time_series with source_name='ais'.
func (h *Handler) LNG(w http.ResponseWriter, r *http.Request) {
	summary, terminals, hasLiveData, err := h.queryLNGStatus(r)
	if err != nil {
		slog.Error("lng status query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	history, err := h.queryLNGHistory(r)
	if err != nil {
		slog.Error("lng history query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	var updatedAt *string
	if hasLiveData {
		t := time.Now().UTC().Format(time.RFC3339)
		updatedAt = &t
	}

	writeJSON(w, http.StatusOK, LNGResponse{
		DataAvailable: hasLiveData,
		UpdatedAt:     updatedAt,
		Summary:       summary,
		Terminals:     terminals,
		History:       history,
	})
}

func (h *Handler) queryLNGStatus(r *http.Request) (LNGSummary, []LNGTerminal, bool, error) {
	// Look for AIS-sourced vessel counts in facts_time_series.
	rows, err := h.DB.QueryContext(r.Context(), `
		SELECT region, series_name, value
		FROM (
		    SELECT region, series_name, value,
		           ROW_NUMBER() OVER (PARTITION BY region, series_name ORDER BY observation_time DESC) AS rn
		    FROM facts_time_series
		    WHERE source_name = 'ais'
		      AND series_name IN ('lng_ships_loading', 'lng_ships_anchored')
		      AND observation_time >= NOW() - INTERVAL '2 hours'
		) ranked
		WHERE rn = 1
	`)
	if err != nil {
		return LNGSummary{}, knownTerminals, false, err
	}
	defer rows.Close()

	type terminalData struct {
		loading  *int
		anchored *int
	}
	dataMap := make(map[string]*terminalData)

	for rows.Next() {
		var region, series string
		var val sql.NullFloat64
		if err := rows.Scan(&region, &series, &val); err != nil {
			slog.Warn("lng status scan failed", "err", err)
			continue
		}
		if dataMap[region] == nil {
			dataMap[region] = &terminalData{}
		}
		if val.Valid {
			v := int(val.Float64)
			switch series {
			case "lng_ships_loading":
				dataMap[region].loading = &v
			case "lng_ships_anchored":
				dataMap[region].anchored = &v
			}
		}
	}
	if err := rows.Err(); err != nil {
		return LNGSummary{}, knownTerminals, false, err
	}

	hasLiveData := len(dataMap) > 0

	terminals := make([]LNGTerminal, len(knownTerminals))
	copy(terminals, knownTerminals)

	for i, t := range terminals {
		d := dataMap[t.Name]
		if d == nil {
			continue
		}
		terminals[i].ShipsLoading = d.loading
		terminals[i].ShipsAnchored = d.anchored

		loading := 0
		if d.loading != nil {
			loading = *d.loading
		}
		anchored := 0
		if d.anchored != nil {
			anchored = *d.anchored
		}

		switch {
		case loading > 0:
			terminals[i].Status = "active"
		case anchored > 0:
			terminals[i].Status = "reduced"
		default:
			terminals[i].Status = "idle"
		}
	}

	// Build summary from features_daily (written by transforms/features_lng.py).
	var summary LNGSummary
	var totalCap float64
	for _, t := range knownTerminals {
		totalCap += t.CapacityBcfd
	}
	summary.TotalCapacityBcfd = totalCap

	featRow := h.DB.QueryRowContext(r.Context(), `
		SELECT
		    MAX(CASE WHEN feature_name = 'lng_implied_exports_bcfd'    THEN value END),
		    MAX(CASE WHEN feature_name = 'lng_terminal_utilization_pct' THEN value END)
		FROM features_daily
		WHERE feature_name IN ('lng_implied_exports_bcfd', 'lng_terminal_utilization_pct')
		  AND region = 'US'
		  AND feature_date >= CURRENT_DATE - INTERVAL 1 DAYS
	`)
	var exp, util sql.NullFloat64
	if err := featRow.Scan(&exp, &util); err == nil {
		summary.ImpliedExportsBcfd = nullFloat64(exp)
		summary.TerminalUtilizationPct = nullFloat64(util)
	}

	return summary, terminals, hasLiveData, nil
}

func (h *Handler) queryLNGHistory(r *http.Request) ([]LNGHistoryPoint, error) {
	rows, err := h.DB.QueryContext(r.Context(), `
		SELECT feature_date::VARCHAR, value
		FROM features_daily
		WHERE feature_name = 'lng_implied_exports_bcfd'
		  AND region = 'US'
		ORDER BY feature_date DESC
		LIMIT 90
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LNGHistoryPoint
	for rows.Next() {
		var date string
		var val sql.NullFloat64
		if err := rows.Scan(&date, &val); err != nil {
			slog.Warn("lng history scan failed", "err", err)
			continue
		}
		out = append(out, LNGHistoryPoint{
			Date:               date,
			ImpliedExportsBcfd: nullFloat64(val),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []LNGHistoryPoint{}
	}
	return out, nil
}
