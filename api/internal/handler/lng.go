package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"sort"
	"time"
)

// LNGTerminal holds the current berth status for one US LNG export terminal.
type LNGTerminal struct {
	Name          string  `json:"name"`
	Location      string  `json:"location"`
	CapacityBcfd  float64 `json:"capacity_bcfd"`
	ShipsLoading  *int    `json:"ships_loading"`
	ShipsAnchored *int    `json:"ships_anchored"`
	Status        string  `json:"status"`
	UpdatedAt     *string `json:"updated_at"`
}

// LNGSummary aggregates across all terminals.
type LNGSummary struct {
	ImpliedExportsBcfd     *float64 `json:"implied_exports_bcfd"`
	TerminalUtilizationPct *float64 `json:"terminal_utilization_pct"`
	TotalCapacityBcfd      float64  `json:"total_capacity_bcfd"`
	ExportPressureIndex    *float64 `json:"export_pressure_index"`
	QueueDepth             *int     `json:"queue_depth"`
	DestinationEuPct       *float64 `json:"destination_eu_pct"`
}

// AISVessel is one vessel currently near an LNG terminal.
type AISVessel struct {
	MMSI         int      `json:"mmsi"`
	Name         string   `json:"name"`
	Terminal     string   `json:"terminal"`
	Status       string   `json:"status"` // "loading" | "anchored"
	Lat          float64  `json:"lat"`
	Lon          float64  `json:"lon"`
	Sog          float64  `json:"sog"`
	NavStatus    int      `json:"nav_status"`
	Destination  *string  `json:"destination"`
	Draught      *float64 `json:"draught"`
	DwellMinutes int      `json:"dwell_minutes"`
	ObservedAt   string   `json:"observed_at"`
}

// LNGHistoryPoint is one daily implied export observation for charting.
type LNGHistoryPoint struct {
	Date               string   `json:"date"`
	ImpliedExportsBcfd *float64 `json:"implied_exports_bcfd"`
}

// LNGResponse is the JSON body returned by GET /api/lng.
type LNGResponse struct {
	DataAvailable bool              `json:"data_available"`
	UpdatedAt     *string           `json:"updated_at"`
	Summary       LNGSummary        `json:"summary"`
	Terminals     []LNGTerminal     `json:"terminals"`
	Vessels       []AISVessel       `json:"vessels"`
	History       []LNGHistoryPoint `json:"history"`
}

// LNGVesselsResponse is the JSON body returned by GET /api/lng/vessels.
type LNGVesselsResponse struct {
	Vessels   []AISVessel `json:"vessels"`
	UpdatedAt *string     `json:"updated_at"`
}

// knownTerminals is the static list of US LNG export terminals with their
// coordinates and design capacities.
var knownTerminals = []LNGTerminal{
	{Name: "Sabine Pass", Location: "Cameron Parish, LA", CapacityBcfd: 5.00, Status: "operational"},
	{Name: "Corpus Christi", Location: "Corpus Christi, TX", CapacityBcfd: 2.40, Status: "operational"},
	{Name: "Freeport LNG", Location: "Freeport, TX", CapacityBcfd: 2.40, Status: "operational"},
	{Name: "Cameron LNG", Location: "Hackberry, LA", CapacityBcfd: 2.10, Status: "operational"},
	{Name: "Calcasieu Pass", Location: "Calcasieu, LA", CapacityBcfd: 1.40, Status: "operational"},
	{Name: "Cove Point", Location: "Lusby, MD", CapacityBcfd: 0.75, Status: "operational"},
	{Name: "Elba Island", Location: "Savannah, GA", CapacityBcfd: 0.35, Status: "operational"},
}

// LNG handles GET /api/lng.
func (h *Handler) LNG(w http.ResponseWriter, r *http.Request) {
	db, err := h.openDB()
	if err != nil {
		slog.Error("db open failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer db.Close()

	summary, terminals, hasLiveData, err := h.queryLNGStatus(r, db)
	if err != nil {
		slog.Error("lng status query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	vessels, err := h.queryLNGVessels(r)
	if err != nil {
		slog.Warn("lng vessels query failed (non-fatal)", "err", err)
		vessels = []AISVessel{}
	}

	history, err := h.queryLNGHistory(r, db)
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
		Vessels:       vessels,
		History:       history,
	})
}

// LNGVessels handles GET /api/lng/vessels — dedicated vessel manifest endpoint.
func (h *Handler) LNGVessels(w http.ResponseWriter, r *http.Request) {
	vessels, err := h.queryLNGVessels(r)
	if err != nil {
		slog.Error("lng vessels query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	var updatedAt *string
	if len(vessels) > 0 {
		t := time.Now().UTC().Format(time.RFC3339)
		updatedAt = &t
	}

	writeJSON(w, http.StatusOK, LNGVesselsResponse{
		Vessels:   vessels,
		UpdatedAt: updatedAt,
	})
}

func (h *Handler) queryLNGStatus(r *http.Request, db *sql.DB) (LNGSummary, []LNGTerminal, bool, error) {
	// Read ship counts from in-memory AIS state (updated by POST /internal/ais).
	h.AIS.mu.RLock()
	aisCounts := h.AIS.Counts
	aisAge := time.Since(h.AIS.UpdatedAt)
	h.AIS.mu.RUnlock()

	// Treat data as stale after 2 hours (matches features_lng.py fallback window).
	hasAISData := len(aisCounts) > 0 && aisAge < 2*time.Hour

	terminals := make([]LNGTerminal, len(knownTerminals))
	copy(terminals, knownTerminals)

	if hasAISData {
		for i, t := range terminals {
			c, ok := aisCounts[t.Name]
			if !ok {
				continue
			}
			loading := c[0]
			anchored := c[1]
			terminals[i].ShipsLoading = &loading
			terminals[i].ShipsAnchored = &anchored

			switch {
			case loading > 0:
				terminals[i].Status = "active"
			case anchored > 0:
				terminals[i].Status = "reduced"
			default:
				terminals[i].Status = "idle"
			}
		}
	}

	// Build summary from features_daily (written by transforms/features_lng.py).
	var summary LNGSummary
	var totalCap float64
	for _, t := range knownTerminals {
		totalCap += t.CapacityBcfd
	}
	summary.TotalCapacityBcfd = totalCap

	featRow := db.QueryRowContext(r.Context(), `
		SELECT
		    MAX(CASE WHEN feature_name = 'lng_implied_exports_bcfd'     THEN value END),
		    MAX(CASE WHEN feature_name = 'lng_terminal_utilization_pct' THEN value END),
		    MAX(CASE WHEN feature_name = 'lng_export_pressure_index'    THEN value END),
		    MAX(CASE WHEN feature_name = 'lng_queue_depth'              THEN value END),
		    MAX(CASE WHEN feature_name = 'lng_destination_eu_pct'       THEN value END)
		FROM features_daily
		WHERE feature_name IN (
		        'lng_implied_exports_bcfd', 'lng_terminal_utilization_pct',
		        'lng_export_pressure_index', 'lng_queue_depth', 'lng_destination_eu_pct'
		      )
		  AND region = 'US'
		  AND feature_date >= CURRENT_DATE - INTERVAL 45 DAYS
	`)

	var exp, util, epi, queue, euPct sql.NullFloat64
	hasFeatureData := false
	if err := featRow.Scan(&exp, &util, &epi, &queue, &euPct); err == nil {
		summary.ImpliedExportsBcfd = nullFloat64(exp)
		summary.TerminalUtilizationPct = nullFloat64(util)
		summary.ExportPressureIndex = nullFloat64(epi)
		if queue.Valid {
			v := int(queue.Float64)
			summary.QueueDepth = &v
		}
		summary.DestinationEuPct = nullFloat64(euPct)
		hasFeatureData = exp.Valid || util.Valid
	}

	hasLiveData := hasAISData || hasFeatureData

	return summary, terminals, hasLiveData, nil
}

func (h *Handler) queryLNGVessels(_ *http.Request) ([]AISVessel, error) {
	h.AIS.mu.RLock()
	vessels := h.AIS.Vessels
	updatedAt := h.AIS.UpdatedAt
	h.AIS.mu.RUnlock()

	if len(vessels) == 0 {
		return []AISVessel{}, nil
	}

	observedAt := updatedAt.Format(time.RFC3339)
	out := make([]AISVessel, 0, len(vessels))
	for _, v := range vessels {
		av := AISVessel{
			MMSI:         v.MMSI,
			Name:         v.Name,
			Terminal:     v.Terminal,
			Status:       v.Status,
			Lat:          v.Lat,
			Lon:          v.Lon,
			Sog:          v.Sog,
			NavStatus:    v.NavStatus,
			DwellMinutes: h.AIS.DwellMinutes(v.MMSI, v.Terminal),
			ObservedAt:   observedAt,
		}
		if v.Destination != "" {
			d := v.Destination
			av.Destination = &d
		}
		if v.Draught > 0 {
			d := v.Draught
			av.Draught = &d
		}
		out = append(out, av)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Terminal != out[j].Terminal {
			return out[i].Terminal < out[j].Terminal
		}
		if out[i].Status != out[j].Status {
			return out[i].Status > out[j].Status // "loading" before "anchored"
		}
		return out[i].DwellMinutes > out[j].DwellMinutes
	})

	return out, nil
}

func (h *Handler) queryLNGHistory(r *http.Request, db *sql.DB) ([]LNGHistoryPoint, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT feature_date::VARCHAR, value
		FROM features_daily
		WHERE feature_name = 'lng_implied_exports_bcfd'
		  AND region = 'US'
		ORDER BY feature_date DESC
		LIMIT 120
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
