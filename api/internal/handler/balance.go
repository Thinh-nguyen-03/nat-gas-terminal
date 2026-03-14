package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// BalanceComponent is one line item in the supply or demand table (Bcf/d).
type BalanceComponent struct {
	Name        string   `json:"name"`
	ValueBcfd   *float64 `json:"value_bcfd"`
	Source      string   `json:"source"`
	UpdatedAt   *string  `json:"updated_at"`
}

// BalanceSummary aggregates the net position and storage estimate.
type BalanceSummary struct {
	TotalSupplyBcfd      *float64 `json:"total_supply_bcfd"`
	TotalDemandBcfd      *float64 `json:"total_demand_bcfd"`
	NetBalanceBcfd       *float64 `json:"net_balance_bcfd"`
	ImpliedWeeklyBcf     *float64 `json:"implied_weekly_bcf"`
	ModelEstimateBcf     *float64 `json:"model_estimate_bcf"`
	ModelErrorBcf        *float64 `json:"model_error_bcf"`
	ActiveOFOCount       int      `json:"active_ofo_count"`
}

// BalanceResponse is the JSON body returned by GET /api/balance.
type BalanceResponse struct {
	// UpdatedAt is when the balance was last computed. Nil until EBB data flows.
	UpdatedAt *string            `json:"updated_at"`
	Supply    []BalanceComponent `json:"supply"`
	Demand    []BalanceComponent `json:"demand"`
	Summary   BalanceSummary     `json:"summary"`
}

// Balance handles GET /api/balance.
// Returns the live supply/demand balance from EIA supply, EIA-930 power burn,
// HDD-implied residential/commercial demand, and AIS-derived LNG exports.
func (h *Handler) Balance(w http.ResponseWriter, r *http.Request) {
	db, err := h.openDB()
	if err != nil {
		slog.Error("db open failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer db.Close()

	supply, err := h.queryBalanceSupply(r, db)
	if err != nil {
		slog.Error("balance supply query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	demand, err := h.queryBalanceDemand(r, db)
	if err != nil {
		slog.Error("balance demand query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	summary := computeBalanceSummary(supply, demand)

	var updatedAt *string
	if len(supply) > 0 || len(demand) > 0 {
		t := time.Now().UTC().Format(time.RFC3339)
		updatedAt = &t
	}

	writeJSON(w, http.StatusOK, BalanceResponse{
		UpdatedAt: updatedAt,
		Supply:    supply,
		Demand:    demand,
		Summary:   summary,
	})
}

func (h *Handler) queryBalanceSupply(r *http.Request, db *sql.DB) ([]BalanceComponent, error) {
	// Pull the most recent value for each supply component from features_daily.
	// The feature names match what EIASupplyCollector + features_supply write.
	rows, err := db.QueryContext(r.Context(), `
		SELECT feature_name, value, computed_at::VARCHAR
		FROM (
		    SELECT feature_name, value, computed_at,
		           ROW_NUMBER() OVER (PARTITION BY feature_name ORDER BY feature_date DESC) AS rn
		    FROM features_daily
		    WHERE feature_name IN (
		            'dry_gas_production_bcfd',
		            'canada_imports_bcfd'
		          )
		      AND region = 'US'
		) ranked
		WHERE rn = 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nameMap := map[string]string{
		"dry_gas_production_bcfd": "Dry gas production",
		"canada_imports_bcfd":     "Canada imports",
	}
	sourceMap := map[string]string{
		"dry_gas_production_bcfd": "eia_supply",
		"canada_imports_bcfd":     "eia_supply",
	}

	var out []BalanceComponent
	for rows.Next() {
		var name string
		var val sql.NullFloat64
		var updatedAt sql.NullString
		if err := rows.Scan(&name, &val, &updatedAt); err != nil {
			slog.Warn("balance supply scan failed", "err", err)
			continue
		}
		label, ok := nameMap[name]
		if !ok {
			label = name
		}
		out = append(out, BalanceComponent{
			Name:      label,
			ValueBcfd: nullFloat64(val),
			Source:    sourceMap[name],
			UpdatedAt: nullString(updatedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []BalanceComponent{}
	}
	return out, nil
}

func (h *Handler) queryBalanceDemand(r *http.Request, db *sql.DB) ([]BalanceComponent, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT feature_name, value, computed_at::VARCHAR
		FROM (
		    SELECT feature_name, value, computed_at,
		           ROW_NUMBER() OVER (PARTITION BY feature_name ORDER BY feature_date DESC) AS rn
		    FROM features_daily
		    WHERE feature_name IN (
		            'power_burn_bcfd',
		            'weather_implied_resi_comm_bcfd',
		            'lng_implied_exports_bcfd',
		            'mexico_pipeline_exports_bcfd'
		          )
		      AND region = 'US'
		) ranked
		WHERE rn = 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nameMap := map[string]string{
		"power_burn_bcfd":               "Power burn",
		"weather_implied_resi_comm_bcfd": "Residential/commercial (HDD model)",
		"lng_implied_exports_bcfd":       "LNG exports (AIS estimate)",
		"mexico_pipeline_exports_bcfd":   "Mexico pipeline exports",
	}
	sourceMap := map[string]string{
		"power_burn_bcfd":               "eia_930",
		"weather_implied_resi_comm_bcfd": "hdd_model",
		"lng_implied_exports_bcfd":       "ais",
		"mexico_pipeline_exports_bcfd":   "eia_supply",
	}

	var out []BalanceComponent
	for rows.Next() {
		var name string
		var val sql.NullFloat64
		var updatedAt sql.NullString
		if err := rows.Scan(&name, &val, &updatedAt); err != nil {
			slog.Warn("balance demand scan failed", "err", err)
			continue
		}
		label, ok := nameMap[name]
		if !ok {
			label = name
		}
		out = append(out, BalanceComponent{
			Name:      label,
			ValueBcfd: nullFloat64(val),
			Source:    sourceMap[name],
			UpdatedAt: nullString(updatedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []BalanceComponent{}
	}
	return out, nil
}

func computeBalanceSummary(supply, demand []BalanceComponent) BalanceSummary {
	var totalSupply, totalDemand float64
	var hasSupply, hasDemand bool

	for _, c := range supply {
		if c.ValueBcfd != nil {
			totalSupply += *c.ValueBcfd
			hasSupply = true
		}
	}
	for _, c := range demand {
		if c.ValueBcfd != nil {
			totalDemand += *c.ValueBcfd
			hasDemand = true
		}
	}

	var s BalanceSummary
	if hasSupply {
		s.TotalSupplyBcfd = &totalSupply
	}
	if hasDemand {
		s.TotalDemandBcfd = &totalDemand
	}
	if hasSupply && hasDemand {
		net := totalSupply - totalDemand
		s.NetBalanceBcfd = &net
		weekly := net * 7
		s.ImpliedWeeklyBcf = &weekly
	}
	return s
}
