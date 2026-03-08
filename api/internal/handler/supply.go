package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// SupplyLatest is the most recent observation for one supply series.
type SupplyLatest struct {
	Value      *float64  `json:"value"`
	PeriodDate string    `json:"period_date"`
	IngestTime time.Time `json:"ingest_time"`
}

// SupplyHistoryPoint is one monthly observation in a supply time-series.
type SupplyHistoryPoint struct {
	PeriodDate string   `json:"period_date"`
	Value      *float64 `json:"value"`
}

// SupplySeries groups the latest value and trailing 12-month history for one
// EIA supply fundamental series.
type SupplySeries struct {
	Name   string        `json:"name"`
	Unit   string        `json:"unit"`
	Latest *SupplyLatest `json:"latest"`
	// History is up to 12 months of monthly observations, newest first.
	History []SupplyHistoryPoint `json:"history"`
}

// SupplyResponse is the JSON body returned by GET /api/supply.
type SupplyResponse struct {
	Series []SupplySeries `json:"series"`
}

var supplySeriesNames = []string{
	"dry_gas_production_mmcf",
	"lng_exports_mmcf",
	"power_sector_burn_mmcf",
	"mexico_pipeline_exp_mmcf",
	"total_imports_mmcf",
	"total_pipeline_exports_mmcf",
}

// Supply handles GET /api/supply.
// Returns the latest value plus trailing 12-month history for each EIA supply
// fundamental series.
func (h *Handler) Supply(w http.ResponseWriter, r *http.Request) {
	args := make([]any, len(supplySeriesNames))
	for i, n := range supplySeriesNames {
		args[i] = n
	}

	// Fetch the most recent 12 months of observations per series in one query.
	// We use a window function to rank rows per series by date, then filter.
	rows, err := h.DB.QueryContext(r.Context(), `
		SELECT series_name, value, unit,
		       observation_time::TIMESTAMP::DATE::VARCHAR AS period_date,
		       ingest_time
		FROM (
		    SELECT series_name, value, unit, observation_time, ingest_time,
		           ROW_NUMBER() OVER (PARTITION BY series_name ORDER BY observation_time DESC) AS rn
		    FROM facts_time_series
		    WHERE source_name = 'eia_supply'
		      AND series_name IN (`+inClause(len(supplySeriesNames))+`)
		) ranked
		WHERE rn <= 12
		ORDER BY series_name, period_date DESC
	`, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer rows.Close()

	// Collect rows grouped by series name, preserving order.
	type row struct {
		value      sql.NullFloat64
		unit       string
		periodDate string
		ingestTime time.Time
	}
	grouped := make(map[string][]row)
	var seriesOrder []string
	for rows.Next() {
		var name, unit, period string
		var val sql.NullFloat64
		var ingestTime time.Time
		if err := rows.Scan(&name, &val, &unit, &period, &ingestTime); err != nil {
			slog.Warn("supply scan failed", "err", err)
			continue
		}
		if _, exists := grouped[name]; !exists {
			seriesOrder = append(seriesOrder, name)
		}
		grouped[name] = append(grouped[name], row{
			value: val, unit: unit, periodDate: period, ingestTime: ingestTime,
		})
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	// Build response: first row per series is latest, rest is history.
	series := make([]SupplySeries, 0, len(supplySeriesNames))
	for _, name := range supplySeriesNames {
		rr := grouped[name]
		s := SupplySeries{Name: name}
		if len(rr) == 0 {
			series = append(series, s)
			continue
		}
		s.Unit = rr[0].unit
		s.Latest = &SupplyLatest{
			Value:      nullFloat64(rr[0].value),
			PeriodDate: rr[0].periodDate,
			IngestTime: rr[0].ingestTime,
		}
		hist := make([]SupplyHistoryPoint, 0, len(rr))
		for _, r := range rr {
			hist = append(hist, SupplyHistoryPoint{
				PeriodDate: r.periodDate,
				Value:      nullFloat64(r.value),
			})
		}
		s.History = hist
		series = append(series, s)
	}

	// Append rig count from baker_hughes source (separate source_name, separate query).
	rigCount, err := h.queryRigCount(r)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	if rigCount != nil {
		series = append(series, *rigCount)
	}

	writeJSON(w, http.StatusOK, SupplyResponse{Series: series})
}

// queryRigCount returns the US natural gas rig count series from Baker Hughes,
// up to 104 weeks of history (weekly frequency).
func (h *Handler) queryRigCount(r *http.Request) (*SupplySeries, error) {
	rows, err := h.DB.QueryContext(r.Context(), `
		SELECT value, unit,
		       observation_time::TIMESTAMP::DATE::VARCHAR AS period_date,
		       ingest_time
		FROM facts_time_series
		WHERE source_name = 'baker_hughes'
		  AND series_name  = 'ng_rig_count'
		ORDER BY observation_time DESC
		LIMIT 104
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	s := &SupplySeries{Name: "ng_rig_count"}
	var hist []SupplyHistoryPoint
	for rows.Next() {
		var val sql.NullFloat64
		var unit, period string
		var ingestTime time.Time
		if err := rows.Scan(&val, &unit, &period, &ingestTime); err != nil {
			slog.Warn("rig count scan failed", "err", err)
			continue
		}
		if s.Latest == nil {
			s.Unit = unit
			s.Latest = &SupplyLatest{
				Value:      nullFloat64(val),
				PeriodDate: period,
				IngestTime: ingestTime,
			}
		}
		hist = append(hist, SupplyHistoryPoint{PeriodDate: period, Value: nullFloat64(val)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if s.Latest == nil {
		return nil, nil // no data yet
	}
	s.History = hist
	return s, nil
}
