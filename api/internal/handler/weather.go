package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// WeatherCity is the 7-day forecast summary for one city.
type WeatherCity struct {
	City     string   `json:"city"`
	HDD7d    *float64 `json:"hdd_7d"`
	CDD7d    *float64 `json:"cdd_7d"`
	HighF    *float64 `json:"high_temp_f"`
	DataDate string   `json:"data_date"`
}

// WeatherSummary holds the population-weighted aggregates.
type WeatherSummary struct {
	HDD7dWeighted      *float64  `json:"hdd_7d_weighted"`
	CDD7dWeighted      *float64  `json:"cdd_7d_weighted"`
	HDDRevisionDelta   *float64  `json:"hdd_revision_delta"`
	// ImpliedDemandBcfd is the HDD-model estimate of residential/commercial gas demand.
	ImpliedDemandBcfd  *float64  `json:"implied_demand_bcfd"`
	// DemandVsNormalBcfd is the delta above/below seasonal-normal demand.
	DemandVsNormalBcfd *float64  `json:"demand_vs_normal_bcfd"`
	DataDate           string    `json:"data_date"`
	ComputedAt         time.Time `json:"computed_at"`
}

// WeatherHistoryPoint is one daily observation of population-weighted HDD.
type WeatherHistoryPoint struct {
	Date          string   `json:"date"`
	HDD7dWeighted *float64 `json:"hdd_7d_weighted"`
	CDD7dWeighted *float64 `json:"cdd_7d_weighted,omitempty"`
}

// CPCWindow holds the CPC extended-range temperature outlook for one window.
type CPCWindow struct {
	WeightedProbBelow *float64 `json:"weighted_prob_below"`
	Interpretation    string   `json:"interpretation"`
	FcstDate          string   `json:"fcst_date"`
}

// CPCOutlook groups the 6-10 day and 8-14 day CPC outlook windows.
type CPCOutlook struct {
	Day6To10 *CPCWindow `json:"6_10_day"`
	Day8To14 *CPCWindow `json:"8_14_day"`
}

// WeatherResponse is the JSON body returned by GET /api/weather.
type WeatherResponse struct {
	Summary WeatherSummary `json:"summary"`
	Cities  []WeatherCity  `json:"cities"`
	// History is up to 90 days of population-weighted HDD/CDD, newest first.
	History []WeatherHistoryPoint `json:"history"`
	// CPCOutlook is the extended-range (6-10 / 8-14 day) temperature probability outlook.
	CPCOutlook CPCOutlook `json:"cpc_outlook"`
}

var weatherCities = []string{
	"new_york", "chicago", "philadelphia", "boston",
	"houston", "atlanta", "minneapolis", "detroit",
}

// Weather handles GET /api/weather.
func (h *Handler) Weather(w http.ResponseWriter, r *http.Request) {
	db := h.DB

	summary, err := h.queryWeatherSummary(r, db)
	if err != nil {
		slog.Error("weather summary query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	cities, err := h.queryWeatherCities(r, db)
	if err != nil {
		slog.Error("weather cities query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	history, err := h.queryWeatherHistory(r, db)
	if err != nil {
		slog.Error("weather history query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	cpc, err := h.queryCPCOutlook(r, db)
	if err != nil {
		slog.Error("cpc outlook query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	writeJSON(w, http.StatusOK, WeatherResponse{
		Summary:    summary,
		Cities:     cities,
		History:    history,
		CPCOutlook: cpc,
	})
}

func (h *Handler) queryWeatherSummary(r *http.Request, db *sql.DB) (WeatherSummary, error) {
	var s WeatherSummary
	row := db.QueryRowContext(r.Context(), `
		SELECT
		    feature_date::VARCHAR,
		    MAX(CASE WHEN feature_name = 'weather_hdd_7d_weighted'          THEN value END),
		    MAX(CASE WHEN feature_name = 'weather_cdd_7d_weighted'          THEN value END),
		    MAX(CASE WHEN feature_name = 'weather_hdd_revision_delta'       THEN value END),
		    MAX(CASE WHEN feature_name = 'weather_implied_resi_comm_bcfd'   THEN value END),
		    MAX(CASE WHEN feature_name = 'weather_demand_vs_normal_bcfd'    THEN value END),
		    MAX(computed_at)
		FROM features_daily
		WHERE feature_name IN (
		        'weather_hdd_7d_weighted',
		        'weather_cdd_7d_weighted',
		        'weather_hdd_revision_delta',
		        'weather_implied_resi_comm_bcfd',
		        'weather_demand_vs_normal_bcfd'
		      )
		  AND region = 'US'
		GROUP BY feature_date
		ORDER BY feature_date DESC
		LIMIT 1
	`)
	var hdd, cdd, rev, demand, demandDelta sql.NullFloat64
	err := row.Scan(&s.DataDate, &hdd, &cdd, &rev, &demand, &demandDelta, &s.ComputedAt)
	if err != nil && err != sql.ErrNoRows {
		return s, err
	}
	s.HDD7dWeighted = nullFloat64(hdd)
	s.CDD7dWeighted = nullFloat64(cdd)
	s.HDDRevisionDelta = nullFloat64(rev)
	s.ImpliedDemandBcfd = nullFloat64(demand)
	s.DemandVsNormalBcfd = nullFloat64(demandDelta)
	return s, nil
}

// queryWeatherHistory returns up to 90 days of population-weighted HDD/CDD
// from the features_daily table, newest first.
func (h *Handler) queryWeatherHistory(r *http.Request, db *sql.DB) ([]WeatherHistoryPoint, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT
		    feature_date::VARCHAR,
		    MAX(CASE WHEN feature_name = 'weather_hdd_7d_weighted' THEN value END),
		    MAX(CASE WHEN feature_name = 'weather_cdd_7d_weighted' THEN value END)
		FROM features_daily
		WHERE feature_name IN ('weather_hdd_7d_weighted', 'weather_cdd_7d_weighted')
		  AND region = 'US'
		GROUP BY feature_date
		ORDER BY feature_date DESC
		LIMIT 90
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []WeatherHistoryPoint
	for rows.Next() {
		var date string
		var hdd, cdd sql.NullFloat64
		if err := rows.Scan(&date, &hdd, &cdd); err != nil {
			slog.Warn("weather history scan failed", "err", err)
			continue
		}
		out = append(out, WeatherHistoryPoint{
			Date:          date,
			HDD7dWeighted: nullFloat64(hdd),
			CDD7dWeighted: nullFloat64(cdd),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (h *Handler) queryWeatherCities(r *http.Request, db *sql.DB) ([]WeatherCity, error) {
	// NWS data is stored with the stat name in series_name and the city in region.
	// e.g. series_name='forecast_hdd_65', region='chicago'
	const (
		seriesHDD  = "forecast_hdd_65"
		seriesCDD  = "forecast_cdd_65"
		seriesTemp = "forecast_temp_f"
	)

	// Build args: 3 series names + N city names.
	args := make([]any, 0, 3+len(weatherCities))
	args = append(args, seriesHDD, seriesCDD, seriesTemp)
	for _, city := range weatherCities {
		args = append(args, city)
	}

	rows, err := db.QueryContext(r.Context(), `
		SELECT t.series_name, t.region, t.value, t.observation_time::TIMESTAMP::DATE::VARCHAR
		FROM facts_time_series t
		INNER JOIN (
		    SELECT series_name, region, MAX(observation_time) AS max_time
		    FROM facts_time_series
		    WHERE source_name = 'nws'
		      AND series_name IN (?,?,?)
		      AND region IN (`+inClause(len(weatherCities))+`)
		    GROUP BY series_name, region
		) latest ON t.series_name = latest.series_name
		       AND t.region       = latest.region
		       AND t.observation_time = latest.max_time
		       AND t.source_name = 'nws'
		ORDER BY t.region, t.series_name
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type cityData struct {
		hdd  *float64
		cdd  *float64
		high *float64
		date string
	}
	cityMap := make(map[string]*cityData)

	for rows.Next() {
		var series, city, date string
		var val sql.NullFloat64
		if err := rows.Scan(&series, &city, &val, &date); err != nil {
			slog.Warn("weather city scan failed", "err", err)
			continue
		}
		v := nullFloat64(val)
		if cityMap[city] == nil {
			cityMap[city] = &cityData{date: date}
		}
		switch series {
		case seriesHDD:
			cityMap[city].hdd = v
		case seriesCDD:
			cityMap[city].cdd = v
		case seriesTemp:
			cityMap[city].high = v
		}
		if date != "" {
			cityMap[city].date = date
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]WeatherCity, 0, len(weatherCities))
	for _, city := range weatherCities {
		d := cityMap[city]
		if d == nil {
			out = append(out, WeatherCity{City: city})
			continue
		}
		out = append(out, WeatherCity{
			City:     city,
			HDD7d:    d.hdd,
			CDD7d:    d.cdd,
			HighF:    d.high,
			DataDate: d.date,
		})
	}
	return out, nil
}

// queryCPCOutlook returns the most recent CPC 6-10 and 8-14 day outlook windows
// from features_daily (computed by transforms/features_cpc.py).
func (h *Handler) queryCPCOutlook(r *http.Request, db *sql.DB) (CPCOutlook, error) {
	var outlook CPCOutlook

	rows, err := db.QueryContext(r.Context(), `
		SELECT feature_name, value, interpretation, feature_date::VARCHAR
		FROM (
		    SELECT feature_name, value, interpretation, feature_date,
		           ROW_NUMBER() OVER (PARTITION BY feature_name ORDER BY feature_date DESC) AS rn
		    FROM features_daily
		    WHERE feature_name IN (
		            'cpc_6_10_weighted_prob_below',
		            'cpc_8_14_weighted_prob_below'
		          )
		      AND region = 'US'
		) ranked
		WHERE rn = 1
	`)
	if err != nil {
		return outlook, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, interp, date string
		var val sql.NullFloat64
		if err := rows.Scan(&name, &val, &interp, &date); err != nil {
			slog.Warn("cpc outlook scan failed", "err", err)
			continue
		}
		w := &CPCWindow{
			WeightedProbBelow: nullFloat64(val),
			Interpretation:    interp,
			FcstDate:          date,
		}
		switch name {
		case "cpc_6_10_weighted_prob_below":
			outlook.Day6To10 = w
		case "cpc_8_14_weighted_prob_below":
			outlook.Day8To14 = w
		}
	}
	if err := rows.Err(); err != nil {
		return outlook, err
	}
	return outlook, nil
}
