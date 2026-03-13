package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// PriceBar is one day of OHLCV data for the front-month NG=F contract.
type PriceBar struct {
	Date   string   `json:"date"`
	Open   *float64 `json:"open"`
	High   *float64 `json:"high"`
	Low    *float64 `json:"low"`
	Close  *float64 `json:"close"`
	Volume *float64 `json:"volume"`
}

// CurvePoint is one contract month on the forward curve.
type CurvePoint struct {
	// Ticker is the Yahoo Finance ticker, e.g. "NGJ26".
	Ticker string   `json:"ticker"`
	Price  *float64 `json:"price"`
	// ObsTime is when the snapshot was taken (intraday).
	ObsTime time.Time `json:"obs_time"`
}

// SpotPoint is one daily FRED Henry Hub spot price observation.
type SpotPoint struct {
	Date  string   `json:"date"`
	Price *float64 `json:"price"`
}

// LNGArbData holds the current TTF/HH LNG export arbitrage spread.
type LNGArbData struct {
	TTFSpotUSDMMBtu *float64 `json:"ttf_spot_usd_mmbtu"`
	TTFHHNetBack    *float64 `json:"ttf_hh_net_back_usd_mmbtu"`
	ArbSpread       *float64 `json:"arb_spread_usd_mmbtu"`
	Interpretation  *string  `json:"interpretation"`
	// DataDate is the feature_date of the TTF observation (monthly from FRED).
	DataDate *string `json:"data_date"`
}

// PriceResponse is the JSON body returned by GET /api/price.
type PriceResponse struct {
	// History is 90 days of NG=F OHLCV bars, newest first.
	History []PriceBar `json:"history"`
	// Curve is the 13-month forward curve snapshot (most recent intraday values).
	Curve []CurvePoint `json:"forward_curve"`
	// Spot is 90 days of FRED Henry Hub spot price, newest first.
	Spot []SpotPoint `json:"spot_history"`
	// HeatingOil is 90 days of FRED No. 2 Heating Oil (NY Harbor) spot price, newest first.
	HeatingOil []SpotPoint `json:"heating_oil_history"`
	// TTFHistory is up to 24 months of European TTF spot price (USD/MMBtu, monthly), newest first.
	TTFHistory []SpotPoint `json:"ttf_history"`
	// LNGArb is the current TTF→HH LNG export arbitrage spread. Nil until TTF data arrives.
	LNGArb *LNGArbData `json:"lng_arb"`
}

// Price handles GET /api/price.
func (h *Handler) Price(w http.ResponseWriter, r *http.Request) {
	db, err := h.openDB()
	if err != nil {
		slog.Error("db open failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer db.Close()

	history, err := h.queryOHLCV(r, db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	curve, err := h.queryForwardCurve(r, db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	spot, err := h.queryFredSpot(r, db, "ng_spot_price")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	heatingOil, err := h.queryFredSpot(r, db, "heating_oil_spot")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	ttf, err := h.queryFredSpot(r, db, "ttf_spot")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	writeJSON(w, http.StatusOK, PriceResponse{
		History:    history,
		Curve:      curve,
		Spot:       spot,
		HeatingOil: heatingOil,
		TTFHistory: ttf,
		LNGArb:     h.queryLNGArb(r, db),
	})
}

func (h *Handler) queryOHLCV(r *http.Request, db *sql.DB) ([]PriceBar, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT
		    observation_time::TIMESTAMP::DATE::VARCHAR,
		    MAX(CASE WHEN series_name = 'ng_front_open'   THEN value END),
		    MAX(CASE WHEN series_name = 'ng_front_high'   THEN value END),
		    MAX(CASE WHEN series_name = 'ng_front_low'    THEN value END),
		    MAX(CASE WHEN series_name = 'ng_front_close'  THEN value END),
		    MAX(CASE WHEN series_name = 'ng_front_volume' THEN value END)
		FROM facts_time_series
		WHERE source_name = 'yfinance'
		  AND series_name IN (
		        'ng_front_open','ng_front_high',
		        'ng_front_low','ng_front_close','ng_front_volume'
		      )
		GROUP BY observation_time::TIMESTAMP::DATE::VARCHAR
		ORDER BY observation_time::TIMESTAMP::DATE::VARCHAR DESC
		LIMIT 90
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PriceBar
	for rows.Next() {
		var b PriceBar
		var open, high, low, close_, vol sql.NullFloat64
		if err := rows.Scan(&b.Date, &open, &high, &low, &close_, &vol); err != nil {
			slog.Warn("ohlcv scan failed", "err", err)
			continue
		}
		b.Open = nullFloat64(open)
		b.High = nullFloat64(high)
		b.Low = nullFloat64(low)
		b.Close = nullFloat64(close_)
		b.Volume = nullFloat64(vol)
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (h *Handler) queryForwardCurve(r *http.Request, db *sql.DB) ([]CurvePoint, error) {
	// Return the most recent intraday snapshot for each curve contract.
	// Inner query selects the max observation_time per series; outer join fetches value.
	rows, err := db.QueryContext(r.Context(), `
		SELECT t.series_name, t.value, t.observation_time
		FROM facts_time_series t
		INNER JOIN (
		    SELECT series_name, MAX(observation_time) AS max_time
		    FROM facts_time_series
		    WHERE source_name = 'yfinance'
		      AND series_name LIKE 'ng_curve_%'
		      AND frequency = 'intraday'
		      AND observation_time >= NOW()::TIMESTAMP - INTERVAL '7 days'
		    GROUP BY series_name
		) latest ON t.series_name = latest.series_name
		       AND t.observation_time = latest.max_time
		       AND t.source_name = 'yfinance'
		ORDER BY t.series_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CurvePoint
	for rows.Next() {
		var series string
		var val sql.NullFloat64
		var obsTime time.Time
		if err := rows.Scan(&series, &val, &obsTime); err != nil {
			slog.Warn("curve scan failed", "err", err)
			continue
		}
		// Strip "ng_curve_" prefix to get the ticker (e.g. "ngj26").
		ticker := series
		if len(series) > 9 {
			ticker = series[9:]
		}
		out = append(out, CurvePoint{
			Ticker:  ticker,
			Price:   nullFloat64(val),
			ObsTime: obsTime,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (h *Handler) queryFredSpot(r *http.Request, db *sql.DB, seriesName string) ([]SpotPoint, error) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT observation_time::TIMESTAMP::DATE::VARCHAR, value
		FROM facts_time_series
		WHERE source_name = 'fred' AND series_name = ?
		ORDER BY observation_time DESC
		LIMIT 90
	`, seriesName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SpotPoint
	for rows.Next() {
		var date string
		var val sql.NullFloat64
		if err := rows.Scan(&date, &val); err != nil {
			slog.Warn("fred spot scan failed", "err", err)
			continue
		}
		out = append(out, SpotPoint{Date: date, Price: nullFloat64(val)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// queryLNGArb returns the most recent TTF→HH LNG export arbitrage spread
// from features_daily (written by transforms/features_price.py).
// Returns nil if TTF data has not yet arrived from FRED.
func (h *Handler) queryLNGArb(r *http.Request, db *sql.DB) *LNGArbData {
	row := db.QueryRowContext(r.Context(), `
		SELECT
		    feature_date::VARCHAR,
		    MAX(CASE WHEN feature_name = 'ttf_spot_usd_mmbtu' THEN value END),
		    MAX(CASE WHEN feature_name = 'ttf_hh_net_back'    THEN value END),
		    MAX(CASE WHEN feature_name = 'ttf_hh_arb_spread'  THEN value END),
		    MAX(CASE WHEN feature_name = 'ttf_hh_arb_spread'  THEN interpretation END)
		FROM features_daily
		WHERE feature_name IN (
		        'ttf_spot_usd_mmbtu',
		        'ttf_hh_net_back',
		        'ttf_hh_arb_spread'
		      )
		  AND region = 'US'
		GROUP BY feature_date
		ORDER BY feature_date DESC
		LIMIT 1
	`)

	var dataDate string
	var ttf, netBack, spread sql.NullFloat64
	var interp sql.NullString
	if err := row.Scan(&dataDate, &ttf, &netBack, &spread, &interp); err != nil {
		return nil
	}
	if !ttf.Valid {
		return nil
	}
	return &LNGArbData{
		TTFSpotUSDMMBtu: nullFloat64(ttf),
		TTFHHNetBack:    nullFloat64(netBack),
		ArbSpread:       nullFloat64(spread),
		Interpretation:  nullString(interp),
		DataDate:        &dataDate,
	}
}
