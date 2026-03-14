# Data Dictionary

All data lives in DuckDB at `data/db/terminal.duckdb`.

---

## facts_time_series

The central time-series store. One row per `(source_name, series_name, region, observation_time)`.

| Column | Type | Description |
|--------|------|-------------|
| `source_name` | VARCHAR | Which collector wrote this row |
| `series_name` | VARCHAR | The specific data series within that source |
| `region` | VARCHAR | Geographic scope: `US`, `total`, `east`, `midwest`, `south_central`, `mountain`, `pacific`, or a balancing authority code or city name |
| `observation_time` | TIMESTAMPTZ | The time the data point *represents* (not ingest time) |
| `ingest_time` | TIMESTAMPTZ | When this row was written to the DB |
| `value` | DOUBLE | The numeric value |
| `unit` | VARCHAR | Unit of measure |
| `frequency` | VARCHAR | `weekly`, `monthly`, `daily`, `intraday`, `hourly`, `period` |

---

### Source: `eia_storage`

EIA weekly working gas in underground storage. Published every Thursday 10:30am ET. Period = week-ending Saturday.

| series_name | region | Unit | Description |
|-------------|--------|------|-------------|
| `storage_total` | `total` | Bcf | Total Lower 48 working gas |
| `storage_east` | `east` | Bcf | East consuming region |
| `storage_midwest` | `midwest` | Bcf | Midwest consuming region |
| `storage_south_central` | `south_central` | Bcf | South Central (salt + nonsalt) |
| `storage_mountain` | `mountain` | Bcf | Mountain region |
| `storage_pacific` | `pacific` | Bcf | Pacific consuming region |

104 weeks of history. Stored as `YYYY-MM-DDT00:00:00Z`.

---

### Source: `eia_storage_stats`

EIA pre-computed 5-year statistics by week-of-year. Downloaded from `ngsstats.xls` each Thursday. These are the official EIA band values shown on the weekly storage chart. The 5-year window shifts each calendar year.

Series name pattern: `storage_5yr_{stat}_{region}` where stat ∈ `{avg, max, min}`.

| series_name example | Description |
|--------------------|-------------|
| `storage_5yr_avg_total` | 5-year average, Total Lower 48 |
| `storage_5yr_max_total` | 5-year maximum, Total Lower 48 |
| `storage_5yr_min_total` | 5-year minimum, Total Lower 48 |
| `storage_5yr_avg_east` | 5-year average, East region |

Same pattern for `midwest`, `mountain`, `pacific`, `south_central`.

---

### Source: `eia_supply`

Monthly EIA supply fundamentals. All series have a ~6–8 week reporting lag. Period stored as `YYYY-MM-01T00:00:00Z`.

| series_name | Unit | Description |
|-------------|------|-------------|
| `dry_gas_production_mmcf` | MMcf | U.S. dry natural gas marketed production |
| `lng_exports_mmcf` | MMcf | U.S. LNG exports (liquefied, sent overseas) |
| `power_sector_burn_mmcf` | MMcf | Natural gas delivered to electric power consumers |
| `mexico_pipeline_exp_mmcf` | MMcf | U.S. pipeline exports to Mexico |
| `total_imports_mmcf` | MMcf | Total U.S. natural gas imports (~99% Canada pipeline) |
| `total_pipeline_exports_mmcf` | MMcf | Total U.S. pipeline exports (Mexico + Canada) |

EIA v2 series IDs: `NG.N9100US2.M` (total imports), `NG.N9130US2.M` (total pipeline exports).

---

### Source: `yfinance`

NYMEX NG futures prices via Yahoo Finance. 15-minute delayed.

| series_name | Unit | Frequency | Description |
|-------------|------|-----------|-------------|
| `ng_front_open` | USD/MMBtu | daily | NG=F front-month open |
| `ng_front_high` | USD/MMBtu | daily | NG=F front-month daily high |
| `ng_front_low` | USD/MMBtu | daily | NG=F front-month daily low |
| `ng_front_close` | USD/MMBtu | daily | NG=F front-month close |
| `ng_front_volume` | contracts | daily | NG=F front-month volume |
| `ng_curve_ng{code}{yy}` | USD/MMBtu | intraday | One entry per forward month, e.g. `ng_curve_ngj26` = Apr 2026 |

Month codes: F=Jan G=Feb H=Mar J=Apr K=May M=Jun N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec

---

### Source: `fred`

Daily spot prices from FRED. 1-business-day lag.

| series_name | FRED series | Unit | Description |
|-------------|-------------|------|-------------|
| `ng_spot_price` | `DHHNGSP` | USD/MMBtu | Henry Hub Natural Gas Spot Price (daily) |
| `heating_oil_spot` | `DHOILNYH` | USD/gal | No. 2 Heating Oil Spot Price, New York (daily) |

Heating oil is a key substitute demand driver in the Northeast during winter. Its price relative to natural gas influences fuel-switching decisions.

---

### Source: `baker_hughes`

Baker Hughes North America weekly rig count. Published Friday ~1:00pm ET. Downloaded from an Excel (XLSB) file linked on `rigcount.bakerhughes.com/na-rig-count`. 104 weeks of history. Period stored as `YYYY-MM-DDT00:00:00Z` (the US_PublishDate from the file).

| series_name | region | Unit | Description |
|-------------|--------|------|-------------|
| `ng_rig_count` | `US` | rigs | U.S. natural gas-directed drilling rigs |

Filtered from the "NAM Weekly" sheet: `Country='UNITED STATES'`, `DrillFor='Gas'`, summed across all basins.

---

### Source: `cpc`

NOAA Climate Prediction Center extended-range temperature outlook. Published daily. Two windows: 6-10 day and 8-14 day. GIS shapefiles downloaded from the CPC FTP at `ftp.cpc.ncep.noaa.gov/GIS/us_tempprcpfcst/`. Cities outside any probability polygon are assigned Equal Chances (33.33%).

Series stored per city (region = NWS city key, see above):

| series_name | Unit | Description |
|-------------|------|-------------|
| `cpc_6_10_prob_below` | % | Probability of below-normal temperatures, 6-10 day window |
| `cpc_6_10_prob_above` | % | Probability of above-normal temperatures, 6-10 day window |
| `cpc_8_14_prob_below` | % | Probability of below-normal temperatures, 8-14 day window |
| `cpc_8_14_prob_above` | % | Probability of above-normal temperatures, 8-14 day window |

Equal Chances (EC) = 33.33% for all categories; only the dominant category polygon is published by CPC. The collector assigns the dominant probability to its category and EC to the others.

---

### Source: `nws`

NWS 7-day forecasts for 8 cities. Two API calls per city (`/points` → `/forecast`). Only daytime periods stored. Archive snapshots saved daily to `data/forecasts_archive/` for revision delta computation.

Cities and population weights (weights sum to 1.00):

| City | `region` key | Weight |
|------|-------------|--------|
| New York, NY | `new_york` | 0.25 |
| Chicago, IL | `chicago` | 0.15 |
| Philadelphia, PA | `philadelphia` | 0.12 |
| Boston, MA | `boston` | 0.10 |
| Houston, TX | `houston` | 0.10 |
| Minneapolis, MN | `minneapolis` | 0.10 |
| Detroit, MI | `detroit` | 0.10 |
| Atlanta, GA | `atlanta` | 0.08 |

Series stored per city (region = city key):

| series_name | Unit | Description |
|-------------|------|-------------|
| `forecast_temp_f` | °F | Daytime high temperature |
| `forecast_temp_c` | °C | Same, Celsius |
| `forecast_hdd_65` | HDD | Heating degree days, base 65°F |
| `forecast_cdd_65` | CDD | Cooling degree days, base 65°F |
| `forecast_hdd_wtd` | HDD | HDD × city population weight |
| `forecast_cdd_wtd` | CDD | CDD × city population weight |

`observation_time` = forecast period `startTime` from the NWS API response.

---

### Source: `eia_930`

EIA Form EIA-930 hourly gas-fired electric generation. 72-hour lookback covers the ~48h reporting lag.

| series_name | region | Unit | Description |
|-------------|--------|------|-------------|
| `gas_fired_gen_mw` | BA code | MW | Hourly gas-fired generation for balancing authority |

Balancing authorities: `ERCO` (ERCOT/Texas), `MISO`, `PJM`, `SWPP` (SPP), `SOCO` (Southeast), `NYIS`, `ISNE`, `CISO` (California).

Period stored as `YYYY-MM-DDTHH:00:00Z`.

---

### Source: `cftc`

CFTC disaggregated commitments of traders, commodity code `023651` (NYMEX Henry Hub Natural Gas). Published Friday ~3:30pm ET for the prior Tuesday.

| series_name | Unit | Description |
|-------------|------|-------------|
| `cot_mm_long` | contracts | Managed money gross long |
| `cot_mm_short` | contracts | Managed money gross short |
| `cot_mm_spreading` | contracts | Managed money spreading |
| `cot_prod_long` | contracts | Producer/merchant gross long |
| `cot_prod_short` | contracts | Producer/merchant gross short |
| `cot_swap_long` | contracts | Swap dealer gross long |
| `cot_swap_short` | contracts | Swap dealer gross short |
| `cot_open_interest` | contracts | Total open interest |

---

## features_daily

Engineered features computed from `facts_time_series`. One row per `(feature_date, feature_name, region)`. Region is `US` for all features below.

| feature_name | Unit | Interpretation logic | Trading meaning |
|-------------|------|---------------------|-----------------|
| `storage_total_bcf` | Bcf | neutral | Current total storage level |
| `storage_wow_change_bcf` | Bcf | neutral | Week-over-week draw (negative) or injection |
| `storage_deficit_vs_5yr_bcf` | Bcf | Negative = bullish (below avg), positive = bearish | Key market-moving number |
| `storage_deficit_vs_py_bcf` | Bcf | Same thresholds as 5yr deficit | Year-ago comparison |
| `storage_eos_projection_bcf` | Bcf | vs comfortable range: withdrawal 1700–2000, injection 3500–3800 | Where storage will land at season end |
| `storage_eos_deficit_vs_norm_bcf` | Bcf | Higher = more bullish | EOS trajectory as deficit vs range midpoint |
| `storage_weeks_remaining` | weeks | neutral | Weeks until end of injection/withdrawal season |
| `storage_avg_weekly_pace_bcf` | Bcf/wk | neutral | 4-week rolling average draw/injection rate |
| `ng_price_current` | USD/MMBtu | vs recent FRED range | Latest NG=F close or FRED spot |
| `ng_price_daily_chg_pct` | % | >+3% bullish, >+1.5% mildly bullish; mirror negative | Daily price momentum |
| `ng_price_weekly_chg_pct` | % | Same thresholds | Weekly price momentum |
| `ng_price_monthly_chg_pct` | % | Same thresholds | Monthly price momentum |
| `ng_nov_jan_spread` | USD/MMBtu | Negative = winter premium (bullish) | Winter vs summer spread |
| `ng_12m_strip_avg` | USD/MMBtu | neutral | Average price across next 12 months |
| `ng_strip_vs_spot` | USD/MMBtu | neutral | Contango (+) or backwardation (−) |
| `weather_hdd_7d_weighted` | HDD | >75 bullish, >50 mildly bullish, >25 neutral, >10 mildly bearish, else bearish | Demand signal for next 7 days |
| `weather_hdd_revision_delta` | HDD | >4 bullish, >1 mildly bullish, <−1 mildly bearish, <−4 bearish | Did the forecast get colder or warmer overnight? |
| `cpc_6_10_weighted_prob_below` | % | ≥50 bullish, ≥40 mildly bullish, neutral, ≤28 mildly bearish, ≤20 bearish | Population-weighted probability of below-normal temps, 6-10 day window |
| `cpc_8_14_weighted_prob_below` | % | Same thresholds as 6-10 | Population-weighted probability of below-normal temps, 8-14 day window |
| `cot_mm_net_contracts` | contracts | neutral | Managed money net long (long − short) |
| `cot_mm_net_pct_oi` | % of OI | Contrarian: <−20% bullish (crowded short), >+20% bearish (crowded long) | Spec positioning extremes |
| `cot_mm_net_wow` | contracts | neutral | Weekly change in MM net |
| `cot_open_interest` | contracts | neutral | Total market open interest |
| `dry_gas_production_bcfd` | BCF/d | neutral | EIA monthly marketed dry gas production (region=US, ~2-month lag) |
| `canada_imports_bcfd` | BCF/d | neutral | Total US pipeline imports, ~99% from Canada (EIA monthly) |
| `power_burn_bcfd` | BCF/d | neutral | Gas-fired power sector consumption — EIA-930 hourly primary, EIA monthly fallback |
| `mexico_pipeline_exports_bcfd` | BCF/d | neutral | US pipeline exports to Mexico (EIA monthly) |
| `power_demand_stress_index` | 0–100 | >70 bullish (high burn), <30 bearish | Composite ISO LMP stress index, region=US |
| `lmp_stress_score` | z-score | >1.5 elevated, <−1.5 slack | Per-ISO LMP z-score vs 30-day trailing mean; region = ISO code (e.g. PJM, ERCOT) |
| `lng_implied_exports_bcfd` | BCF/d | >12 bullish, <8 bearish | AIS-derived or EIA-fallback LNG export rate |
| `lng_terminal_utilization_pct` | % | >85 bullish, <60 bearish | AIS: implied exports ÷ nameplate capacity (skipped on EIA fallback) |
| `lng_export_pressure_index` | 0–100 | ≥70 bullish (strong export pull), <40 bearish | Composite: 65% utilization + 35% queue pressure |
| `lng_queue_depth` | vessels | >0 = amber; indicates loading backlog | Total anchored ships across all terminals |
| `lng_destination_eu_pct` | % | High = geopolitically supportive | Share of vessels bound for EU regasification terminals |
| `fairvalue_mid` | USD/MMBtu | Model fair value midpoint | OLS estimate (or lookup table fallback); interpretation: price vs. mid |
| `fairvalue_low` | USD/MMBtu | 5th percentile confidence bound | OLS: intercept − 1.645×sigma; lookup: bin floor |
| `fairvalue_high` | USD/MMBtu | 95th percentile confidence bound | OLS: intercept + 1.645×sigma; lookup: bin ceiling |
| `fairvalue_gap` | USD/MMBtu | Positive = overvalued (bearish), negative = undervalued (bullish) | Current price minus `fairvalue_mid`; labeled FAIRLY_PRICED if \|gap\| < sigma |

**Interpretation values:** `very_bullish`, `bullish`, `mildly_bullish`, `neutral`, `fairly_priced`, `mildly_bearish`, `bearish`, `very_bearish`, `unknown`

**Confidence values:** `high` = recent complete data; `medium` = projection with uncertainty; `low` = fewer than 4 years of history or EIA fallback mode

---

## summary_outputs

| summary_type | content format | Description |
|-------------|---------------|-------------|
| `fundamental_score` | `{"score": float, "label": str, "drivers": [str]}` | Composite score −100 to +100, human label, top driver bullets |
| `what_changed` | Array of change objects | Feature deltas vs prior day, sorted by significance |

### Fundamental score weights

| Component | Max contribution | Feature |
|-----------|-----------------|---------|
| Storage deficit vs 5yr avg | ±20 pts | `storage_deficit_vs_5yr_bcf` |
| EOS trajectory | ±20 pts | `storage_eos_projection_bcf` |
| 7-day weather HDD | ±15 pts | `weather_hdd_7d_weighted` |
| Forecast revision | ±15 pts | `weather_hdd_revision_delta` |
| Dry gas production | ±10 pts | `dry_gas_production_bcfd` |
| COT positioning (contrarian) | ±10 pts | `cot_mm_net_pct_oi` |
| LNG exports | ±10 pts | `lng_exports_bcf` |

Score labels: Strongly Bullish (>40), Bullish (>20), Mildly Bullish (>5), Neutral/Mixed (>−5), Mildly Bearish (>−20), Bearish (>−40), Strongly Bearish.

---

## collector_health

| Column | Description |
|--------|-------------|
| `source_name` | Matches the collector's `source_name` attribute |
| `last_attempt` | Timestamp of most recent run attempt |
| `last_success` | Timestamp of last successful run |
| `last_status` | `ok` or `error` |
| `consecutive_failures` | Incremented on error, reset to 0 on success |
| `error_message` | Last error string if applicable |

---

## API Response Shapes

All history arrays are sorted newest-first.

### GET /api/storage

```json
{
  "features": [
    {"name": "storage_total_bcf", "value": 1850.2, "interpretation": "mildly_bullish",
     "confidence": "high", "computed_at": "..."}
  ],
  "five_year_band": {"avg_bcf": 2100.0, "max_bcf": 2400.0, "min_bcf": 1750.0, "week_ending": "2026-03-01"},
  "latest_week_ending": "2026-03-01",
  "history": [
    {"week_ending": "2026-03-01", "total_bcf": 1850.2,
     "avg_5yr_bcf": 2100.0, "max_5yr_bcf": 2400.0, "min_5yr_bcf": 1750.0}
  ]
}
```

### GET /api/supply

Returns up to 12 months of history per EIA monthly series and up to 104 weeks for Baker Hughes rig count.

```json
{
  "series": [
    {
      "name": "dry_gas_production_mmcf",
      "unit": "MMcf",
      "latest": {"value": 3412000.0, "period_date": "2026-01-01", "ingest_time": "..."},
      "history": [
        {"period_date": "2026-01-01", "value": 3412000.0},
        {"period_date": "2025-12-01", "value": 3398000.0}
      ]
    },
    {
      "name": "total_imports_mmcf",
      "unit": "MMcf",
      "latest": {"value": 280000.0, "period_date": "2025-12-01", "ingest_time": "..."},
      "history": [...]
    },
    {
      "name": "total_pipeline_exports_mmcf",
      "unit": "MMcf",
      "latest": {"value": 420000.0, "period_date": "2025-12-01", "ingest_time": "..."},
      "history": [...]
    },
    {
      "name": "ng_rig_count",
      "unit": "rigs",
      "latest": {"value": 102.0, "period_date": "2026-03-07", "ingest_time": "..."},
      "history": [
        {"period_date": "2026-03-07", "value": 102.0},
        {"period_date": "2026-02-28", "value": 104.0}
      ]
    }
  ]
}
```

Series order: `dry_gas_production_mmcf`, `lng_exports_mmcf`, `power_sector_burn_mmcf`, `mexico_pipeline_exp_mmcf`, `total_imports_mmcf`, `total_pipeline_exports_mmcf`, then `ng_rig_count` (appended last).

### GET /api/weather

```json
{
  "summary": {"hdd_7d_weighted": 62.4, "cdd_7d_weighted": 0.0,
              "hdd_revision_delta": 3.1, "data_date": "2026-03-08", "computed_at": "..."},
  "cities": [
    {"city": "chicago", "hdd_7d": 85.0, "cdd_7d": 0.0, "high_temp_f": 28.0, "data_date": "..."}
  ],
  "history": [
    {"date": "2026-03-08", "hdd_7d_weighted": 62.4, "cdd_7d_weighted": 0.0}
  ],
  "cpc_outlook": {
    "6_10_day": {
      "weighted_prob_below": 62.1,
      "interpretation": "bullish",
      "fcst_date": "2026-03-08"
    },
    "8_14_day": {
      "weighted_prob_below": 44.7,
      "interpretation": "mildly_bullish",
      "fcst_date": "2026-03-08"
    }
  }
}
```

`cpc_outlook` windows may be `null` if no CPC data has been collected yet. `weighted_prob_below` is a population-weighted average across the 8 NWS cities. Interpretation thresholds: ≥50 bullish, ≥40 mildly bullish, neutral, ≤28 mildly bearish, ≤20 bearish.

### GET /api/score

```json
{
  "summary_date": "2026-03-14",
  "score": 16.5,
  "label": "Mildly Bullish",
  "drivers": ["EOS trajectory 1574 Bcf — below comfortable range"],
  "what_changed": [
    {"feature": "storage_eos_projection_bcf", "delta": 264.5, "direction": "up"}
  ],
  "generated_at": "...",
  "history": [
    {"date": "2026-03-14", "score": 16.5, "label": "Mildly Bullish"},
    {"date": "2026-03-13", "score": 14.2, "label": "Mildly Bullish"}
  ],
  "fair_value": {
    "mid": 3.15,
    "low": 2.26,
    "high": 6.10,
    "gap": -0.02,
    "interpretation": "fairly_priced",
    "confidence": "medium",
    "history": [
      {
        "date": "2026-03-14",
        "mid": 3.15,
        "low": 2.26,
        "high": 6.10,
        "gap": -0.02,
        "price": 3.13
      }
    ]
  }
}
```

`fair_value` is `null` (field omitted) until `transforms/features_fairvalue.py` has run at least once. `confidence` is `"medium"` in lookup-table mode and `"high"` after OLS refit. The `history` array covers up to 90 days, joined with actual Henry Hub prices from `facts_time_series`.

### GET /api/price

```json
{
  "history": [
    {"date": "2026-03-08", "open": 4.21, "high": 4.35, "low": 4.18, "close": 4.30, "volume": 152000}
  ],
  "forward_curve": [
    {"ticker": "ngj26", "price": 4.30, "obs_time": "..."}
  ],
  "spot_history": [
    {"date": "2026-03-07", "price": 4.28}
  ],
  "heating_oil_history": [
    {"date": "2026-03-07", "price": 2.51}
  ]
}
```

`spot_history` = Henry Hub (FRED `DHHNGSP`), up to 90 days. `heating_oil_history` = NY No. 2 Heating Oil (FRED `DHOILNYH`), up to 90 days.

### GET /api/cot

```json
{
  "features": [
    {"name": "cot_mm_net_contracts", "value": -45000.0, "interpretation": "bullish", "computed_at": "..."}
  ],
  "history": [
    {"report_date": "2026-03-04", "mm_net": -45000.0, "mm_net_pct_oi": -18.2, "open_interest": 247000.0}
  ]
}
```

### GET /api/health

```json
{
  "db_ok": true,
  "collectors": [
    {"source_name": "eia_storage", "last_attempt": "...", "last_success": "...",
     "last_status": "ok", "consecutive_failures": 0, "error_message": null}
  ],
  "server_time": "2026-03-08T14:00:00Z"
}
```

Returns HTTP 503 (with `db_ok: false`) if the database is unreachable.
