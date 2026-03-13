// /api/score
export interface ScoreHistoryPoint {
  date: string
  score: number
  label: string
}
export interface ScoreResponse {
  summary_date: string
  score: number
  label: string
  drivers: string[]
  what_changed: Record<string, unknown>[]
  generated_at: string
  history: ScoreHistoryPoint[]
}

// /api/storage
export interface StorageFeature {
  name: string
  value: number | null
  interpretation: string
  confidence: string
  computed_at: string
}
export interface StorageBand {
  avg_bcf: number | null
  max_bcf: number | null
  min_bcf: number | null
  week_ending: string
}
export interface StorageHistoryPoint {
  week_ending: string
  total_bcf: number | null
  avg_5yr_bcf?: number | null
  max_5yr_bcf?: number | null
  min_5yr_bcf?: number | null
}
export interface StorageConsensus {
  report_date: string
  low_bcf: number | null
  consensus_bcf: number | null
  high_bcf: number | null
  model_estimate_bcf: number | null
  source: string | null
}
export interface StorageSurprisePoint {
  report_date: string
  actual_bcf: number | null
  consensus_bcf: number | null
  surprise_bcf: number | null
}
export interface StorageResponse {
  features: StorageFeature[]
  five_year_band: StorageBand | null
  latest_week_ending: string | null
  consensus: StorageConsensus | null
  surprise_history: StorageSurprisePoint[]
  history: StorageHistoryPoint[]
}

// /api/price
export interface PriceBar {
  date: string
  open: number | null
  high: number | null
  low: number | null
  close: number | null
  volume: number | null
}
export interface CurvePoint {
  ticker: string
  price: number | null
  obs_time: string
}
export interface SpotPoint {
  date: string
  price: number | null
}
export interface LNGArbData {
  ttf_spot_usd_mmbtu: number | null
  ttf_hh_net_back_usd_mmbtu: number | null
  arb_spread_usd_mmbtu: number | null
  interpretation: string | null
  data_date: string | null
}
export interface PriceResponse {
  history: PriceBar[]
  forward_curve: CurvePoint[]
  spot_history: SpotPoint[]
  heating_oil_history: SpotPoint[]
  ttf_history: SpotPoint[]
  lng_arb: LNGArbData | null
}

// /api/weather
export interface WeatherCity {
  city: string
  hdd_7d: number | null
  cdd_7d: number | null
  high_temp_f: number | null
  data_date: string
}
export interface WeatherSummary {
  hdd_7d_weighted: number | null
  cdd_7d_weighted: number | null
  hdd_revision_delta: number | null
  implied_demand_bcfd: number | null
  demand_vs_normal_bcfd: number | null
  data_date: string
  computed_at: string
}
export interface WeatherHistoryPoint {
  date: string
  hdd_7d_weighted: number | null
  cdd_7d_weighted?: number | null
}
export interface CPCWindow {
  weighted_prob_below: number | null
  interpretation: string
  fcst_date: string
}
export interface CPCOutlook {
  '6_10_day': CPCWindow | null
  '8_14_day': CPCWindow | null
}
export interface WeatherResponse {
  summary: WeatherSummary
  cities: WeatherCity[]
  history: WeatherHistoryPoint[]
  cpc_outlook: CPCOutlook
}

// /api/cot
export interface COTFeature {
  name: string
  value: number | null
  interpretation: string
  computed_at: string
}
export interface COTHistoryPoint {
  report_date: string
  mm_net: number | null
  mm_net_pct_oi: number | null
  open_interest: number | null
}
export interface COTResponse {
  features: COTFeature[]
  history: COTHistoryPoint[]
}

// /api/lng
export interface LNGTerminal {
  name: string
  location: string
  capacity_bcfd: number
  ships_loading: number | null
  ships_anchored: number | null
  status: string
  updated_at: string | null
}
export interface LNGSummary {
  implied_exports_bcfd: number | null
  terminal_utilization_pct: number | null
  total_capacity_bcfd: number
  export_pressure_index: number | null
  queue_depth: number | null
  destination_eu_pct: number | null
}
export interface AISVessel {
  mmsi: number
  name: string
  terminal: string
  status: string // 'loading' | 'anchored'
  lat: number
  lon: number
  sog: number
  nav_status: number
  destination: string | null
  draught: number | null
  dwell_minutes: number
  observed_at: string
}
export interface LNGHistoryPoint {
  date: string
  implied_exports_bcfd: number | null
}
export interface LNGResponse {
  data_available: boolean
  updated_at: string | null
  summary: LNGSummary
  terminals: LNGTerminal[]
  vessels: AISVessel[]
  history: LNGHistoryPoint[]
}
export interface LNGVesselsResponse {
  vessels: AISVessel[]
  updated_at: string | null
}

// /api/power
export interface ISOPowerData {
  iso: string
  region: string
  hub_node: string
  lmp_usd_mwh: number | null
  avg_lmp_30d_usd_mwh: number | null
  z_score: number | null
  signal: string
  updated_at: string | null
}
export interface PowerDemandSummary {
  stress_index: number | null
  interpretation: string
  updated_at: string | null
}
export interface PowerHistoryPoint {
  ts: string
  stress_index: number | null
}
export interface PowerResponse {
  data_available: boolean
  summary: PowerDemandSummary
  isos: ISOPowerData[]
  history: PowerHistoryPoint[]
}

// /api/balance
export interface BalanceComponent {
  name: string
  value_bcfd: number | null
  source: string
  updated_at: string | null
}
export interface BalanceSummary {
  total_supply_bcfd: number | null
  total_demand_bcfd: number | null
  net_balance_bcfd: number | null
  implied_weekly_bcf: number | null
  model_estimate_bcf: number | null
  model_error_bcf: number | null
  active_ofo_count: number
}
export interface BalanceResponse {
  updated_at: string | null
  supply: BalanceComponent[]
  demand: BalanceComponent[]
  summary: BalanceSummary
}

// /api/calendar
export interface CalendarEvent {
  id: string
  event_date: string
  event_time_et: string | null
  event_type: string
  description: string
  impact: string | null
  days_until: number
  is_auto: boolean
  notes: string | null
}
export interface CalendarResponse {
  events: CalendarEvent[]
  as_of: string
}

// /api/analogs
export interface AnalogFeatureCompare {
  feature: string
  analog_value: number | null
  current_value: number | null
  matched: boolean
}
export interface AnalogPriceOutcome {
  return_4w_pct: number | null
  return_8w_pct: number | null
  return_12w_pct: number | null
}
export interface Analog {
  rank: number
  period_date: string
  similarity_score: number
  label: string
  features: AnalogFeatureCompare[]
  price_outcome: AnalogPriceOutcome
}
export interface AnalogsResponse {
  computed_at: string | null
  analogs: Analog[]
}

// /api/news
export interface NewsItem {
  id: string
  source: string
  title: string
  url: string | null
  published_at: string | null
  score: number
  sentiment: 'bullish' | 'bearish' | 'neutral'
  tags: string[]
  implication?: string | null
}
export interface NewsResponse {
  items: NewsItem[]
  as_of: string
}

// /api/brief
export interface BriefContent {
  outlook: string
  drivers: string[]
  risk: string
  model: string
  generated_at: string
}
export interface BriefResponse {
  date: string
  content: BriefContent
  as_of: string
}

// /api/health
export interface CollectorStatus {
  source_name: string
  last_attempt: string
  last_success: string | null
  last_status: string
  consecutive_failures: number
  error_message: string | null
}
export interface HealthResponse {
  db_ok: boolean
  collectors: CollectorStatus[]
  server_time: string
}
