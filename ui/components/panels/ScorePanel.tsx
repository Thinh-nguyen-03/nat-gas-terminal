'use client'

import { useMemo } from 'react'
import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts'
import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import { SignalBadge } from '@/components/ui/SignalBadge'
import { scoreColor, signalColor } from '@/lib/signals'
import { fmt, fmtDate } from '@/lib/fmt'
import type { ScoreResponse } from '@/lib/types'

const SSE_SOURCES = ['summary']

/** Map raw DB feature keys to readable labels */
const FEATURE_LABELS: Record<string, string> = {
  storage_eos_projection_bcf: 'EOS Projection',
  storage_deficit_vs_5yr_bcf: 'Storage vs 5yr',
  storage_injection_vs_5yr_pct: 'Injection vs 5yr',
  hdd_deviation_7d: 'HDD Deviation 7d',
  hdd_weighted_7d: 'HDD Weighted 7d',
  cdd_weighted_7d: 'CDD Weighted 7d',
  demand_vs_normal_pct: 'Demand vs Normal',
  lng_exports_implied_bcfd: 'LNG Exports',
  lng_utilization_pct: 'LNG Utilization',
  ttf_hh_arb_usd_mmbtu: 'TTF-HH Arb',
  arb_spread_usd_mmbtu: 'Arb Spread',
  mm_net_contracts: 'MM Net Position',
  mm_net_pct_oi: 'MM % of OI',
  mm_net_change: 'MM Net Change',
  price_zscore_90d: 'Price Z-Score 90d',
  price_vs_fair_value_pct: 'Price vs Fair Value',
  power_burn_bcfd: 'Power Burn',
  power_stress_index: 'Power Stress',
  cpc_6_10_outlook: 'CPC 6-10 Day',
  cpc_8_14_outlook: 'CPC 8-14 Day',
  balance_net_bcfd: 'Net Balance',
  supply_total_bcfd: 'Total Supply',
  demand_total_bcfd: 'Total Demand',
  weather_hdd_7d_weighted: 'HDD 7d Weighted',
  weather_cdd_7d_weighted: 'CDD 7d Weighted',
  weather_demand_vs_normal: 'Demand vs Normal',
  ng_price_current: 'NG Front Month',
  ng_price_zscore: 'Price Z-Score',
  ng_price_1d_chg: 'Price 1d Change',
  ng_price_5d_chg: 'Price 5d Change',
  storage_current_bcf: 'Storage (BCF)',
  storage_week_change_bcf: 'Storage Week Chg',
  cot_mm_net_contracts: 'MM Net Contracts',
  cot_mm_net_pct_oi: 'MM % of OI',
  cot_mm_net_wow: 'MM WoW Change',
  cot_open_interest: 'Open Interest',
}

function humanize(key: string): string {
  if (FEATURE_LABELS[key]) return FEATURE_LABELS[key]
  return key
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .replace(/Bcf$/, 'BCF')
    .replace(/Bcfd$/, 'BCF/d')
    .replace(/Pct$/, '%')
    .replace(/Usd/, 'USD')
    .replace(/Mmbtu/, 'MMBtu')
}

export function ScorePanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<ScoreResponse>('/api/score', SSE_SOURCES)

  const fv = data?.fair_value ?? null

  // Build fair value band chart data (oldest → newest for recharts)
  const fvChartData = useMemo(() => {
    if (!fv?.history?.length) return []
    return [...fv.history].reverse().map((p) => ({
      date: p.date.slice(5, 10),
      low: p.low ?? null,
      // stacked on top of low to form the band
      bandSize: p.high != null && p.low != null ? p.high - p.low : null,
      mid: p.mid ?? null,
      price: p.price ?? null,
    }))
  }, [fv])

  // Fallback: score history chart when no fair value data yet
  const scoreChartData = useMemo(() => {
    if (!data?.history) return []
    return [...data.history].reverse().map((p) => ({
      date: p.date.slice(5, 10),
      score: p.score,
    }))
  }, [data])

  const score = data?.score ?? null
  const color = score !== null ? scoreColor(score) : '#94a3b8'
  const label = data?.label ?? 'neutral'
  const whatChanged = data?.what_changed ?? []

  const hasFairValue = fv != null && fv.mid != null
  const fvGap = fv?.gap ?? null
  const fvInterp = fv?.interpretation ?? null
  const gapColor = fvInterp ? signalColor(fvInterp) : '#94a3b8'

  return (
    <PanelShell
      title="COMPOSITE SCORE"
      source="MODEL"
      updatedAt={updatedAt}
      flash={flash}
      loading={loading}
      error={error}
    >
      <div className="flex flex-col h-full p-3 gap-2">
        {/* Score headline */}
        <div className="flex items-baseline justify-between gap-3">
          <div className="flex items-baseline gap-3">
            <span
              className="text-4xl font-bold num"
              style={{ color, fontFamily: 'JetBrains Mono, monospace' }}
            >
              {score !== null ? fmtSign(score) : '—'}
            </span>
            <SignalBadge interpretation={label} size="md" />
          </div>
          <span
            className="text-xs"
            style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace' }}
          >
            {fmtDate(data?.summary_date)}
          </span>
        </div>

        {/* What Changed */}
        {whatChanged.length > 0 && (
          <div
            className="text-xs shrink-0"
            style={{ borderTop: '1px solid #1e2433', paddingTop: '6px' }}
          >
            <div
              className="uppercase tracking-wider mb-1"
              style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: '10px' }}
            >
              What Changed
            </div>
            <div className="flex flex-col gap-0.5">
              {whatChanged.slice(0, 4).map((item, i) => {
                const feature = String(item.feature ?? item.name ?? '')
                const delta = item.delta as number | null ?? null
                const dir = item.direction as string ?? ''
                return (
                  <div key={i} className="flex justify-between items-center">
                    <span style={{ color: '#cbd5e1', fontFamily: 'JetBrains Mono, monospace', textTransform: 'uppercase', fontSize: 10, letterSpacing: '0.04em' }}>
                      {humanize(feature)}
                    </span>
                    <span
                      className="num"
                      style={{
                        color:
                          dir === 'up' || (typeof delta === 'number' && delta > 0)
                            ? '#4ade80'
                            : dir === 'down' || (typeof delta === 'number' && delta < 0)
                            ? '#f87171'
                            : '#94a3b8',
                      }}
                    >
                      {typeof delta === 'number' ? fmtSign(delta) : String(item.value ?? '—')}
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        )}

        {/* Key Drivers */}
        {data?.drivers && data.drivers.length > 0 && (
          <div
            className="shrink-0"
            style={{ borderTop: '1px solid #1e2433', paddingTop: '6px' }}
          >
            <div style={{ color: '#fbbf24', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.1em', fontFamily: 'JetBrains Mono, monospace', marginBottom: 5 }}>
              Key Drivers
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
              {data.drivers.slice(0, 4).map((d, i) => (
                <div key={i} style={{ display: 'flex', gap: 7, alignItems: 'baseline' }}>
                  <span style={{ color: '#22d3ee', flexShrink: 0, fontSize: 11 }}>›</span>
                  <span style={{ color: '#b8c4d0', fontSize: 11, lineHeight: 1.45, fontFamily: 'JetBrains Mono, monospace' }}>{d}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Fair Value Summary */}
        {hasFairValue && (
          <div
            className="shrink-0"
            style={{ borderTop: '1px solid #1e2433', paddingTop: '6px' }}
          >
            <div style={{ color: '#94a3b8', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.1em', fontFamily: 'JetBrains Mono, monospace', marginBottom: 5 }}>
              Fair Value Model
            </div>
            <div className="flex flex-col gap-1">
              <div className="flex justify-between items-baseline">
                <span style={{ color: '#94a3b8', fontSize: 10, fontFamily: 'JetBrains Mono, monospace' }}>
                  MODEL RANGE
                </span>
                <span className="num" style={{ color: '#22d3ee', fontSize: 11 }}>
                  ${fmt(fv.low, 2)} — ${fmt(fv.high, 2)}
                </span>
              </div>
              <div className="flex justify-between items-baseline">
                <span style={{ color: '#94a3b8', fontSize: 10, fontFamily: 'JetBrains Mono, monospace' }}>
                  MID ESTIMATE
                </span>
                <span className="num" style={{ color: '#e2e8f0', fontSize: 11 }}>
                  ${fmt(fv.mid, 2)}/MMBtu
                </span>
              </div>
              {fvGap != null && (
                <div className="flex justify-between items-baseline">
                  <span style={{ color: '#94a3b8', fontSize: 10, fontFamily: 'JetBrains Mono, monospace' }}>
                    GAP
                  </span>
                  <div className="flex items-baseline gap-2">
                    <span className="num" style={{ color: gapColor, fontSize: 11 }}>
                      {fvGap > 0 ? '+' : ''}{fmt(fvGap, 2)}
                    </span>
                    {fvInterp && <SignalBadge interpretation={fvInterp} size="sm" />}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Chart — fair value band + price if available, else score history */}
        <div style={{ flex: 1, minHeight: 200, marginTop: 'auto' }}>
          <ResponsiveContainer width="100%" height="100%">
            {hasFairValue && fvChartData.length > 0 ? (
              <ComposedChart data={fvChartData} margin={{ top: 4, right: 0, left: 4, bottom: 0 }}>
                <XAxis
                  dataKey="date"
                  tick={{ fill: '#94a3b8', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
                  axisLine={{ stroke: '#1e2433' }}
                  tickLine={false}
                  interval="preserveStartEnd"
                  padding={{ left: 10, right: 4 }}
                />
                <YAxis
                  tick={{ fill: '#94a3b8', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
                  axisLine={false}
                  tickLine={false}
                  width={32}
                  tickFormatter={(v) => `$${v.toFixed(1)}`}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#141720',
                    border: '1px solid #1e2433',
                    borderRadius: 0,
                    fontFamily: 'JetBrains Mono, monospace',
                    fontSize: 11,
                    color: '#e2e8f0',
                  }}
                  labelStyle={{ color: '#e2e8f0' }}
                  formatter={(value: number, name: string) => {
                    if (name === 'bandSize') return [`$${value.toFixed(2)}`, 'Band Width']
                    if (name === 'low') return [`$${value.toFixed(2)}`, 'FV Low']
                    if (name === 'mid') return [`$${value.toFixed(2)}`, 'FV Mid']
                    if (name === 'price') return [`$${value.toFixed(2)}`, 'Price']
                    return [value, name]
                  }}
                />
                {/* Transparent base — fills from 0 to low, creating the floor of the band */}
                <Area
                  dataKey="low"
                  stackId="fv"
                  stroke="none"
                  fill="transparent"
                  legendType="none"
                  isAnimationActive={false}
                />
                {/* Cyan band from low → high */}
                <Area
                  dataKey="bandSize"
                  stackId="fv"
                  stroke="#22d3ee"
                  strokeWidth={0.5}
                  strokeOpacity={0.4}
                  fill="#22d3ee"
                  fillOpacity={0.12}
                  legendType="none"
                  isAnimationActive={false}
                />
                {/* Fair value mid — dashed cyan */}
                <Line
                  dataKey="mid"
                  stroke="#22d3ee"
                  strokeWidth={1}
                  strokeDasharray="4 3"
                  dot={false}
                  isAnimationActive={false}
                />
                {/* Actual price — amber */}
                <Line
                  dataKey="price"
                  stroke="#f59e0b"
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                  activeDot={{ r: 3, fill: '#f59e0b' }}
                />
              </ComposedChart>
            ) : (
              // Fallback: score history area chart
              <ComposedChart data={scoreChartData} margin={{ top: 4, right: 0, left: 4, bottom: 0 }}>
                <defs>
                  <linearGradient id="scoreGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#34d399" stopOpacity={0.25} />
                    <stop offset="95%" stopColor="#34d399" stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                <XAxis
                  dataKey="date"
                  tick={{ fill: '#94a3b8', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
                  axisLine={{ stroke: '#1e2433' }}
                  tickLine={false}
                  interval="preserveStartEnd"
                  padding={{ left: 10, right: 4 }}
                />
                <YAxis
                  tick={{ fill: '#94a3b8', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
                  axisLine={false}
                  tickLine={false}
                  width={28}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#141720',
                    border: '1px solid #1e2433',
                    borderRadius: 0,
                    fontFamily: 'JetBrains Mono, monospace',
                    fontSize: 11,
                    color: '#e2e8f0',
                  }}
                  labelStyle={{ color: '#e2e8f0' }}
                  itemStyle={{ color: '#e2e8f0' }}
                  formatter={(value: number) => [fmt(value, 1), 'Score']}
                />
                <ReferenceLine y={0} stroke="#1e2433" strokeDasharray="3 3" />
                <Area
                  type="monotone"
                  dataKey="score"
                  stroke="#34d399"
                  strokeWidth={1.5}
                  fill="url(#scoreGrad)"
                  dot={false}
                  activeDot={{ r: 3, fill: '#34d399' }}
                />
              </ComposedChart>
            )}
          </ResponsiveContainer>
        </div>

        {/* Chart legend */}
        {hasFairValue && fvChartData.length > 0 && (
          <div className="flex gap-4 justify-center shrink-0" style={{ marginTop: -4 }}>
            <div className="flex items-center gap-1">
              <div style={{ width: 16, height: 2, background: '#f59e0b' }} />
              <span style={{ color: '#94a3b8', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}>PRICE</span>
            </div>
            <div className="flex items-center gap-1">
              <div style={{ width: 16, height: 2, background: '#22d3ee', opacity: 0.6, borderTop: '1px dashed #22d3ee' }} />
              <span style={{ color: '#94a3b8', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}>FAIR VALUE</span>
            </div>
          </div>
        )}
      </div>
    </PanelShell>
  )
}

function fmtSign(v: number): string {
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(1)}`
}
