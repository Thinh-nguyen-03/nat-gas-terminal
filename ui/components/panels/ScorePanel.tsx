'use client'

import { useMemo } from 'react'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts'
import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import { SignalBadge } from '@/components/ui/SignalBadge'
import { scoreColor } from '@/lib/signals'
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
  // Fallback: strip underscores, title-case
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

  const chartData = useMemo(() => {
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
        {/* Headline */}
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

        {/* Drivers */}
        {data?.drivers && data.drivers.length > 0 && (
          <div
            className="text-xs shrink-0"
            style={{ borderTop: '1px solid #1e2433', paddingTop: '6px' }}
          >
            <div
              className="uppercase tracking-wider mb-1"
              style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: '10px' }}
            >
              Key Drivers
            </div>
            <ul className="flex flex-col gap-0.5">
              {data.drivers.slice(0, 4).map((d, i) => (
                <li key={i} className="flex gap-1.5 items-start" style={{ color: '#94a3b8' }}>
                  <span style={{ color: '#22d3ee', flexShrink: 0 }}>›</span>
                  <span style={{ fontFamily: 'Inter, sans-serif' }}>{d}</span>
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* Chart */}
        <div className="flex-1 min-h-0" style={{ minHeight: 80, maxHeight: 140 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 4, right: 0, left: -20, bottom: 0 }}>
              <defs>
                <linearGradient id="scoreGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#22d3ee" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#22d3ee" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="date"
                tick={{ fill: '#94a3b8', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
                axisLine={{ stroke: '#1e2433' }}
                tickLine={false}
                interval="preserveStartEnd"
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
                formatter={(value: number) => [fmt(value, 1), 'Score']}
              />
              <ReferenceLine y={0} stroke="#1e2433" strokeDasharray="3 3" />
              <Area
                type="monotone"
                dataKey="score"
                stroke="#22d3ee"
                strokeWidth={1.5}
                fill="url(#scoreGrad)"
                dot={false}
                activeDot={{ r: 3, fill: '#22d3ee' }}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>
    </PanelShell>
  )
}

function fmtSign(v: number): string {
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(1)}`
}
