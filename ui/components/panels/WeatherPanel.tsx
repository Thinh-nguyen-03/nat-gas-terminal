'use client'

import React, { useMemo } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import { SignalBadge } from '@/components/ui/SignalBadge'
import { fmt, fmtSign } from '@/lib/fmt'
import type { WeatherResponse, CPCWindow } from '@/lib/types'

const SSE_SOURCES = ['weather', 'feat_weather', 'feat_cpc']

function hddColor(v: number | null): string {
  if (v === null) return '#e2e8f0'
  if (v > 80) return '#93c5fd'   // very cold → blue
  if (v > 30) return '#e2e8f0'   // moderate → white
  return '#fbbf24'               // warm → amber
}

function cityHddColor(v: number | null): string {
  if (v === null) return '#cbd5e1'
  if (v > 30) return '#93c5fd'
  if (v > 10) return '#e2e8f0'
  return '#cbd5e1'
}

function cityTempColor(v: number | null): string {
  if (v === null) return '#94a3b8'
  if (v < 35) return '#93c5fd'   // freezing → blue
  if (v > 75) return '#fbbf24'   // hot → amber
  return '#e2e8f0'
}

function probBelowColor(v: number | null): string {
  if (v === null) return '#94a3b8'
  if (v > 55) return '#4ade80'   // high prob cold → bullish green
  if (v < 40) return '#f87171'   // low prob cold → bearish red
  return '#94a3b8'
}

function CPCBadge({ label, window }: { label: string; window: CPCWindow | null | undefined }) {
  if (!window) return null
  const prob = window.weighted_prob_below
  return (
    <div className="flex flex-col gap-0.5">
      <span
        style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.06em' }}
      >
        {label}
      </span>
      <SignalBadge interpretation={window.interpretation} size="sm" />
      {prob !== null && (
        <span
          className="num"
          style={{ fontSize: 10, color: probBelowColor(prob), fontFamily: 'JetBrains Mono, monospace' }}
        >
          P(Below): {fmt(prob, 0, '%')}
        </span>
      )}
    </div>
  )
}

export function WeatherPanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<WeatherResponse>('/api/weather', SSE_SOURCES)

  const chartData = useMemo(() => {
    if (!data?.history) return []
    return [...data.history].reverse().map((p) => ({
      date: p.date?.slice(5, 10) ?? '',
      hdd: p.hdd_7d_weighted,
      cdd: p.cdd_7d_weighted ?? null,
    }))
  }, [data])

  const summary = data?.summary
  const hdd = summary?.hdd_7d_weighted ?? null
  const demandVsNormal = summary?.demand_vs_normal_bcfd ?? null
  const impliedDemand = summary?.implied_demand_bcfd ?? null
  const revDelta = summary?.hdd_revision_delta ?? null

  return (
    <PanelShell
      title="WEATHER"
      source="NOAA/NWS"
      updatedAt={updatedAt}
      flash={flash}
      loading={loading}
      error={error}
    >
      <div className="flex flex-col h-full p-3 gap-2">
        <div className="flex items-start justify-between gap-2">
          <div>
            <div className="flex items-baseline gap-2">
              <span
                className="text-3xl font-bold num"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: hddColor(hdd) }}
              >
                {fmt(hdd, 0)}
              </span>
              <span style={{ color: '#94a3b8', fontSize: 12 }}>HDD 7d</span>
            </div>
            {demandVsNormal !== null && (
              <div
                className="text-sm num"
                style={{
                  fontFamily: 'JetBrains Mono, monospace',
                  color: demandVsNormal >= 0 ? '#4ade80' : '#f87171',
                  fontWeight: 600,
                }}
              >
                {fmtSign(demandVsNormal, 1, ' BCF/D vs normal')}
              </div>
            )}
            {impliedDemand !== null && (
              <div
                className="text-xs"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#cbd5e1' }}
              >
                Implied demand:{' '}
                <span style={{ color: '#cbd5e1' }}>{fmt(impliedDemand, 1, ' BCF/D')}</span>
              </div>
            )}
          </div>
          {revDelta !== null && (
            <div className="text-xs text-right">
              <div style={{ color: '#cbd5e1', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}>
                HDD REVISION
              </div>
              <span
                className="num"
                style={{
                  fontFamily: 'JetBrains Mono, monospace',
                  color: revDelta > 0 ? '#4ade80' : revDelta < 0 ? '#f87171' : '#94a3b8',
                  fontSize: 13,
                  fontWeight: 600,
                }}
              >
                {fmtSign(revDelta, 1)}
              </span>
            </div>
          )}
        </div>

        {data?.cpc_outlook && (
          <div
            className="flex gap-4"
            style={{ borderTop: '1px solid #1e2433', paddingTop: 6 }}
          >
            <CPCBadge label="6-10 Day" window={data.cpc_outlook['6_10_day']} />
            <CPCBadge label="8-14 Day" window={data.cpc_outlook['8_14_day']} />
          </div>
        )}

        {data?.cities && data.cities.length > 0 && (
          <div style={{ borderTop: '1px solid #1e2433', paddingTop: 6 }}>
            <div
              className="grid text-xs gap-x-2"
              style={{
                gridTemplateColumns: '1fr 40px 50px',
                fontFamily: 'JetBrains Mono, monospace',
              }}
            >
              <span style={{ fontSize: 9, color: '#94a3b8' }}>CITY</span>
              <span className="text-right" style={{ fontSize: 9, color: '#94a3b8' }}>HDD</span>
              <span className="text-right" style={{ fontSize: 9, color: '#94a3b8' }}>HIGH °F</span>
              {data.cities.slice(0, 5).map((city) => (
                <React.Fragment key={city.city}>
                  <span style={{ color: '#cbd5e1', fontFamily: 'JetBrains Mono, monospace' }}>
                    {city.city.replace(/_/g, ' ').toUpperCase()}
                  </span>
                  <span className="text-right num" style={{ color: cityHddColor(city.hdd_7d) }}>
                    {fmt(city.hdd_7d, 0)}
                  </span>
                  <span className="text-right num" style={{ color: cityTempColor(city.high_temp_f) }}>
                    {fmt(city.high_temp_f, 0)}°
                  </span>
                </React.Fragment>
              ))}
            </div>
          </div>
        )}

        <div style={{ flex: 1, minHeight: 180, marginTop: 'auto' }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
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
                width={30}
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
                formatter={(value: number, name: string) => [
                  fmt(value, 0),
                  name === 'hdd' ? 'HDD 7d' : 'CDD 7d',
                ]}
              />
              <Line
                type="monotone"
                dataKey="hdd"
                stroke="#93c5fd"
                strokeWidth={1.5}
                dot={false}
                activeDot={{ r: 3, fill: '#93c5fd' }}
              />
              <Line
                type="monotone"
                dataKey="cdd"
                stroke="#fbbf24"
                strokeWidth={1}
                dot={false}
                activeDot={{ r: 3, fill: '#fbbf24' }}
                strokeDasharray="3 3"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </PanelShell>
  )
}
