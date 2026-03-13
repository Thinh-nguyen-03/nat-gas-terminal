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

function CPCBadge({ label, window }: { label: string; window: CPCWindow | null | undefined }) {
  if (!window) return null
  const prob = window.weighted_prob_below
  return (
    <div className="flex flex-col gap-0.5">
      <span
        className="text-xs uppercase tracking-wider"
        style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}
      >
        {label}
      </span>
      <SignalBadge interpretation={window.interpretation} size="sm" />
      {prob !== null && (
        <span
          className="num"
          style={{ fontSize: 10, color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace' }}
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
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#e2e8f0' }}
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
                }}
              >
                {fmtSign(demandVsNormal, 1, ' BCF/D vs normal')}
              </div>
            )}
            {impliedDemand !== null && (
              <div
                className="text-xs"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8' }}
              >
                Implied demand: {fmt(impliedDemand, 1, ' BCF/D')}
              </div>
            )}
          </div>
          {revDelta !== null && (
            <div className="text-xs text-right">
              <div style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}>
                HDD REVISION
              </div>
              <span
                className="num"
                style={{
                  fontFamily: 'JetBrains Mono, monospace',
                  color: revDelta > 0 ? '#4ade80' : revDelta < 0 ? '#f87171' : '#94a3b8',
                  fontSize: 13,
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
                color: '#94a3b8',
              }}
            >
              <span style={{ fontSize: 9 }}>CITY</span>
              <span className="text-right" style={{ fontSize: 9 }}>HDD</span>
              <span className="text-right" style={{ fontSize: 9 }}>HIGH °F</span>
              {data.cities.slice(0, 5).map((city) => (
                <React.Fragment key={city.city}>
                  <span style={{ color: '#cbd5e1', fontFamily: 'JetBrains Mono, monospace' }}>
                    {city.city.replace(/_/g, ' ').toUpperCase()}
                  </span>
                  <span className="text-right num" style={{ color: '#e2e8f0' }}>
                    {fmt(city.hdd_7d, 0)}
                  </span>
                  <span className="text-right num" style={{ color: '#e2e8f0' }}>
                    {fmt(city.high_temp_f, 0)}°
                  </span>
                </React.Fragment>
              ))}
            </div>
          </div>
        )}

        <div className="flex-1 min-h-0" style={{ minHeight: 70 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 4, right: 0, left: -10, bottom: 0 }}>
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
                width={24}
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
