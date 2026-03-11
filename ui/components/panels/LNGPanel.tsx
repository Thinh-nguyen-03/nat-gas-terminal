'use client'

import { useMemo } from 'react'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import { fmt } from '@/lib/fmt'
import type { LNGResponse, LNGTerminal, AISVessel } from '@/lib/types'

const SSE_SOURCES = ['lng_vessels', 'feat_lng']

function StatusDot({ status }: { status: string }) {
  const colorMap: Record<string, string> = {
    operational: '#4ade80',
    active:      '#4ade80',
    maintenance: '#fbbf24',
    reduced:     '#fbbf24',
    offline:     '#f87171',
    idle:        '#475569',
  }
  const color = colorMap[status.toLowerCase()] ?? '#94a3b8'
  return (
    <span
      style={{
        display: 'inline-block',
        width: 7,
        height: 7,
        borderRadius: '50%',
        backgroundColor: color,
        flexShrink: 0,
      }}
    />
  )
}

function VesselRow({ vessel }: { vessel: AISVessel }) {
  const hours = vessel.dwell_minutes > 0 ? Math.round(vessel.dwell_minutes / 60) : null
  const dest = vessel.destination?.trim() || null
  const isLoading = vessel.status === 'loading'

  return (
    <div
      className="flex items-center gap-1.5 pl-5"
      style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 9, color: '#94a3b8' }}
    >
      <span style={{ color: isLoading ? '#4ade80' : '#fbbf24', fontSize: 8 }}>└</span>
      <span style={{ color: '#94a3b8', maxWidth: 90, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {vessel.name || `MMSI:${vessel.mmsi}`}
      </span>
      {hours !== null && hours > 0 && (
        <span style={{ color: '#64748b' }}>{hours}h</span>
      )}
      {dest && (
        <span
          style={{ color: '#64748b', maxWidth: 70, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
          title={dest}
        >
          →{dest}
        </span>
      )}
    </div>
  )
}

export function LNGPanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<LNGResponse>('/api/lng', SSE_SOURCES)

  const chartData = useMemo(() => {
    if (!data?.history) return []
    return [...data.history].reverse().map((p) => ({
      date: p.date?.slice(5, 10) ?? '',
      exports: p.implied_exports_bcfd,
    }))
  }, [data])

  const summary   = data?.summary
  const exports   = summary?.implied_exports_bcfd ?? null
  const utilPct   = summary?.terminal_utilization_pct ?? null
  const capacity  = summary?.total_capacity_bcfd ?? null
  const epi       = summary?.export_pressure_index ?? null
  const queue     = summary?.queue_depth ?? null
  const euPct     = summary?.destination_eu_pct ?? null

  const vessels   = data?.vessels ?? []

  const dataAvailable = data?.data_available ?? true

  const epiColor = epi === null ? '#94a3b8'
    : epi >= 70 ? '#4ade80'
    : epi >= 40 ? '#fbbf24'
    : '#f87171'

  return (
    <PanelShell
      title="LNG EXPORTS"
      source="EIA/AIS"
      updatedAt={updatedAt}
      flash={flash}
      loading={loading}
      error={error}
    >
      <div className="flex flex-col h-full p-3 gap-2">
        {!dataAvailable && (
          <div
            className="text-xs font-semibold tracking-widest"
            style={{ color: '#fbbf24', fontFamily: 'JetBrains Mono, monospace' }}
          >
            NO DATA
          </div>
        )}

        {/* Headline */}
        <div className="flex items-start justify-between gap-2">
          <div>
            <div className="flex items-baseline gap-2">
              <span
                className="text-2xl font-bold num"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#e2e8f0' }}
              >
                {fmt(exports, 2, ' BCF/D')}
              </span>
            </div>
            {utilPct !== null && (
              <div
                className="text-sm num"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8' }}
              >
                {fmt(utilPct, 1, '% util')} of {fmt(capacity, 1, ' BCF/D cap')}
              </div>
            )}
          </div>

          {/* EPI + Queue badges */}
          <div className="flex flex-col items-end gap-0.5 shrink-0">
            {epi !== null && (
              <div
                className="text-xs num font-semibold"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: epiColor }}
                title="Export Pressure Index (0-100): weighted composite of utilization and queue depth"
              >
                EPI {fmt(epi, 0)}
              </div>
            )}
            {queue !== null && queue > 0 && (
              <div
                className="text-xs num"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#fbbf24' }}
                title="Ships anchored waiting for a berth — demand exceeds current throughput"
              >
                {queue} QUEUED
              </div>
            )}
            {euPct !== null && (
              <div
                className="text-xs num"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8' }}
                title="% of vessels with known destination bound for European ports"
              >
                EU {fmt(euPct, 0, '%')}
              </div>
            )}
          </div>
        </div>

        {/* Terminal Table with inline vessel manifest */}
        {data?.terminals && data.terminals.length > 0 && (
          <div style={{ borderTop: '1px solid #1e2433', paddingTop: 6 }}>
            {/* Header row with column labels */}
            <div
              className="flex items-center justify-between mb-1"
              style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}
            >
              <span style={{ color: '#475569' }}>TERMINALS</span>
              <div className="flex shrink-0 num" style={{ gap: 0 }}>
                <span className="w-10 text-right" style={{ color: '#4ade80', opacity: 0.6 }}>LOAD</span>
                <span className="w-10 text-right" style={{ color: '#fbbf24', opacity: 0.6 }}>ANCH</span>
                <span className="w-10 text-right" style={{ color: '#475569' }}>CAP</span>
              </div>
            </div>
            <div className="flex flex-col gap-0.5">
              {data.terminals.map((t: LNGTerminal) => {
                const termVessels = vessels.filter((v: AISVessel) => v.terminal === t.name)
                const loadColor = (t.ships_loading ?? 0) > 0 ? '#4ade80' : '#334155'
                const anchColor = (t.ships_anchored ?? 0) > 0 ? '#fbbf24' : '#334155'
                return (
                  <div key={t.name}>
                    {/* Terminal row */}
                    <div className="flex items-center justify-between text-xs gap-1">
                      <div className="flex items-center gap-1.5 min-w-0">
                        <StatusDot status={t.status} />
                        <span
                          className="truncate"
                          style={{ color: '#cbd5e1', fontFamily: 'JetBrains Mono, monospace', textTransform: 'uppercase', fontSize: 10 }}
                        >
                          {t.name}
                        </span>
                      </div>
                      <div
                        className="flex shrink-0 num"
                        style={{ fontFamily: 'JetBrains Mono, monospace', gap: 0 }}
                      >
                        <span className="w-10 text-right" style={{ color: loadColor }} title="Ships at berth loading">
                          {t.ships_loading ?? '—'}
                        </span>
                        <span className="w-10 text-right" style={{ color: anchColor }} title="Ships anchored waiting for berth">
                          {t.ships_anchored ?? '—'}
                        </span>
                        <span className="w-10 text-right" style={{ color: '#475569' }}>
                          {fmt(t.capacity_bcfd, 1)}
                        </span>
                      </div>
                    </div>
                    {/* Per-vessel rows (only shown when AIS data is live) */}
                    {termVessels.map((v: AISVessel) => (
                      <VesselRow key={v.mmsi} vessel={v} />
                    ))}
                  </div>
                )
              })}
            </div>
          </div>
        )}

        {/* Chart */}
        <div style={{ height: 250, flexShrink: 0 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 4, right: 0, left: -10, bottom: 0 }}>
              <defs>
                <linearGradient id="lngGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#22d3ee" stopOpacity={0.2} />
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
                domain={['auto', 'auto']}
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
                formatter={(value: number) => [fmt(value, 2, ' BCF/D'), 'Exports']}
              />
              <Area
                type="monotone"
                dataKey="exports"
                stroke="#22d3ee"
                strokeWidth={1.5}
                fill="url(#lngGrad)"
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
