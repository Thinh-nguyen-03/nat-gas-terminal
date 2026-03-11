'use client'

import { useMemo } from 'react'
import {
  LineChart,
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
import { fmt } from '@/lib/fmt'
import type { PowerResponse, ISOPowerData } from '@/lib/types'

const SSE_SOURCES = ['power_burn', 'iso_lmp', 'feat_power_demand']

export function PowerPanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<PowerResponse>('/api/power', SSE_SOURCES)

  const chartData = useMemo(() => {
    if (!data?.history) return []
    return [...data.history].reverse().map((p) => ({
      ts: p.ts?.slice(11, 16) ?? '',
      stress: p.stress_index,
    }))
  }, [data])

  const summary = data?.summary
  const stressIndex = summary?.stress_index ?? null
  const interpretation = summary?.interpretation ?? 'neutral'
  const dataAvailable = data?.data_available ?? true

  return (
    <PanelShell
      title="POWER DEMAND"
      source="ISO/LMP"
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
            ISO LMP DATA PENDING
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
                {fmt(stressIndex, 2)}
              </span>
              <span style={{ color: '#94a3b8', fontSize: 11 }}>stress idx</span>
            </div>
          </div>
          <SignalBadge interpretation={interpretation} size="sm" />
        </div>

        {/* ISO Table */}
        {data?.isos && data.isos.length > 0 && (
          <div style={{ borderTop: '1px solid #1e2433', paddingTop: 6 }}>
            <div
              className="text-xs uppercase tracking-wider mb-1"
              style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}
            >
              ISO LMP
            </div>
            <div className="flex flex-col gap-0.5">
              {data.isos.map((iso: ISOPowerData) => (
                <div key={iso.iso} className="flex items-center justify-between text-xs gap-1">
                  <span
                    style={{
                      color: '#cbd5e1',
                      fontFamily: 'JetBrains Mono, monospace',
                      minWidth: 48,
                    }}
                  >
                    {iso.iso}
                  </span>
                  <div
                    className="flex gap-2 items-center"
                    style={{ fontFamily: 'JetBrains Mono, monospace' }}
                  >
                    <span className="num" style={{ color: '#e2e8f0' }}>
                      ${fmt(iso.lmp_usd_mwh, 1)}
                    </span>
                    <span
                      className="num"
                      style={{
                        color:
                          (iso.z_score ?? 0) > 1.5
                            ? '#f87171'
                            : (iso.z_score ?? 0) < -1.5
                            ? '#4ade80'
                            : '#94a3b8',
                        fontSize: 10,
                      }}
                    >
                      z:{fmt(iso.z_score, 1)}
                    </span>
                    <SignalBadge interpretation={iso.signal} size="sm" />
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Stress Index Chart */}
        <div className="flex-1 min-h-0" style={{ minHeight: 70 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 4, right: 0, left: -10, bottom: 0 }}>
              <XAxis
                dataKey="ts"
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
                formatter={(value: number) => [fmt(value, 2), 'Stress Index']}
              />
              <ReferenceLine y={0} stroke="#1e2433" strokeDasharray="3 3" />
              <Line
                type="monotone"
                dataKey="stress"
                stroke="#fbbf24"
                strokeWidth={1.5}
                dot={false}
                activeDot={{ r: 3, fill: '#fbbf24' }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </PanelShell>
  )
}
