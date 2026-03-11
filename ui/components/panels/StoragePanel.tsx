'use client'

import { useMemo } from 'react'
import {
  ComposedChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import { SignalBadge } from '@/components/ui/SignalBadge'
import { fmt } from '@/lib/fmt'
import type { StorageResponse, StorageFeature } from '@/lib/types'

const SSE_SOURCES = ['eia_storage', 'eia_storage_stats', 'feat_storage']

function getFeature(features: StorageFeature[], name: string): StorageFeature | undefined {
  return features.find((f) => f.name === name)
}

export function StoragePanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<StorageResponse>('/api/storage', SSE_SOURCES)

  const chartData = useMemo(() => {
    if (!data?.history) return []
    return [...data.history].reverse().map((p) => ({
      date: p.week_ending?.slice(5, 10) ?? '',
      total: p.total_bcf,
      avg5yr: p.avg_5yr_bcf ?? null,
      max5yr: p.max_5yr_bcf ?? null,
      min5yr: p.min_5yr_bcf ?? null,
      band: p.max_5yr_bcf !== null && p.min_5yr_bcf !== null
        ? [p.min_5yr_bcf, p.max_5yr_bcf] as [number, number]
        : null,
    }))
  }, [data])

  const totalFeat = data?.features ? getFeature(data.features, 'storage_total_bcf') : null
  const deficitFeat = data?.features ? getFeature(data.features, 'storage_deficit_vs_5yr_bcf') : null
  const surplus = deficitFeat?.value ?? null
  const surplusSign = surplus !== null ? (surplus > 0 ? '+' : '') : ''

  const confidence = totalFeat?.confidence ?? undefined

  return (
    <PanelShell
      title="EIA STORAGE"
      source="EIA"
      updatedAt={updatedAt}
      confidence={confidence}
      flash={flash}
      loading={loading}
      error={error}
    >
      <div className="flex flex-col h-full p-3 gap-2">
        {/* Headline */}
        <div className="flex items-start justify-between gap-2">
          <div>
            <div className="flex items-baseline gap-2">
              <span
                className="text-3xl font-bold num"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#e2e8f0' }}
              >
                {fmt(totalFeat?.value ?? null, 0, ' BCF')}
              </span>
            </div>
            {surplus !== null && (
              <div
                className="text-sm num"
                style={{
                  fontFamily: 'JetBrains Mono, monospace',
                  color: surplus < 0 ? '#f87171' : '#4ade80',
                }}
              >
                {surplusSign}{fmt(surplus, 0, ' BCF')} vs 5yr avg
              </div>
            )}
            <div className="text-xs mt-1" style={{ color: '#94a3b8' }}>
              Week ending: {data?.latest_week_ending ?? '—'}
            </div>
          </div>
          {totalFeat && (
            <SignalBadge interpretation={totalFeat.interpretation} size="sm" />
          )}
        </div>

        {/* Consensus */}
        {data?.consensus && (
          <div
            className="text-xs flex gap-4"
            style={{
              borderTop: '1px solid #1e2433',
              paddingTop: 6,
              fontFamily: 'JetBrains Mono, monospace',
              color: '#94a3b8',
            }}
          >
            <span>
              Consensus:{' '}
              <span style={{ color: '#e2e8f0' }}>
                {fmt(data.consensus.consensus_bcf, 0, ' BCF')}
              </span>
            </span>
            <span>
              Model:{' '}
              <span style={{ color: '#22d3ee' }}>
                {fmt(data.consensus.model_estimate_bcf, 0, ' BCF')}
              </span>
            </span>
          </div>
        )}

        {/* Chart */}
        <div className="flex-1 min-h-0" style={{ minHeight: 100 }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={chartData} margin={{ top: 4, right: 0, left: -10, bottom: 0 }}>
              <defs>
                <linearGradient id="storageGrad" x1="0" y1="0" x2="0" y2="1">
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
                width={36}
                tickFormatter={(v: number) => v === 0 ? '0' : `${(v / 1000).toFixed(1)}k`}
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
                formatter={(value: number, name: string) => {
                  const labels: Record<string, string> = {
                    total: 'Storage',
                    avg5yr: '5yr Avg',
                    max5yr: '5yr Max',
                    min5yr: '5yr Min',
                  }
                  return [fmt(value, 0, ' BCF'), labels[name] ?? name]
                }}
              />
              {/* 5yr band */}
              <Area
                type="monotone"
                dataKey="max5yr"
                stroke="none"
                fill="#1a1f2e"
                fillOpacity={1}
                dot={false}
                legendType="none"
              />
              <Area
                type="monotone"
                dataKey="min5yr"
                stroke="none"
                fill="#141720"
                fillOpacity={1}
                dot={false}
                legendType="none"
              />
              {/* 5yr avg */}
              <Area
                type="monotone"
                dataKey="avg5yr"
                stroke="#94a3b8"
                strokeWidth={1}
                strokeDasharray="3 3"
                fill="none"
                dot={false}
              />
              {/* Total storage */}
              <Area
                type="monotone"
                dataKey="total"
                stroke="#22d3ee"
                strokeWidth={1.5}
                fill="url(#storageGrad)"
                dot={false}
                activeDot={{ r: 3, fill: '#22d3ee' }}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>
    </PanelShell>
  )
}
