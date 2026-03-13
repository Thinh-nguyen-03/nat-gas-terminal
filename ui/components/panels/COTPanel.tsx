'use client'

import { useMemo } from 'react'
import {
  BarChart,
  Bar,
  Cell,
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
import type { COTResponse, COTFeature } from '@/lib/types'

const SSE_SOURCES = ['cftc_cot', 'feat_cot']

function getFeature(features: COTFeature[], name: string): COTFeature | undefined {
  return features.find((f) => f.name === name)
}

export function COTPanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<COTResponse>('/api/cot', SSE_SOURCES)

  const chartData = useMemo(() => {
    if (!data?.history) return []
    return [...data.history].reverse().map((p) => ({
      date: p.report_date?.slice(5, 10) ?? '',
      mm_net: p.mm_net,
      pct_oi: p.mm_net_pct_oi,
    }))
  }, [data])

  const mmNetFeat = data?.features ? getFeature(data.features, 'cot_mm_net_contracts') : null
  const pctOiFeat = data?.features ? getFeature(data.features, 'cot_mm_net_pct_oi') : null

  const mmNet = mmNetFeat?.value ?? null
  const pctOi = pctOiFeat?.value ?? null

  return (
    <PanelShell
      title="CFTC COT"
      source="CFTC"
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
                className="text-2xl font-bold num"
                style={{
                  fontFamily: 'JetBrains Mono, monospace',
                  color: mmNet !== null ? (mmNet >= 0 ? '#4ade80' : '#f87171') : '#94a3b8',
                }}
              >
                {mmNet !== null
                  ? `${mmNet >= 0 ? '+' : ''}${Math.round(mmNet).toLocaleString('en-US')}`
                  : '—'}
              </span>
              <span style={{ color: '#94a3b8', fontSize: 11 }}>contracts</span>
            </div>
            {pctOi !== null && (
              <div
                className="text-sm num"
                style={{
                  fontFamily: 'JetBrains Mono, monospace',
                  color: '#94a3b8',
                }}
              >
                {fmt(pctOi, 1, '% of OI')}
              </div>
            )}
          </div>
          {pctOiFeat && (
            <SignalBadge interpretation={pctOiFeat.interpretation} size="sm" />
          )}
        </div>

        {data?.features && data.features.length > 0 && (
          <div style={{ borderTop: '1px solid #1e2433', paddingTop: 6 }}>
            <div className="flex flex-col gap-0.5">
              {data.features.slice(0, 4).map((f) => (
                <div key={f.name} className="flex justify-between items-center text-xs">
                  <span style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', textTransform: 'uppercase', letterSpacing: '0.05em', fontSize: 10 }}>
                    {f.name.replace('cot_', '').replace(/_/g, ' ')}
                  </span>
                  <span
                    className="num"
                    style={{ fontFamily: 'JetBrains Mono, monospace', color: '#e2e8f0', fontSize: 11 }}
                  >
                    {f.value !== null ? f.value.toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="flex-1 min-h-0" style={{ minHeight: 80 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} margin={{ top: 4, right: 0, left: 4, bottom: 0 }}>
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
                tickFormatter={(v: number) => `${(v / 1000).toFixed(0)}k`}
              />
              <Tooltip
                cursor={{ fill: '#e2e8f0', fillOpacity: 0.06 }}
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
                formatter={(value: number) => [
                  value !== null ? Math.round(value).toLocaleString('en-US') : '—',
                  'MM Net',
                ]}
              />
              <ReferenceLine y={0} stroke="#1e2433" />
              <Bar dataKey="mm_net" radius={0}>
                {chartData.map((entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={(entry.mm_net ?? 0) >= 0 ? '#4ade80' : '#f87171'}
                    fillOpacity={0.7}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </PanelShell>
  )
}
