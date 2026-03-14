'use client'

import { useMemo } from 'react'
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import { SignalBadge } from '@/components/ui/SignalBadge'
import { fmt } from '@/lib/fmt'
import type { PriceResponse } from '@/lib/types'

const SSE_SOURCES = ['price', 'feat_price', 'feat_fairvalue']

export function PricePanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<PriceResponse>('/api/price', SSE_SOURCES)

  const priceChartData = useMemo(() => {
    if (!data?.history) return []
    return [...data.history].reverse().map((p) => ({
      date: p.date?.slice(5, 10) ?? '',
      close: p.close,
      high: p.high,
      low: p.low,
    }))
  }, [data])

  // NYMEX natural gas futures month codes → calendar month abbreviations
  const MONTH_CODE: Record<string, string> = {
    F: 'Jan', G: 'Feb', H: 'Mar', J: 'Apr', K: 'May', M: 'Jun',
    N: 'Jul', Q: 'Aug', U: 'Sep', V: 'Oct', X: 'Nov', Z: 'Dec',
  }

  const curveChartData = useMemo(() => {
    if (!data?.forward_curve) return []
    return data.forward_curve.map((p) => {
      const raw = (p.ticker?.replace('NG=F', 'F1').replace(/^NG/, '') ?? '').toUpperCase()
      const monthLetter = raw[0]
      const yearSuffix = raw.slice(1)
      const label = MONTH_CODE[monthLetter] ? `${MONTH_CODE[monthLetter]}'${yearSuffix}` : raw
      return { ticker: label, price: p.price }
    })
  }, [data])

  const latestBar = data?.history?.[0]
  const latestPrice = latestBar?.close ?? null
  const prevPrice = data?.history?.[1]?.close ?? null
  const priceChange = latestPrice !== null && prevPrice !== null ? latestPrice - prevPrice : null
  const priceChangePct = priceChange !== null && prevPrice ? (priceChange / prevPrice) * 100 : null

  const arb = data?.lng_arb ?? null

  return (
    <PanelShell
      title="NG PRICE"
      source="NYMEX"
      updatedAt={updatedAt}
      flash={flash}
      loading={loading}
      error={error}
    >
      <div className="flex flex-col h-full p-3 gap-2">
        <div className="flex items-start justify-between gap-2">
          <div>
            <div className="flex items-baseline gap-3">
              <span
                className="text-3xl font-bold num"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#e2e8f0' }}
              >
                ${fmt(latestPrice, 3)}
              </span>
              {priceChange !== null && (
                <span
                  className="text-sm num"
                  style={{
                    fontFamily: 'JetBrains Mono, monospace',
                    color: priceChange >= 0 ? '#4ade80' : '#f87171',
                  }}
                >
                  {priceChange >= 0 ? '+' : ''}{fmt(priceChange, 3)} ({priceChangePct !== null ? `${priceChangePct >= 0 ? '+' : ''}${fmt(priceChangePct, 2)}%` : ''})
                </span>
              )}
            </div>
            <div
              className="text-xs"
              style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace' }}
            >
              NG Front Month · {latestBar?.date ?? '—'}
            </div>
          </div>
          {arb?.interpretation && (
            <SignalBadge interpretation={arb.interpretation} size="sm" />
          )}
        </div>

        {/* LNG Arb */}
        {arb && (
          <div
            className="flex gap-4"
            style={{
              borderTop: '1px solid #1e2433',
              paddingTop: 6,
              fontFamily: 'JetBrains Mono, monospace',
              color: '#94a3b8',
              fontSize: 10,
            }}
          >
            <span>TTF: <span style={{ color: '#e2e8f0' }}>${fmt(arb.ttf_spot_usd_mmbtu, 3)}</span></span>
            <span>Netback: <span style={{ color: '#e2e8f0' }}>${fmt(arb.ttf_hh_net_back_usd_mmbtu, 3)}</span></span>
            <span>
              Arb:{' '}
              <span style={{ color: (arb.arb_spread_usd_mmbtu ?? 0) >= 0 ? '#4ade80' : '#f87171' }}>
                {arb.arb_spread_usd_mmbtu !== null && arb.arb_spread_usd_mmbtu >= 0 ? '+' : ''}
                ${fmt(arb.arb_spread_usd_mmbtu, 3)}
              </span>
            </span>
          </div>
        )}

        <div className="flex-1 min-h-0" style={{ minHeight: 80 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={priceChartData} margin={{ top: 4, right: 0, left: 4, bottom: 0 }}>
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
                width={42}
                domain={['auto', 'auto']}
                tickFormatter={(v: number) => `$${v.toFixed(2)}`}
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
                formatter={(value: number) => [`$${fmt(value, 3)}`, 'Close']}
              />
              <Line
                type="monotone"
                dataKey="close"
                stroke="#f59e0b"
                strokeWidth={1.5}
                dot={false}
                activeDot={{ r: 3, fill: '#f59e0b' }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Forward Curve */}
        {curveChartData.length > 0 && (
          <div>
            <div
              className="text-xs mb-1 uppercase tracking-wider"
              style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 10 }}
            >
              Forward Curve
            </div>
            <div style={{ height: 165 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={curveChartData} margin={{ top: 4, right: 0, left: 4, bottom: 0 }}>
                  <XAxis
                    dataKey="ticker"
                    tick={{ fill: '#94a3b8', fontSize: 8, fontFamily: 'JetBrains Mono, monospace' }}
                    axisLine={{ stroke: '#1e2433' }}
                    tickLine={false}
                    padding={{ left: 10, right: 4 }}
                  />
                  <YAxis
                    tick={{ fill: '#94a3b8', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
                    axisLine={false}
                    tickLine={false}
                    width={40}
                    domain={[(dataMin: number) => Math.floor(dataMin * 10 - 1) / 10, (dataMax: number) => Math.ceil(dataMax * 10 + 1) / 10]}
                    tickFormatter={(v: number) => `$${v.toFixed(1)}`}
                  />
                  <Tooltip
                    cursor={{ fill: '#e2e8f0', fillOpacity: 0.06 }}
                    content={({ payload }) => {
                      if (!payload?.length) return null
                      const p = payload[0] as { payload?: { ticker?: string }; value?: number }
                      return (
                        <div style={{ backgroundColor: '#141720', border: '1px solid #1e2433', padding: '4px 8px', fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: '#e2e8f0' }}>
                          {p.payload?.ticker} : ${fmt(p.value ?? 0, 3)}
                        </div>
                      )
                    }}
                  />
                  <Bar dataKey="price" fill="#a5b4fc" fillOpacity={0.6} radius={[2, 2, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}
      </div>
    </PanelShell>
  )
}
