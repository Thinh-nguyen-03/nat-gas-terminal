'use client'

import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import { fmt, fmtSign } from '@/lib/fmt'
import type { BalanceResponse, BalanceComponent } from '@/lib/types'

const SSE_SOURCES = ['feat_supply', 'feat_storage', 'feat_weather', 'feat_lng']

function ComponentRow({ comp }: { comp: BalanceComponent }) {
  return (
    <div className="flex items-center justify-between text-xs gap-1">
      <span
        className="truncate"
        style={{ color: '#cbd5e1', fontFamily: 'JetBrains Mono, monospace', textTransform: 'uppercase', fontSize: 11, letterSpacing: '0.03em' }}
      >
        {comp.name}
      </span>
      <span
        className="num shrink-0"
        style={{ fontFamily: 'JetBrains Mono, monospace', color: '#e2e8f0' }}
      >
        {fmt(comp.value_bcfd, 1)}
      </span>
    </div>
  )
}

export function BalancePanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<BalanceResponse>('/api/balance', SSE_SOURCES)

  const summary = data?.summary
  const netBalance = summary?.net_balance_bcfd ?? null
  const totalSupply = summary?.total_supply_bcfd ?? null
  const totalDemand = summary?.total_demand_bcfd ?? null
  const impliedWeekly = summary?.implied_weekly_bcf ?? null
  const modelEstimate = summary?.model_estimate_bcf ?? null
  const modelError = summary?.model_error_bcf ?? null
  const ofoCount = summary?.active_ofo_count ?? 0

  const netColor = netBalance !== null
    ? netBalance > 0.5
      ? '#4ade80'
      : netBalance < -0.5
      ? '#f87171'
      : '#94a3b8'
    : '#94a3b8'

  return (
    <PanelShell
      title="SUPPLY / DEMAND BALANCE"
      source="EIA/NOAA"
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
                style={{ fontFamily: 'JetBrains Mono, monospace', color: netColor }}
              >
                NET: {netBalance !== null ? fmtSign(netBalance, 1, ' BCF/D') : '—'}
              </span>
            </div>
            {impliedWeekly !== null && (
              <div
                className="text-sm num"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8' }}
              >
                Implied weekly: {fmtSign(impliedWeekly, 1, ' BCF')}
              </div>
            )}
          </div>
          {ofoCount > 0 && (
            <div
              className="text-xs shrink-0"
              style={{ color: '#fbbf24', fontFamily: 'JetBrains Mono, monospace' }}
            >
              {ofoCount} ACTIVE OFO
            </div>
          )}
        </div>

        <div
          className="grid gap-3"
          style={{ gridTemplateColumns: '1fr 1fr', borderTop: '1px solid #1e2433', paddingTop: 6 }}
        >
          <div>
            <div
              className="text-xs uppercase tracking-wider mb-1 flex justify-between"
              style={{ fontFamily: 'JetBrains Mono, monospace', color: '#4ade80', fontSize: 13 }}
            >
              <span>SUPPLY</span>
              <span className="num" style={{ color: '#e2e8f0' }}>
                {fmt(totalSupply, 1)} BCF/D
              </span>
            </div>
            <div className="flex flex-col gap-0.5">
              {(data?.supply ?? []).map((c) => (
                <ComponentRow key={c.name} comp={c} />
              ))}
            </div>
          </div>

          <div>
            <div
              className="text-xs uppercase tracking-wider mb-1 flex justify-between"
              style={{ fontFamily: 'JetBrains Mono, monospace', color: '#f87171', fontSize: 13 }}
            >
              <span>DEMAND</span>
              <span className="num" style={{ color: '#e2e8f0' }}>
                {fmt(totalDemand, 1)} BCF/D
              </span>
            </div>
            <div className="flex flex-col gap-0.5">
              {(data?.demand ?? []).map((c) => (
                <ComponentRow key={c.name} comp={c} />
              ))}
            </div>
          </div>
        </div>

        {/* Model summary */}
        {(modelEstimate !== null || modelError !== null) && (
          <div
            className="flex gap-4 text-xs"
            style={{ borderTop: '1px solid #1e2433', paddingTop: 6 }}
          >
            {modelEstimate !== null && (
              <span style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8' }}>
                Model est:{' '}
                <span style={{ color: '#22d3ee' }}>{fmtSign(modelEstimate, 1, ' BCF')}</span>
              </span>
            )}
            {modelError !== null && (
              <span style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8' }}>
                Error:{' '}
                <span
                  style={{
                    color: Math.abs(modelError) > 5 ? '#f87171' : '#94a3b8',
                  }}
                >
                  {fmtSign(modelError, 1, ' BCF')}
                </span>
              </span>
            )}
          </div>
        )}
      </div>
    </PanelShell>
  )
}
