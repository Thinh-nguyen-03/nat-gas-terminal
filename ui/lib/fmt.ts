export function fmt(
  v: number | null | undefined,
  decimals: number = 1,
  suffix: string = ''
): string {
  if (v === null || v === undefined) return '—'
  const formatted = v.toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
  return suffix ? `${formatted}${suffix}` : formatted
}

const CT_FMT = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/Chicago',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
})

export function fmtTime(ts: string | Date | null | undefined): string {
  if (!ts) return '—'
  try {
    const d = typeof ts === 'string' ? new Date(ts) : ts
    return CT_FMT.format(d) + ' CT'
  } catch {
    return '—'
  }
}

export function fmtDate(ts: string | null | undefined): string {
  if (!ts) return '—'
  try {
    return ts.slice(0, 10)
  } catch {
    return '—'
  }
}

export function fmtSign(v: number | null | undefined, decimals: number = 1, suffix: string = ''): string {
  if (v === null || v === undefined) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${fmt(v, decimals, suffix)}`
}
