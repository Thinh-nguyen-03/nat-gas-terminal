export const SIGNAL_LABELS: Record<string, string> = {
  bullish_mispricing: 'BULLISH MISPRICING',
  bullish: 'BULLISH',
  mildly_bullish: 'MILDLY BULLISH',
  neutral: 'NEUTRAL',
  mildly_bearish: 'MILDLY BEARISH',
  bearish: 'BEARISH',
  bearish_mispricing: 'BEARISH MISPRICING',
  crowded_long: 'CROWDED LONG',
  crowded_short: 'CROWDED SHORT',
  oversold: 'OVERSOLD',
  overbought: 'OVERBOUGHT',
  elevated: 'ELEVATED',
  suppressed: 'SUPPRESSED',
  high: 'HIGH',
  normal: 'NORMAL',
  no_data: 'NO DATA',
  wide: 'WIDE',
  narrow: 'NARROW',
  closed: 'CLOSED',
  below_normal: 'BELOW NORMAL',
  above_normal: 'ABOVE NORMAL',
  near_normal: 'NEAR NORMAL',
}

export function signalLabel(interp: string): string {
  return SIGNAL_LABELS[interp] ?? interp.toUpperCase().replace(/_/g, ' ')
}

export function signalColor(interp: string): string {
  const bull = ['bullish', 'mildly_bullish', 'bullish_mispricing', 'oversold', 'suppressed', 'below_normal']
  const bear = ['bearish', 'mildly_bearish', 'bearish_mispricing', 'overbought', 'elevated', 'above_normal']
  const amber = ['crowded_long', 'crowded_short', 'high', 'wide']
  const neutral = ['neutral', 'near_normal', 'normal']

  if (bull.includes(interp)) return '#4ade80'
  if (bear.includes(interp)) return '#f87171'
  if (amber.includes(interp)) return '#fbbf24'
  if (neutral.includes(interp)) return '#94a3b8'
  return '#94a3b8'
}

export function scoreColor(score: number): string {
  if (score > 20) return '#4ade80'
  if (score > 5) return '#4ade80'
  if (score >= -5) return '#94a3b8'
  if (score >= -20) return '#f87171'
  return '#f87171'
}

export function scoreLabel(score: number): string {
  if (score > 20) return 'BULLISH'
  if (score > 5) return 'MILDLY BULLISH'
  if (score >= -5) return 'NEUTRAL'
  if (score >= -20) return 'MILDLY BEARISH'
  return 'BEARISH'
}
