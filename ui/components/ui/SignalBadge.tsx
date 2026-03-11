import { signalLabel, signalColor } from '@/lib/signals'

interface SignalBadgeProps {
  interpretation: string
  size?: 'sm' | 'md' | 'lg'
}

export function SignalBadge({ interpretation, size = 'md' }: SignalBadgeProps) {
  const label = signalLabel(interpretation)
  const color = signalColor(interpretation)

  const sizeClass = {
    sm: 'text-xs',
    md: 'text-sm',
    lg: 'text-base',
  }[size]

  return (
    <span
      className={`${sizeClass} font-semibold tracking-wider`}
      style={{ fontFamily: 'JetBrains Mono, monospace', color }}
    >
      [ {label} ]
    </span>
  )
}
