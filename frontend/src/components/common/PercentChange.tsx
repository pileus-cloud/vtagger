import { TrendingUp, TrendingDown, Minus } from 'lucide-react'
import { cn } from '@/lib/utils'

interface PercentChangeProps {
  value: number
  className?: string
  invertColor?: boolean
}

export function PercentChange({ value, className, invertColor = false }: PercentChangeProps) {
  const isPositive = value > 0
  const isZero = value === 0

  const colorClass = isZero
    ? 'text-gray-500 bg-gray-100'
    : isPositive
      ? invertColor ? 'text-rose-700 bg-rose-100' : 'text-emerald-700 bg-emerald-100'
      : invertColor ? 'text-emerald-700 bg-emerald-100' : 'text-rose-700 bg-rose-100'

  const Icon = isZero ? Minus : isPositive ? TrendingUp : TrendingDown

  return (
    <span className={cn("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-semibold", colorClass, className)}>
      <Icon className="h-3 w-3" />
      {isPositive ? '+' : ''}{value.toFixed(1)}%
    </span>
  )
}
