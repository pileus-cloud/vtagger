import { Card } from '@/components/ui/card'
import { Sparkline } from '@/components/charts/Sparkline'
import { cn } from '@/lib/utils'

interface MetricCardProps {
  title: string
  value: string | number
  subtitle?: string
  trend?: number[]
  trendColor?: string
  icon?: React.ReactNode
  className?: string
}

export function MetricCard({ title, value, subtitle, trend, trendColor, icon, className }: MetricCardProps) {
  return (
    <Card className={cn("p-5", className)}>
      <div className="flex items-start justify-between">
        <div className="space-y-1">
          <p className="text-sm font-medium text-gray-500">{title}</p>
          <p className="text-3xl font-bold text-gray-900 font-heading">{value}</p>
          {subtitle && (
            <p className="text-xs text-gray-400">{subtitle}</p>
          )}
        </div>
        <div className="flex flex-col items-end gap-2">
          {icon && <div className="text-gray-400">{icon}</div>}
          {trend && trend.length > 1 && (
            <Sparkline data={trend} color={trendColor} />
          )}
        </div>
      </div>
    </Card>
  )
}
