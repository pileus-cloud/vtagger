import { PieChart, Pie, Cell, ResponsiveContainer } from 'recharts'
import { CHART_COLORS } from '@/lib/colors'

interface DonutChartProps {
  data: Array<{ name: string; value: number }>
  centerLabel?: string
  centerValue?: string | number
  height?: number
}

export function DonutChart({ data, centerLabel, centerValue, height = 200 }: DonutChartProps) {
  return (
    <div className="flex items-center gap-6">
      <div className="relative" style={{ width: height, height }}>
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={data}
              cx="50%"
              cy="50%"
              innerRadius="60%"
              outerRadius="80%"
              paddingAngle={2}
              dataKey="value"
              stroke="none"
            >
              {data.map((_, index) => (
                <Cell key={`cell-${index}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
        {(centerLabel || centerValue) && (
          <div className="absolute inset-0 flex flex-col items-center justify-center">
            {centerValue !== undefined && (
              <span className="text-2xl font-bold text-gray-900">{centerValue}</span>
            )}
            {centerLabel && (
              <span className="text-xs text-gray-500">{centerLabel}</span>
            )}
          </div>
        )}
      </div>
      <div className="flex flex-col gap-2">
        {data.map((entry, index) => (
          <div key={entry.name} className="flex items-center gap-2">
            <div
              className="h-3 w-3 rounded-full"
              style={{ backgroundColor: CHART_COLORS[index % CHART_COLORS.length] }}
            />
            <span className="text-sm text-gray-600">{entry.name}</span>
            <span className="text-sm font-medium text-gray-900">{entry.value.toLocaleString()}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
