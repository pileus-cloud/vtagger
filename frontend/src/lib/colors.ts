export const CHART_COLORS = [
  '#6366F1', // indigo
  '#8B5CF6', // purple
  '#14B8A6', // teal
  '#3B82F6', // blue
  '#EC4899', // pink
  '#06B6D4', // cyan
  '#F59E0B', // amber
  '#F43F5E', // rose
  '#10B981', // emerald
  '#6B7280', // gray
]

export function getChartColor(index: number): string {
  return CHART_COLORS[index % CHART_COLORS.length]
}
