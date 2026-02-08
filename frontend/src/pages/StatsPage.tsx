import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { MetricCard } from '@/components/common/MetricCard'
import { DonutChart } from '@/components/charts/DonutChart'
import { BarChart } from '@/components/charts/BarChart'
import { formatNumber, formatPercentage } from '@/lib/formatters'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

export function StatsPage() {
  const summaryQuery = useQuery({
    queryKey: ['stats-summary'],
    queryFn: () => api.getStatsSummary(30),
  })

  const trendsQuery = useQuery({
    queryKey: ['weekly-trends'],
    queryFn: () => api.getWeeklyTrend(12),
  })

  const breakdownQuery = useQuery({
    queryKey: ['mapping-breakdown'],
    queryFn: () => api.getMappingBreakdown(30),
  })

  const dailyQuery = useQuery({
    queryKey: ['daily-stats'],
    queryFn: () => api.getDailyStats(14),
  })

  const summary = summaryQuery.data
  const trends = trendsQuery.data?.weekly_trends || []
  const breakdown = breakdownQuery.data?.breakdown || []
  const dailyStats = dailyQuery.data?.daily_stats || []

  return (
    <div className="space-y-6">
      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-4">
        <MetricCard
          title="Total Processed"
          value={summary ? formatNumber(summary.total_statements) : '-'}
          subtitle="Last 30 days"
        />
        <MetricCard
          title="Dimension Matches"
          value={summary ? formatNumber(summary.total_dimension_matches) : '-'}
          subtitle={summary ? formatPercentage(summary.avg_dimension_percentage) : ''}
        />
        <MetricCard
          title="Match Rate"
          value={summary ? formatPercentage(summary.avg_match_rate) : '-'}
          subtitle="Average"
        />
        <MetricCard
          title="API Calls"
          value={summary ? formatNumber(summary.total_api_calls) : '-'}
          subtitle={`${summary?.total_errors || 0} errors`}
        />
      </div>

      <div className="grid grid-cols-2 gap-6">
        {/* Mapping Breakdown Donut */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">Mapping Breakdown</CardTitle>
          </CardHeader>
          <CardContent>
            {breakdown.length > 0 ? (
              <DonutChart
                data={breakdown.map(b => ({ name: b.source, value: b.count }))}
                centerValue={breakdownQuery.data?.total?.toLocaleString()}
                centerLabel="Total"
              />
            ) : (
              <p className="text-sm text-gray-400 text-center py-8">No data available</p>
            )}
          </CardContent>
        </Card>

        {/* Weekly Trend Bar Chart */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">Weekly Trend</CardTitle>
          </CardHeader>
          <CardContent>
            {trends.length > 0 ? (
              <BarChart
                data={trends.map(t => ({
                  week: t.week_start.slice(5),
                  matched: t.tagged_statements,
                  unmatched: t.unmatched_statements,
                }))}
                bars={[
                  { dataKey: 'matched', label: 'Matched', color: '#6366F1' },
                  { dataKey: 'unmatched', label: 'Unmatched', color: '#F43F5E' },
                ]}
                xAxisKey="week"
                height={220}
              />
            ) : (
              <p className="text-sm text-gray-400 text-center py-8">No data available</p>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Daily Stats Table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Daily Statistics</CardTitle>
        </CardHeader>
        <CardContent>
          {dailyStats.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Date</TableHead>
                  <TableHead>Total</TableHead>
                  <TableHead>Tagged</TableHead>
                  <TableHead>Dim Matches</TableHead>
                  <TableHead>Unmatched</TableHead>
                  <TableHead>Match Rate</TableHead>
                  <TableHead>API Calls</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {dailyStats.map((stat) => (
                  <TableRow key={stat.stat_date}>
                    <TableCell className="font-medium">{stat.stat_date}</TableCell>
                    <TableCell>{stat.total_statements.toLocaleString()}</TableCell>
                    <TableCell>{stat.tagged_statements.toLocaleString()}</TableCell>
                    <TableCell>{stat.dimension_matches.toLocaleString()}</TableCell>
                    <TableCell>{stat.unmatched_statements.toLocaleString()}</TableCell>
                    <TableCell>{stat.match_rate.toFixed(1)}%</TableCell>
                    <TableCell>{stat.api_calls}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <p className="text-sm text-gray-400 text-center py-8">No data available</p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
