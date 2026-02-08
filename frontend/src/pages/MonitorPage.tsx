import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Activity, Wifi, WifiOff } from 'lucide-react'
import { api } from '@/lib/api'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import type { AgentStatus } from '@/types'

export function MonitorPage() {
  const [status, setStatus] = useState<AgentStatus | null>(null)
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    const es = api.subscribeToStatus((newStatus) => {
      setStatus(newStatus)
      setConnected(true)
    })

    es.onerror = () => setConnected(false)
    es.onopen = () => setConnected(true)

    return () => es.close()
  }, [])

  const statusQuery = useQuery({
    queryKey: ['agent-status'],
    queryFn: () => api.getStatus(),
    refetchInterval: connected ? false : 5000,
  })

  const currentStatus = status || statusQuery.data

  const stateColor = (state: string) => {
    switch (state?.toLowerCase()) {
      case 'idle': return 'secondary'
      case 'complete': return 'success'
      case 'error': return 'error'
      case 'cancelled': return 'warning'
      default: return 'default'
    }
  }

  const progressPercent = currentStatus
    ? currentStatus.total_steps > 0
      ? Math.round((currentStatus.step / currentStatus.total_steps) * 100)
      : currentStatus.sub_progress || 0
    : 0

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <div className={`h-3 w-3 rounded-full ${connected ? 'bg-emerald-500 animate-pulse' : 'bg-gray-300'}`} />
        <span className="text-sm text-gray-500">
          {connected ? 'Connected (live)' : 'Disconnected (polling)'}
        </span>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2">
              <Activity className="h-4 w-4" /> Agent Status
            </CardTitle>
            {currentStatus && (
              <Badge variant={stateColor(currentStatus.state) as 'default' | 'secondary' | 'destructive'}>
                {currentStatus.state}
              </Badge>
            )}
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {currentStatus ? (
            <>
              <div>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-gray-600">{currentStatus.message || 'Waiting...'}</span>
                  <span className="text-sm font-medium">{progressPercent}%</span>
                </div>
                <Progress value={progressPercent} />
              </div>

              {currentStatus.total_steps > 0 && (
                <p className="text-xs text-gray-400">
                  Step {currentStatus.step} of {currentStatus.total_steps}
                </p>
              )}

              {currentStatus.stats && Object.keys(currentStatus.stats).length > 0 && (
                <div className="grid grid-cols-3 gap-3 mt-4">
                  {Object.entries(currentStatus.stats).map(([key, value]) => (
                    <div key={key} className="p-3 bg-gray-50 rounded-lg">
                      <p className="text-xs text-gray-500">{key.replace(/_/g, ' ')}</p>
                      <p className="text-lg font-bold">{typeof value === 'number' ? value.toLocaleString() : value}</p>
                    </div>
                  ))}
                </div>
              )}
            </>
          ) : (
            <p className="text-sm text-gray-400">No status data available</p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
