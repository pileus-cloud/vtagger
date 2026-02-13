import { useState, useEffect } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Copy, Check, Layers, FlaskConical, Activity, BarChart3, Wrench, Settings2 } from 'lucide-react'
import { api } from '@/lib/api'
import { PageLayout } from '@/components/layout/PageLayout'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { DimensionsPage } from '@/pages/DimensionsPage'
import { SimulationPage } from '@/pages/SimulationPage'
import { MonitorPage } from '@/pages/MonitorPage'
import { StatsPage } from '@/pages/StatsPage'
import { ToolsPage } from '@/pages/ToolsPage'
import { SettingsPage } from '@/pages/SettingsPage'

const NAV_ITEMS = [
  { id: 'dimensions', icon: <Layers className="h-5 w-5" />, label: 'Dimensions' },
  { id: 'simulation', icon: <FlaskConical className="h-5 w-5" />, label: 'Simulation' },
  { id: 'monitor', icon: <Activity className="h-5 w-5" />, label: 'Monitor' },
  { id: 'stats', icon: <BarChart3 className="h-5 w-5" />, label: 'Stats' },
  { id: 'tools', icon: <Wrench className="h-5 w-5" />, label: 'Tools' },
  { id: 'settings', icon: <Settings2 className="h-5 w-5" />, label: 'Settings' },
]

const PAGE_TITLES: Record<string, string> = {
  dimensions: 'Dimensions',
  simulation: 'Simulation',
  monitor: 'Monitor',
  stats: 'Statistics',
  tools: 'Tools',
  settings: 'Settings',
}

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false)
  const [apiKey, setApiKey] = useState('')
  const [isChecking, setIsChecking] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [keyExists, setKeyExists] = useState<boolean | null>(null)
  const [generatedKey, setGeneratedKey] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)
  const [activePage, setActivePage] = useState(() => {
    return localStorage.getItem('vtagger_active_page') || 'dimensions'
  })

  useEffect(() => {
    localStorage.setItem('vtagger_active_page', activePage)
  }, [activePage])

  useEffect(() => {
    const checkAuth = async () => {
      const existingKey = api.getApiKey()
      if (existingKey) {
        try {
          await api.validateKey()
          setIsAuthenticated(true)
          setKeyExists(true)
        } catch {
          api.clearApiKey()
        }
      }
      try {
        const result = await api.checkKeyExists()
        setKeyExists(result.exists)
      } catch {
        setKeyExists(false)
      }
      setIsChecking(false)
    }
    checkAuth()
  }, [])

  const loginMutation = useMutation({
    mutationFn: async (key: string) => {
      api.setApiKey(key)
      await api.validateKey()
    },
    onSuccess: () => {
      setIsAuthenticated(true)
      setError(null)
    },
    onError: (err: Error) => {
      api.clearApiKey()
      setError(err.message)
    },
  })

  const createKeyMutation = useMutation({
    mutationFn: () => api.createKey('web-client'),
    onSuccess: (data) => {
      setGeneratedKey(data.key)
      setKeyExists(true)
      setError(null)
    },
    onError: (err: Error) => {
      setError(err.message)
    },
  })

  const handleCopyKey = () => {
    if (generatedKey) {
      navigator.clipboard.writeText(generatedKey)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  const handleLogin = (e: React.FormEvent) => {
    e.preventDefault()
    if (apiKey.trim()) {
      loginMutation.mutate(apiKey.trim())
    }
  }

  const handleLogout = () => {
    api.clearApiKey()
    setIsAuthenticated(false)
    setApiKey('')
    setGeneratedKey(null)
    setError(null)
  }

  if (isChecking) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-gray-400">Loading...</div>
      </div>
    )
  }

  if (!isAuthenticated) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <Card className="w-full max-w-md">
          <CardHeader className="text-center">
            <div className="mx-auto mb-4 h-12 w-12 rounded-xl bg-primary-600 flex items-center justify-center">
              <span className="text-xl font-bold text-white">VT</span>
            </div>
            <CardTitle className="text-2xl">VTagger</CardTitle>
            <CardDescription>Virtual Tagging Agent</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {generatedKey && (
              <div className="border border-primary-300 rounded-lg overflow-hidden">
                <div className="bg-primary-600 px-3 py-2 flex items-center justify-between">
                  <span className="text-sm font-medium text-white">Your Login Key</span>
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-7 px-2 text-white hover:bg-primary-700"
                    onClick={handleCopyKey}
                  >
                    {copied ? <><Check className="h-3 w-3 mr-1" /> Copied</> : <><Copy className="h-3 w-3 mr-1" /> Copy</>}
                  </Button>
                </div>
                <div className="px-3 py-2 bg-primary-50 overflow-x-auto">
                  <code className="font-mono text-xs text-primary-800 whitespace-nowrap">{generatedKey}</code>
                </div>
              </div>
            )}

            <form onSubmit={handleLogin} className="space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Login Key</label>
                <Input
                  type="password"
                  placeholder="Enter your login key"
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                />
              </div>
              <Button type="submit" className="w-full" disabled={loginMutation.isPending}>
                {loginMutation.isPending ? 'Authenticating...' : 'Login'}
              </Button>
            </form>

            {keyExists === false && !generatedKey && (
              <>
                <div className="relative">
                  <div className="absolute inset-0 flex items-center">
                    <span className="w-full border-t" />
                  </div>
                  <div className="relative flex justify-center text-xs uppercase">
                    <span className="bg-white px-2 text-muted-foreground">First time?</span>
                  </div>
                </div>
                <Button
                  variant="outline"
                  className="w-full"
                  onClick={() => createKeyMutation.mutate()}
                  disabled={createKeyMutation.isPending}
                >
                  {createKeyMutation.isPending ? 'Generating...' : 'Generate Login Key'}
                </Button>
              </>
            )}

            {keyExists === true && !generatedKey && (
              <div className="text-center text-xs text-muted-foreground space-y-1 pt-2">
                <p>Forgot your key? Run:</p>
                <code className="block bg-muted px-2 py-1 rounded font-mono">vtagger show-key</code>
              </div>
            )}

            {error && (
              <div className="p-3 bg-red-50 text-red-700 rounded-md text-sm">
                {error}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    )
  }

  const renderPage = () => {
    switch (activePage) {
      case 'dimensions': return <DimensionsPage />
      case 'simulation': return <SimulationPage />
      case 'monitor': return <MonitorPage />
      case 'stats': return <StatsPage />
      case 'tools': return <ToolsPage />
      case 'settings': return <SettingsPage />
      default: return <DimensionsPage />
    }
  }

  return (
    <PageLayout
      navItems={NAV_ITEMS}
      activePage={activePage}
      pageTitle={PAGE_TITLES[activePage] || 'VTagger'}
      onNavigate={setActivePage}
      onLogout={handleLogout}
    >
      {renderPage()}
    </PageLayout>
  )
}

export default App
