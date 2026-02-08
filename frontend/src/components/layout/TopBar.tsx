import { LogOut } from 'lucide-react'
import { Button } from '@/components/ui/button'

interface TopBarProps {
  title: string
  onLogout: () => void
}

export function TopBar({ title, onLogout }: TopBarProps) {
  return (
    <header className="sticky top-0 z-30 h-14 border-b border-gray-200 bg-white/95 backdrop-blur">
      <div className="flex h-full items-center justify-between px-6">
        <div>
          <h1 className="text-lg font-semibold font-heading text-gray-900">{title}</h1>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-400">VTagger v1.0</span>
          <Button variant="ghost" size="sm" onClick={onLogout} className="text-gray-500 hover:text-gray-700">
            <LogOut className="h-4 w-4 mr-1" />
            Logout
          </Button>
        </div>
      </div>
    </header>
  )
}
