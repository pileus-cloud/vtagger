import { cn } from '@/lib/utils'
import * as Tooltip from '@radix-ui/react-tooltip'

interface NavItem {
  id: string
  icon: React.ReactNode
  label: string
}

interface SidebarProps {
  items: NavItem[]
  activeItem: string
  onItemClick: (id: string) => void
}

export function Sidebar({ items, activeItem, onItemClick }: SidebarProps) {
  return (
    <Tooltip.Provider delayDuration={200}>
      <aside className="fixed left-0 top-0 z-40 h-screen w-16 border-r border-gray-200 bg-white flex flex-col">
        <div className="flex h-14 items-center justify-center border-b border-gray-200">
          <div className="h-8 w-8 rounded-lg bg-primary-600 flex items-center justify-center">
            <span className="text-sm font-bold text-white">VT</span>
          </div>
        </div>
        <nav className="flex-1 flex flex-col items-center gap-1 py-3">
          {items.map((item) => (
            <Tooltip.Root key={item.id}>
              <Tooltip.Trigger asChild>
                <button
                  onClick={() => onItemClick(item.id)}
                  className={cn(
                    "relative flex h-10 w-10 items-center justify-center rounded-lg transition-colors",
                    activeItem === item.id
                      ? "bg-primary-50 text-primary-600"
                      : "text-gray-500 hover:bg-gray-100 hover:text-gray-700"
                  )}
                >
                  {activeItem === item.id && (
                    <div className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-5 bg-primary-600 rounded-r" />
                  )}
                  {item.icon}
                </button>
              </Tooltip.Trigger>
              <Tooltip.Portal>
                <Tooltip.Content
                  side="right"
                  sideOffset={8}
                  className="z-50 rounded-md bg-gray-900 px-3 py-1.5 text-xs text-white shadow-md animate-in fade-in-0 zoom-in-95"
                >
                  {item.label}
                  <Tooltip.Arrow className="fill-gray-900" />
                </Tooltip.Content>
              </Tooltip.Portal>
            </Tooltip.Root>
          ))}
        </nav>
      </aside>
    </Tooltip.Provider>
  )
}
