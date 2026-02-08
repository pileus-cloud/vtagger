import { ReactNode } from 'react'
import { Sidebar } from './Sidebar'
import { TopBar } from './TopBar'

interface NavItem {
  id: string
  icon: ReactNode
  label: string
}

interface PageLayoutProps {
  children: ReactNode
  navItems: NavItem[]
  activePage: string
  pageTitle: string
  onNavigate: (id: string) => void
  onLogout: () => void
}

export function PageLayout({ children, navItems, activePage, pageTitle, onNavigate, onLogout }: PageLayoutProps) {
  return (
    <div className="min-h-screen bg-gray-50">
      <Sidebar items={navItems} activeItem={activePage} onItemClick={onNavigate} />
      <div className="pl-16">
        <TopBar title={pageTitle} onLogout={onLogout} />
        <main className="p-6">
          {children}
        </main>
      </div>
    </div>
  )
}
