'use client'

import { Sidenav } from '@/components/sidenav/Sidenav'
import { useKBStore } from '@/stores'

export function AppShell({ children }: { children: React.ReactNode }) {
  const knowledgeBases = useKBStore((s) => s.knowledgeBases)
  const loading = useKBStore((s) => s.loading)
  const showSidenav = !loading && knowledgeBases.length > 0

  return (
    <div className="flex h-screen overflow-hidden">
      {showSidenav && <Sidenav />}
      <main className="flex-1 overflow-y-auto">{children}</main>
    </div>
  )
}
