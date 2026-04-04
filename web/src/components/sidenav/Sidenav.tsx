'use client'

import * as React from 'react'
import { usePathname, useRouter } from 'next/navigation'
import { useTheme } from 'next-themes'
import { useSidebarStore, useKBStore, useUserStore } from '@/stores'
import {
  PanelLeftOpen, PanelLeftClose, Plus, FolderOpen,
  Settings, LogOut, Moon, Sun, Pencil, Trash2,
  ChevronDown, ChevronRight,
} from 'lucide-react'
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  ContextMenu, ContextMenuContent, ContextMenuItem,
  ContextMenuSeparator, ContextMenuTrigger,
} from '@/components/ui/context-menu'
import {
  Dialog, DialogContent, DialogHeader, DialogTitle,
  DialogDescription, DialogFooter, DialogClose,
} from '@/components/ui/dialog'
import { createClient } from '@/lib/supabase/client'
import { cn } from '@/lib/utils'

export function Sidenav() {
  const pathname = usePathname()
  const router = useRouter()
  const { theme, setTheme } = useTheme()
  const [mounted, setMounted] = React.useState(false)

  const expanded = useSidebarStore((s) => s.expanded)
  const toggle = useSidebarStore((s) => s.toggle)

  const user = useUserStore((s) => s.user)
  const signOutLocal = useUserStore((s) => s.signOut)

  const knowledgeBases = useKBStore((s) => s.knowledgeBases)
  const renameKB = useKBStore((s) => s.renameKB)
  const deleteKB = useKBStore((s) => s.deleteKB)
  const createKB = useKBStore((s) => s.createKB)

  const [kbSectionOpen, setKbSectionOpen] = React.useState(true)
  const [renamingId, setRenamingId] = React.useState<string | null>(null)
  const [renameValue, setRenameValue] = React.useState('')
  const [deleteTarget, setDeleteTarget] = React.useState<{ id: string; name: string } | null>(null)
  const [deleteConfirmText, setDeleteConfirmText] = React.useState('')
  const [deleting, setDeleting] = React.useState(false)
  const [createOpen, setCreateOpen] = React.useState(false)
  const [createName, setCreateName] = React.useState('')
  const [creating, setCreating] = React.useState(false)

  React.useEffect(() => {
    setMounted(true)
  }, [])

  const handleSignOut = async () => {
    try {
      const supabase = createClient()
      await supabase.auth.signOut()
      signOutLocal()
      router.push('/login')
    } catch (error) {
      console.error('Error signing out:', error)
    }
  }

  const handleCreateKB = async (e: React.FormEvent) => {
    e.preventDefault()
    const trimmed = createName.trim()
    if (!trimmed) return
    setCreating(true)
    try {
      const kb = await createKB(trimmed)
      setCreateOpen(false)
      setCreateName('')
      router.push(`/kb/${kb.slug}`)
    } catch (err) {
      console.error('Failed to create KB:', err)
    } finally {
      setCreating(false)
    }
  }

  const initials = React.useMemo(() => {
    if (!user?.email) return '?'
    return user.email.slice(0, 2).toUpperCase()
  }, [user?.email])

  return (
    <aside
      className={cn(
        'hidden sm:flex flex-col h-full border-r border-border bg-sidebar text-sidebar-foreground text-sm transition-[width] duration-300 ease-in-out flex-shrink-0',
        expanded ? 'w-56' : 'w-14'
      )}
    >
      <div className="flex items-center justify-between px-3 py-3">
        {expanded && (
          <span className="text-sm font-semibold text-foreground pl-1 truncate">
            Supavault
          </span>
        )}
        <button
          className={cn(
            'flex items-center justify-center w-8 h-8 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground transition-colors cursor-pointer',
            !expanded && 'mx-auto'
          )}
          onClick={toggle}
          aria-label={expanded ? 'Collapse sidebar' : 'Expand sidebar'}
        >
          {expanded ? (
            <PanelLeftClose size={16} strokeWidth={1.5} />
          ) : (
            <PanelLeftOpen size={18} strokeWidth={1.25} />
          )}
        </button>
      </div>

      <div className="flex-1 overflow-y-auto no-scrollbar px-2 mt-2">
        {expanded ? (
          <>
            <div className="flex items-center justify-between px-1 mb-1">
              <button
                onClick={() => setKbSectionOpen(!kbSectionOpen)}
                className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors cursor-pointer py-1"
              >
                {kbSectionOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                Knowledge Bases
              </button>
              <button
                onClick={() => setCreateOpen(true)}
                className="flex items-center justify-center w-5 h-5 rounded text-muted-foreground hover:text-primary hover:bg-primary/10 transition-colors cursor-pointer"
                aria-label="Create knowledge base"
              >
                <Plus size={13} />
              </button>
            </div>

            {kbSectionOpen && (
              <div className="space-y-0.5">
                {knowledgeBases.map((kb) => {
                  const isActive = pathname?.startsWith(`/kb/${kb.slug}`)
                  const isRenaming = renamingId === kb.id

                  if (isRenaming) {
                    return (
                      <form
                        key={kb.id}
                        className="flex items-center gap-2 rounded-md px-2 py-1 w-full"
                        onSubmit={(e) => {
                          e.preventDefault()
                          const trimmed = renameValue.trim()
                          if (trimmed && trimmed !== kb.name) renameKB(kb.id, trimmed)
                          setRenamingId(null)
                        }}
                      >
                        <FolderOpen size={13} className="text-muted-foreground flex-shrink-0" />
                        <input
                          autoFocus
                          value={renameValue}
                          onChange={(e) => setRenameValue(e.target.value)}
                          onBlur={() => {
                            const trimmed = renameValue.trim()
                            if (trimmed && trimmed !== kb.name) renameKB(kb.id, trimmed)
                            setRenamingId(null)
                          }}
                          onKeyDown={(e) => { if (e.key === 'Escape') setRenamingId(null) }}
                          className="flex-1 text-xs bg-transparent border border-border rounded px-1 py-0.5 outline-none focus:ring-1 focus:ring-primary text-foreground min-w-0"
                        />
                      </form>
                    )
                  }

                  return (
                    <ContextMenu key={kb.id}>
                      <ContextMenuTrigger asChild>
                        <button
                          onClick={() => router.push(`/kb/${kb.slug}`)}
                          className={cn(
                            'flex items-center gap-2 rounded-md px-2 py-1.5 text-xs hover:bg-accent transition-colors w-full text-left cursor-pointer',
                            isActive && 'bg-accent font-semibold'
                          )}
                        >
                          <FolderOpen
                            size={13}
                            className={cn(
                              'flex-shrink-0',
                              isActive ? 'text-primary' : 'text-muted-foreground'
                            )}
                          />
                          <span className="truncate text-foreground">{kb.name}</span>
                        </button>
                      </ContextMenuTrigger>
                      <ContextMenuContent>
                        <ContextMenuItem onClick={() => { setRenameValue(kb.name); setRenamingId(kb.id) }}>
                          <Pencil className="size-3.5 mr-2" />
                          Rename
                        </ContextMenuItem>
                        <ContextMenuSeparator />
                        <ContextMenuItem
                          variant="destructive"
                          onClick={() => setDeleteTarget({ id: kb.id, name: kb.name })}
                        >
                          <Trash2 className="size-3.5 mr-2" />
                          Delete
                        </ContextMenuItem>
                      </ContextMenuContent>
                    </ContextMenu>
                  )
                })}

                {knowledgeBases.length === 0 && (
                  <p className="text-xs text-muted-foreground/60 px-2 py-2">
                    No knowledge bases yet
                  </p>
                )}
              </div>
            )}
          </>
        ) : (
          <div className="flex flex-col items-center gap-1 mt-2">
            {knowledgeBases.slice(0, 5).map((kb) => {
              const isActive = pathname?.startsWith(`/kb/${kb.slug}`)
              return (
                <button
                  key={kb.id}
                  onClick={() => router.push(`/kb/${kb.slug}`)}
                  className={cn(
                    'flex items-center justify-center w-8 h-8 rounded-md hover:bg-accent transition-colors cursor-pointer',
                    isActive && 'bg-accent'
                  )}
                  title={kb.name}
                >
                  <FolderOpen
                    size={14}
                    className={cn(
                      isActive ? 'text-primary' : 'text-muted-foreground'
                    )}
                  />
                </button>
              )
            })}
          </div>
        )}
      </div>

      {user && (
        <div className="px-2 py-2 border-t border-border">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button
                className={cn(
                  'flex items-center gap-3 hover:bg-accent p-1.5 cursor-pointer transition-colors rounded-md w-full',
                  !expanded && 'justify-center'
                )}
              >
                <div className="h-7 w-7 bg-muted border border-border rounded-md flex items-center justify-center flex-shrink-0">
                  <span className="text-[10px] font-medium text-foreground">
                    {initials}
                  </span>
                </div>
                {expanded && (
                  <span className="text-xs text-foreground truncate min-w-0">
                    {user.email}
                  </span>
                )}
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent className="w-56" side={expanded ? 'top' : 'right'} align={expanded ? 'start' : 'end'}>
              <DropdownMenuLabel className="text-xs font-normal text-muted-foreground">
                {user.email}
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={() => router.push('/settings')}>
                <Settings className="mr-2 h-4 w-4" />
                <span>Settings</span>
              </DropdownMenuItem>
              {mounted && (
                <DropdownMenuItem onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>
                  {theme === 'dark' ? (
                    <>
                      <Sun className="mr-2 h-4 w-4" />
                      <span>Light Mode</span>
                    </>
                  ) : (
                    <>
                      <Moon className="mr-2 h-4 w-4" />
                      <span>Dark Mode</span>
                    </>
                  )}
                </DropdownMenuItem>
              )}
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={handleSignOut}>
                <LogOut className="mr-2 h-4 w-4" />
                <span>Sign Out</span>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      )}

      <Dialog open={createOpen} onOpenChange={(open) => { if (!open) { setCreateOpen(false); setCreateName('') } }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Create Knowledge Base</DialogTitle>
            <DialogDescription>
              Give your knowledge base a name to get started.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleCreateKB}>
            <input
              autoFocus
              value={createName}
              onChange={(e) => setCreateName(e.target.value)}
              placeholder="My Research"
              className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm outline-none focus:ring-1 focus:ring-primary"
            />
            <DialogFooter className="mt-4 gap-2 sm:gap-0">
              <DialogClose asChild>
                <button type="button" className="rounded-md px-4 py-2 text-sm text-muted-foreground hover:bg-accent transition-colors cursor-pointer">
                  Cancel
                </button>
              </DialogClose>
              <button
                type="submit"
                disabled={!createName.trim() || creating}
                className="rounded-md px-4 py-2 text-sm font-medium bg-primary text-primary-foreground hover:bg-primary/90 transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed"
              >
                {creating ? 'Creating...' : 'Create'}
              </button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={!!deleteTarget} onOpenChange={(open) => { if (!open) { setDeleteTarget(null); setDeleteConfirmText(''); setDeleting(false) } }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="text-destructive">Delete Knowledge Base</DialogTitle>
            <DialogDescription className="text-sm pt-2 space-y-3">
              <span className="block">
                This will permanently delete <span className="font-semibold text-foreground">{deleteTarget?.name}</span>, all its documents, embeddings, and associated data.
              </span>
              <span className="block font-medium text-destructive">
                This action is not recoverable.
              </span>
              <span className="block">
                Type <span className="font-mono font-semibold text-foreground">delete</span> to confirm.
              </span>
            </DialogDescription>
          </DialogHeader>
          <input
            autoFocus
            value={deleteConfirmText}
            onChange={(e) => setDeleteConfirmText(e.target.value)}
            placeholder="delete"
            className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm outline-none focus:ring-1 focus:ring-destructive"
          />
          <DialogFooter className="gap-2 sm:gap-0">
            <DialogClose asChild>
              <button className="rounded-md px-4 py-2 text-sm text-muted-foreground hover:bg-accent transition-colors cursor-pointer">
                Cancel
              </button>
            </DialogClose>
            <button
              disabled={deleteConfirmText.toLowerCase() !== 'delete' || deleting}
              onClick={async () => {
                if (!deleteTarget) return
                setDeleting(true)
                try {
                  await deleteKB(deleteTarget.id)
                  setDeleteTarget(null)
                  setDeleteConfirmText('')
                  if (pathname?.startsWith(`/kb/`)) {
                    router.push('/kb')
                  }
                } catch {
                  // error handled in store
                } finally {
                  setDeleting(false)
                }
              }}
              className="rounded-md px-4 py-2 text-sm font-medium bg-destructive text-white hover:bg-destructive/90 transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {deleting ? 'Deleting...' : 'Delete permanently'}
            </button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </aside>
  )
}
