'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { useKBStore } from '@/stores'
import { Plus, FolderOpen, Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'

export default function KnowledgeBasesPage() {
  const router = useRouter()
  const knowledgeBases = useKBStore((s) => s.knowledgeBases)
  const loading = useKBStore((s) => s.loading)
  const createKB = useKBStore((s) => s.createKB)
  const [creating, setCreating] = React.useState(false)

  const handleQuickCreate = async () => {
    setCreating(true)
    try {
      const kb = await createKB('Untitled')
      router.push(`/kb/${kb.slug}`)
    } catch (err) {
      console.error('Failed to create KB:', err)
    } finally {
      setCreating(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="size-5 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (knowledgeBases.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-8 p-8">
        <div className="flex flex-col items-center gap-4">
          <svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 32 32">
            <rect width="32" height="32" rx="7" fill="currentColor" className="text-foreground" />
            <polyline points="11,8 21,16 11,24" fill="none" stroke="currentColor" className="text-background" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          <div className="text-center">
            <h1 className="text-2xl font-semibold tracking-tight">Welcome to Supavault</h1>
            <p className="mt-2 text-muted-foreground">
              Get started in two steps.
            </p>
          </div>
        </div>
        <div className="flex flex-col items-center gap-4 max-w-xs w-full">
          <button
            onClick={() => router.push('/kb/new')}
            className="w-full rounded-lg bg-primary px-4 py-2.5 text-sm font-medium text-primary-foreground hover:opacity-90 transition-opacity cursor-pointer"
          >
            Create a knowledge base
          </button>
          <div className="flex items-center gap-3 w-full">
            <div className="h-px flex-1 bg-border" />
            <span className="text-xs text-muted-foreground">or</span>
            <div className="h-px flex-1 bg-border" />
          </div>
          <button
            onClick={() => router.push('/settings')}
            className="w-full rounded-lg border border-input bg-background px-4 py-2.5 text-sm font-medium hover:bg-accent transition-colors cursor-pointer"
          >
            Connect to Claude.ai
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="max-w-4xl mx-auto p-8">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-xl font-semibold tracking-tight">Knowledge Bases</h1>
        <button
          onClick={() => router.push('/kb/new')}
          className="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors cursor-pointer"
        >
          <Plus size={14} />
          New
        </button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {knowledgeBases.map((kb) => (
          <button
            key={kb.id}
            onClick={() => router.push(`/kb/${kb.slug}`)}
            className="flex flex-col items-start gap-3 p-5 rounded-xl border border-border bg-card hover:bg-accent/50 transition-colors cursor-pointer text-left group"
          >
            <div className="flex items-center gap-3">
              <div className="flex items-center justify-center w-9 h-9 rounded-lg bg-muted group-hover:bg-accent transition-colors">
                <FolderOpen size={16} className="text-muted-foreground group-hover:text-foreground transition-colors" />
              </div>
              <div className="min-w-0">
                <h2 className="text-sm font-medium text-foreground truncate">{kb.name}</h2>
                <p className="text-xs text-muted-foreground mt-0.5">
                  {new Date(kb.created_at).toLocaleDateString(undefined, {
                    month: 'short',
                    day: 'numeric',
                    year: 'numeric',
                  })}
                </p>
              </div>
            </div>
          </button>
        ))}

        <button
          onClick={handleQuickCreate}
          disabled={creating}
          className={cn(
            'flex flex-col items-center justify-center gap-2 p-5 rounded-xl border border-dashed border-border hover:border-primary/50 hover:bg-accent/30 transition-colors cursor-pointer min-h-[88px]',
            creating && 'opacity-50 cursor-not-allowed'
          )}
        >
          {creating ? (
            <Loader2 size={16} className="animate-spin text-muted-foreground" />
          ) : (
            <>
              <Plus size={16} className="text-muted-foreground" />
              <span className="text-xs text-muted-foreground">New Knowledge Base</span>
            </>
          )}
        </button>
      </div>
    </div>
  )
}
