'use client'

import * as React from 'react'
import { Copy, Check, ArrowLeft, Loader2 } from 'lucide-react'
import { useRouter } from 'next/navigation'
import { cn } from '@/lib/utils'
import { apiFetch } from '@/lib/api'
import { buildOAuthMcpConfig, MCP_URL } from '@/lib/mcp'
import { useKBStore, useUserStore } from '@/stores'
import { toast } from 'sonner'
import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'

interface Usage {
  total_pages: number
  total_storage_bytes: number
  document_count: number
  max_pages: number
  max_storage_bytes: number
}

interface CompileRun {
  id: string
  status: string
  model: string
  source_count: number
  response_excerpt: string | null
  error_message: string | null
  started_at: string
  finished_at: string | null
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB']
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const value = bytes / Math.pow(1024, i)
  return `${value < 10 ? value.toFixed(1) : Math.round(value)} ${units[i]}`
}

export default function SettingsPage() {
  const router = useRouter()
  const token = useUserStore((s) => s.accessToken)
  const knowledgeBases = useKBStore((s) => s.knowledgeBases)
  const kbLoading = useKBStore((s) => s.loading)
  const fetchKBs = useKBStore((s) => s.fetchKBs)
  const [usage, setUsage] = React.useState<Usage | null>(null)
  const [loading, setLoading] = React.useState(true)
  const [configCopied, setConfigCopied] = React.useState(false)
  const [runningKbId, setRunningKbId] = React.useState<string | null>(null)
  const [pendingCounts, setPendingCounts] = React.useState<Record<string, number>>({})
  const [compileRuns, setCompileRuns] = React.useState<Record<string, CompileRun[]>>({})

  const oauthConfigJson = buildOAuthMcpConfig()

  React.useEffect(() => {
    if (!token) return
    apiFetch<Usage>('/v1/usage', token)
      .then((u) => setUsage(u))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [token])

  React.useEffect(() => {
    if (!token || knowledgeBases.length > 0 || kbLoading) return
    fetchKBs().catch(() => {})
  }, [token, knowledgeBases.length, kbLoading, fetchKBs])

  React.useEffect(() => {
    if (!token || knowledgeBases.length === 0) return

    let cancelled = false
    Promise.all(
      knowledgeBases.map(async (kb) => {
        try {
          const preview = await apiFetch<{ pending_source_count: number }>(
            `/v1/knowledge-bases/${kb.id}/compile-preview`,
            token,
          )
          return [kb.id, preview.pending_source_count] as const
        } catch {
          return [kb.id, 0] as const
        }
      }),
    ).then((entries) => {
      if (cancelled) return
      setPendingCounts(Object.fromEntries(entries))
    })

    return () => { cancelled = true }
  }, [token, knowledgeBases])

  React.useEffect(() => {
    if (!token || knowledgeBases.length === 0) return

    let cancelled = false
    Promise.all(
      knowledgeBases.map(async (kb) => {
        try {
          const runs = await apiFetch<CompileRun[]>(
            `/v1/knowledge-bases/${kb.id}/compile-runs?limit=5`,
            token,
          )
          return [kb.id, runs] as const
        } catch {
          return [kb.id, []] as const
        }
      }),
    ).then((entries) => {
      if (cancelled) return
      setCompileRuns(Object.fromEntries(entries))
    })

    return () => { cancelled = true }
  }, [token, knowledgeBases])

  const handleCopyConfig = async () => {
    try {
      await navigator.clipboard.writeText(oauthConfigJson)
      setConfigCopied(true)
      setTimeout(() => setConfigCopied(false), 2000)
    } catch {
      console.error('Failed to copy')
    }
  }

  const handleCompileNow = async (kbId: string, kbName: string) => {
    if (!token) return
    setRunningKbId(kbId)
    try {
      const result = await apiFetch<{ status: string; source_count: number }>(
        `/v1/knowledge-bases/${kbId}/compile-now`,
        token,
        { method: 'POST' },
      )
      if (result.status === 'skipped') {
        toast.success(`No new sources to compile for ${kbName}`)
      } else {
        toast.success(`Compiled ${result.source_count} source${result.source_count === 1 ? '' : 's'} for ${kbName}`)
      }
      try {
        const preview = await apiFetch<{ pending_source_count: number }>(
          `/v1/knowledge-bases/${kbId}/compile-preview`,
          token,
        )
        setPendingCounts((prev) => ({ ...prev, [kbId]: preview.pending_source_count }))
      } catch {
        // Ignore preview refresh failures after compile
      }
      try {
        const runs = await apiFetch<CompileRun[]>(
          `/v1/knowledge-bases/${kbId}/compile-runs?limit=5`,
          token,
        )
        setCompileRuns((prev) => ({ ...prev, [kbId]: runs }))
      } catch {
        // Ignore history refresh failures after compile
      }
    } catch (err) {
      toast.error((err as Error).message || 'Compile failed')
    } finally {
      setRunningKbId(null)
    }
  }

  return (
    <div className="max-w-2xl mx-auto p-8">
      <div className="flex items-center gap-3 mb-8">
        <button
          onClick={() => router.back()}
          className="p-1 rounded-md hover:bg-accent transition-colors cursor-pointer text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-4" />
        </button>
        <h1 className="text-xl font-semibold tracking-tight">Settings</h1>
      </div>

      {/* Usage */}
      {usage && (
        <section>
          <h2 className="text-base font-medium">Usage</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {usage.document_count} document{usage.document_count !== 1 ? 's' : ''} uploaded
          </p>
          <div className="mt-4 space-y-4">
            <div>
              <div className="flex items-center justify-between text-sm mb-1.5">
                <span className="text-muted-foreground">Storage</span>
                <span className="font-mono text-xs">
                  {formatBytes(usage.total_storage_bytes)} / {formatBytes(usage.max_storage_bytes)}
                </span>
              </div>
              <div className="h-2 rounded-full bg-muted overflow-hidden">
                <div
                  className={cn(
                    'h-full rounded-full transition-all',
                    usage.total_storage_bytes / usage.max_storage_bytes > 0.9
                      ? 'bg-destructive'
                      : usage.total_storage_bytes / usage.max_storage_bytes > 0.7
                        ? 'bg-yellow-500'
                        : 'bg-primary'
                  )}
                  style={{ width: `${Math.min(100, (usage.total_storage_bytes / usage.max_storage_bytes) * 100)}%` }}
                />
              </div>
            </div>
            <div>
              <div className="flex items-center justify-between text-sm mb-1.5">
                <span className="text-muted-foreground">OCR Pages</span>
                <span className="font-mono text-xs">
                  {usage.total_pages.toLocaleString()} / {usage.max_pages.toLocaleString()}
                </span>
              </div>
              <div className="h-2 rounded-full bg-muted overflow-hidden">
                <div
                  className={cn(
                    'h-full rounded-full transition-all',
                    usage.total_pages / usage.max_pages > 0.9
                      ? 'bg-destructive'
                      : usage.total_pages / usage.max_pages > 0.7
                        ? 'bg-yellow-500'
                        : 'bg-primary'
                  )}
                  style={{ width: `${Math.min(100, (usage.total_pages / usage.max_pages) * 100)}%` }}
                />
              </div>
            </div>
          </div>
        </section>
      )}

      {usage && <div className="h-px bg-border my-8" />}

      {/* MCP Config */}
      <section>
        <h2 className="text-base font-medium">Connect via OAuth</h2>
        <p className="mt-2 text-sm text-muted-foreground">
          Add this configuration to your MCP client. On first connection, it should prompt you to sign in with Supabase.
        </p>
        <div className="relative mt-4">
          <pre className="rounded-lg bg-muted border border-border p-4 text-sm font-mono overflow-x-auto text-foreground">
            {oauthConfigJson}
          </pre>
          <button
            onClick={handleCopyConfig}
            className={cn(
              'absolute top-3 right-3 flex items-center gap-1.5 rounded-md px-2.5 py-1.5 text-xs transition-colors cursor-pointer',
              configCopied
                ? 'bg-green-500/10 text-green-600 dark:text-green-400'
                : 'bg-background border border-border text-muted-foreground hover:text-foreground hover:bg-accent'
            )}
          >
            {configCopied ? <><Check size={12} />Copied</> : <><Copy size={12} />Copy</>}
          </button>
        </div>
        <p className="mt-3 text-xs text-muted-foreground">
          MCP URL:
          {' '}
          <code className="text-xs bg-muted px-1.5 py-0.5 rounded font-mono">{MCP_URL}</code>
        </p>
      </section>

      <div className="h-px bg-border my-8" />

      <section>
        <h2 className="text-base font-medium">Compile now</h2>
        <p className="mt-2 text-sm text-muted-foreground">
          Run the server-side Claude compiler immediately for a knowledge base using your current sign-in token.
        </p>
        <div className="mt-4 space-y-2">
          {knowledgeBases.map((kb) => {
            const running = runningKbId === kb.id
            const pendingCount = pendingCounts[kb.id]
            const runs = compileRuns[kb.id] ?? []
            return (
              <div key={kb.id} className="rounded-lg border border-border px-4 py-3">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <div className="text-sm font-medium">{kb.name}</div>
                    <div className="mt-0.5 text-xs text-muted-foreground">{kb.slug}</div>
                  </div>
                  <div className="flex items-center gap-2">
                    {pendingCount !== undefined && (
                      <span className="inline-flex items-center rounded-full bg-muted px-2 py-1 text-xs text-muted-foreground">
                        {pendingCount} pending
                      </span>
                    )}
                    <HoverCard openDelay={150}>
                      <HoverCardTrigger asChild>
                        <button
                          onClick={() => handleCompileNow(kb.id, kb.name)}
                          disabled={running}
                          className="inline-flex items-center gap-2 rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:opacity-90 disabled:opacity-50 cursor-pointer"
                        >
                          {running ? <Loader2 className="size-4 animate-spin" /> : null}
                          {running ? 'Running…' : 'Compile now'}
                        </button>
                      </HoverCardTrigger>
                      <HoverCardContent align="end" className="w-64 text-sm">
                        {pendingCount === undefined ? (
                          <p className="text-muted-foreground">Checking how many changed sources are pending…</p>
                        ) : pendingCount === 0 ? (
                          <p className="text-muted-foreground">No new or changed sources are pending for compilation.</p>
                        ) : (
                          <p className="text-muted-foreground">
                            This will compile {pendingCount} new or changed source{pendingCount === 1 ? '' : 's'}.
                          </p>
                        )}
                      </HoverCardContent>
                    </HoverCard>
                  </div>
                </div>
                <div className="mt-3 space-y-1.5">
                  <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground/70">
                    Recent compile runs
                  </div>
                  {runs.length === 0 ? (
                    <p className="text-xs text-muted-foreground">No compile runs yet.</p>
                  ) : (
                    runs.map((run) => (
                      <div key={run.id} className="rounded-md bg-muted/40 px-3 py-2 text-xs">
                        <div className="flex items-center justify-between gap-3">
                          <div className="flex items-center gap-2">
                            <span className={cn(
                              'inline-flex rounded-full px-2 py-0.5 text-[10px] font-medium uppercase',
                              run.status === 'succeeded'
                                ? 'bg-green-500/10 text-green-600 dark:text-green-400'
                                : run.status === 'failed'
                                  ? 'bg-destructive/10 text-destructive'
                                  : run.status === 'skipped'
                                    ? 'bg-muted text-muted-foreground'
                                    : 'bg-yellow-500/10 text-yellow-700 dark:text-yellow-400',
                            )}>
                              {run.status}
                            </span>
                            <span className="text-muted-foreground">
                              {run.source_count} source{run.source_count === 1 ? '' : 's'}
                            </span>
                          </div>
                          <span className="text-muted-foreground">
                            {new Date(run.started_at).toLocaleString()}
                          </span>
                        </div>
                        {run.error_message ? (
                          <p className="mt-1 text-destructive/80">{run.error_message}</p>
                        ) : run.response_excerpt ? (
                          <p className="mt-1 line-clamp-2 text-muted-foreground">{run.response_excerpt}</p>
                        ) : null}
                      </div>
                    ))
                  )}
                </div>
              </div>
            )
          })}
          {!kbLoading && knowledgeBases.length === 0 && (
            <p className="text-sm text-muted-foreground">No knowledge bases found.</p>
          )}
        </div>
      </section>
    </div>
  )
}
