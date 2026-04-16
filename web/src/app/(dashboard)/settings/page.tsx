'use client'

import * as React from 'react'
import { ArrowLeft, Check, Copy, Loader2, Plus, Trash2, X } from 'lucide-react'
import { useRouter } from 'next/navigation'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { toast } from 'sonner'

import { apiFetch } from '@/lib/api'
import { getPublicEnv } from '@/lib/public-env'
import { buildOAuthMcpConfig, MCP_URL } from '@/lib/mcp'
import type { KnowledgeBase } from '@/lib/types'
import { useKBStore, useUserStore } from '@/stores'
import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog'

interface Usage {
  total_pages: number
  total_storage_bytes: number
  document_count: number
  max_pages: number
  max_storage_bytes: number
  page_limits_enabled: boolean
}

interface Member {
  user_id: string
  email: string | null
  display_name: string | null
  role: string
  created_at: string
}

interface CompileRun {
  id: string
  status: string
  model: string
  provider: string
  source_count: number
  response_excerpt: string | null
  error_message: string | null
  started_at: string
  finished_at: string | null
  telemetry?: { comments_skipped_count?: number } | null
}

interface Guideline {
  id: string
  kb_id: string
  body: string
  position: number
  is_active: boolean
  created_by: string | null
  created_at: string
  updated_at: string
  archived_at: string | null
}

interface StreamliningRun {
  id: string
  status: string
  model: string
  provider: string
  scope_type: string
  response_excerpt: string | null
  error_message: string | null
  started_at: string
  finished_at: string | null
}

interface CompileSchedule {
  knowledge_base: string
  enabled: boolean
  provider: string
  model: string | null
  wiki_direct_editing_enabled: boolean
  interval_minutes: number
  max_sources: number
  prompt: string
  max_tool_rounds: number
  max_tokens: number
  has_provider_secret: boolean
  provider_secret?: string
  last_run_at: string | null
  last_status: string | null
  last_error: string | null
  next_run_at: string | null
  streamlining_enabled: boolean
  streamlining_interval_minutes: number
  streamlining_provider: string | null
  streamlining_model: string | null
  streamlining_prompt: string
  has_streamlining_provider_secret: boolean
  streamlining_provider_secret?: string
  last_streamlining_at: string | null
  last_streamlining_status: string | null
  last_streamlining_error: string | null
  next_streamlining_at: string | null
}

interface CompilePreview {
  pending_source_count: number
  pending_comment_count?: number
}

const ADMIN_ROLES = new Set(['owner', 'admin'])
const GUIDELINES_COMMENTS_DISABLED = getPublicEnv('NEXT_PUBLIC_GUIDELINES_COMMENTS_DISABLED') === 'true'
const DEFAULT_MAX_SOURCES = 20
const DEFAULT_MAX_TOOL_ROUNDS = 50
const DEFAULT_MAX_TOKENS = 50_000

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB']
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const value = bytes / Math.pow(1024, i)
  return `${value < 10 ? value.toFixed(1) : Math.round(value)} ${units[i]}`
}

function GuidelineBody({ body, isActive }: { body: string; isActive: boolean }) {
  return (
    <div
      className={
        'min-w-0 flex-1 text-sm leading-relaxed ' +
        '[&>ul]:list-disc [&>ul]:pl-5 [&>ul]:space-y-0.5 ' +
        '[&_ul_ul]:list-[circle] [&_ul_ul]:pl-5 [&_ul_ul]:mt-0.5 ' +
        '[&>ol]:list-decimal [&>ol]:pl-5 [&>ol]:space-y-0.5 ' +
        '[&>p]:my-0 [&>p+p]:mt-1.5 ' +
        '[&_code]:rounded [&_code]:bg-muted [&_code]:px-1 [&_code]:py-0.5 [&_code]:text-[0.85em] ' +
        '[&_a]:underline [&_a]:underline-offset-2 ' +
        (isActive ? '' : 'line-through text-muted-foreground')
      }
    >
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{body}</ReactMarkdown>
    </div>
  )
}

function DeleteWikiDialog({
  open,
  name,
  value,
  deleting,
  onValueChange,
  onOpenChange,
  onConfirm,
}: {
  open: boolean
  name: string
  value: string
  deleting: boolean
  onValueChange: (value: string) => void
  onOpenChange: (open: boolean) => void
  onConfirm: () => void
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete wiki</DialogTitle>
          <DialogDescription>
            This permanently deletes <strong>{name}</strong>, including sources, wiki pages, collaborators, and compile history.
            Type the wiki name exactly to confirm.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2">
          <div className="rounded-md border border-border bg-muted/40 px-3 py-2 text-sm font-medium">{name}</div>
          <input
            autoFocus
            value={value}
            onChange={(e) => onValueChange(e.target.value)}
            placeholder={name}
            className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
          />
        </div>
        <DialogFooter>
          <button
            onClick={() => onOpenChange(false)}
            className="rounded-md border border-border px-3 py-2 text-sm hover:bg-accent cursor-pointer"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={deleting || value !== name}
            className="inline-flex items-center gap-2 rounded-md bg-destructive px-3 py-2 text-sm font-medium text-destructive-foreground hover:opacity-90 disabled:opacity-50 cursor-pointer"
          >
            {deleting ? <Loader2 className="size-4 animate-spin" /> : null}
            Delete wiki
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function ScheduleCard({
  kb,
  schedule,
  pendingCount,
  pendingCommentCount,
  runs,
  streamliningRuns,
  members,
  saving,
  running,
  rebuilding,
  runningStreamlining,
  deleting,
  onScheduleChange,
  onSaveSchedule,
  onCompileNow,
  onRecompileFromScratch,
  onStreamlineNow,
  onCreateInvite,
  onUpdateMember,
  onRemoveMember,
  onDeleteWiki,
  guidelines,
  onAddGuideline,
  onAddGuidelines,
  onUpdateGuideline,
  onToggleGuideline,
  onDeleteGuideline,
}: {
  kb: KnowledgeBase
  schedule: CompileSchedule | undefined
  pendingCount: number | undefined
  pendingCommentCount: number | undefined
  runs: CompileRun[]
  streamliningRuns: StreamliningRun[]
  members: Member[]
  saving: boolean
  running: boolean
  rebuilding: boolean
  runningStreamlining: boolean
  deleting: boolean
  onScheduleChange: (kbId: string, patch: Partial<CompileSchedule>) => void
  onSaveSchedule: (kbId: string) => void
  onCompileNow: (kbId: string, kbName: string) => void
  onRecompileFromScratch: (kbId: string, kbName: string) => void
  onStreamlineNow: (kbId: string, kbName: string) => void
  onCreateInvite: (kbId: string, email: string, role: string) => void
  onUpdateMember: (kbId: string, memberId: string, role: string) => void
  onRemoveMember: (kbId: string, memberId: string) => void
  onDeleteWiki: (kbId: string, kbName: string) => void
  guidelines: Guideline[]
  onAddGuideline: (kbId: string, body: string) => Promise<void>
  onAddGuidelines: (kbId: string, bodies: string[]) => Promise<void>
  onUpdateGuideline: (kbId: string, guidelineId: string, body: string) => Promise<void>
  onToggleGuideline: (kbId: string, guidelineId: string, isActive: boolean) => Promise<void>
  onDeleteGuideline: (kbId: string, guidelineId: string) => Promise<void>
}) {
  const isAdmin = ADMIN_ROLES.has(kb.role)
  const [inviteEmail, setInviteEmail] = React.useState('')
  const [inviteRole, setInviteRole] = React.useState('viewer')
  const [drafts, setDrafts] = React.useState<string[]>([''])
  const [addingGuideline, setAddingGuideline] = React.useState(false)
  const [editingGuidelineId, setEditingGuidelineId] = React.useState<string | null>(null)
  const [editingBody, setEditingBody] = React.useState('')

  React.useEffect(() => {
    setInviteRole('viewer')
  }, [kb.id])

  if (!schedule) return null

  return (
    <div className="rounded-lg border border-border px-4 py-4 space-y-4">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-sm font-medium">{kb.name}</div>
          <div className="mt-0.5 text-xs text-muted-foreground">{kb.slug} · role: {kb.role}</div>
        </div>
        {isAdmin && (
          <div className="flex items-center gap-2">
            {pendingCount !== undefined && (
              <span className="inline-flex items-center rounded-full bg-muted px-2 py-1 text-xs text-muted-foreground">
                {pendingCount} source{pendingCount === 1 ? '' : 's'}
                {pendingCommentCount !== undefined && pendingCommentCount > 0 && ` · ${pendingCommentCount} comment${pendingCommentCount === 1 ? '' : 's'}`}
                {' '}pending
              </span>
            )}
            <HoverCard openDelay={150}>
              <HoverCardTrigger asChild>
                <button
                  onClick={() => onCompileNow(kb.id, kb.name)}
                  disabled={running}
                  className="inline-flex items-center gap-2 rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:opacity-90 disabled:opacity-50 cursor-pointer"
                >
                  {running ? <Loader2 className="size-4 animate-spin" /> : null}
                  {running ? 'Running…' : 'Compile now'}
                </button>
              </HoverCardTrigger>
              <HoverCardContent align="end" className="w-64 text-sm">
                {pendingCount === undefined ? (
                  <p className="text-muted-foreground">Checking pending sources…</p>
                ) : pendingCount === 0 && (!pendingCommentCount || pendingCommentCount === 0) ? (
                  <p className="text-muted-foreground">No new or changed sources are pending.</p>
                ) : (
                  <div className="text-muted-foreground space-y-1">
                    {pendingCount > 0 && <p>This will compile {pendingCount} new or changed source{pendingCount === 1 ? '' : 's'}.</p>}
                    {pendingCommentCount !== undefined && pendingCommentCount > 0 && <p>…and {pendingCommentCount} unresolved comment{pendingCommentCount === 1 ? '' : 's'}.</p>}
                  </div>
                )}
              </HoverCardContent>
            </HoverCard>
            <button
              onClick={() => onRecompileFromScratch(kb.id, kb.name)}
              disabled={rebuilding}
              className="inline-flex items-center gap-2 rounded-md border border-border px-3 py-2 text-sm font-medium hover:bg-accent disabled:opacity-50 cursor-pointer"
            >
              {rebuilding ? <Loader2 className="size-4 animate-spin" /> : null}
              {rebuilding ? 'Rebuilding…' : 'Recompile from scratch'}
            </button>
            <button
              onClick={() => onStreamlineNow(kb.id, kb.name)}
              disabled={runningStreamlining}
              className="inline-flex items-center gap-2 rounded-md border border-border px-3 py-2 text-sm font-medium hover:bg-accent disabled:opacity-50 cursor-pointer"
            >
              {runningStreamlining ? <Loader2 className="size-4 animate-spin" /> : null}
              {runningStreamlining ? 'Streamlining…' : 'Streamline now'}
            </button>
          </div>
        )}
      </div>

      {isAdmin ? (
        <div className="grid gap-4 lg:grid-cols-2">
          <section className="rounded-md border border-border/60 p-3 space-y-3">
            <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground/70">Collaboration</div>
            <p className="text-xs text-muted-foreground">
              Add existing users by email. They are added immediately and the wiki appears in their list with the assigned role.
            </p>
            <div className="space-y-2">
              {members.map((member) => (
                <div key={member.user_id} className="flex items-center justify-between gap-3 rounded-md bg-muted/40 px-3 py-2 text-sm">
                  <div>
                    <div className="font-medium">{member.display_name || member.email || member.user_id}</div>
                    <div className="text-xs text-muted-foreground">{member.email || member.user_id}</div>
                  </div>
                  <div className="flex items-center gap-2">
                    <select
                      value={member.role}
                      onChange={(e) => onUpdateMember(kb.id, member.user_id, e.target.value)}
                      disabled={member.role === 'owner'}
                      className="rounded-md border border-input bg-background px-2 py-1 text-xs"
                    >
                      <option value="owner">Owner</option>
                      <option value="admin">Admin</option>
                      <option value="editor">Editor</option>
                      <option value="viewer">Viewer</option>
                    </select>
                    {member.role !== 'owner' && (
                      <button onClick={() => onRemoveMember(kb.id, member.user_id)} className="rounded-md p-1 text-destructive hover:bg-destructive/10 cursor-pointer">
                        <Trash2 className="size-4" />
                      </button>
                    )}
                  </div>
                </div>
              ))}
            </div>
            <div className="space-y-2 rounded-md border border-dashed border-border p-3">
              <div className="text-xs text-muted-foreground">Add existing collaborator</div>
              <input
                value={inviteEmail}
                onChange={(e) => setInviteEmail(e.target.value)}
                placeholder="email@example.com"
                className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              />
              <div className="flex items-center gap-2">
                <select
                  value={inviteRole}
                  onChange={(e) => setInviteRole(e.target.value)}
                  className="rounded-md border border-input bg-background px-3 py-2 text-sm"
                >
                  <option value="viewer">Viewer</option>
                  <option value="editor">Editor</option>
                  <option value="admin">Admin</option>
                </select>
                <button
                  onClick={() => {
                    onCreateInvite(kb.id, inviteEmail, inviteRole)
                    setInviteEmail('')
                    setInviteRole('viewer')
                  }}
                  disabled={!inviteEmail.trim()}
                  className="rounded-md border border-border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50 cursor-pointer"
                >
                  Add collaborator
                </button>
              </div>
            </div>
          </section>

          <section className="rounded-md border border-border/60 p-3 space-y-3">
            <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground/70">Periodic compile</div>
            <div className="rounded-md border border-border/60 bg-muted/20 px-3 py-3 space-y-2">
              <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground/70">Wiki editing</div>
              <label className="flex items-start gap-3 text-sm">
                <input
                  type="checkbox"
                  checked={schedule.wiki_direct_editing_enabled}
                  onChange={(e) => onScheduleChange(kb.id, { wiki_direct_editing_enabled: e.target.checked })}
                  className="mt-0.5"
                />
                <div>
                  <div className="font-medium text-foreground">Allow direct wiki editing in the app</div>
                  <p className="mt-1 text-xs text-muted-foreground leading-relaxed">
                    {schedule.wiki_direct_editing_enabled
                      ? 'Editors can open wiki pages and edit them directly.'
                      : 'Wiki pages stay source-driven in the app. Add sources or ask Claude via MCP to update them.'}
                  </p>
                </div>
              </label>
            </div>
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={schedule.enabled}
                onChange={(e) => onScheduleChange(kb.id, { enabled: e.target.checked })}
              />
              Enable periodic compile
            </label>
            <div className="grid gap-3 md:grid-cols-2">
              <label className="text-sm">
                <span className="mb-1 block text-muted-foreground">Provider</span>
                <select
                  value={schedule.provider}
                  onChange={(e) => onScheduleChange(kb.id, { provider: e.target.value })}
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                >
                  <option value="anthropic">Anthropic</option>
                  <option value="openrouter">OpenRouter</option>
                </select>
              </label>
              <label className="text-sm">
                <span className="mb-1 block text-muted-foreground">Model</span>
                <input
                  value={schedule.model ?? ''}
                  onChange={(e) => onScheduleChange(kb.id, { model: e.target.value || null })}
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                />
              </label>
              <label className="text-sm">
                <span className="mb-1 block text-muted-foreground">Every N minutes</span>
                <input
                  type="number"
                  min={5}
                  max={10080}
                  value={schedule.interval_minutes}
                  onChange={(e) => onScheduleChange(kb.id, { interval_minutes: Number(e.target.value) })}
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                />
              </label>
              <label className="text-sm">
                <span className="mb-1 block text-muted-foreground">Max sources per run</span>
                <input
                  type="number"
                  min={1}
                  max={200}
                  value={schedule.max_sources}
                  onChange={(e) => onScheduleChange(kb.id, { max_sources: Number(e.target.value) })}
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                />
              </label>
              <label className="text-sm">
                <span className="mb-1 block text-muted-foreground">Max tool rounds</span>
                <input
                  type="number"
                  min={1}
                  max={200}
                  value={schedule.max_tool_rounds}
                  onChange={(e) => onScheduleChange(kb.id, { max_tool_rounds: Number(e.target.value) })}
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                />
              </label>
              <label className="text-sm">
                <span className="mb-1 block text-muted-foreground">Max tokens</span>
                <input
                  type="number"
                  min={256}
                  max={200000}
                  value={schedule.max_tokens}
                  onChange={(e) => onScheduleChange(kb.id, { max_tokens: Number(e.target.value) })}
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                />
              </label>
            </div>
            <div className="rounded-md border border-dashed border-border/70 bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
              LLM Wiki uses a built-in incremental compile guide, so you only configure the budget and provider here.
              If more than {schedule.max_sources} sources are pending, the extra ones stay queued for the next run.
              If the model hits its token or tool-round limit, the run is marked failed and no sources are checkpointed.
            </div>
            <label className="block text-sm">
              <span className="mb-1 block text-muted-foreground">Provider API key / secret</span>
              <input
                type="password"
                placeholder={schedule.has_provider_secret ? 'Configured — enter a new secret to rotate' : 'Enter provider secret'}
                onChange={(e) => onScheduleChange(kb.id, { provider_secret: e.target.value })}
                className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              />
            </label>
            <div className="flex items-center justify-between gap-3 text-xs text-muted-foreground">
              <div>
                {schedule.next_run_at ? `Next run: ${new Date(schedule.next_run_at).toLocaleString()}` : schedule.enabled ? 'Next run will be scheduled after save.' : 'Schedule is disabled.'}
              </div>
              <button
                onClick={() => onSaveSchedule(kb.id)}
                disabled={saving}
                className="inline-flex items-center gap-2 rounded-md border border-border px-3 py-2 text-sm font-medium hover:bg-accent disabled:opacity-50 cursor-pointer"
              >
                {saving ? <Loader2 className="size-4 animate-spin" /> : null}
                Save schedule
              </button>
            </div>
            <div className="rounded-md border border-border/60 bg-muted/20 px-3 py-3 space-y-3">
              <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground/70">Wiki streamlining</div>
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={schedule.streamlining_enabled}
                  onChange={(e) => onScheduleChange(kb.id, { streamlining_enabled: e.target.checked })}
                />
                Enable periodic streamlining
              </label>
              <div className="grid gap-3 md:grid-cols-2">
                <label className="text-sm">
                  <span className="mb-1 block text-muted-foreground">Provider</span>
                  <select
                    value={schedule.streamlining_provider ?? schedule.provider}
                    onChange={(e) => onScheduleChange(kb.id, { streamlining_provider: e.target.value })}
                    className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                  >
                    <option value="anthropic">Anthropic</option>
                    <option value="openrouter">OpenRouter</option>
                  </select>
                </label>
                <label className="text-sm">
                  <span className="mb-1 block text-muted-foreground">Model</span>
                  <input
                    value={schedule.streamlining_model ?? ''}
                    onChange={(e) => onScheduleChange(kb.id, { streamlining_model: e.target.value || null })}
                    className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                  />
                </label>
                <label className="text-sm">
                  <span className="mb-1 block text-muted-foreground">Every N minutes</span>
                  <input
                    type="number"
                    min={60}
                    max={10080}
                    value={schedule.streamlining_interval_minutes}
                    onChange={(e) => onScheduleChange(kb.id, { streamlining_interval_minutes: Number(e.target.value) })}
                    className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                  />
                </label>
                <label className="text-sm">
                  <span className="mb-1 block text-muted-foreground">Provider API key / secret</span>
                  <input
                    type="password"
                    placeholder={schedule.has_streamlining_provider_secret ? 'Configured — enter a new secret to rotate' : 'Leave blank to reuse compile secret'}
                    onChange={(e) => onScheduleChange(kb.id, { streamlining_provider_secret: e.target.value })}
                    className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                  />
                </label>
              </div>
              <label className="block text-sm">
                <span className="mb-1 block text-muted-foreground">Prompt override</span>
                <textarea
                  value={schedule.streamlining_prompt}
                  onChange={(e) => onScheduleChange(kb.id, { streamlining_prompt: e.target.value })}
                  rows={4}
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                />
              </label>
              <div className="text-xs text-muted-foreground">
                {schedule.next_streamlining_at
                  ? `Next streamlining: ${new Date(schedule.next_streamlining_at).toLocaleString()}`
                  : schedule.streamlining_enabled
                    ? 'Next streamlining will be scheduled after save.'
                    : 'Streamlining is disabled.'}
              </div>
            </div>
            {kb.role === 'owner' && (
              <div className="rounded-md border border-destructive/30 bg-destructive/5 px-3 py-3 text-sm">
                <div className="font-medium text-foreground">Danger zone</div>
                <p className="mt-1 text-xs text-muted-foreground">
                  Deleting a wiki permanently removes its sources, wiki pages, collaborators, invites, and compile history.
                </p>
                <button
                  onClick={() => onDeleteWiki(kb.id, kb.name)}
                  disabled={deleting}
                  className="mt-3 inline-flex items-center gap-2 rounded-md border border-destructive/40 px-3 py-2 text-sm text-destructive hover:bg-destructive/10 disabled:opacity-50 cursor-pointer"
                >
                  {deleting ? <Loader2 className="size-4 animate-spin" /> : <Trash2 className="size-4" />}
                  Delete wiki
                </button>
              </div>
            )}
          </section>
        </div>
      ) : (
        <div className="rounded-md border border-border/60 p-3 text-sm text-muted-foreground">
          Collaboration and automation settings are visible only to KB admins.
        </div>
      )}

      {isAdmin && !GUIDELINES_COMMENTS_DISABLED && (
        <div className="space-y-2">
          <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground/70">KB Guidelines</div>
          {guidelines.length === 0 && (
            <p className="text-xs text-muted-foreground">No guidelines yet. Add standing rules the AI should follow when compiling this wiki.</p>
          )}
          {guidelines.map((g) => (
            editingGuidelineId === g.id ? (
              <div key={g.id} className="rounded-md bg-muted/40 px-3 py-2 space-y-2">
                <textarea
                  value={editingBody}
                  onChange={(e) => setEditingBody(e.target.value)}
                  rows={Math.max(3, editingBody.split('\n').length + 1)}
                  autoFocus
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono leading-relaxed"
                />
                <div className="flex gap-2">
                  <button
                    onClick={async () => { await onUpdateGuideline(kb.id, g.id, editingBody); setEditingGuidelineId(null) }}
                    disabled={!editingBody.trim()}
                    className="rounded-md border border-border px-3 py-1.5 text-xs hover:bg-accent disabled:opacity-50 cursor-pointer"
                  >Save</button>
                  <button
                    onClick={() => setEditingGuidelineId(null)}
                    className="rounded-md px-3 py-1.5 text-xs text-muted-foreground hover:bg-accent cursor-pointer"
                  >Cancel</button>
                </div>
              </div>
            ) : (
              <div key={g.id} className="rounded-md bg-muted/40 px-3 py-2 text-sm flex items-start justify-between gap-3">
                <div className="flex items-start gap-2 min-w-0 flex-1">
                  <input
                    type="checkbox"
                    checked={g.is_active}
                    onChange={(e) => onToggleGuideline(kb.id, g.id, e.target.checked)}
                    className="mt-1 shrink-0 cursor-pointer"
                  />
                  <GuidelineBody body={g.body} isActive={g.is_active} />
                </div>
                <div className="flex items-center gap-1 shrink-0">
                  <button
                    onClick={() => { setEditingGuidelineId(g.id); setEditingBody(g.body) }}
                    className="rounded px-2 py-1 text-xs text-muted-foreground hover:bg-accent cursor-pointer"
                  >Edit</button>
                  <button
                    onClick={() => onDeleteGuideline(kb.id, g.id)}
                    className="rounded p-1 text-destructive hover:bg-destructive/10 cursor-pointer"
                  ><Trash2 className="size-3.5" /></button>
                </div>
              </div>
            )
          ))}
          <div className="space-y-2 rounded-md border border-dashed border-border p-3">
            <p className="text-xs text-muted-foreground">
              Each card is one guideline. Use the textarea like a markdown editor — bullet lists, sub-bullets, line breaks all preserved.
            </p>
            {drafts.map((draft, idx) => (
              <div key={idx} className="flex items-start gap-2">
                <textarea
                  value={draft}
                  onChange={(e) => {
                    const next = [...drafts]
                    next[idx] = e.target.value
                    setDrafts(next)
                  }}
                  placeholder={
                    idx === 0
                      ? 'Write a guideline…\n\n- supports bullets\n- and sub-bullets:\n  - like this'
                      : 'Another guideline'
                  }
                  rows={3}
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono leading-relaxed"
                />
                {drafts.length > 1 && (
                  <button
                    type="button"
                    onClick={() => setDrafts(drafts.filter((_, i) => i !== idx))}
                    className="mt-1 rounded p-1 text-muted-foreground hover:bg-accent cursor-pointer"
                    title="Remove this draft"
                  >
                    <X className="size-3.5" />
                  </button>
                )}
              </div>
            ))}
            {(() => {
              const cleaned = drafts.map((d) => d.trim()).filter(Boolean)
              const count = cleaned.length
              const label = count <= 1 ? 'Add guideline' : `Add ${count} guidelines`
              return (
                <div className="flex items-center justify-between gap-2 pt-1">
                  <button
                    type="button"
                    onClick={() => setDrafts([...drafts, ''])}
                    className="inline-flex items-center gap-1.5 rounded-md px-2 py-1 text-xs text-muted-foreground hover:bg-accent cursor-pointer"
                  >
                    <Plus className="size-3.5" />
                    Add another
                  </button>
                  <button
                    type="button"
                    onClick={async () => {
                      if (cleaned.length === 0) return
                      setAddingGuideline(true)
                      try {
                        if (cleaned.length === 1) {
                          await onAddGuideline(kb.id, cleaned[0])
                        } else {
                          await onAddGuidelines(kb.id, cleaned)
                        }
                        setDrafts([''])
                      } finally {
                        setAddingGuideline(false)
                      }
                    }}
                    disabled={addingGuideline || count === 0}
                    className="inline-flex items-center gap-2 rounded-md border border-border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50 cursor-pointer"
                  >
                    {addingGuideline ? <Loader2 className="size-3.5 animate-spin" /> : null}
                    {label}
                  </button>
                </div>
              )
            })()}
          </div>
        </div>
      )}

      {isAdmin && (
        <div className="space-y-1.5">
          <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground/70">Recent compile runs</div>
          {runs.length === 0 ? (
            <p className="text-xs text-muted-foreground">No compile runs yet.</p>
          ) : (
            runs.map((run) => (
              <div key={run.id} className="rounded-md bg-muted/40 px-3 py-2 text-xs">
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <span className={run.status === 'succeeded' ? 'text-green-600 dark:text-green-400' : run.status === 'failed' ? 'text-destructive' : 'text-muted-foreground'}>
                      {run.status}
                    </span>
                    <span className="text-muted-foreground">{run.provider}</span>
                    <span className="text-muted-foreground">{run.source_count} source{run.source_count === 1 ? '' : 's'}</span>
                    {!GUIDELINES_COMMENTS_DISABLED && (run.telemetry?.comments_skipped_count ?? 0) > 0 && (
                      <span className="text-muted-foreground">{run.telemetry!.comments_skipped_count} comment{run.telemetry!.comments_skipped_count === 1 ? '' : 's'} skipped</span>
                    )}
                  </div>
                  <span className="text-muted-foreground">{new Date(run.started_at).toLocaleString()}</span>
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
      )}

      {isAdmin && (
        <div className="space-y-1.5">
          <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground/70">Recent streamlining runs</div>
          {streamliningRuns.length === 0 ? (
            <p className="text-xs text-muted-foreground">No streamlining runs yet.</p>
          ) : (
            streamliningRuns.map((run) => (
              <div key={run.id} className="rounded-md bg-muted/40 px-3 py-2 text-xs">
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <span className={run.status === 'succeeded' ? 'text-green-600 dark:text-green-400' : run.status === 'failed' ? 'text-destructive' : 'text-muted-foreground'}>
                      {run.status}
                    </span>
                    <span className="text-muted-foreground">{run.provider}</span>
                    <span className="text-muted-foreground">{run.scope_type}</span>
                  </div>
                  <span className="text-muted-foreground">{new Date(run.started_at).toLocaleString()}</span>
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
      )}
    </div>
  )
}

export default function SettingsPage() {
  const router = useRouter()
  const token = useUserStore((s) => s.accessToken)
  const knowledgeBases = useKBStore((s) => s.knowledgeBases)
  const kbLoading = useKBStore((s) => s.loading)
  const fetchKBs = useKBStore((s) => s.fetchKBs)
  const deleteKB = useKBStore((s) => s.deleteKB)

  const [usage, setUsage] = React.useState<Usage | null>(null)
  const [configCopied, setConfigCopied] = React.useState(false)
  const [runningKbId, setRunningKbId] = React.useState<string | null>(null)
  const [rebuildingKbId, setRebuildingKbId] = React.useState<string | null>(null)
  const [runningStreamliningKbId, setRunningStreamliningKbId] = React.useState<string | null>(null)
  const [savingScheduleKbId, setSavingScheduleKbId] = React.useState<string | null>(null)
  const [deletingKbId, setDeletingKbId] = React.useState<string | null>(null)
  const [deleteDialog, setDeleteDialog] = React.useState<{ kbId: string; kbName: string } | null>(null)
  const [deleteConfirmation, setDeleteConfirmation] = React.useState('')
  const [pendingCounts, setPendingCounts] = React.useState<Record<string, number>>({})
  const [pendingCommentCounts, setPendingCommentCounts] = React.useState<Record<string, number>>({})
  const [compileRuns, setCompileRuns] = React.useState<Record<string, CompileRun[]>>({})
  const [streamliningRuns, setStreamliningRuns] = React.useState<Record<string, StreamliningRun[]>>({})
  const [schedules, setSchedules] = React.useState<Record<string, CompileSchedule>>({})
  const [membersByKb, setMembersByKb] = React.useState<Record<string, Member[]>>({})
  const [guidelinesByKb, setGuidelinesByKb] = React.useState<Record<string, Guideline[]>>({})
  const oauthConfigJson = buildOAuthMcpConfig()

  React.useEffect(() => {
    if (!token) return
    fetchKBs().catch(() => {})
    apiFetch<Usage>('/v1/usage', token).then(setUsage).catch(() => {})
  }, [token, fetchKBs])

  React.useEffect(() => {
    if (!token || kbLoading || knowledgeBases.length === 0) return
    const adminKbs = knowledgeBases.filter((kb) => ADMIN_ROLES.has(kb.role))
    Promise.all(adminKbs.map(async (kb) => {
      const [preview, runs, streamlining, schedule, members, guidelines] = await Promise.all([
        apiFetch<CompilePreview>(`/v1/knowledge-bases/${kb.id}/compile-preview`, token).catch(() => ({ pending_source_count: 0, pending_comment_count: 0 })),
        apiFetch<CompileRun[]>(`/v1/knowledge-bases/${kb.id}/compile-runs?limit=5`, token).catch(() => []),
        apiFetch<StreamliningRun[]>(`/v1/knowledge-bases/${kb.id}/streamlining-runs?limit=5`, token).catch(() => []),
        apiFetch<CompileSchedule>(`/v1/knowledge-bases/${kb.id}/compile-schedule`, token).catch(() => ({
          knowledge_base: kb.slug,
          enabled: false,
          provider: 'anthropic',
          model: null,
          wiki_direct_editing_enabled: kb.wiki_direct_editing_enabled,
          interval_minutes: 60,
          max_sources: DEFAULT_MAX_SOURCES,
          prompt: '',
          max_tool_rounds: DEFAULT_MAX_TOOL_ROUNDS,
          max_tokens: DEFAULT_MAX_TOKENS,
          has_provider_secret: false,
          last_run_at: null,
          last_status: null,
          last_error: null,
          next_run_at: null,
          streamlining_enabled: false,
          streamlining_interval_minutes: 1440,
          streamlining_provider: 'anthropic',
          streamlining_model: null,
          streamlining_prompt: '',
          has_streamlining_provider_secret: false,
          last_streamlining_at: null,
          last_streamlining_status: null,
          last_streamlining_error: null,
          next_streamlining_at: null,
        })),
        apiFetch<Member[]>(`/v1/knowledge-bases/${kb.id}/members`, token).catch(() => []),
        !GUIDELINES_COMMENTS_DISABLED
          ? apiFetch<Guideline[]>(`/v1/knowledge-bases/${kb.id}/guidelines`, token).catch(() => [])
          : Promise.resolve([] as Guideline[]),
      ])
      return { kbId: kb.id, preview, runs, streamlining, schedule, members, guidelines }
    })).then((results) => {
      setPendingCounts(Object.fromEntries(results.map((r) => [r.kbId, r.preview.pending_source_count])))
      setPendingCommentCounts(Object.fromEntries(results.map((r) => [r.kbId, r.preview.pending_comment_count ?? 0])))
      setCompileRuns(Object.fromEntries(results.map((r) => [r.kbId, r.runs])))
      setStreamliningRuns(Object.fromEntries(results.map((r) => [r.kbId, r.streamlining])))
      setSchedules(Object.fromEntries(results.map((r) => [r.kbId, r.schedule])))
      setMembersByKb(Object.fromEntries(results.map((r) => [r.kbId, r.members])))
      setGuidelinesByKb(Object.fromEntries(results.map((r) => [r.kbId, r.guidelines])))
    }).catch(() => {})
  }, [token, kbLoading, knowledgeBases])

  const handleCopyConfig = async () => {
    try {
      await navigator.clipboard.writeText(oauthConfigJson)
      setConfigCopied(true)
      setTimeout(() => setConfigCopied(false), 2000)
    } catch {
      toast.error('Failed to copy config')
    }
  }

  const onScheduleChange = (kbId: string, patch: Partial<CompileSchedule>) => {
    setSchedules((prev) => ({ ...prev, [kbId]: { ...prev[kbId], ...patch } }))
  }

  const refreshKbAdminData = async (kb: KnowledgeBase) => {
    if (!token || !ADMIN_ROLES.has(kb.role)) return
    const [preview, runs, streamlining, schedule, members] = await Promise.all([
      apiFetch<CompilePreview>(`/v1/knowledge-bases/${kb.id}/compile-preview`, token).catch(() => ({ pending_source_count: 0, pending_comment_count: 0 })),
      apiFetch<CompileRun[]>(`/v1/knowledge-bases/${kb.id}/compile-runs?limit=5`, token).catch(() => []),
      apiFetch<StreamliningRun[]>(`/v1/knowledge-bases/${kb.id}/streamlining-runs?limit=5`, token).catch(() => []),
      apiFetch<CompileSchedule>(`/v1/knowledge-bases/${kb.id}/compile-schedule`, token),
      apiFetch<Member[]>(`/v1/knowledge-bases/${kb.id}/members`, token),
    ])
    setPendingCounts((prev) => ({ ...prev, [kb.id]: preview.pending_source_count }))
    setPendingCommentCounts((prev) => ({ ...prev, [kb.id]: preview.pending_comment_count ?? 0 }))
    setCompileRuns((prev) => ({ ...prev, [kb.id]: runs }))
    setStreamliningRuns((prev) => ({ ...prev, [kb.id]: streamlining }))
    setSchedules((prev) => ({ ...prev, [kb.id]: schedule }))
    setMembersByKb((prev) => ({ ...prev, [kb.id]: members }))
  }

  const handleCompileNow = async (kbId: string, kbName: string) => {
    if (!token) return
    const kb = knowledgeBases.find((item) => item.id === kbId)
    setRunningKbId(kbId)
    try {
      const result = await apiFetch<{ status: string; source_count: number }>(`/v1/knowledge-bases/${kbId}/compile-now`, token, { method: 'POST' })
      if (result.status === 'skipped') toast.success(`No new sources to compile for ${kbName}`)
      else toast.success(`Compiled ${result.source_count} source${result.source_count === 1 ? '' : 's'} for ${kbName}`)
    } catch (err) {
      toast.error((err as Error).message || 'Compile failed')
    } finally {
      if (kb) await refreshKbAdminData(kb).catch(() => {})
      setRunningKbId(null)
    }
  }

  const handleStreamlineNow = async (kbId: string, kbName: string) => {
    if (!token) return
    const kb = knowledgeBases.find((item) => item.id === kbId)
    setRunningStreamliningKbId(kbId)
    try {
      const result = await apiFetch<{ status: string; scope_type?: string }>(`/v1/knowledge-bases/${kbId}/streamline-now?force_full=true`, token, { method: 'POST' })
      if (result.status === 'skipped') toast.success(`No streamlining changes needed for ${kbName}`)
      else toast.success(`Streamlining completed for ${kbName}`)
    } catch (err) {
      toast.error((err as Error).message || 'Streamlining failed')
    } finally {
      if (kb) await refreshKbAdminData(kb).catch(() => {})
      setRunningStreamliningKbId(null)
    }
  }

  const handleRecompileFromScratch = async (kbId: string, kbName: string) => {
    if (!token) return
    const kb = knowledgeBases.find((item) => item.id === kbId)
    setRebuildingKbId(kbId)
    try {
      const result = await apiFetch<{ status: string; source_count: number; reset_source_count: number }>(
        `/v1/knowledge-bases/${kbId}/recompile-from-scratch`,
        token,
        { method: 'POST' },
      )
      toast.success(
        result.status === 'succeeded'
          ? `Recompiled ${result.source_count} source${result.source_count === 1 ? '' : 's'} from scratch for ${kbName}`
          : `Recompile reset ${result.reset_source_count} source${result.reset_source_count === 1 ? '' : 's'} for ${kbName}`,
      )
    } catch (err) {
      toast.error((err as Error).message || 'Recompile from scratch failed')
    } finally {
      if (kb) await refreshKbAdminData(kb).catch(() => {})
      setRebuildingKbId(null)
    }
  }

  const handleSaveSchedule = async (kbId: string) => {
    if (!token) return
    const schedule = schedules[kbId]
    if (!schedule) return
    setSavingScheduleKbId(kbId)
    try {
      await apiFetch<CompileSchedule>(`/v1/knowledge-bases/${kbId}/compile-schedule`, token, {
        method: 'PUT',
        body: JSON.stringify({
          enabled: schedule.enabled,
          provider: schedule.provider,
          model: schedule.model,
          wiki_direct_editing_enabled: schedule.wiki_direct_editing_enabled,
          interval_minutes: schedule.interval_minutes,
          max_sources: schedule.max_sources,
          provider_secret: (schedule as CompileSchedule & { provider_secret?: string }).provider_secret,
          max_tool_rounds: schedule.max_tool_rounds,
          max_tokens: schedule.max_tokens,
          streamlining_enabled: schedule.streamlining_enabled,
          streamlining_interval_minutes: schedule.streamlining_interval_minutes,
          streamlining_provider: schedule.streamlining_provider,
          streamlining_model: schedule.streamlining_model,
          streamlining_prompt: schedule.streamlining_prompt,
          streamlining_provider_secret: (schedule as CompileSchedule & { streamlining_provider_secret?: string }).streamlining_provider_secret,
        }),
      })
      const refreshedKbs = await fetchKBs()
      const kb = refreshedKbs.find((item) => item.id === kbId) ?? knowledgeBases.find((item) => item.id === kbId)
      if (kb) await refreshKbAdminData(kb)
      toast.success('Schedule saved')
    } catch (err) {
      toast.error((err as Error).message || 'Failed to save schedule')
    } finally {
      setSavingScheduleKbId(null)
    }
  }

  const handleCreateInvite = async (kbId: string, email: string, role: string) => {
    if (!token) return
    if (!email.trim()) return
    try {
      const member = await apiFetch<Member>(`/v1/knowledge-bases/${kbId}/invites`, token, {
        method: 'POST',
        body: JSON.stringify({ email, role }),
      })
      setMembersByKb((prev) => ({ ...prev, [kbId]: [...(prev[kbId] || []), member] }))
      toast.success(`Added ${member.display_name || member.email || 'collaborator'} to the wiki`)
    } catch (err) {
      toast.error((err as Error).message || 'Failed to add collaborator')
    }
  }

  const handleUpdateMember = async (kbId: string, memberId: string, role: string) => {
    if (!token) return
    try {
      const updated = await apiFetch<Member>(`/v1/knowledge-bases/${kbId}/members/${memberId}`, token, {
        method: 'PATCH',
        body: JSON.stringify({ role }),
      })
      setMembersByKb((prev) => ({
        ...prev,
        [kbId]: (prev[kbId] || []).map((member) => (member.user_id === memberId ? updated : member)),
      }))
      toast.success('Member updated')
    } catch (err) {
      toast.error((err as Error).message || 'Failed to update member')
    }
  }

  const handleRemoveMember = async (kbId: string, memberId: string) => {
    if (!token) return
    try {
      await apiFetch(`/v1/knowledge-bases/${kbId}/members/${memberId}`, token, { method: 'DELETE' })
      setMembersByKb((prev) => ({ ...prev, [kbId]: (prev[kbId] || []).filter((member) => member.user_id !== memberId) }))
      toast.success('Member removed')
    } catch (err) {
      toast.error((err as Error).message || 'Failed to remove member')
    }
  }

  const handleAddGuideline = async (kbId: string, body: string) => {
    if (!token) return
    try {
      const g = await apiFetch<Guideline>(`/v1/knowledge-bases/${kbId}/guidelines`, token, {
        method: 'POST',
        body: JSON.stringify({ body }),
      })
      setGuidelinesByKb((prev) => ({ ...prev, [kbId]: [...(prev[kbId] || []), g] }))
    } catch (err) {
      toast.error((err as Error).message || 'Failed to add guideline')
      throw err
    }
  }

  const handleAddGuidelines = async (kbId: string, bodies: string[]) => {
    if (!token || bodies.length === 0) return
    try {
      const created = await apiFetch<Guideline[]>(`/v1/knowledge-bases/${kbId}/guidelines/batch`, token, {
        method: 'POST',
        body: JSON.stringify({ bodies }),
      })
      setGuidelinesByKb((prev) => ({ ...prev, [kbId]: [...(prev[kbId] || []), ...created] }))
      toast.success(`Added ${created.length} guideline${created.length === 1 ? '' : 's'}`)
    } catch (err) {
      toast.error((err as Error).message || 'Failed to add guidelines')
      throw err
    }
  }

  const handleUpdateGuideline = async (kbId: string, guidelineId: string, body: string) => {
    if (!token) return
    try {
      const g = await apiFetch<Guideline>(`/v1/knowledge-bases/${kbId}/guidelines/${guidelineId}`, token, {
        method: 'PATCH',
        body: JSON.stringify({ body }),
      })
      setGuidelinesByKb((prev) => ({
        ...prev,
        [kbId]: (prev[kbId] || []).map((item) => (item.id === guidelineId ? g : item)),
      }))
    } catch (err) {
      toast.error((err as Error).message || 'Failed to update guideline')
      throw err
    }
  }

  const handleToggleGuideline = async (kbId: string, guidelineId: string, isActive: boolean) => {
    if (!token) return
    // Optimistic update
    setGuidelinesByKb((prev) => ({
      ...prev,
      [kbId]: (prev[kbId] || []).map((item) => (item.id === guidelineId ? { ...item, is_active: isActive } : item)),
    }))
    try {
      await apiFetch<Guideline>(`/v1/knowledge-bases/${kbId}/guidelines/${guidelineId}`, token, {
        method: 'PATCH',
        body: JSON.stringify({ is_active: isActive }),
      })
    } catch (err) {
      // Revert optimistic update
      setGuidelinesByKb((prev) => ({
        ...prev,
        [kbId]: (prev[kbId] || []).map((item) => (item.id === guidelineId ? { ...item, is_active: !isActive } : item)),
      }))
      toast.error((err as Error).message || 'Failed to update guideline')
    }
  }

  const handleDeleteGuideline = async (kbId: string, guidelineId: string) => {
    if (!token) return
    try {
      await apiFetch(`/v1/knowledge-bases/${kbId}/guidelines/${guidelineId}`, token, { method: 'DELETE' })
      setGuidelinesByKb((prev) => ({
        ...prev,
        [kbId]: (prev[kbId] || []).filter((item) => item.id !== guidelineId),
      }))
    } catch (err) {
      toast.error((err as Error).message || 'Failed to delete guideline')
    }
  }

  const handleDeleteWiki = async () => {
    if (!deleteDialog) return
    setDeletingKbId(deleteDialog.kbId)
    try {
      await deleteKB(deleteDialog.kbId)
      setDeleteDialog(null)
      setDeleteConfirmation('')
      toast.success(`Deleted ${deleteDialog.kbName}`)
      await fetchKBs()
    } catch (err) {
      toast.error((err as Error).message || 'Failed to delete wiki')
    } finally {
      setDeletingKbId(null)
    }
  }

  return (
    <div className="max-w-5xl mx-auto p-8 space-y-8">
      <div className="flex items-center gap-3">
        <button
          onClick={() => router.back()}
          className="p-1 rounded-md hover:bg-accent transition-colors cursor-pointer text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-4" />
        </button>
        <h1 className="text-xl font-semibold tracking-tight">Settings</h1>
      </div>

      {usage && (
        <section>
          <h2 className="text-base font-medium">Usage</h2>
          <p className="mt-1 text-sm text-muted-foreground">{usage.document_count} document{usage.document_count !== 1 ? 's' : ''} uploaded</p>
          <div className="mt-4 grid gap-4 md:grid-cols-2">
            <div>
              <div className="flex items-center justify-between text-sm mb-1.5">
                <span className="text-muted-foreground">Storage</span>
                <span className="font-mono text-xs">{formatBytes(usage.total_storage_bytes)} / {formatBytes(usage.max_storage_bytes)}</span>
              </div>
              <div className="h-2 rounded-full bg-muted overflow-hidden">
                <div className="h-full rounded-full bg-primary" style={{ width: `${Math.min(100, (usage.total_storage_bytes / usage.max_storage_bytes) * 100)}%` }} />
              </div>
            </div>
            {usage.page_limits_enabled && (
            <div>
              <div className="flex items-center justify-between text-sm mb-1.5">
                <span className="text-muted-foreground">OCR Pages</span>
                <span className="font-mono text-xs">{usage.total_pages.toLocaleString()} / {usage.max_pages.toLocaleString()}</span>
              </div>
              <div className="h-2 rounded-full bg-muted overflow-hidden">
                <div className="h-full rounded-full bg-primary" style={{ width: `${Math.min(100, (usage.total_pages / usage.max_pages) * 100)}%` }} />
              </div>
            </div>
            )}
          </div>
        </section>
      )}

      <section>
        <h2 className="text-base font-medium">Connect via OAuth</h2>
        <p className="mt-2 text-sm text-muted-foreground">Add this configuration to your MCP client. On first connection, it should prompt you to sign in with Supabase.</p>
        <div className="relative mt-4">
          <pre className="rounded-lg bg-muted border border-border p-4 text-sm font-mono overflow-x-auto text-foreground">{oauthConfigJson}</pre>
          <button
            onClick={handleCopyConfig}
            className="absolute top-3 right-3 flex items-center gap-1.5 rounded-md bg-background border border-border px-2.5 py-1.5 text-xs text-muted-foreground hover:text-foreground hover:bg-accent cursor-pointer"
          >
            {configCopied ? <><Check size={12} />Copied</> : <><Copy size={12} />Copy</>}
          </button>
        </div>
        <p className="mt-3 text-xs text-muted-foreground">MCP URL: <code className="text-xs bg-muted px-1.5 py-0.5 rounded font-mono">{MCP_URL}</code></p>
      </section>

      <section>
        <h2 className="text-base font-medium">Knowledge bases</h2>
        <div className="mt-4 space-y-4">
          {knowledgeBases.map((kb) => (
            <ScheduleCard
              key={kb.id}
              kb={kb}
              schedule={schedules[kb.id]}
              pendingCount={pendingCounts[kb.id]}
              pendingCommentCount={pendingCommentCounts[kb.id]}
              runs={compileRuns[kb.id] || []}
              streamliningRuns={streamliningRuns[kb.id] || []}
              members={membersByKb[kb.id] || []}
              saving={savingScheduleKbId === kb.id}
              running={runningKbId === kb.id}
              rebuilding={rebuildingKbId === kb.id}
              runningStreamlining={runningStreamliningKbId === kb.id}
              deleting={deletingKbId === kb.id}
              onScheduleChange={onScheduleChange}
              onSaveSchedule={handleSaveSchedule}
              onCompileNow={handleCompileNow}
              onRecompileFromScratch={handleRecompileFromScratch}
              onStreamlineNow={handleStreamlineNow}
              onCreateInvite={handleCreateInvite}
              onUpdateMember={handleUpdateMember}
              onRemoveMember={handleRemoveMember}
              onDeleteWiki={(kbId, kbName) => {
                setDeleteDialog({ kbId, kbName })
                setDeleteConfirmation('')
              }}
              guidelines={guidelinesByKb[kb.id] || []}
              onAddGuideline={handleAddGuideline}
              onAddGuidelines={handleAddGuidelines}
              onUpdateGuideline={handleUpdateGuideline}
              onToggleGuideline={handleToggleGuideline}
              onDeleteGuideline={handleDeleteGuideline}
            />
          ))}
          {!kbLoading && knowledgeBases.length === 0 && <p className="text-sm text-muted-foreground">No knowledge bases found.</p>}
        </div>
      </section>

      <DeleteWikiDialog
        open={deleteDialog !== null}
        name={deleteDialog?.kbName ?? ''}
        value={deleteConfirmation}
        deleting={deleteDialog !== null && deletingKbId === deleteDialog.kbId}
        onValueChange={setDeleteConfirmation}
        onOpenChange={(open) => {
          if (!open) {
            setDeleteDialog(null)
            setDeleteConfirmation('')
          }
        }}
        onConfirm={handleDeleteWiki}
      />
    </div>
  )
}
