'use client'

import * as React from 'react'
import { Copy, Check, Key, Plus, Trash2, Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'

const MCP_CONFIG = {
  mcpServers: {
    supavault: {
      url: 'https://api.supavault.dev/mcp',
      headers: {
        Authorization: 'Bearer sk_...',
      },
    },
  },
}

export default function SettingsPage() {
  const [copied, setCopied] = React.useState(false)
  const configJson = JSON.stringify(MCP_CONFIG, null, 2)

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(configJson)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      console.error('Failed to copy')
    }
  }

  return (
    <div className="max-w-2xl mx-auto p-8">
      <h1 className="text-xl font-semibold tracking-tight">Settings</h1>

      <section className="mt-8">
        <h2 className="text-base font-medium">Connect to Claude</h2>
        <p className="mt-2 text-sm text-muted-foreground">
          Add this configuration to your Claude Desktop settings to connect Claude to your vault.
        </p>
        <div className="relative mt-4">
          <pre className="rounded-lg bg-muted border border-border p-4 text-sm font-mono overflow-x-auto text-foreground">
            {configJson}
          </pre>
          <button
            onClick={handleCopy}
            className={cn(
              'absolute top-3 right-3 flex items-center gap-1.5 rounded-md px-2.5 py-1.5 text-xs transition-colors cursor-pointer',
              copied
                ? 'bg-green-500/10 text-green-600 dark:text-green-400'
                : 'bg-background border border-border text-muted-foreground hover:text-foreground hover:bg-accent'
            )}
          >
            {copied ? (
              <>
                <Check size={12} />
                Copied
              </>
            ) : (
              <>
                <Copy size={12} />
                Copy
              </>
            )}
          </button>
        </div>
        <p className="mt-3 text-xs text-muted-foreground">
          Replace <code className="text-xs bg-muted px-1.5 py-0.5 rounded font-mono">sk_...</code> with your API key from below.
        </p>
      </section>

      <div className="h-px bg-border my-8" />

      <section>
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-base font-medium">API Keys</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              Generate an API key to connect Claude to your vault.
            </p>
          </div>
          <button
            className="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors cursor-pointer"
          >
            <Plus size={14} />
            Generate Key
          </button>
        </div>

        <div className="mt-6 rounded-lg border border-border">
          <div className="flex items-center justify-center py-10 text-muted-foreground">
            <div className="text-center">
              <Key size={24} className="mx-auto mb-2 text-muted-foreground/50" />
              <p className="text-sm">No API keys yet</p>
              <p className="text-xs text-muted-foreground/60 mt-1">
                Generate a key to get started
              </p>
            </div>
          </div>
        </div>
      </section>
    </div>
  )
}
