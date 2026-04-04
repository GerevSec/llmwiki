'use client'

import * as React from 'react'
import {
  Bold, Italic, Heading2, List, ListOrdered, LinkIcon,
  Table2, Undo2, Redo2,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { Popover, PopoverTrigger, PopoverContent } from '@/components/ui/popover'
import type { Editor } from '@tiptap/react'

interface NoteToolbarProps {
  editor: Editor | null
  saveStatus: 'saved' | 'saving' | 'idle'
  wordCount: number
}

export function NoteToolbar({ editor, saveStatus, wordCount }: NoteToolbarProps) {
  const [linkOpen, setLinkOpen] = React.useState(false)
  const [linkUrl, setLinkUrl] = React.useState('')

  const handleLinkSubmit = () => {
    const url = linkUrl.trim()
    if (url) {
      editor?.chain().focus().setLink({ href: url }).run()
    }
    setLinkUrl('')
    setLinkOpen(false)
  }

  return (
    <div className="flex items-center gap-0.5 px-6 py-1.5 border-b border-border shrink-0">
      <ToolbarButton
        onClick={() => editor?.chain().focus().undo().run()}
        disabled={!editor?.can().undo()}
        title="Undo"
      >
        <Undo2 className="size-3.5" />
      </ToolbarButton>
      <ToolbarButton
        onClick={() => editor?.chain().focus().redo().run()}
        disabled={!editor?.can().redo()}
        title="Redo"
      >
        <Redo2 className="size-3.5" />
      </ToolbarButton>

      <div className="w-px h-4 bg-border mx-1" />

      <ToolbarButton
        active={editor?.isActive('bold')}
        onClick={() => editor?.chain().focus().toggleBold().run()}
        title="Bold"
      >
        <Bold className="size-3.5" />
      </ToolbarButton>
      <ToolbarButton
        active={editor?.isActive('italic')}
        onClick={() => editor?.chain().focus().toggleItalic().run()}
        title="Italic"
      >
        <Italic className="size-3.5" />
      </ToolbarButton>
      <ToolbarButton
        active={editor?.isActive('heading', { level: 2 })}
        onClick={() => editor?.chain().focus().toggleHeading({ level: 2 }).run()}
        title="Heading"
      >
        <Heading2 className="size-3.5" />
      </ToolbarButton>

      <div className="w-px h-4 bg-border mx-1" />

      <ToolbarButton
        active={editor?.isActive('bulletList')}
        onClick={() => editor?.chain().focus().toggleBulletList().run()}
        title="Bullet List"
      >
        <List className="size-3.5" />
      </ToolbarButton>
      <ToolbarButton
        active={editor?.isActive('orderedList')}
        onClick={() => editor?.chain().focus().toggleOrderedList().run()}
        title="Ordered List"
      >
        <ListOrdered className="size-3.5" />
      </ToolbarButton>
      {editor?.isActive('link') ? (
        <ToolbarButton
          active
          onClick={() => editor.chain().focus().unsetLink().run()}
          title="Remove link"
        >
          <LinkIcon className="size-3.5" />
        </ToolbarButton>
      ) : (
        <Popover open={linkOpen} onOpenChange={(open) => {
          setLinkOpen(open)
          if (!open) setLinkUrl('')
        }}>
          <PopoverTrigger asChild>
            <button
              title="Link"
              className={cn(
                'p-1.5 rounded-md transition-colors cursor-pointer',
                'text-muted-foreground hover:text-foreground hover:bg-accent'
              )}
            >
              <LinkIcon className="size-3.5" />
            </button>
          </PopoverTrigger>
          <PopoverContent className="w-72 p-2" align="start" side="bottom">
            <form
              onSubmit={(e) => { e.preventDefault(); handleLinkSubmit() }}
              className="flex items-center gap-1.5"
            >
              <input
                type="url"
                value={linkUrl}
                onChange={(e) => setLinkUrl(e.target.value)}
                placeholder="https://..."
                autoFocus
                className="flex-1 text-sm bg-transparent border border-border rounded-md px-2 py-1 focus:outline-none focus:ring-1 focus:ring-ring placeholder:text-muted-foreground/40"
              />
              <button
                type="submit"
                disabled={!linkUrl.trim()}
                className="text-sm px-2 py-1 rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-40 cursor-pointer"
              >
                Add
              </button>
            </form>
          </PopoverContent>
        </Popover>
      )}

      <div className="w-px h-4 bg-border mx-1" />

      <ToolbarButton
        onClick={() => editor?.chain().focus().insertTable({ rows: 3, cols: 3, withHeaderRow: true }).run()}
        title="Insert Table"
      >
        <Table2 className="size-3.5" />
      </ToolbarButton>

      <div className="flex-1" />

      <span className="text-[10px] text-muted-foreground mr-2">
        {saveStatus === 'saving' ? 'Saving...' : saveStatus === 'saved' ? 'Saved' : ''}
      </span>
      <span className="text-[10px] text-muted-foreground tabular-nums mr-2">
        {wordCount} {wordCount === 1 ? 'word' : 'words'}
      </span>
    </div>
  )
}

function ToolbarButton({
  children,
  active,
  onClick,
  title,
  disabled,
}: {
  children: React.ReactNode
  active?: boolean
  onClick?: () => void
  title?: string
  disabled?: boolean
}) {
  return (
    <button
      onClick={onClick}
      title={title}
      disabled={disabled}
      className={cn(
        'p-1.5 rounded-md transition-colors cursor-pointer',
        disabled
          ? 'text-muted-foreground/30 cursor-default'
          : active
            ? 'bg-accent text-foreground'
            : 'text-muted-foreground hover:text-foreground hover:bg-accent'
      )}
    >
      {children}
    </button>
  )
}
