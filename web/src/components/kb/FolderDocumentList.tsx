'use client'

import * as React from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'
import {
  Folder, ChevronRight, ChevronUp, ChevronDown, FileText, NotepadText,
  Trash2, MoreHorizontal, Pencil, Upload,
} from 'lucide-react'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { cn } from '@/lib/utils'
import { joinPath } from '@/lib/utils/folders'
import type { DocumentListItem } from '@/lib/types'

export type SortField = 'name' | 'date'
export type SortDir = 'asc' | 'desc'

export type DragItem =
  | { type: 'document'; documentId: string; currentPath: string }
  | { type: 'folder'; folderName: string; folderPath: string }
  | { type: 'multi'; ids: string[]; currentPath: string }

const DRAG_MIME = 'application/x-supavault-item'

type Props = {
  kbId: string
  folders: string[]
  documents: DocumentListItem[]
  currentPath: string
  onNavigateFolder: (folderName: string) => void
  onOpenDocument: (doc: DocumentListItem) => void
  onDeleteDocument: (docId: string) => void
  onDeleteFolder: (folderName: string) => void
  onRenameFolder: (oldName: string, newName: string) => void
  onMoveDocument: (docId: string, targetPath: string) => void
  onCreateNote: () => void
  onCreateFolder: () => void
  sortField?: SortField
  sortDir?: SortDir
  onSortChange?: (field: SortField) => void
  selectedIds?: Set<string>
  onSelect?: (itemId: string, e: React.MouseEvent) => void
}

type ListItem =
  | { kind: 'folder'; name: string }
  | { kind: 'document'; doc: DocumentListItem }

const ROW_HEIGHT = 36

export function FolderDocumentList({
  folders,
  documents,
  currentPath,
  onNavigateFolder,
  onOpenDocument,
  onDeleteDocument,
  onDeleteFolder,
  onRenameFolder,
  onMoveDocument,
  onCreateNote,
  onCreateFolder,
  sortField = 'name',
  sortDir = 'asc',
  onSortChange,
  selectedIds = new Set(),
  onSelect,
}: Props) {
  const [dragOverFolder, setDragOverFolder] = React.useState<string | null>(null)

  const items = React.useMemo<ListItem[]>(() => {
    const arr: ListItem[] = []
    folders.forEach((name) => arr.push({ kind: 'folder', name }))
    documents.forEach((doc) => arr.push({ kind: 'document', doc }))
    return arr
  }, [folders, documents])

  const parentRef = React.useRef<HTMLDivElement>(null)
  const useVirtual = items.length > 60

  const virtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 15,
    enabled: useVirtual,
  })

  const isRoot = currentPath === '/'

  if (items.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-20 gap-6">
        <div className="text-center">
          <p className="text-sm text-muted-foreground">{isRoot ? 'This knowledge base is empty' : 'This folder is empty'}</p>
          <p className="text-xs text-muted-foreground/60 mt-1">Add documents or notes to get started</p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={onCreateFolder}
            className="flex flex-col items-center gap-2 px-6 py-4 rounded-lg border border-dashed border-border hover:border-foreground/20 hover:bg-muted/50 transition-colors cursor-pointer"
          >
            <Upload className="size-5 text-muted-foreground" />
            <span className="text-xs text-muted-foreground">Upload files</span>
          </button>
          <button
            onClick={onCreateNote}
            className="flex flex-col items-center gap-2 px-6 py-4 rounded-lg border border-dashed border-border hover:border-foreground/20 hover:bg-muted/50 transition-colors cursor-pointer"
          >
            <NotepadText className="size-5 text-muted-foreground" />
            <span className="text-xs text-muted-foreground">New note</span>
          </button>
        </div>
      </div>
    )
  }

  const handleOpen = (item: ListItem) => {
    if (item.kind === 'folder') onNavigateFolder(item.name)
    else onOpenDocument(item.doc)
  }

  const handleDelete = (item: ListItem) => {
    if (item.kind === 'folder') onDeleteFolder(item.name)
    else onDeleteDocument(item.doc.id)
  }

  const renderItem = (item: ListItem) => (
    <ItemRow
      key={getItemKey(item)}
      item={item}
      currentPath={currentPath}
      dragOverFolder={dragOverFolder}
      selectedIds={selectedIds}
      onSelect={onSelect}
      onOpen={handleOpen}
      onDelete={() => handleDelete(item)}
      onRenameFolder={item.kind === 'folder' ? onRenameFolder : undefined}
      onMoveDocument={onMoveDocument}
      onDragOverFolder={setDragOverFolder}
    />
  )

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center h-8 px-2 text-[11px] font-medium text-muted-foreground/70 uppercase tracking-wider border-b border-border/50 select-none flex-shrink-0">
        <ColumnHeader
          label="Name"
          field="name"
          sortField={sortField}
          sortDir={sortDir}
          onSort={onSortChange}
          className="flex-1 min-w-0 pl-7"
        />
        <ColumnHeader
          label="Kind"
          field="name"
          sortField={sortField}
          sortDir={sortDir}
          className="w-20 flex-shrink-0"
        />
        <ColumnHeader
          label="Modified"
          field="date"
          sortField={sortField}
          sortDir={sortDir}
          onSort={onSortChange}
          className="w-[100px] flex-shrink-0 text-right justify-end"
        />
        <div className="w-7 flex-shrink-0" />
      </div>

      {!useVirtual ? (
        <ul className="flex-1 overflow-y-auto">
          {items.map(renderItem)}
        </ul>
      ) : (
        <div ref={parentRef} className="flex-1 overflow-y-auto">
          <ul
            className="relative"
            style={{ height: `${virtualizer.getTotalSize()}px` }}
          >
            {virtualizer.getVirtualItems().map((virtualRow) => {
              const item = items[virtualRow.index]
              return (
                <li
                  key={virtualRow.key}
                  className="absolute left-0 right-0"
                  style={{
                    height: `${virtualRow.size}px`,
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  {renderItem(item)}
                </li>
              )
            })}
          </ul>
        </div>
      )}
    </div>
  )
}

function ColumnHeader({
  label,
  field,
  sortField,
  sortDir,
  onSort,
  className,
}: {
  label: string
  field: SortField
  sortField: SortField
  sortDir: SortDir
  onSort?: (field: SortField) => void
  className?: string
}) {
  const active = sortField === field
  return (
    <button
      onClick={() => onSort?.(field)}
      className={cn(
        'flex items-center gap-1 px-1 py-0.5 rounded transition-colors cursor-pointer',
        'hover:text-foreground',
        active && 'text-foreground',
        className,
      )}
    >
      {label}
      {active && (
        sortDir === 'asc'
          ? <ChevronUp className="size-3" />
          : <ChevronDown className="size-3" />
      )}
    </button>
  )
}

function ItemRow({
  item,
  currentPath,
  dragOverFolder,
  selectedIds,
  onSelect,
  onOpen,
  onDelete,
  onRenameFolder,
  onMoveDocument,
  onDragOverFolder,
}: {
  item: ListItem
  currentPath: string
  dragOverFolder: string | null
  selectedIds: Set<string>
  onSelect?: (itemId: string, e: React.MouseEvent) => void
  onOpen: (item: ListItem) => void
  onDelete: () => void
  onRenameFolder?: (oldName: string, newName: string) => void
  onMoveDocument: (docId: string, targetPath: string) => void
  onDragOverFolder: (name: string | null) => void
}) {
  const isFolder = item.kind === 'folder'
  const folderPath = isFolder ? joinPath(currentPath, item.name) : ''
  const isDragOver = isFolder && dragOverFolder === item.name

  const selKey = isFolder ? `folder:${item.name}` : `doc:${item.doc.id}`
  const isSelected = selectedIds.has(selKey)

  const handleDragStart = (e: React.DragEvent) => {
    if (isFolder) {
      const payload: DragItem = { type: 'folder', folderName: item.name, folderPath }
      e.dataTransfer.setData(DRAG_MIME, JSON.stringify(payload))
      e.dataTransfer.effectAllowed = 'move'
    } else {
      const payload: DragItem = { type: 'document', documentId: item.doc.id, currentPath }
      e.dataTransfer.setData(DRAG_MIME, JSON.stringify(payload))
      e.dataTransfer.effectAllowed = 'move'
    }
  }

  const handleDragOver = isFolder ? (e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
    onDragOverFolder(item.name)
  } : undefined

  const handleDragLeave = isFolder ? (e: React.DragEvent) => {
    if (e.currentTarget.contains(e.relatedTarget as Node)) return
    onDragOverFolder(null)
  } : undefined

  const handleDrop = isFolder ? (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    onDragOverFolder(null)
    const raw = e.dataTransfer.getData(DRAG_MIME)
    if (!raw) return
    const dragItem: DragItem = JSON.parse(raw)
    if (dragItem.type === 'document') {
      onMoveDocument(dragItem.documentId, folderPath)
    }
  } : undefined

  const handleClick = (e: React.MouseEvent) => {
    if (e.metaKey || e.ctrlKey || e.shiftKey) {
      onSelect?.(selKey, e)
      return
    }
    onOpen(item)
  }

  const { icon, label, hoverColor, kindLabel } = getRowMeta(item)
  const doc = !isFolder ? item.doc : null
  const date = doc ? (doc.updated_at || doc.created_at) : null

  return (
    <li
      draggable
      onDragStart={handleDragStart}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
      onClick={handleClick}
      className={cn(
        'flex items-center h-9 px-2 cursor-pointer group transition-colors',
        isDragOver
          ? 'ring-1 ring-primary bg-primary/5'
          : isSelected
            ? 'bg-primary/5'
            : hoverColor,
      )}
    >
      <span className="flex-shrink-0 w-5 flex items-center justify-center mr-2">
        {icon}
      </span>

      <span className="flex-1 min-w-0 text-[13px] text-foreground truncate">
        {label}
      </span>

      {isFolder && (
        <ChevronRight className="size-3 text-muted-foreground/50 flex-shrink-0 mr-1" />
      )}

      <span className="w-20 flex-shrink-0 text-[11px] text-muted-foreground">
        {kindLabel}
      </span>

      <span className="w-[100px] flex-shrink-0 text-right text-[11px] text-muted-foreground tabular-nums">
        {date ? formatDate(date) : ''}
      </span>

      <div className="w-7 flex-shrink-0 flex items-center justify-center">
        <RowMenu
          item={item}
          onDelete={onDelete}
          onRename={isFolder && onRenameFolder ? () => {
            const newName = window.prompt('Rename folder', item.name)
            if (newName && newName !== item.name) onRenameFolder(item.name, newName)
          } : undefined}
        />
      </div>
    </li>
  )
}

function RowMenu({
  item,
  onDelete,
  onRename,
}: {
  item: ListItem
  onDelete: () => void
  onRename?: () => void
}) {
  const isFolder = item.kind === 'folder'

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button
          onClick={(e) => e.stopPropagation()}
          className="opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-foreground transition-all p-0.5 cursor-pointer"
        >
          <MoreHorizontal className="size-3.5" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" onClick={(e) => e.stopPropagation()}>
        {isFolder && onRename && (
          <>
            <DropdownMenuItem onClick={onRename}>
              <Pencil className="size-3.5 mr-2" />
              Rename
            </DropdownMenuItem>
            <DropdownMenuSeparator />
          </>
        )}
        <DropdownMenuItem className="text-destructive focus:text-destructive" onClick={onDelete}>
          <Trash2 className="size-3.5 mr-2" />
          Delete
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

const kindLabels: Record<string, string> = {
  pdf: 'PDF',
  docx: 'Word',
  pptx: 'PowerPoint',
  html: 'HTML',
  md: 'Note',
  txt: 'Text',
  csv: 'CSV',
  xlsx: 'Excel',
}

const fileTypeIcons: Record<string, React.ReactNode> = {
  md: <NotepadText className="size-4 text-amber-500/70" />,
  txt: <FileText className="size-4 text-muted-foreground" />,
}

function getRowMeta(item: ListItem) {
  if (item.kind === 'folder') {
    return {
      icon: <Folder className="size-4 text-muted-foreground" />,
      label: item.name,
      hoverColor: 'hover:bg-accent',
      kindLabel: 'Folder',
    }
  }

  const doc = item.doc
  const isNote = doc.file_type === 'md'

  return {
    icon: fileTypeIcons[doc.file_type] ?? <FileText className="size-4 text-muted-foreground" />,
    label: doc.title || doc.filename,
    hoverColor: isNote ? 'hover:bg-amber-500/5' : 'hover:bg-accent',
    kindLabel: kindLabels[doc.file_type] ?? doc.file_type.toUpperCase(),
  }
}

function getItemKey(item: ListItem) {
  if (item.kind === 'folder') return `folder-${item.name}`
  return `doc-${item.doc.id}`
}

function formatDate(dateStr: string): string {
  const d = new Date(dateStr)
  const now = new Date()
  const diffMs = now.getTime() - d.getTime()
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))

  if (diffDays === 0) return 'Today'
  if (diffDays === 1) return 'Yesterday'
  if (diffDays < 7) return `${diffDays}d ago`

  if (d.getFullYear() === now.getFullYear()) {
    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
  }

  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: '2-digit' })
}
