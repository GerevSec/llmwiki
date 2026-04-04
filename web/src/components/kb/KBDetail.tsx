'use client'

import * as React from 'react'
import { Loader2, Upload, Trash2, X, NotepadText, Folder, ChevronLeft, ChevronRight } from 'lucide-react'
import {
  ContextMenu,
  ContextMenuTrigger,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
} from '@/components/ui/context-menu'
import { useUserStore } from '@/stores'
import { useKBDocuments } from '@/hooks/useKBDocuments'
import { apiFetch } from '@/lib/api'
import { toast } from 'sonner'
import {
  getChildFolderNames,
  filterDocumentsAtPath,
  joinPath,
  parseBreadcrumbs,
  isDescendant,
  rebasePath,
} from '@/lib/utils/folders'
import { FolderDocumentList, type SortField, type SortDir } from '@/components/kb/FolderDocumentList'
import type { DocumentListItem } from '@/lib/types'
import { cn } from '@/lib/utils'

const pathCache = new Map<string, string>()

type Props = {
  kbId: string
  kbSlug: string
}

export function KBDetail({ kbId, kbSlug }: Props) {
  const userId = useUserStore((s) => s.user?.id)
  const { documents, setDocuments, loading, refetchDocuments } = useKBDocuments(kbId)

  const [currentPath, setCurrentPathRaw] = React.useState(() => pathCache.get(kbId) ?? '/')
  const historyRef = React.useRef<string[]>([pathCache.get(kbId) ?? '/'])
  const historyIndexRef = React.useRef(0)
  const [canGoBack, setCanGoBack] = React.useState(false)
  const [canGoForward, setCanGoForward] = React.useState(false)

  const updateHistoryBooleans = React.useCallback(() => {
    setCanGoBack(historyIndexRef.current > 0)
    setCanGoForward(historyIndexRef.current < historyRef.current.length - 1)
  }, [])

  const setCurrentPath = React.useCallback((path: string) => {
    pathCache.set(kbId, path)
    setCurrentPathRaw(path)
  }, [kbId])

  const navigateFolder = React.useCallback((path: string) => {
    const idx = historyIndexRef.current
    historyRef.current = historyRef.current.slice(0, idx + 1)
    historyRef.current.push(path)
    historyIndexRef.current = historyRef.current.length - 1
    updateHistoryBooleans()
    setCurrentPath(path)
  }, [setCurrentPath, updateHistoryBooleans])

  const goBack = React.useCallback(() => {
    if (historyIndexRef.current <= 0) return
    historyIndexRef.current--
    const path = historyRef.current[historyIndexRef.current]
    updateHistoryBooleans()
    setCurrentPath(path)
  }, [setCurrentPath, updateHistoryBooleans])

  const goForward = React.useCallback(() => {
    if (historyIndexRef.current >= historyRef.current.length - 1) return
    historyIndexRef.current++
    const path = historyRef.current[historyIndexRef.current]
    updateHistoryBooleans()
    setCurrentPath(path)
  }, [setCurrentPath, updateHistoryBooleans])

  const [sortField, setSortField] = React.useState<SortField>(() => {
    if (typeof window === 'undefined') return 'date'
    const stored = localStorage.getItem('supavault:sort-field')
    return stored === 'name' || stored === 'date' ? stored : 'date'
  })
  const [sortDir, setSortDir] = React.useState<SortDir>(() => {
    if (typeof window === 'undefined') return 'desc'
    const stored = localStorage.getItem('supavault:sort-direction') as SortDir
    return stored === 'asc' || stored === 'desc' ? stored : 'desc'
  })

  const handleSortChange = (field: SortField) => {
    if (field === sortField) {
      const next = sortDir === 'asc' ? 'desc' : 'asc'
      setSortDir(next)
      localStorage.setItem('supavault:sort-direction', next)
    } else {
      setSortField(field)
      setSortDir('asc')
      localStorage.setItem('supavault:sort-field', field)
      localStorage.setItem('supavault:sort-direction', 'asc')
    }
  }

  const [selectedIds, setSelectedIds] = React.useState<Set<string>>(new Set())
  const [lastSelectedId, setLastSelectedId] = React.useState<string | null>(null)

  const [fileDragOver, setFileDragOver] = React.useState(false)
  const dragCounterRef = React.useRef(0)

  const allFolders = React.useMemo(() => {
    const paths = new Set<string>()
    for (const doc of documents) {
      const p = doc.path ?? '/'
      if (p !== '/') {
        const parts = p.split('/').filter(Boolean)
        let acc = '/'
        for (const part of parts) {
          acc += part + '/'
          paths.add(acc)
        }
      }
    }
    return ['/', ...Array.from(paths)]
  }, [documents])

  const childFolderNames = React.useMemo(() => {
    let names = getChildFolderNames(allFolders, currentPath).filter((n) => !n.startsWith('.'))
    names.sort((a, b) => {
      const cmp = a.localeCompare(b)
      return sortField === 'name' && sortDir === 'desc' ? -cmp : cmp
    })
    return names
  }, [allFolders, currentPath, sortField, sortDir])

  const documentsAtPath = React.useMemo(() => {
    let docs = filterDocumentsAtPath(documents, currentPath).filter((d) => !d.archived)
    docs = sortDocuments(docs, sortField, sortDir)
    return docs
  }, [documents, currentPath, sortField, sortDir])

  const allItemIds = React.useMemo(() => {
    const ids: string[] = []
    childFolderNames.forEach((name) => ids.push(`folder:${name}`))
    documentsAtPath.forEach((d) => ids.push(`doc:${d.id}`))
    return ids
  }, [childFolderNames, documentsAtPath])

  React.useEffect(() => {
    setSelectedIds(new Set())
    setLastSelectedId(null)
  }, [currentPath])

  const handleSelect = React.useCallback((itemId: string, e: React.MouseEvent) => {
    if (e.metaKey || e.ctrlKey) {
      setSelectedIds((prev) => {
        const next = new Set(prev)
        if (next.has(itemId)) next.delete(itemId)
        else next.add(itemId)
        return next
      })
      setLastSelectedId(itemId)
    } else if (e.shiftKey && lastSelectedId) {
      const startIdx = allItemIds.indexOf(lastSelectedId)
      const endIdx = allItemIds.indexOf(itemId)
      if (startIdx !== -1 && endIdx !== -1) {
        const [lo, hi] = startIdx < endIdx ? [startIdx, endIdx] : [endIdx, startIdx]
        setSelectedIds(new Set(allItemIds.slice(lo, hi + 1)))
      }
    } else {
      setSelectedIds(new Set([itemId]))
      setLastSelectedId(itemId)
    }
  }, [lastSelectedId, allItemIds])

  const handleDeselectAll = React.useCallback(() => {
    setSelectedIds(new Set())
    setLastSelectedId(null)
  }, [])

  const getToken = () => {
    const token = useUserStore.getState().accessToken
    if (!token) {
      toast.error('Not authenticated')
      return null
    }
    return token
  }

  const handleDeleteDocument = async (docId: string) => {
    const token = getToken()
    if (!token) return

    const doc = documents.find((d) => d.id === docId)
    if (!doc) return

    setDocuments((prev) => prev.filter((d) => d.id !== docId))

    try {
      await apiFetch(`/v1/documents/${docId}`, token, { method: 'DELETE' })
      toast('Document deleted')
    } catch (err) {
      console.error('Failed to delete:', err)
      toast.error('Failed to delete document')
      if (doc) setDocuments((prev) => [doc, ...prev])
    }
  }

  const handleDeleteSelected = async () => {
    for (const id of selectedIds) {
      if (id.startsWith('doc:')) {
        await handleDeleteDocument(id.slice(4))
      } else if (id.startsWith('folder:')) {
        handleDeleteFolder(id.slice(7))
      }
    }
    setSelectedIds(new Set())
  }

  React.useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return

      if ((e.key === 'Delete' || e.key === 'Backspace') && selectedIds.size > 0) {
        e.preventDefault()
        handleDeleteSelected()
      } else if (e.key === 'Escape') {
        handleDeselectAll()
      } else if (e.key === 'a' && (e.metaKey || e.ctrlKey)) {
        e.preventDefault()
        setSelectedIds(new Set(allItemIds))
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  })

  const handleCreateNote = async () => {
    const token = getToken()
    if (!token || !userId) return

    try {
      const data = await apiFetch<DocumentListItem>(`/v1/knowledge-bases/${kbId}/documents/note`, token, {
        method: 'POST',
        body: JSON.stringify({ path: currentPath }),
      })
      setDocuments((prev) => [data, ...prev])
      toast.success('Note created')
    } catch {
      toast.error('Failed to create note')
    }
  }

  const handleCreateFolder = () => {
    let name = 'Untitled Folder'
    let i = 1
    while (childFolderNames.includes(name)) { name = `Untitled Folder ${i++}` }

    const newPath = joinPath(currentPath, name)

    const docsNeedingUpdate = documents.filter((d) => (d.path ?? '/') === currentPath)
    if (docsNeedingUpdate.length === 0) {
      const fakePlaceholder = documents[0]
      if (fakePlaceholder) {
        setDocuments((prev) => [...prev, { ...fakePlaceholder, id: `_folder_${newPath}`, path: newPath, filename: '.folder', archived: false }])
      }
    }

    toast.success(`Folder "${name}" created`)
  }

  const handleDeleteFolder = (folderName: string) => {
    const token = getToken()
    if (!token) return
    const folderPath = joinPath(currentPath, folderName)

    const affectedDocs = documents.filter(
      (d) => (d.path ?? '/') === folderPath || isDescendant(d.path ?? '/', folderPath),
    )

    setDocuments((prev) => prev.filter((d) => !affectedDocs.some((a) => a.id === d.id)))

    for (const doc of affectedDocs) {
      apiFetch(`/v1/documents/${doc.id}`, token, { method: 'DELETE' }).catch(() => {})
    }

    toast(`Deleted folder "${folderName}" and ${affectedDocs.length} item${affectedDocs.length === 1 ? '' : 's'}`)
  }

  const handleRenameFolder = async (oldName: string, newName: string) => {
    const token = getToken()
    if (!token) return

    const oldPath = joinPath(currentPath, oldName)
    const newPath = joinPath(currentPath, newName)

    const affectedDocs = documents.filter(
      (d) => (d.path ?? '/') === oldPath || isDescendant(d.path ?? '/', oldPath),
    )

    setDocuments((prev) =>
      prev.map((d) => {
        const docPath = d.path ?? '/'
        if (docPath === oldPath || isDescendant(docPath, oldPath)) {
          return { ...d, path: rebasePath(docPath, oldPath, newPath) }
        }
        return d
      }),
    )

    for (const doc of affectedDocs) {
      const docPath = doc.path ?? '/'
      const np = rebasePath(docPath, oldPath, newPath)
      apiFetch(`/v1/documents/${doc.id}`, token, {
        method: 'PATCH',
        body: JSON.stringify({ path: np }),
      }).catch(() => {})
    }
  }

  const handleMoveDocument = async (docId: string, targetPath: string) => {
    const token = getToken()
    if (!token) return

    const doc = documents.find((d) => d.id === docId)
    if (!doc) return
    const prevPath = doc.path ?? '/'
    if (prevPath === targetPath) return

    setDocuments((prev) =>
      prev.map((d) => d.id === docId ? { ...d, path: targetPath } : d),
    )

    try {
      await apiFetch(`/v1/documents/${docId}`, token, {
        method: 'PATCH',
        body: JSON.stringify({ path: targetPath }),
      })
    } catch {
      toast.error('Failed to move document')
      setDocuments((prev) =>
        prev.map((d) => d.id === docId ? { ...d, path: prevPath } : d),
      )
    }
  }

  const handleFileDragEnter = (e: React.DragEvent) => {
    if (e.dataTransfer.types.includes('application/x-supavault-item')) return
    e.preventDefault()
    dragCounterRef.current++
    if (dragCounterRef.current === 1) setFileDragOver(true)
  }

  const handleFileDragLeave = (e: React.DragEvent) => {
    e.preventDefault()
    dragCounterRef.current--
    if (dragCounterRef.current === 0) setFileDragOver(false)
  }

  const handleFileDragOver = (e: React.DragEvent) => {
    if (e.dataTransfer.types.includes('application/x-supavault-item')) return
    e.preventDefault()
    e.dataTransfer.dropEffect = 'copy'
  }

  const handleFileDrop = async (e: React.DragEvent) => {
    if (e.dataTransfer.types.includes('application/x-supavault-item')) return
    e.preventDefault()
    dragCounterRef.current = 0
    setFileDragOver(false)

    const files = Array.from(e.dataTransfer.files)
    if (files.length === 0) return

    uploadFiles(files, currentPath)
  }

  const handleUploadClick = () => {
    const input = document.createElement('input')
    input.type = 'file'
    input.accept = '.pdf,.docx,.pptx,.csv,.xlsx,.xls,.md,.txt'
    input.multiple = true
    input.onchange = () => {
      if (input.files) uploadFiles(Array.from(input.files), currentPath)
    }
    input.click()
  }

  const uploadFiles = React.useCallback((files: File[], targetPath: string) => {
    const token = getToken()
    if (!token || !userId) return

    for (const file of files) {
      const upload = async () => {
        const ext = file.name.split('.').pop()?.toLowerCase()
        if (ext === 'md' || ext === 'txt') {
          const content = await file.text()
          const title = file.name.replace(/\.(md|txt)$/i, '')
          try {
            const data = await apiFetch<DocumentListItem>(`/v1/knowledge-bases/${kbId}/documents/note`, token, {
              method: 'POST',
              body: JSON.stringify({ title, content, path: targetPath }),
            })
            setDocuments((prev) => [data, ...prev])
            toast.success(`Imported ${file.name}`)
          } catch {
            toast.error(`Failed to import ${file.name}`)
          }
        } else {
          toast.info(`File upload for ${ext} not yet supported`)
        }
      }
      upload()
    }
  }, [kbId, userId])

  const breadcrumbs = parseBreadcrumbs(currentPath)

  return (
    <div
      className="flex flex-col h-full relative"
      onDragEnter={handleFileDragEnter}
      onDragLeave={handleFileDragLeave}
      onDragOver={handleFileDragOver}
      onDrop={handleFileDrop}
    >
      {selectedIds.size > 0 && (
        <div className="absolute bottom-4 left-1/2 -translate-x-1/2 z-40 flex items-center gap-3 bg-card border border-border rounded-lg shadow-lg px-4 py-2">
          <span className="text-sm text-foreground font-medium">
            {selectedIds.size} item{selectedIds.size > 1 ? 's' : ''} selected
          </span>
          <button
            onClick={handleDeleteSelected}
            className="flex items-center gap-1.5 text-sm text-red-500 hover:text-red-400 transition-colors cursor-pointer"
          >
            <Trash2 className="size-3.5" />
            Delete
          </button>
          <button
            onClick={handleDeselectAll}
            className="text-muted-foreground hover:text-foreground transition-colors cursor-pointer"
          >
            <X className="size-3.5" />
          </button>
        </div>
      )}

      {fileDragOver && (
        <div className="absolute inset-0 z-50 bg-background/80 backdrop-blur-sm flex items-center justify-center pointer-events-none">
          <div className="flex flex-col items-center gap-3 border-2 border-dashed border-primary rounded-xl px-12 py-10">
            <Upload className="size-8 text-primary" />
            <p className="text-sm font-medium text-primary">Drop files to upload</p>
            <p className="text-xs text-muted-foreground">Markdown, text files</p>
          </div>
        </div>
      )}

      <div
        className="flex-1 overflow-y-auto"
        onClick={(e) => {
          if (e.target === e.currentTarget) handleDeselectAll()
        }}
      >
        <div className="px-5 py-4 w-full min-h-full flex flex-col">
          <div className="flex items-center justify-between mb-4 flex-shrink-0">
            <div className="flex items-center gap-2">
              <button
                onClick={goBack}
                disabled={!canGoBack}
                className={cn(
                  'p-1 rounded transition-colors',
                  canGoBack ? 'hover:bg-accent cursor-pointer text-foreground' : 'text-muted-foreground/30 cursor-default',
                )}
              >
                <ChevronLeft className="size-4" />
              </button>
              <button
                onClick={goForward}
                disabled={!canGoForward}
                className={cn(
                  'p-1 rounded transition-colors',
                  canGoForward ? 'hover:bg-accent cursor-pointer text-foreground' : 'text-muted-foreground/30 cursor-default',
                )}
              >
                <ChevronRight className="size-4" />
              </button>

              <nav className="flex items-center gap-1 text-sm">
                {breadcrumbs.map((crumb, i) => (
                  <React.Fragment key={crumb.path}>
                    {i > 0 && <span className="text-muted-foreground/40">/</span>}
                    <button
                      onClick={() => navigateFolder(crumb.path)}
                      className={cn(
                        'px-1.5 py-0.5 rounded transition-colors cursor-pointer',
                        i === breadcrumbs.length - 1
                          ? 'text-foreground font-medium'
                          : 'text-muted-foreground hover:text-foreground hover:bg-accent',
                      )}
                    >
                      {crumb.label}
                    </button>
                  </React.Fragment>
                ))}
              </nav>
            </div>

            <div className="flex items-center gap-2">
              <button
                onClick={handleCreateNote}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md border border-border hover:bg-accent transition-colors cursor-pointer"
              >
                <NotepadText className="size-3.5" />
                New Note
              </button>
              <button
                onClick={handleCreateFolder}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md border border-border hover:bg-accent transition-colors cursor-pointer"
              >
                <Folder className="size-3.5" />
                New Folder
              </button>
              <button
                onClick={handleUploadClick}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md border border-border hover:bg-accent transition-colors cursor-pointer"
              >
                <Upload className="size-3.5" />
                Upload
              </button>
            </div>
          </div>

          <ContextMenu>
            <ContextMenuTrigger asChild>
              <div className="flex-1" onClick={(e) => { if (e.target === e.currentTarget) handleDeselectAll() }}>
                {loading ? (
                  <div className="flex items-center justify-center py-16">
                    <Loader2 className="size-5 animate-spin text-muted-foreground" />
                  </div>
                ) : (
                  <FolderDocumentList
                    kbId={kbId}
                    folders={childFolderNames}
                    documents={documentsAtPath}
                    currentPath={currentPath}
                    onNavigateFolder={(name: string) => navigateFolder(joinPath(currentPath, name))}
                    onOpenDocument={(doc: DocumentListItem) => {
                      // Navigate to document detail — for now just log
                      console.log('Open document:', doc.id, doc.filename)
                    }}
                    onDeleteDocument={(id: string) => {
                      if (selectedIds.has(`doc:${id}`) && selectedIds.size > 1) handleDeleteSelected()
                      else handleDeleteDocument(id)
                    }}
                    onDeleteFolder={(name: string) => {
                      if (selectedIds.has(`folder:${name}`) && selectedIds.size > 1) handleDeleteSelected()
                      else handleDeleteFolder(name)
                    }}
                    onRenameFolder={handleRenameFolder}
                    onMoveDocument={handleMoveDocument}
                    onCreateNote={handleCreateNote}
                    onCreateFolder={handleCreateFolder}
                    sortField={sortField}
                    sortDir={sortDir}
                    onSortChange={handleSortChange}
                    selectedIds={selectedIds}
                    onSelect={handleSelect}
                  />
                )}
              </div>
            </ContextMenuTrigger>
            <ContextMenuContent>
              <ContextMenuItem onClick={handleCreateNote}>
                <NotepadText className="size-3.5 mr-2" />
                New Note
              </ContextMenuItem>
              <ContextMenuItem onClick={handleCreateFolder}>
                <Folder className="size-3.5 mr-2" />
                New Folder
              </ContextMenuItem>
              <ContextMenuSeparator />
              <ContextMenuItem onClick={handleUploadClick}>
                <Upload className="size-3.5 mr-2" />
                Upload Files
              </ContextMenuItem>
              <ContextMenuSeparator />
              <ContextMenuItem onClick={() => setSelectedIds(new Set(allItemIds))}>
                Select All
              </ContextMenuItem>
            </ContextMenuContent>
          </ContextMenu>
        </div>
      </div>

      <div className="shrink-0 flex items-center justify-between px-5 py-1.5 border-t border-border">
        <span className="text-xs text-muted-foreground">
          {allItemIds.length} item{allItemIds.length !== 1 ? 's' : ''}
          {selectedIds.size > 0 && ` \u00B7 ${selectedIds.size} selected`}
        </span>
      </div>
    </div>
  )
}

function sortDocuments(docs: DocumentListItem[], field: SortField, dir: SortDir): DocumentListItem[] {
  return [...docs].sort((a, b) => {
    let cmp: number
    if (field === 'name') {
      const aName = a.title || a.filename
      const bName = b.title || b.filename
      cmp = aName.localeCompare(bName)
    } else {
      const aDate = a.updated_at || a.created_at
      const bDate = b.updated_at || b.created_at
      cmp = aDate.localeCompare(bDate)
    }
    return dir === 'asc' ? cmp : -cmp
  })
}
