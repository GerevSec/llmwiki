'use client'

import * as React from 'react'
import { useParams } from 'next/navigation'
import { useKBStore } from '@/stores'
import { useKBDocuments } from '@/hooks/useKBDocuments'
import { NoteEditor } from '@/components/editor/NoteEditor'
import { Loader2, FileText } from 'lucide-react'

export default function FilePage() {
  const params = useParams<{ slug: string; path: string[] }>()
  const knowledgeBases = useKBStore((s) => s.knowledgeBases)
  const kbLoading = useKBStore((s) => s.loading)

  const kb = React.useMemo(
    () => knowledgeBases.find((k) => k.slug === params.slug),
    [knowledgeBases, params.slug]
  )

  const { documents, loading: docsLoading } = useKBDocuments(kb?.id ?? '')

  const pathSegments = params.path ?? []
  const filename = pathSegments[pathSegments.length - 1] ?? ''
  const folderPath = pathSegments.length > 1
    ? '/' + pathSegments.slice(0, -1).join('/') + '/'
    : '/'

  const document = React.useMemo(() => {
    if (!documents.length || !filename) return null
    return documents.find((d) => {
      const docFilename = d.title || d.filename
      const docPath = d.path ?? '/'
      return docFilename === decodeURIComponent(filename) && docPath === folderPath
    }) ?? documents.find((d) => {
      const docFilename = d.title || d.filename
      return docFilename === decodeURIComponent(filename)
    }) ?? null
  }, [documents, filename, folderPath])

  if (kbLoading || (kb && docsLoading)) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="size-5 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (!kb) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-2">
        <h1 className="text-lg font-medium">Knowledge base not found</h1>
        <p className="text-sm text-muted-foreground">
          The knowledge base &ldquo;{params.slug}&rdquo; does not exist.
        </p>
      </div>
    )
  }

  if (!document) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-2">
        <h1 className="text-lg font-medium">Document not found</h1>
        <p className="text-sm text-muted-foreground">
          Could not find &ldquo;{decodeURIComponent(filename)}&rdquo; in this knowledge base.
        </p>
      </div>
    )
  }

  const isNote = document.file_type === 'md' || document.file_type === 'txt' || document.file_type === 'note'

  if (isNote) {
    return (
      <div className="h-full">
        <NoteEditor
          documentId={document.id}
          initialTitle={document.title ?? document.filename}
          initialTags={document.tags}
        />
      </div>
    )
  }

  return (
    <div className="flex flex-col items-center justify-center h-full gap-4 p-8">
      <div className="flex items-center justify-center w-16 h-16 rounded-xl bg-muted">
        <FileText size={28} className="text-muted-foreground" />
      </div>
      <div className="text-center">
        <h1 className="text-lg font-medium">{document.title || document.filename}</h1>
        <p className="text-sm text-muted-foreground mt-1">
          {document.file_type?.toUpperCase()} document
          {document.page_count ? ` \u00B7 ${document.page_count} page${document.page_count > 1 ? 's' : ''}` : ''}
        </p>
        <p className="text-xs text-muted-foreground mt-2">
          File viewer coming soon
        </p>
      </div>
    </div>
  )
}
