'use client'

import * as React from 'react'
import { useEditor, EditorContent } from '@tiptap/react'
import StarterKit from '@tiptap/starter-kit'
import Placeholder from '@tiptap/extension-placeholder'
import Typography from '@tiptap/extension-typography'
import Link from '@tiptap/extension-link'
import Image from '@tiptap/extension-image'
import { Table, TableRow, TableHeader, TableCell } from '@tiptap/extension-table'
import { Markdown } from 'tiptap-markdown'
import { format, parse, isValid } from 'date-fns'
import {
  X, CalendarIcon, Tag, Plus, Hash, Type, CheckSquare, Square,
  List, LinkIcon, ExternalLink,
} from 'lucide-react'
import { createClient } from '@/lib/supabase/client'
import { apiFetch } from '@/lib/api'
import { cn, sanitizeTitle } from '@/lib/utils'
import { NoteToolbar } from './NoteToolbar'
import { Popover, PopoverTrigger, PopoverContent } from '@/components/ui/popover'
import { Calendar } from '@/components/ui/calendar'
import {
  DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuItem,
} from '@/components/ui/dropdown-menu'
import type { Editor } from '@tiptap/react'
import type { PropertyType, TypedProperty, PropertyMap } from '@/lib/types'

const AUTOSAVE_DELAY = 1500

const ALLOWED_SCHEMES = new Set(['http:', 'https:', 'mailto:'])

function sanitizeUrl(url: string): string | undefined {
  try {
    const parsed = new URL(url, 'https://placeholder.invalid')
    if (ALLOWED_SCHEMES.has(parsed.protocol)) return url
  } catch { /* invalid URL */ }
  return undefined
}

function getMarkdown(editor: Editor): string {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (editor.storage as any).markdown.getMarkdown()
}

function defaultValue(type: PropertyType): TypedProperty {
  switch (type) {
    case 'text': return { type, value: '' }
    case 'number': return { type, value: null }
    case 'date': return { type, value: null }
    case 'checkbox': return { type, value: false }
    case 'select': return { type, value: null, options: [] }
    case 'url': return { type, value: '' }
  }
}

function migrateProperties(raw: Record<string, unknown>): PropertyMap {
  const result: PropertyMap = {}
  for (const [k, v] of Object.entries(raw)) {
    if (typeof v === 'string') {
      result[k] = { type: 'text', value: v }
    } else if (v && typeof v === 'object' && 'type' in v) {
      result[k] = v as TypedProperty
    }
  }
  return result
}

const PROPERTY_TYPE_ICON: Record<PropertyType, React.ElementType> = {
  text: Type,
  number: Hash,
  date: CalendarIcon,
  checkbox: CheckSquare,
  select: List,
  url: LinkIcon,
}

function PropertyIcon({ type }: { type: PropertyType }) {
  const Icon = PROPERTY_TYPE_ICON[type]
  return <Icon className="size-3.5 text-muted-foreground" />
}

function PropertyValueEditor({
  property,
  onChange,
  onOptionsChange,
}: {
  property: TypedProperty
  onChange: (value: TypedProperty['value']) => void
  onOptionsChange?: (options: string[]) => void
}) {
  switch (property.type) {
    case 'text':
      return (
        <input
          type="text"
          value={(property.value as string) ?? ''}
          onChange={(e) => onChange(e.target.value)}
          placeholder="Empty"
          className="text-sm text-foreground bg-transparent border-none outline-none flex-1 h-7 px-1.5 placeholder:text-muted-foreground/30"
        />
      )

    case 'number':
      return (
        <input
          type="number"
          value={property.value != null ? String(property.value) : ''}
          onChange={(e) => onChange(e.target.value === '' ? null : Number(e.target.value))}
          placeholder="Empty"
          className="text-sm text-foreground bg-transparent border-none outline-none flex-1 h-7 px-1.5 placeholder:text-muted-foreground/30 [&::-webkit-inner-spin-button]:appearance-none"
        />
      )

    case 'date':
      return <DatePropertyEditor value={property.value as string | null} onChange={onChange} />

    case 'checkbox':
      return (
        <button
          onClick={() => onChange(!property.value)}
          className="flex items-center h-7 px-1.5 cursor-pointer"
        >
          {property.value
            ? <CheckSquare className="size-4 text-foreground" />
            : <Square className="size-4 text-muted-foreground/40" />}
        </button>
      )

    case 'select':
      return (
        <SelectPropertyEditor
          value={property.value as string | null}
          options={property.options ?? []}
          onChange={onChange}
          onOptionsChange={onOptionsChange!}
        />
      )

    case 'url':
      return (
        <div className="flex items-center flex-1 gap-1">
          <input
            type="text"
            value={(property.value as string) ?? ''}
            onChange={(e) => onChange(e.target.value)}
            placeholder="https://..."
            className="text-sm text-foreground bg-transparent border-none outline-none flex-1 h-7 px-1.5 placeholder:text-muted-foreground/30"
          />
          {property.value && sanitizeUrl(property.value as string) && (
            <a
              href={sanitizeUrl(property.value as string)!}
              target="_blank"
              rel="noopener noreferrer"
              className="text-muted-foreground hover:text-foreground p-0.5"
              onClick={(e) => e.stopPropagation()}
            >
              <ExternalLink className="size-3.5" />
            </a>
          )}
        </div>
      )
  }
}

function DatePropertyEditor({
  value,
  onChange,
}: {
  value: string | null
  onChange: (value: string | null) => void
}) {
  const [open, setOpen] = React.useState(false)

  const dateObj = React.useMemo(() => {
    if (!value) return undefined
    const parsed = parse(value, 'yyyy-MM-dd', new Date())
    return isValid(parsed) ? parsed : undefined
  }, [value])

  return (
    <div className="flex items-center flex-1 gap-1">
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <button
            className={cn(
              'text-sm h-7 transition-colors cursor-pointer px-1.5',
              value ? 'text-foreground' : 'text-muted-foreground/40'
            )}
          >
            {dateObj ? format(dateObj, 'PPP') : 'Pick a date'}
          </button>
        </PopoverTrigger>
        <PopoverContent className="w-auto p-0" align="start">
          <Calendar
            mode="single"
            selected={dateObj}
            onSelect={(d) => {
              onChange(d ? format(d, 'yyyy-MM-dd') : null)
              setOpen(false)
            }}
            defaultMonth={dateObj ?? new Date()}
          />
        </PopoverContent>
      </Popover>
      {value && (
        <button
          onClick={() => onChange(null)}
          className="text-muted-foreground/40 hover:text-muted-foreground cursor-pointer"
        >
          <X className="size-3" />
        </button>
      )}
    </div>
  )
}

function SelectPropertyEditor({
  value,
  options,
  onChange,
  onOptionsChange,
}: {
  value: string | null
  options: string[]
  onChange: (value: string | null) => void
  onOptionsChange: (options: string[]) => void
}) {
  const [open, setOpen] = React.useState(false)
  const [input, setInput] = React.useState('')

  const filtered = options.filter((o) =>
    !input || o.toLowerCase().includes(input.toLowerCase())
  )

  const handleAdd = () => {
    const trimmed = input.trim()
    if (!trimmed || options.includes(trimmed)) return
    onOptionsChange([...options, trimmed])
    onChange(trimmed)
    setInput('')
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          className={cn(
            'text-sm h-7 transition-colors cursor-pointer px-1.5 text-left flex-1 truncate',
            value ? 'text-foreground' : 'text-muted-foreground/40'
          )}
        >
          {value || 'Select...'}
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-48 p-1" align="start">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault()
              if (filtered.length > 0 && !options.includes(input.trim())) {
                onChange(filtered[0])
                setOpen(false)
                setInput('')
              } else if (input.trim()) {
                handleAdd()
                setOpen(false)
              }
            }
          }}
          placeholder="Search or add..."
          className="w-full text-sm bg-transparent border-none outline-none px-2 py-1.5 placeholder:text-muted-foreground/40"
          autoFocus
        />
        <div className="max-h-40 overflow-y-auto">
          {value && (
            <button
              onClick={() => { onChange(null); setOpen(false); setInput('') }}
              className="w-full text-left text-sm px-2 py-1.5 rounded-sm hover:bg-accent text-muted-foreground/60 cursor-pointer"
            >
              Clear
            </button>
          )}
          {filtered.map((opt) => (
            <button
              key={opt}
              onClick={() => { onChange(opt); setOpen(false); setInput('') }}
              className={cn(
                'w-full text-left text-sm px-2 py-1.5 rounded-sm hover:bg-accent cursor-pointer flex items-center justify-between',
                opt === value && 'bg-accent'
              )}
            >
              {opt}
              {opt === value && <span className="text-xs text-muted-foreground">&#10003;</span>}
            </button>
          ))}
          {input.trim() && !options.includes(input.trim()) && (
            <button
              onClick={() => { handleAdd(); setOpen(false) }}
              className="w-full text-left text-sm px-2 py-1.5 rounded-sm hover:bg-accent text-muted-foreground cursor-pointer"
            >
              <Plus className="size-3 inline mr-1.5" />
              Create &ldquo;{input.trim()}&rdquo;
            </button>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}

interface NoteEditorProps {
  documentId: string
  initialContent?: string
  initialTitle?: string
  initialTags?: string[]
  initialDate?: string | null
  initialProperties?: Record<string, unknown>
  onTitleChange?: (title: string) => void
}

async function getAccessToken(): Promise<string | null> {
  const supabase = createClient()
  const { data } = await supabase.auth.getSession()
  return data.session?.access_token ?? null
}

export function NoteEditor({
  documentId,
  initialContent,
  initialTitle,
  initialTags,
  initialDate,
  initialProperties,
  onTitleChange,
}: NoteEditorProps) {
  const [title, setTitle] = React.useState(initialTitle ?? '')
  const [date, setDate] = React.useState<string>(initialDate ?? '')
  const [tags, setTags] = React.useState<string[]>(initialTags ?? [])
  const [tagInput, setTagInput] = React.useState('')
  const [properties, setProperties] = React.useState<PropertyMap>(() =>
    initialProperties ? migrateProperties(initialProperties) : {}
  )
  const [loaded, setLoaded] = React.useState(false)
  const [calendarOpen, setCalendarOpen] = React.useState(false)
  const [saveStatus, setSaveStatus] = React.useState<'saved' | 'saving' | 'idle'>('idle')
  const [wordCount, setWordCount] = React.useState(0)
  const saveTimerRef = React.useRef<ReturnType<typeof setTimeout> | null>(null)
  const latestContentRef = React.useRef<string>('')
  const latestTitleRef = React.useRef<string>(initialTitle ?? '')
  const latestDateRef = React.useRef<string>(initialDate ?? '')
  const latestTagsRef = React.useRef<string[]>(initialTags ?? [])
  const latestPropertiesRef = React.useRef<PropertyMap>(
    initialProperties ? migrateProperties(initialProperties) : {}
  )
  const dirtyRef = React.useRef(false)
  const metaDirtyRef = React.useRef(false)

  const editor = useEditor({
    immediatelyRender: false,
    extensions: [
      StarterKit.configure({
        heading: { levels: [1, 2, 3] },
        link: false,
      }),
      Placeholder.configure({ placeholder: 'Start writing...' }),
      Typography,
      Link.configure({ autolink: true, openOnClick: false }),
      Image.configure({ inline: false, allowBase64: true }),
      Table.configure({ resizable: false }),
      TableRow,
      TableHeader,
      TableCell,
      Markdown.configure({
        html: false,
        transformCopiedText: true,
        transformPastedText: true,
      }),
    ],
    editorProps: {
      attributes: {
        class: 'prose prose-sm dark:prose-invert max-w-none focus:outline-none min-h-[200px]',
      },
      handleClick: (_view, _pos, event) => {
        const anchor = (event.target as HTMLElement).closest('a')
        if (!anchor) return false
        const href = anchor.getAttribute('href')
        if (!href) return false
        const safeHref = sanitizeUrl(href)
        if (safeHref) window.open(safeHref, '_blank', 'noopener')
        return true
      },
    },
    onUpdate: ({ editor }) => {
      latestContentRef.current = getMarkdown(editor)
      dirtyRef.current = true
      setSaveStatus('idle')
      scheduleSave()

      const text = editor.state.doc.textContent
      setWordCount(text.trim() ? text.trim().split(/\s+/).length : 0)
    },
  })

  const dateValue = React.useMemo(() => {
    if (!date) return undefined
    const parsed = parse(date, 'yyyy-MM-dd', new Date())
    return isValid(parsed) ? parsed : undefined
  }, [date])

  React.useEffect(() => {
    let cancelled = false

    const load = async () => {
      const token = await getAccessToken()
      if (!token || cancelled) return

      try {
        const { content } = await apiFetch<{ id: string; content: string; version: number }>(
          `/v1/documents/${documentId}/content`,
          token,
        )

        if (cancelled) return
        latestContentRef.current = content ?? ''
        editor?.commands.setContent(content ?? '')
      } catch (err) {
        console.error('[NoteEditor] Failed to load content:', err)
        if (initialContent != null && !cancelled) {
          latestContentRef.current = initialContent
          editor?.commands.setContent(initialContent)
        }
      }

      if (!cancelled) {
        setLoaded(true)
        if (editor) {
          const text = editor.state.doc.textContent
          setWordCount(text.trim() ? text.trim().split(/\s+/).length : 0)
        }
      }
    }

    load()
    return () => { cancelled = true }
  }, [documentId, editor, initialContent])

  const save = React.useCallback(async () => {
    if (!dirtyRef.current) return
    dirtyRef.current = false
    const shouldPatchMeta = metaDirtyRef.current
    metaDirtyRef.current = false
    setSaveStatus('saving')

    const token = await getAccessToken()
    if (!token) {
      dirtyRef.current = true
      setSaveStatus('idle')
      return
    }

    try {
      const promises: Promise<unknown>[] = [
        apiFetch(`/v1/documents/${documentId}/content`, token, {
          method: 'PUT',
          body: JSON.stringify({ content: latestContentRef.current }),
        }),
      ]

      if (shouldPatchMeta) {
        const hasProps = Object.keys(latestPropertiesRef.current).length > 0
        promises.push(
          apiFetch(`/v1/documents/${documentId}`, token, {
            method: 'PATCH',
            body: JSON.stringify({
              title: latestTitleRef.current || null,
              tags: latestTagsRef.current.length > 0 ? latestTagsRef.current : null,
              date: latestDateRef.current || null,
              metadata: hasProps ? { properties: latestPropertiesRef.current } : null,
            }),
          }),
        )
      }

      await Promise.all(promises)
      setSaveStatus('saved')
    } catch (err) {
      console.error('[NoteEditor] save failed:', err)
      dirtyRef.current = true
      setSaveStatus('idle')
    }
  }, [documentId])

  const scheduleSave = React.useCallback(() => {
    if (saveTimerRef.current) clearTimeout(saveTimerRef.current)
    saveTimerRef.current = setTimeout(save, AUTOSAVE_DELAY)
  }, [save])

  React.useEffect(() => {
    return () => {
      if (saveTimerRef.current) clearTimeout(saveTimerRef.current)
      if (dirtyRef.current) {
        const contentToSave = latestContentRef.current
        const titleToSave = latestTitleRef.current
        const tagsToSave = latestTagsRef.current
        const dateToSave = latestDateRef.current
        const propsToSave = latestPropertiesRef.current
        const shouldPatchMeta = metaDirtyRef.current
        const docId = documentId

        ;(async () => {
          const token = await getAccessToken()
          if (!token) return

          try {
            const promises: Promise<unknown>[] = [
              apiFetch(`/v1/documents/${docId}/content`, token, {
                method: 'PUT',
                body: JSON.stringify({ content: contentToSave }),
              }),
            ]

            if (shouldPatchMeta) {
              const hasProps = Object.keys(propsToSave).length > 0
              promises.push(
                apiFetch(`/v1/documents/${docId}`, token, {
                  method: 'PATCH',
                  body: JSON.stringify({
                    title: titleToSave || null,
                    tags: tagsToSave.length > 0 ? tagsToSave : null,
                    date: dateToSave || null,
                    metadata: hasProps ? { properties: propsToSave } : null,
                  }),
                }),
              )
            }

            await Promise.all(promises)
          } catch (err) {
            console.error('[NoteEditor] flush on unmount failed:', err)
          }
        })()
      }
    }
  }, [documentId])

  React.useEffect(() => {
    const handleOnline = () => {
      if (dirtyRef.current) save()
    }
    window.addEventListener('online', handleOnline)
    return () => window.removeEventListener('online', handleOnline)
  }, [save])

  const handleTitleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = sanitizeTitle(e.target.value)
    setTitle(val)
    latestTitleRef.current = val
    dirtyRef.current = true
    metaDirtyRef.current = true
    setSaveStatus('idle')
    scheduleSave()
    onTitleChange?.(val)
  }

  const handleDateSelect = (selected: Date | undefined) => {
    const val = selected ? format(selected, 'yyyy-MM-dd') : ''
    setDate(val)
    latestDateRef.current = val
    dirtyRef.current = true
    metaDirtyRef.current = true
    setSaveStatus('idle')
    scheduleSave()
    setCalendarOpen(false)
  }

  const handleClearDate = (e: React.MouseEvent) => {
    e.stopPropagation()
    setDate('')
    latestDateRef.current = ''
    dirtyRef.current = true
    metaDirtyRef.current = true
    scheduleSave()
  }

  const handleAddTag = () => {
    const tag = tagInput.trim()
    if (!tag || tags.includes(tag)) { setTagInput(''); return }
    const next = [...tags, tag]
    setTags(next)
    latestTagsRef.current = next
    setTagInput('')
    dirtyRef.current = true
    metaDirtyRef.current = true
    scheduleSave()
  }

  const handleRemoveTag = (tag: string) => {
    const next = tags.filter((t) => t !== tag)
    setTags(next)
    latestTagsRef.current = next
    dirtyRef.current = true
    metaDirtyRef.current = true
    scheduleSave()
  }

  const handleTagKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') { e.preventDefault(); handleAddTag() }
    if (e.key === 'Backspace' && !tagInput && tags.length > 0) {
      handleRemoveTag(tags[tags.length - 1])
    }
  }

  const handlePropertyChange = (key: string, value: TypedProperty['value']) => {
    const prev = properties[key]
    if (!prev) return
    const next = { ...properties, [key]: { ...prev, value } }
    setProperties(next)
    latestPropertiesRef.current = next
    dirtyRef.current = true
    setSaveStatus('idle')
    scheduleSave()
  }

  const handlePropertyKeyRename = (oldKey: string, newKey: string) => {
    if (!newKey.trim() || (newKey !== oldKey && newKey in properties)) return
    const next: PropertyMap = {}
    for (const [k, v] of Object.entries(properties)) {
      next[k === oldKey ? newKey.trim() : k] = v
    }
    setProperties(next)
    latestPropertiesRef.current = next
    dirtyRef.current = true
    scheduleSave()
  }

  const handleAddProperty = (type: PropertyType) => {
    let name = 'property'
    let i = 1
    while (name in properties) { name = `property ${i++}` }
    const next = { ...properties, [name]: defaultValue(type) }
    setProperties(next)
    latestPropertiesRef.current = next
    dirtyRef.current = true
    scheduleSave()
  }

  const handleRemoveProperty = (key: string) => {
    const next = { ...properties }
    delete next[key]
    setProperties(next)
    latestPropertiesRef.current = next
    dirtyRef.current = true
    scheduleSave()
  }

  const handleSelectOptionsChange = (key: string, options: string[]) => {
    const prev = properties[key]
    if (!prev || prev.type !== 'select') return
    const next = { ...properties, [key]: { ...prev, options } }
    setProperties(next)
    latestPropertiesRef.current = next
    dirtyRef.current = true
    scheduleSave()
  }

  if (!loaded) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="size-5 border-2 border-muted-foreground/30 border-t-muted-foreground rounded-full animate-spin" />
      </div>
    )
  }

  return (
    <div className="h-full flex flex-col overflow-hidden">
      <NoteToolbar
        editor={editor}
        saveStatus={saveStatus}
        wordCount={wordCount}
      />

      <div className="flex-1 overflow-y-auto">
        <div className="max-w-3xl mx-auto px-8 py-8">
          <input
            type="text"
            value={title}
            onChange={handleTitleChange}
            placeholder="Untitled"
            className="w-full text-2xl font-bold text-foreground bg-transparent border-none outline-none placeholder:text-muted-foreground/30 mb-4"
          />

          <div className="mb-6 space-y-0.5">
            <div className="flex items-center h-8">
              <div className="flex items-center gap-2 w-24 shrink-0">
                <CalendarIcon className="size-3.5 text-muted-foreground" />
                <span className="text-sm text-muted-foreground">date</span>
              </div>
              <Popover open={calendarOpen} onOpenChange={setCalendarOpen}>
                <PopoverTrigger asChild>
                  <button
                    className={cn(
                      'text-sm h-7 transition-colors cursor-pointer px-1.5',
                      date ? 'text-foreground' : 'text-muted-foreground/40'
                    )}
                  >
                    {dateValue ? format(dateValue, 'PPP') : 'Pick a date'}
                  </button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <Calendar
                    mode="single"
                    selected={dateValue}
                    onSelect={handleDateSelect}
                    defaultMonth={dateValue ?? new Date()}
                  />
                </PopoverContent>
              </Popover>
              {date && (
                <button
                  onClick={handleClearDate}
                  className="ml-1 text-muted-foreground/40 hover:text-muted-foreground cursor-pointer"
                >
                  <X className="size-3" />
                </button>
              )}
            </div>

            <div className="flex items-start min-h-8">
              <div className="flex items-center gap-2 w-24 shrink-0 h-8">
                <Tag className="size-3.5 text-muted-foreground" />
                <span className="text-sm text-muted-foreground">tags</span>
              </div>
              <div className="flex items-center gap-1.5 flex-wrap flex-1 h-8 px-1.5">
                {tags.map((tag) => (
                  <span
                    key={tag}
                    className="inline-flex items-center gap-1 text-sm bg-muted text-muted-foreground rounded-md px-2 py-0.5"
                  >
                    {tag}
                    <button
                      onClick={() => handleRemoveTag(tag)}
                      className="hover:text-foreground cursor-pointer"
                    >
                      <X className="size-3" />
                    </button>
                  </span>
                ))}
                <input
                  type="text"
                  value={tagInput}
                  onChange={(e) => setTagInput(e.target.value)}
                  onKeyDown={handleTagKeyDown}
                  onBlur={handleAddTag}
                  placeholder={tags.length === 0 ? 'Add tags...' : '+'}
                  className="text-sm bg-transparent border-none outline-none text-muted-foreground placeholder:text-muted-foreground/30 w-24"
                />
              </div>
            </div>

            {Object.entries(properties).map(([key, prop]) => (
              <div key={key} className="flex items-center min-h-8 group/prop">
                <div className="flex items-center gap-2 w-24 shrink-0">
                  <PropertyIcon type={prop.type} />
                  <input
                    type="text"
                    defaultValue={key}
                    onBlur={(e) => handlePropertyKeyRename(key, e.target.value)}
                    onKeyDown={(e) => { if (e.key === 'Enter') e.currentTarget.blur() }}
                    className="text-sm text-muted-foreground bg-transparent border-none outline-none w-full truncate"
                  />
                </div>
                <PropertyValueEditor
                  property={prop}
                  onChange={(value) => handlePropertyChange(key, value)}
                  onOptionsChange={prop.type === 'select' ? (opts) => handleSelectOptionsChange(key, opts) : undefined}
                />
                <button
                  onClick={() => handleRemoveProperty(key)}
                  className="opacity-0 group-hover/prop:opacity-100 text-muted-foreground/40 hover:text-muted-foreground cursor-pointer p-0.5"
                >
                  <X className="size-3" />
                </button>
              </div>
            ))}

            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button
                  className="flex items-center gap-2 h-7 text-sm text-muted-foreground/40 hover:text-muted-foreground transition-colors cursor-pointer"
                >
                  <Plus className="size-3.5" />
                  <span>Add property</span>
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="start">
                <DropdownMenuItem onClick={() => handleAddProperty('text')}>
                  <Type className="size-4" />
                  Text
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => handleAddProperty('number')}>
                  <Hash className="size-4" />
                  Number
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => handleAddProperty('date')}>
                  <CalendarIcon className="size-4" />
                  Date
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => handleAddProperty('checkbox')}>
                  <CheckSquare className="size-4" />
                  Checkbox
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => handleAddProperty('select')}>
                  <List className="size-4" />
                  Select
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => handleAddProperty('url')}>
                  <LinkIcon className="size-4" />
                  URL
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>

          <EditorContent editor={editor} />
        </div>
      </div>
    </div>
  )
}
