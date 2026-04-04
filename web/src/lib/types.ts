export interface KnowledgeBase {
  id: string
  user_id: string
  name: string
  slug: string
  description: string | null
  created_at: string
  updated_at: string
}

export interface Document {
  id: string
  knowledge_base_id: string
  user_id: string
  filename: string
  title: string | null
  path: string
  file_type: string
  file_size: number
  status: string
  page_count: number | null
  content: string | null
  tags: string[]
  url: string | null
  version: number
  archived: boolean
  created_at: string
  updated_at: string
}

export type DocumentListItem = Omit<Document, 'content'>

export type PropertyType = 'text' | 'number' | 'date' | 'checkbox' | 'select' | 'url'

export interface TypedProperty {
  type: PropertyType
  value: string | number | boolean | null
  options?: string[]
}

export type PropertyMap = Record<string, TypedProperty>
