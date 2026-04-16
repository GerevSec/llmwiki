CREATE TABLE wiki_releases (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    knowledge_base_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('draft', 'published', 'failed', 'superseded')),
    base_release_id UUID REFERENCES wiki_releases(id) ON DELETE SET NULL,
    created_by TEXT NOT NULL,
    created_by_run_id UUID,
    quality_report JSONB NOT NULL DEFAULT '{}'::jsonb,
    change_report JSONB NOT NULL DEFAULT '{}'::jsonb,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE wiki_release_pages (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    release_id UUID NOT NULL REFERENCES wiki_releases(id) ON DELETE CASCADE,
    page_key UUID NOT NULL,
    path TEXT NOT NULL,
    filename TEXT NOT NULL,
    title TEXT,
    content TEXT NOT NULL DEFAULT '',
    tags TEXT[] NOT NULL DEFAULT '{}'::text[],
    sort_order INTEGER NOT NULL DEFAULT 0,
    UNIQUE (release_id, page_key),
    UNIQUE (release_id, path, filename)
);

CREATE TABLE wiki_path_aliases (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    release_id UUID NOT NULL REFERENCES wiki_releases(id) ON DELETE CASCADE,
    knowledge_base_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    alias_path TEXT NOT NULL,
    alias_filename TEXT NOT NULL,
    target_page_key UUID NOT NULL,
    reason TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    UNIQUE (release_id, alias_path, alias_filename)
);

CREATE TABLE streamlining_runs (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    knowledge_base_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('targeted', 'full')),
    scope_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    quality_report JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_excerpt TEXT,
    error_message TEXT,
    started_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    finished_at TIMESTAMPTZ
);

CREATE TABLE wiki_dirty_scope (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    knowledge_base_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    filename TEXT,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

ALTER TABLE knowledge_base_settings
    ADD COLUMN IF NOT EXISTS streamlining_enabled BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS streamlining_interval_minutes INTEGER NOT NULL DEFAULT 1440 CHECK (streamlining_interval_minutes >= 60 AND streamlining_interval_minutes <= 10080),
    ADD COLUMN IF NOT EXISTS streamlining_provider TEXT CHECK (streamlining_provider IN ('anthropic', 'openrouter')),
    ADD COLUMN IF NOT EXISTS streamlining_model TEXT,
    ADD COLUMN IF NOT EXISTS streamlining_prompt TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS streamlining_provider_secret_encrypted TEXT,
    ADD COLUMN IF NOT EXISTS last_streamlining_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_streamlining_status TEXT,
    ADD COLUMN IF NOT EXISTS last_streamlining_error TEXT,
    ADD COLUMN IF NOT EXISTS next_streamlining_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS active_wiki_release_id UUID REFERENCES wiki_releases(id) ON DELETE SET NULL;

CREATE INDEX idx_wiki_releases_kb_status ON wiki_releases(knowledge_base_id, status, published_at DESC);
CREATE INDEX idx_wiki_release_pages_release_path ON wiki_release_pages(release_id, path, filename);
CREATE INDEX idx_wiki_path_aliases_release_alias ON wiki_path_aliases(release_id, alias_path, alias_filename);
CREATE INDEX idx_streamlining_runs_kb_started_at ON streamlining_runs(knowledge_base_id, started_at DESC);
CREATE INDEX idx_wiki_dirty_scope_kb_created_at ON wiki_dirty_scope(knowledge_base_id, created_at DESC);
CREATE INDEX idx_kb_settings_due_streamlining ON knowledge_base_settings(streamlining_enabled, next_streamlining_at);

DO $$
DECLARE
    kb_row RECORD;
    release_uuid UUID;
BEGIN
    FOR kb_row IN
        SELECT DISTINCT kb.id
        FROM knowledge_bases kb
        LEFT JOIN knowledge_base_settings s ON s.knowledge_base_id = kb.id
        WHERE COALESCE(s.active_wiki_release_id, NULL) IS NULL
    LOOP
        release_uuid := gen_random_uuid();
        INSERT INTO wiki_releases (id, knowledge_base_id, status, created_by, published_at)
        VALUES (release_uuid, kb_row.id, 'published', 'backfill', now());

        INSERT INTO wiki_release_pages (release_id, page_key, path, filename, title, content, tags, sort_order)
        SELECT release_uuid, deduped.id, deduped.path, deduped.filename, deduped.title, COALESCE(deduped.content, ''), COALESCE(deduped.tags, '{}'::text[]), COALESCE(deduped.sort_order, 0)
        FROM (
            SELECT DISTINCT ON (d.path, d.filename)
                d.id, d.path, d.filename, d.title, d.content, d.tags, d.sort_order
            FROM documents d
            WHERE d.knowledge_base_id = kb_row.id
              AND NOT d.archived
              AND d.path LIKE '/wiki/%'
              AND d.file_type IN ('md', 'txt', 'note')
            ORDER BY d.path, d.filename, d.updated_at DESC, d.created_at DESC, d.id DESC
        ) deduped;

        INSERT INTO knowledge_base_settings (knowledge_base_id, active_wiki_release_id)
        VALUES (kb_row.id, release_uuid)
        ON CONFLICT (knowledge_base_id) DO UPDATE SET active_wiki_release_id = EXCLUDED.active_wiki_release_id, updated_at = now();
    END LOOP;
END $$;
