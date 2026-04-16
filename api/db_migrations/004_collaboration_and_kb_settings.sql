DO $$
DECLARE
    record_row RECORD;
BEGIN
    FOR record_row IN
        SELECT id, slug,
               ROW_NUMBER() OVER (PARTITION BY slug ORDER BY created_at, id) AS slug_rank
        FROM knowledge_bases
    LOOP
        IF record_row.slug_rank > 1 THEN
            UPDATE knowledge_bases
            SET slug = record_row.slug || '-' || SUBSTRING(md5(record_row.id::text), 1, 6)
            WHERE id = record_row.id;
        END IF;
    END LOOP;
END $$;

ALTER TABLE knowledge_bases
    ALTER COLUMN slug SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_bases_slug_unique
    ON knowledge_bases(slug);

CREATE TABLE IF NOT EXISTS knowledge_base_memberships (
    knowledge_base_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'editor', 'viewer')),
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    PRIMARY KEY (knowledge_base_id, user_id)
);

CREATE TABLE IF NOT EXISTS knowledge_base_invites (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    knowledge_base_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    invited_by UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'editor', 'viewer')),
    token_hash TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'revoked', 'expired')),
    expires_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge_base_settings (
    knowledge_base_id UUID PRIMARY KEY REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    auto_compile_enabled BOOLEAN NOT NULL DEFAULT false,
    compile_provider TEXT NOT NULL DEFAULT 'anthropic' CHECK (compile_provider IN ('anthropic', 'openrouter')),
    compile_model TEXT,
    compile_interval_minutes INTEGER NOT NULL DEFAULT 60 CHECK (compile_interval_minutes >= 5 AND compile_interval_minutes <= 10080),
    compile_max_sources INTEGER NOT NULL DEFAULT 10 CHECK (compile_max_sources >= 1 AND compile_max_sources <= 100),
    compile_prompt TEXT NOT NULL DEFAULT '',
    compile_max_tool_rounds INTEGER NOT NULL DEFAULT 24 CHECK (compile_max_tool_rounds >= 1 AND compile_max_tool_rounds <= 200),
    compile_max_tokens INTEGER NOT NULL DEFAULT 4096 CHECK (compile_max_tokens >= 256 AND compile_max_tokens <= 32000),
    provider_secret_encrypted TEXT,
    provider_secret_updated_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    last_status TEXT,
    last_error TEXT,
    next_run_at TIMESTAMPTZ,
    updated_by UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kb_memberships_user ON knowledge_base_memberships(user_id);
CREATE INDEX IF NOT EXISTS idx_kb_memberships_kb_role ON knowledge_base_memberships(knowledge_base_id, role);
CREATE INDEX IF NOT EXISTS idx_kb_invites_email_status ON knowledge_base_invites(email, status);
CREATE INDEX IF NOT EXISTS idx_kb_settings_due_compile ON knowledge_base_settings(auto_compile_enabled, next_run_at);

GRANT SELECT, INSERT, UPDATE, DELETE ON knowledge_base_memberships TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON knowledge_base_invites TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON knowledge_base_settings TO authenticated;

INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role)
SELECT id, user_id, 'owner' FROM knowledge_bases
ON CONFLICT (knowledge_base_id, user_id) DO NOTHING;

INSERT INTO knowledge_base_settings (
    knowledge_base_id,
    auto_compile_enabled,
    compile_provider,
    compile_model,
    compile_interval_minutes,
    compile_max_sources,
    compile_prompt,
    last_run_at,
    last_status,
    last_error,
    next_run_at,
    updated_by
)
SELECT
    cs.knowledge_base_id,
    cs.enabled,
    cs.provider,
    cs.model,
    cs.interval_minutes,
    cs.max_sources,
    cs.prompt,
    cs.last_run_at,
    cs.last_status,
    cs.last_error,
    cs.next_run_at,
    cs.user_id
FROM compile_schedules cs
ON CONFLICT (knowledge_base_id) DO NOTHING;

ALTER TABLE knowledge_base_memberships ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_base_invites ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_base_settings ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS knowledge_bases_select ON knowledge_bases;
CREATE POLICY knowledge_bases_select ON knowledge_bases
    FOR SELECT USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = knowledge_bases.id
              AND m.user_id = auth.uid()
        )
    );

DROP POLICY IF EXISTS documents_select ON documents;
CREATE POLICY documents_select ON documents
    FOR SELECT USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = documents.knowledge_base_id
              AND m.user_id = auth.uid()
        )
    );

DROP POLICY IF EXISTS document_chunks_select ON document_chunks;
CREATE POLICY document_chunks_select ON document_chunks
    FOR SELECT USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = document_chunks.knowledge_base_id
              AND m.user_id = auth.uid()
        )
    );

DROP POLICY IF EXISTS document_pages_select ON document_pages;
CREATE POLICY document_pages_select ON document_pages
    FOR SELECT USING (
        EXISTS (
            SELECT 1
            FROM documents d
            JOIN knowledge_base_memberships m ON m.knowledge_base_id = d.knowledge_base_id
            WHERE d.id = document_pages.document_id
              AND m.user_id = auth.uid()
        )
    );

DROP POLICY IF EXISTS kb_memberships_select ON knowledge_base_memberships;
CREATE POLICY kb_memberships_select ON knowledge_base_memberships
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY kb_invites_select ON knowledge_base_invites
    FOR SELECT USING (
        lower(email) = lower(current_setting('request.jwt.claims', true)::jsonb ->> 'email')
        OR EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = knowledge_base_invites.knowledge_base_id
              AND m.user_id = auth.uid()
              AND m.role IN ('owner', 'admin')
        )
    );

CREATE POLICY kb_settings_select ON knowledge_base_settings
    FOR SELECT USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = knowledge_base_settings.knowledge_base_id
              AND m.user_id = auth.uid()
              AND m.role IN ('owner', 'admin')
        )
    );

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'set_kb_memberships_updated_at'
    ) THEN
        CREATE TRIGGER set_kb_memberships_updated_at
            BEFORE UPDATE ON knowledge_base_memberships
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'set_kb_invites_updated_at'
    ) THEN
        CREATE TRIGGER set_kb_invites_updated_at
            BEFORE UPDATE ON knowledge_base_invites
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'set_kb_settings_updated_at'
    ) THEN
        CREATE TRIGGER set_kb_settings_updated_at
            BEFORE UPDATE ON knowledge_base_settings
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
    END IF;
END $$;
