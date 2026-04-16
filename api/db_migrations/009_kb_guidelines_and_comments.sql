-- kb_guidelines: per-KB standing rules authored by admins/owners
CREATE TABLE kb_guidelines (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    kb_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    body TEXT NOT NULL CHECK (char_length(body) BETWEEN 1 AND 2000),
    position INT NOT NULL DEFAULT 0,
    is_active BOOL NOT NULL DEFAULT true,
    created_by UUID NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ NULL
);

-- wiki_page_comments: per-page feedback delivered to compile runs, promotable to guidelines
CREATE TABLE wiki_page_comments (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    kb_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    -- page_key is not globally unique; see 007_wiki_streamlining.sql:25 where uniqueness is on
    -- (release_id, page_key). No FK. Orphan detection is lazy-at-fetch on GET comments.
    page_key UUID NOT NULL,
    body TEXT NOT NULL CHECK (char_length(body) BETWEEN 1 AND 4000),
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'delivered', 'resolved', 'archived', 'promoted')),
    system_note TEXT NULL,
    author_id UUID NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ NULL,
    delivered_compile_id UUID NULL REFERENCES compile_runs(id) ON DELETE SET NULL,
    resolved_at TIMESTAMPTZ NULL,
    resolved_by UUID NULL REFERENCES users(id),
    promoted_to_guideline_id UUID NULL REFERENCES kb_guidelines(id) ON DELETE SET NULL
);

-- Indexes
CREATE INDEX idx_kb_guidelines_kb_position ON kb_guidelines (kb_id, position) WHERE is_active;
CREATE INDEX idx_wiki_page_comments_kb_page_status ON wiki_page_comments (kb_id, page_key, status);
CREATE INDEX idx_wiki_page_comments_kb_open ON wiki_page_comments (kb_id, status) WHERE status = 'open';

-- Grants
GRANT SELECT, INSERT, UPDATE, DELETE ON kb_guidelines TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON wiki_page_comments TO authenticated;

-- RLS
ALTER TABLE kb_guidelines ENABLE ROW LEVEL SECURITY;
ALTER TABLE wiki_page_comments ENABLE ROW LEVEL SECURITY;

-- kb_guidelines: any KB member can SELECT; only admins/owners can INSERT/UPDATE/DELETE
CREATE POLICY kb_guidelines_select ON kb_guidelines
    FOR SELECT USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = kb_guidelines.kb_id
              AND m.user_id = auth.uid()
        )
    );

CREATE POLICY kb_guidelines_insert ON kb_guidelines
    FOR INSERT WITH CHECK (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = kb_guidelines.kb_id
              AND m.user_id = auth.uid()
              AND m.role IN ('owner', 'admin')
        )
    );

CREATE POLICY kb_guidelines_update ON kb_guidelines
    FOR UPDATE USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = kb_guidelines.kb_id
              AND m.user_id = auth.uid()
              AND m.role IN ('owner', 'admin')
        )
    );

CREATE POLICY kb_guidelines_delete ON kb_guidelines
    FOR DELETE USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = kb_guidelines.kb_id
              AND m.user_id = auth.uid()
              AND m.role IN ('owner', 'admin')
        )
    );

-- wiki_page_comments: any KB member can SELECT+INSERT; only admins/owners can UPDATE
CREATE POLICY wiki_page_comments_select ON wiki_page_comments
    FOR SELECT USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = wiki_page_comments.kb_id
              AND m.user_id = auth.uid()
        )
    );

CREATE POLICY wiki_page_comments_insert ON wiki_page_comments
    FOR INSERT WITH CHECK (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = wiki_page_comments.kb_id
              AND m.user_id = auth.uid()
        )
    );

CREATE POLICY wiki_page_comments_update ON wiki_page_comments
    FOR UPDATE USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = wiki_page_comments.kb_id
              AND m.user_id = auth.uid()
              AND m.role IN ('owner', 'admin')
        )
    );

-- Trigger: keep updated_at current on kb_guidelines
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'set_kb_guidelines_updated_at'
    ) THEN
        CREATE TRIGGER set_kb_guidelines_updated_at
            BEFORE UPDATE ON kb_guidelines
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
    END IF;
END $$;
