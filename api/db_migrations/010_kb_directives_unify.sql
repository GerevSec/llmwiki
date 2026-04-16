-- 010_kb_directives_unify.sql
-- Unify kb_guidelines + wiki_page_comments into a single kb_directives table.
--
-- v1 feature flag was OFF in prod so prod tables are empty; dev rows are migrated
-- with status remap (Critic Amendment 2):
--   v1 'delivered' → v2 'resolved'  (auto-compiled; resolved_at ← v1 resolved_at or delivered_at)
--   v1 'promoted'  → v2 'resolved'  (compiled+promoted; resolved_at ← v1 resolved_at)
--   v1 'resolved'  → v2 'archived'  (manual bookkeeping; v1 resolved_at → archived_at)
--   v1 'open' / 'archived' pass through unchanged
--
-- Column renames:
--   delivered_at         → compiled_at
--   delivered_compile_id → compiled_run_id

-- ─── 1. Create kb_directives ─────────────────────────────────────────────────

CREATE TABLE kb_directives (
    id                       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    kb_id                    UUID        NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    kind                     TEXT        NOT NULL CHECK (kind IN ('guideline', 'comment')),
    scope_page_key           UUID        NULL,       -- NULL for guidelines; page_key for comments
    body                     TEXT        NOT NULL CHECK (char_length(body) BETWEEN 1 AND 4000),
    -- status is NULL for guidelines; one of the four values for comments
    status                   TEXT        NULL CHECK (status IS NULL OR status IN ('open', 'resolved', 'failed', 'archived')),
    failure_reason           TEXT        NULL,       -- populated on status='failed'
    system_note              TEXT        NULL,
    author_id                UUID        NULL REFERENCES users(id),
    position                 INT         NULL,       -- ordering for guidelines only
    is_active                BOOL        NULL DEFAULT true, -- for guidelines only
    archived_at              TIMESTAMPTZ NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    compiled_at              TIMESTAMPTZ NULL,       -- renamed from delivered_at
    compiled_run_id          UUID        NULL REFERENCES compile_runs(id) ON DELETE SET NULL,  -- renamed from delivered_compile_id
    resolved_at              TIMESTAMPTZ NULL,       -- auto-compile timestamp (compiled→resolved path only)
    resolved_by              UUID        NULL REFERENCES users(id),
    promoted_to_directive_id UUID        NULL REFERENCES kb_directives(id) ON DELETE SET NULL,

    -- kind/status consistency: guidelines have no status; comments always have one
    CONSTRAINT kbd_kind_status_check CHECK (
        (kind = 'guideline' AND status IS NULL) OR
        (kind = 'comment'   AND status IN ('open', 'resolved', 'failed', 'archived'))
    ),
    -- scope_page_key: null for guidelines, required for comments
    CONSTRAINT kbd_scope_page_key_check CHECK (
        (kind = 'guideline' AND scope_page_key IS NULL) OR
        (kind = 'comment'   AND scope_page_key IS NOT NULL)
    )
);

-- ─── 2. Indexes ──────────────────────────────────────────────────────────────

-- General lookup (covers most query shapes)
CREATE INDEX idx_kbd_lookup ON kb_directives (kb_id, kind, scope_page_key, status);

-- Active guidelines ordered by position
CREATE INDEX idx_kbd_active_guidelines ON kb_directives (kb_id, position)
    WHERE kind = 'guideline' AND is_active;

-- Open comments by page (used for GET comments on a page)
CREATE INDEX idx_kbd_open_comments ON kb_directives (kb_id, scope_page_key)
    WHERE kind = 'comment' AND status = 'open';

-- Compile snapshot: open + previously-failed comments eligible for retry
CREATE INDEX idx_kbd_compile_snapshot ON kb_directives (kb_id, scope_page_key)
    WHERE kind = 'comment' AND status IN ('open', 'failed');

-- ─── 3. Grants + RLS ─────────────────────────────────────────────────────────

GRANT SELECT, INSERT, UPDATE, DELETE ON kb_directives TO authenticated;
ALTER TABLE kb_directives ENABLE ROW LEVEL SECURITY;

-- Any KB member can SELECT all directives (both kinds)
CREATE POLICY kbd_select ON kb_directives
    FOR SELECT USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = kb_directives.kb_id
              AND m.user_id = auth.uid()
        )
    );

-- Only admin/owner can INSERT guidelines
CREATE POLICY kbd_guideline_insert ON kb_directives
    FOR INSERT WITH CHECK (
        kind = 'guideline' AND
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = kb_directives.kb_id
              AND m.user_id = auth.uid()
              AND m.role IN ('owner', 'admin')
        )
    );

-- Any KB member can INSERT comments
CREATE POLICY kbd_comment_insert ON kb_directives
    FOR INSERT WITH CHECK (
        kind = 'comment' AND
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = kb_directives.kb_id
              AND m.user_id = auth.uid()
        )
    );

-- Only admin/owner can UPDATE (guideline edits + comment state transitions)
CREATE POLICY kbd_update ON kb_directives
    FOR UPDATE USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = kb_directives.kb_id
              AND m.user_id = auth.uid()
              AND m.role IN ('owner', 'admin')
        )
    );

-- Only admin/owner can DELETE
CREATE POLICY kbd_delete ON kb_directives
    FOR DELETE USING (
        EXISTS (
            SELECT 1 FROM knowledge_base_memberships m
            WHERE m.knowledge_base_id = kb_directives.kb_id
              AND m.user_id = auth.uid()
              AND m.role IN ('owner', 'admin')
        )
    );

-- ─── 4. Dev safety backups (data preserved even if migration fails after DROP) ──

CREATE TABLE _backup_010_kb_guidelines      AS TABLE kb_guidelines;
CREATE TABLE _backup_010_wiki_page_comments AS TABLE wiki_page_comments;

-- ─── 5. Pre-flight validation ─────────────────────────────────────────────────

DO $$
BEGIN
    -- Abort early if any unknown status values would violate the CASE remap
    IF EXISTS (
        SELECT 1 FROM wiki_page_comments
        WHERE status NOT IN ('open', 'delivered', 'resolved', 'archived', 'promoted')
        LIMIT 1
    ) THEN
        RAISE EXCEPTION
            'wiki_page_comments contains unknown status values; migration aborted. '
            'Check: SELECT DISTINCT status FROM wiki_page_comments';
    END IF;
END $$;

-- ─── 6. Copy guidelines ──────────────────────────────────────────────────────
-- Guidelines are inserted FIRST so promoted_to_directive_id FK targets exist
-- when comments are inserted in step 7.
-- v1 column name: created_by → v2 author_id

INSERT INTO kb_directives (
    id, kb_id, kind,
    body, position, is_active,
    archived_at, author_id,
    created_at, updated_at
)
SELECT
    id, kb_id, 'guideline',
    body, position, is_active,
    archived_at, created_by,
    created_at, updated_at
FROM kb_guidelines;

-- ─── 7. Copy comments (with status remap per Critic Amendment 2) ─────────────
-- Remap detail:
--   'delivered' → 'resolved': compiled_at ← delivered_at, resolved_at ← resolved_at OR delivered_at
--   'promoted'  → 'resolved': same; promoted_to_directive_id carries the guideline link (same UUID)
--   'resolved'  → 'archived': was manual bookkeeping; v1 resolved_at → v2 archived_at
--   'open' / 'archived': pass through; no timestamp changes

INSERT INTO kb_directives (
    id, kb_id, kind,
    scope_page_key, body, status,
    is_active,      -- explicitly NULL for comments; DEFAULT true must not apply here
    system_note, author_id,
    created_at, updated_at,
    compiled_at, compiled_run_id,
    resolved_at, resolved_by,
    archived_at,
    promoted_to_directive_id
)
SELECT
    id, kb_id, 'comment',
    page_key, body,
    CASE status  -- status remap
        WHEN 'delivered' THEN 'resolved'
        WHEN 'promoted'  THEN 'resolved'
        WHEN 'resolved'  THEN 'archived'
        ELSE status                         -- 'open', 'archived' pass through
    END,
    NULL,       -- is_active: not applicable to comments
    system_note, author_id,
    created_at, now(),
    -- compiled_at / compiled_run_id: renamed from delivered_* columns
    delivered_at, delivered_compile_id,
    -- resolved_at: v2 meaning is "auto-compile timestamp"; only set on delivered/promoted path
    CASE WHEN status IN ('delivered', 'promoted')
         THEN COALESCE(resolved_at, delivered_at)
         ELSE NULL
    END,
    CASE WHEN status IN ('delivered', 'promoted') THEN resolved_by ELSE NULL END,
    -- archived_at: set for v1 'resolved' (manual archive action); v1 'archived' has no timestamp
    CASE WHEN status = 'resolved' THEN COALESCE(resolved_at, now()) ELSE NULL END,
    -- promoted_to_directive_id: same UUID now resolves to kb_directives (guideline was inserted above)
    promoted_to_guideline_id
FROM wiki_page_comments;

-- ─── 8. Post-insert sanity checks (before dropping source tables) ──────────────

DO $$
DECLARE
    v_guidelines_in   BIGINT;
    v_guidelines_out  BIGINT;
    v_comments_in     BIGINT;
    v_comments_out    BIGINT;
    v_orphan_promos   BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_guidelines_in  FROM kb_guidelines;
    SELECT COUNT(*) INTO v_guidelines_out FROM kb_directives WHERE kind = 'guideline';
    SELECT COUNT(*) INTO v_comments_in    FROM wiki_page_comments;
    SELECT COUNT(*) INTO v_comments_out   FROM kb_directives WHERE kind = 'comment';

    IF v_guidelines_out < v_guidelines_in THEN
        RAISE EXCEPTION 'guideline remap lost rows: expected %, got %',
            v_guidelines_in, v_guidelines_out;
    END IF;

    IF v_comments_out < v_comments_in THEN
        RAISE EXCEPTION 'comment remap lost rows: expected %, got %',
            v_comments_in, v_comments_out;
    END IF;

    -- All promoted_to_directive_id values must resolve to a guideline row
    SELECT COUNT(*) INTO v_orphan_promos
    FROM kb_directives c
    WHERE c.kind = 'comment'
      AND c.promoted_to_directive_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM kb_directives g
          WHERE g.id = c.promoted_to_directive_id
            AND g.kind = 'guideline'
      );

    IF v_orphan_promos > 0 THEN
        RAISE EXCEPTION
            'promoted_to_directive_id FK is broken for % comment row(s); '
            'guideline UUIDs may not have been preserved correctly',
            v_orphan_promos;
    END IF;

    RAISE NOTICE 'Sanity OK: % guidelines and % comments migrated to kb_directives',
        v_guidelines_out, v_comments_out;
END $$;

-- ─── 9. Drop old tables ───────────────────────────────────────────────────────
-- wiki_page_comments first (it held FK → kb_guidelines.id via promoted_to_guideline_id)

DROP TABLE wiki_page_comments;
DROP TABLE kb_guidelines;

-- ─── 10. updated_at trigger ──────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'set_kb_directives_updated_at'
    ) THEN
        CREATE TRIGGER set_kb_directives_updated_at
            BEFORE UPDATE ON kb_directives
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
    END IF;
END $$;
