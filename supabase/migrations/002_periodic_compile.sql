CREATE TABLE compile_runs (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    knowledge_base_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
    model TEXT NOT NULL,
    source_count INTEGER NOT NULL DEFAULT 0,
    source_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    response_excerpt TEXT,
    error_message TEXT,
    started_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    finished_at TIMESTAMPTZ
);

CREATE TABLE compiled_source_checkpoints (
    knowledge_base_id UUID NOT NULL REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    compiled_version INTEGER NOT NULL,
    compiled_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    last_run_id UUID REFERENCES compile_runs(id) ON DELETE SET NULL,
    PRIMARY KEY (knowledge_base_id, document_id)
);

CREATE INDEX idx_compile_runs_kb_started_at ON compile_runs(knowledge_base_id, started_at DESC);
CREATE INDEX idx_compile_runs_user_started_at ON compile_runs(user_id, started_at DESC);
CREATE INDEX idx_compiled_source_checkpoints_doc ON compiled_source_checkpoints(document_id);

ALTER TABLE compile_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE compiled_source_checkpoints ENABLE ROW LEVEL SECURITY;

CREATE POLICY compile_runs_select ON compile_runs
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY compiled_source_checkpoints_select ON compiled_source_checkpoints
    FOR SELECT USING (
        EXISTS (
            SELECT 1 FROM knowledge_bases
            WHERE knowledge_bases.id = compiled_source_checkpoints.knowledge_base_id
              AND knowledge_bases.user_id = auth.uid()
        )
    );
