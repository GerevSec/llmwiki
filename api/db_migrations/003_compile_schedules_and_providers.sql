ALTER TABLE compile_runs
    ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'anthropic';

CREATE TABLE IF NOT EXISTS compile_schedules (
    knowledge_base_id UUID PRIMARY KEY REFERENCES knowledge_bases(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT false,
    provider TEXT NOT NULL DEFAULT 'anthropic' CHECK (provider IN ('anthropic', 'openrouter')),
    model TEXT,
    interval_minutes INTEGER NOT NULL DEFAULT 60 CHECK (interval_minutes >= 5 AND interval_minutes <= 10080),
    max_sources INTEGER NOT NULL DEFAULT 10 CHECK (max_sources >= 1 AND max_sources <= 100),
    prompt TEXT NOT NULL DEFAULT '',
    last_run_at TIMESTAMPTZ,
    last_status TEXT,
    last_error TEXT,
    next_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_compile_schedules_enabled_next_run
    ON compile_schedules(enabled, next_run_at);

ALTER TABLE compile_schedules ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'public' AND tablename = 'compile_schedules' AND policyname = 'compile_schedules_select'
    ) THEN
        CREATE POLICY compile_schedules_select ON compile_schedules
            FOR SELECT USING (user_id = auth.uid());
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'set_compile_schedules_updated_at'
    ) THEN
        CREATE TRIGGER set_compile_schedules_updated_at
            BEFORE UPDATE ON compile_schedules
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
    END IF;
END $$;
