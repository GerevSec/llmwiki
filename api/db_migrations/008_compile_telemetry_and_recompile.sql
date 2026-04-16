ALTER TABLE compile_runs
    ADD COLUMN IF NOT EXISTS telemetry JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS last_progress_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_compile_runs_status_started_at
    ON compile_runs(status, started_at DESC);
