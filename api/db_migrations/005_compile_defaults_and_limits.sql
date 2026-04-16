ALTER TABLE knowledge_base_settings
    ALTER COLUMN compile_max_sources SET DEFAULT 20,
    ALTER COLUMN compile_max_tool_rounds SET DEFAULT 50,
    ALTER COLUMN compile_max_tokens SET DEFAULT 50000;

ALTER TABLE knowledge_base_settings
    DROP CONSTRAINT IF EXISTS knowledge_base_settings_compile_max_sources_check,
    DROP CONSTRAINT IF EXISTS knowledge_base_settings_compile_max_tool_rounds_check,
    DROP CONSTRAINT IF EXISTS knowledge_base_settings_compile_max_tokens_check;

ALTER TABLE knowledge_base_settings
    ADD CONSTRAINT knowledge_base_settings_compile_max_sources_check
        CHECK (compile_max_sources >= 1 AND compile_max_sources <= 200),
    ADD CONSTRAINT knowledge_base_settings_compile_max_tool_rounds_check
        CHECK (compile_max_tool_rounds >= 1 AND compile_max_tool_rounds <= 500),
    ADD CONSTRAINT knowledge_base_settings_compile_max_tokens_check
        CHECK (compile_max_tokens >= 256 AND compile_max_tokens <= 200000);

UPDATE knowledge_base_settings
SET
    compile_max_sources = CASE
        WHEN compile_max_sources IS NULL OR compile_max_sources = 10 THEN 20
        ELSE compile_max_sources
    END,
    compile_max_tool_rounds = CASE
        WHEN compile_max_tool_rounds IS NULL OR compile_max_tool_rounds = 24 THEN 50
        ELSE compile_max_tool_rounds
    END,
    compile_max_tokens = CASE
        WHEN compile_max_tokens IS NULL OR compile_max_tokens = 4096 THEN 50000
        ELSE compile_max_tokens
    END,
    updated_at = now();
