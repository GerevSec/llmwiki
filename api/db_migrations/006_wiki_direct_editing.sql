ALTER TABLE knowledge_base_settings
    ADD COLUMN IF NOT EXISTS wiki_direct_editing_enabled BOOLEAN NOT NULL DEFAULT false;
