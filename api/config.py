from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="../.env", extra="ignore")

    DATABASE_URL: str
    SUPABASE_URL: str = ""
    SUPABASE_JWT_SECRET: str = ""
    VOYAGE_API_KEY: str = ""
    TURBOPUFFER_API_KEY: str = ""
    EMBEDDING_MODEL: str = "voyage-4-lite"
    EMBEDDING_DIM: int = 512
    LOGFIRE_TOKEN: str = ""
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = "us-east-1"
    S3_BUCKET: str = "supavault-documents"
    MISTRAL_API_KEY: str = ""
    PDF_BACKEND: str = "pdf_oxide"  # "pdf_oxide" or "mistral"
    STAGE: str = "dev"
    APP_URL: str = "http://localhost:3000"
    API_URL: str = "http://localhost:8000"
    MCP_URL: str = "http://localhost:8080/mcp"
    CORS_ALLOWED_ORIGINS: str = ""
    CORS_ALLOWED_ORIGIN_REGEX: str = r"https://.*"
    LLMWIKI_AUTOMATION_SECRET: str = ""
    LLMWIKI_SETTINGS_ENCRYPTION_KEY: str = ""
    ANTHROPIC_MODEL: str = ""
    ANTHROPIC_MAX_TOKENS: int = 50_000
    OPENROUTER_MODEL: str = ""
    OPENROUTER_MAX_TOKENS: int = 50_000
    LLMWIKI_COMPILE_KB: str = ""
    LLMWIKI_COMPILE_MAX_SOURCES: int = 20
    LLMWIKI_COMPILE_TIMEOUT_SECONDS: int = 300
    LLMWIKI_COMPILE_MAX_CONTINUATIONS: int = 2
    LLMWIKI_COMPILE_MAX_TOOL_ROUNDS: int = 50
    LLMWIKI_COMPILE_DEFAULT_MAX_TOKENS: int = 50_000
    LLMWIKI_COMPILE_DRY_RUN: bool = False
    LLMWIKI_COMPILE_RUN_TIMEOUT_SECONDS: int = 900
    LLMWIKI_RECOMPILE_RUN_TIMEOUT_SECONDS: int = 3600
    LLMWIKI_COMPILE_NO_PROGRESS_ROUNDS: int = 8
    LLMWIKI_COMPILE_NO_PROGRESS_GRACE_SECONDS: int = 300
    LLMWIKI_RECOMPILE_BATCH_MAX_SOURCES: int = 4
    LLMWIKI_COMPILE_STALE_AFTER_SECONDS: int = 1800

    QUOTA_MAX_PAGES: int = 500  # per-user page limit (free tier)
    QUOTA_MAX_PAGES_PER_DOC: int = 300  # max pages per single document
    QUOTA_MAX_STORAGE_BYTES: int = 1_073_741_824  # 1 GB per user

    CONVERTER_URL: str = ""
    CONVERTER_SECRET: str = ""

    GLOBAL_OCR_ENABLED: bool = True
    OCR_PAGE_LIMITS_ENABLED: bool = False
    GLOBAL_MAX_PAGES: int = 50_000
    GLOBAL_MAX_USERS: int = 200

    SENTRY_DSN: str = ""


settings = Settings()
