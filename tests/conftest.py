import os
import sys

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5434/supavault_test")
os.environ["SUPABASE_URL"] = ""
os.environ["SUPABASE_JWT_SECRET"] = "test-jwt-secret-that-is-at-least-32-characters-long-for-hs256"
os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""
os.environ["S3_BUCKET"] = ""
os.environ["LOGFIRE_TOKEN"] = ""
os.environ["SENTRY_DSN"] = ""
os.environ["APP_URL"] = "http://localhost:3000"
os.environ["GLOBAL_MAX_USERS"] = "1000"
os.environ["LLMWIKI_SETTINGS_ENCRYPTION_KEY"] = "ygT-8oo5CTamBw2a4GuJgFnph2C6J6g4t1ZyJy8ImDY="
os.environ["LLMWIKI_AUTOMATION_SECRET"] = "test-automation-secret"
os.environ["ANTHROPIC_MODEL"] = "claude-test"
os.environ["OPENROUTER_MODEL"] = "openrouter-test"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))
