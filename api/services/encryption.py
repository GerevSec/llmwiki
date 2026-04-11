from __future__ import annotations

from cryptography.fernet import Fernet

from config import settings


def _get_fernet() -> Fernet:
    if not settings.LLMWIKI_SETTINGS_ENCRYPTION_KEY:
        raise RuntimeError("LLMWIKI_SETTINGS_ENCRYPTION_KEY is not configured")
    return Fernet(settings.LLMWIKI_SETTINGS_ENCRYPTION_KEY.encode())


def encrypt_secret(value: str | None) -> str | None:
    if not value:
        return None
    return _get_fernet().encrypt(value.encode()).decode()


def decrypt_secret(value: str | None) -> str | None:
    if not value:
        return None
    return _get_fernet().decrypt(value.encode()).decode()
