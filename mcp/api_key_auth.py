import hashlib

from db import service_execute, service_queryrow


def hash_api_key(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def verify_api_key_token(token: str) -> str | None:
    if not token.startswith("sv_"):
        return None

    key_hash = hash_api_key(token)
    row = await service_queryrow(
        "SELECT user_id::text AS user_id "
        "FROM api_keys "
        "WHERE key_hash = $1 AND revoked_at IS NULL",
        key_hash,
    )
    if not row:
        return None

    await service_execute(
        "UPDATE api_keys SET last_used_at = now() WHERE key_hash = $1",
        key_hash,
    )
    return row["user_id"]
