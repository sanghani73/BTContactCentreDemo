from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import settings

_client: Optional[AsyncIOMotorClient] = None
_db: Optional[AsyncIOMotorDatabase] = None


def get_db() -> AsyncIOMotorDatabase:
    """Return the lazily-initialised Motor database.
    In mock mode (no MONGO_URI), returns None and callers must fall through
    to canned data via app.services.mock_store."""
    global _client, _db
    if settings.mock_mode:
        return None  # type: ignore[return-value]
    if _db is None:
        _client = AsyncIOMotorClient(settings.mongo_uri)
        _db = _client[settings.mongo_db]
    return _db


async def close_db() -> None:
    global _client, _db
    if _client is not None:
        _client.close()
        _client = None
        _db = None
