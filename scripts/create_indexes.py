"""Create the three Atlas Search indexes from JSON definitions in
data/atlas/. Idempotent — skips indexes that already exist, and creates
empty collections if they're missing. Normally you don't need to run this
yourself; `python -m data.seed_database --drop` does it as its final step."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import settings  # noqa: E402
from app.services.atlas_indexes import ensure_indexes  # noqa: E402


async def main() -> None:
    if not settings.mongo_uri:
        print("MONGO_URI not set.", file=sys.stderr)
        sys.exit(1)

    import motor.motor_asyncio
    client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    try:
        await ensure_indexes(db)
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
