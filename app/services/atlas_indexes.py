"""Shared helpers for creating Atlas Search / Vector Search indexes from
JSON definition files, plus the regular B-tree compound indexes that back
the hot-path queries. Reused by scripts/create_indexes.py (standalone CLI)
and data/seed_database.py (post-seed step)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

INDEX_DEF_DIR = Path(__file__).resolve().parents[2] / "data" / "atlas"


# ---------------------------------------------------------------------------
# Compound (B-tree) indexes.
#
# Each entry: (collection, keys, name). Keys follow the ESR (Equality, Sort,
# Range) rule. These are regular MongoDB indexes — visible in Compass under
# the collection's Indexes tab and used by the queries listed in comments.
# ---------------------------------------------------------------------------
COMPOUND_INDEXES: list[tuple[str, list[tuple[str, int]], str]] = [
    # Materializer: recent interactions for a household, newest first.
    # Hot query: db.interactions.find({uprn, createdAt: {$gte: ...}}).sort({createdAt: -1})
    ("interactions",   [("uprn", 1), ("createdAt", -1)],   "uprn_1_createdAt_-1"),

    # Agent 3 live_ops + materializer active-work-orders scan.
    # Hot query: db.operations_wfm.find({uprn: {$in}, updatedAt: {$gte}})
    ("operations_wfm", [("uprn", 1), ("updatedAt", -1)],   "uprn_1_updatedAt_-1"),

    # Materializer open orders projection.
    # Hot query: db.orders.find({uprn, status: {$ne/$in: ...}}).sort({createdAt: -1})
    ("orders",         [("uprn", 1), ("status", 1), ("createdAt", -1)],
                        "uprn_1_status_1_createdAt_-1"),
]


def load_index_specs() -> list[dict[str, Any]]:
    return [json.loads(p.read_text()) for p in sorted(INDEX_DEF_DIR.glob("*.json"))]


async def ensure_collection(db, coll_name: str) -> None:
    """Atlas Search / Vector Search requires the target collection to exist."""
    existing = await db.list_collection_names()
    if coll_name in existing:
        return
    try:
        await db.create_collection(coll_name)
        print(f"[created collection] {coll_name}")
    except Exception as exc:
        if "already exists" not in str(exc).lower():
            raise


async def ensure_search_indexes(db) -> None:
    """Idempotently create every Atlas Search / Vector Search index defined
    in data/atlas/."""
    for spec in load_index_specs():
        coll_name = spec["collectionName"]
        await ensure_collection(db, coll_name)
        coll = db[coll_name]
        existing = [i["name"] async for i in coll.list_search_indexes()]
        if spec["name"] in existing:
            print(f"[skip] search index {spec['name']} already exists on {coll_name}")
            continue
        await coll.create_search_index(
            {
                "name": spec["name"],
                "type": spec["type"],
                "definition": spec["definition"],
            }
        )
        print(f"[created search index] {spec['name']} on {coll_name}")


async def ensure_compound_indexes(db) -> None:
    """Idempotently create the regular B-tree compound indexes that back
    the hot-path queries. Visible in Compass under each collection's Indexes
    tab."""
    for coll_name, keys, name in COMPOUND_INDEXES:
        await ensure_collection(db, coll_name)
        coll = db[coll_name]
        existing = await coll.index_information()
        if name in existing:
            print(f"[skip] compound index {name} already exists on {coll_name}")
            continue
        created = await coll.create_index(keys, name=name)
        print(f"[created compound index] {created} on {coll_name}")


async def ensure_indexes(db) -> None:
    """Public entry point used by scripts/create_indexes.py and the seeder.
    Creates every index (search + compound) idempotently."""
    await ensure_compound_indexes(db)
    await ensure_search_indexes(db)
