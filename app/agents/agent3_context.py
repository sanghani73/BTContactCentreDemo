"""Agent 3 — Multi-Domain Context Retrieval.

Parallel fan-out: materialized C360 dossier + $rankFusion hybrid search over
interactions + fresh WFM state across all searchable UPRNs."""
from __future__ import annotations

import asyncio
from typing import Any

from app.config import one_hour_ago, settings
from app.database import get_db
from app.services import mock_store
from app.services.hybrid_search import hybrid_interaction_search


async def _dossier(uprn: str) -> dict[str, Any] | None:
    if settings.mock_mode:
        return mock_store.c360(uprn)
    return await get_db().customer_360.find_one({"_id": f"c360_{uprn}"})


async def _live_ops(uprns: list[str]) -> list[dict[str, Any]]:
    if settings.mock_mode:
        return mock_store.wfm_recent(uprns, one_hour_ago())
    return await get_db().operations_wfm.find(
        {"uprn": {"$in": uprns}, "updatedAt": {"$gte": one_hour_ago()}}
    ).to_list(10)


async def agent_3_fetch_context(
    household_uprn: str, search_uprns: list[str], utterance: str
) -> dict[str, Any]:
    dossier, hybrid_hits, live_ops = await asyncio.gather(
        _dossier(household_uprn),
        hybrid_interaction_search(search_uprns, utterance, limit=5),
        _live_ops(search_uprns),
    )
    return {
        "dossier": dossier,
        "hybridHits": hybrid_hits,
        "liveOps": live_ops,
    }
