"""Agent 2 — Premise & Household Resolver.

From the caller profile, expand to co-residents and to related parties
(partner / guarantor / carer), including related-party UPRNs so Agent 3's
hybrid search covers them too."""
from __future__ import annotations

from typing import Any, Optional

from app.config import settings
from app.database import get_db
from app.services import mock_store


RELATED_TYPES = {"PARTNER", "GUARANTOR", "CARER"}


async def _profiles_at_uprn(uprn: str) -> list[dict[str, Any]]:
    if settings.mock_mode:
        return mock_store.profiles_at_uprn(uprn)
    return await get_db().profiles.find({"uprn": uprn}).to_list(20)


async def _profiles_by_ids(ids: list[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    if settings.mock_mode:
        out = [mock_store.profile_by_id(i) for i in ids]
        return [p for p in out if p]
    return await get_db().profiles.find({"_id": {"$in": ids}}).to_list(20)


async def agent_2_expand_household(caller_profile: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not caller_profile:
        return {"uprn": None, "household": [], "relatedParties": [], "searchUprns": []}

    uprn = caller_profile["uprn"]
    co_residents = await _profiles_at_uprn(uprn)

    related_ids: set[str] = set()
    for p in co_residents:
        for rel in p.get("relationships", []) or []:
            if rel.get("type") in RELATED_TYPES:
                related_ids.add(rel["profileId"])

    co_resident_ids = {p["_id"] for p in co_residents}
    related_only = [i for i in related_ids if i not in co_resident_ids]
    related_parties = await _profiles_by_ids(related_only)

    related_uprns = {rp["uprn"] for rp in related_parties if rp.get("uprn")}
    search_uprns = [uprn, *sorted(related_uprns)]
    return {
        "uprn": uprn,
        "household": co_residents,
        "relatedParties": related_parties,
        "searchUprns": search_uprns,
    }
