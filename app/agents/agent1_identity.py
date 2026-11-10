"""Agent 1 — Triage & Identity Resolver.

Deterministic stage: CLI/AUTH match first; fall back to BM25 over names +
addresses extracted from the utterance. No LLM in the live path for identity
decisions; a small extractor is used for name/address tokens, mocked here."""
from __future__ import annotations

import re
from typing import Any, Optional

from app.config import settings
from app.database import get_db
from app.models import IvrContext
from app.services import mock_store

_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d[A-Z]{2})?\b", re.IGNORECASE)
_BRAND_HINTS = {
    "ee": "EE",
    "plusnet": "PLUSNET",
    "old connection": "PLUSNET",   # legacy migration hint
    "bt": "BT",
}

_KNOWN_FIRST_NAMES = {"sarah", "mark", "david", "priya", "anita"}


def _extract_identity(utterance: str) -> dict[str, Any]:
    u = utterance or ""
    low = u.lower()

    first_name = next((w.capitalize() for w in low.split() if w in _KNOWN_FIRST_NAMES), None)
    # Simple two-word name after "my name is" / "I'm" patterns
    m = re.search(r"(?:my name is|i'?m)\s+([A-Z][a-z]+)\s+([A-Z][a-z]+)", u)
    last_name = m.group(2) if m else None
    if m and not first_name:
        first_name = m.group(1)

    pc_match = _POSTCODE_RE.search(u)
    postcode = (" ".join([p for p in pc_match.groups() if p]).upper()
                if pc_match else None)

    # Thoroughfare: "<Number> <Words> Road|Street|Terrace|Crescent"
    tf = re.search(
        r"\b\d{1,4}\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\s+"
        r"(Road|Street|Terrace|Crescent|Lane|Way|Avenue|Drive)",
        u,
    )
    thoroughfare = f"{tf.group(1)} {tf.group(2)}" if tf else None

    post_town = None
    for town in ("Bristol", "Leeds", "Edinburgh", "Manchester", "Glasgow"):
        if town.lower() in low:
            post_town = town
            break

    brand_hint: Optional[str] = None
    for key, brand in _BRAND_HINTS.items():
        if " " in key:
            if key in low:
                brand_hint = brand
                break
        else:
            if re.search(rf"\b{re.escape(key)}\b", low):
                brand_hint = brand
                break

    return {
        "firstName": first_name,
        "lastName": last_name,
        "thoroughfare": thoroughfare,
        "postTown": post_town,
        "postcode": postcode,
        "brandHint": brand_hint,
    }


async def _profile_by_phone(phone: str) -> Optional[dict[str, Any]]:
    if settings.mock_mode:
        return mock_store.profile_by_phone(phone)
    return await get_db().profiles.find_one({"contact.phone": phone})


async def _premise_search(tokens: str) -> list[dict[str, Any]]:
    if settings.mock_mode:
        return mock_store.premise_search(tokens)
    db = get_db()
    pipeline = [
        {"$search": {
            "index": "search_index_premises",
            "compound": {
                "should": [
                    {"autocomplete": {
                        "path": "address.thoroughfareName",
                        "query": tokens,
                        "score": {"boost": {"value": 3}},
                    }},
                    {"text": {"path": "address.postcode",
                              "query": tokens,
                              "score": {"boost": {"value": 5}}}},
                    {"text": {"path": "address.postTown", "query": tokens}},
                ],
                "minimumShouldMatch": 1,
            },
        }},
        {"$limit": 20},
    ]
    return await db.premises.aggregate(pipeline).to_list(20)


async def _disambiguate(
    premise_hits: list[dict[str, Any]],
    full_name: str,
    brand_hint: Optional[str],
) -> dict[str, Any]:
    best = None
    best_score = -1.0
    for pr in premise_hits:
        if settings.mock_mode:
            profiles = mock_store.profiles_at_uprn(pr["uprn"])
        else:
            profiles = await get_db().profiles.find({"uprn": pr["uprn"]}).to_list(20)
        for p in profiles:
            s = 0.0
            name = f"{p.get('firstName','')} {p.get('lastName','')}".lower()
            if full_name.strip().lower() in name:
                s += 0.7
            elif p.get("firstName", "").lower() in full_name.lower():
                s += 0.4
            if brand_hint and p.get("brand") == brand_hint:
                s += 0.3
            if s > best_score:
                best_score, best = s, p
    if best is None:
        return {"caller": None, "confidence": 0.0, "method": "NO_MATCH"}
    return {
        "caller": best,
        "confidence": min(0.95, 0.5 + best_score / 2),
        "method": "SEARCH_FALLBACK",
    }


async def agent_1_resolve_identity(ivr: IvrContext) -> dict[str, Any]:
    if ivr.cli and ivr.authState == "AUTH":
        caller = await _profile_by_phone(ivr.cli)
        if caller:
            return {"caller": caller, "confidence": 0.99, "method": "AUTH_CLI_MATCH"}

    if ivr.cli:
        caller = await _profile_by_phone(ivr.cli)
        if caller:
            return {"caller": caller, "confidence": 0.85, "method": "CLI_HINT"}

    extracted = _extract_identity(ivr.utterance)
    search_tokens = " ".join(filter(None, [
        extracted.get("thoroughfare"),
        extracted.get("postcode"),
        extracted.get("postTown"),
    ]))
    hits = await _premise_search(search_tokens) if search_tokens else []
    full_name = " ".join(filter(None, [extracted.get("firstName"),
                                       extracted.get("lastName")])) or ""
    result = await _disambiguate(hits, full_name, extracted.get("brandHint"))
    result["extracted"] = extracted
    return result
