"""Hybrid search over the `interactions` collection using native `$rankFusion`.
In mock mode, a deterministic in-memory RRF against the canned store so the
demo runs without an Atlas cluster."""
from __future__ import annotations

import re
from typing import Any

from app.config import seven_days_ago, settings
from app.database import get_db
from app.services import mock_store

RANK_CONSTANT = 60
SEMANTIC_WEIGHT = 0.6
KEYWORD_WEIGHT = 0.4

_TOKEN_RE = re.compile(r"[A-Za-z0-9\-]+")


def _tokens(s: str) -> list[str]:
    return [t.lower() for t in _TOKEN_RE.findall(s or "")]


async def hybrid_interaction_search(
    uprns: list[str], utterance: str, limit: int = 5
) -> list[dict[str, Any]]:
    if settings.mock_mode:
        return _mock_rankfusion(uprns, utterance, limit)
    return await _atlas_rankfusion(uprns, utterance, limit)


def _flatten_atlas_score_details(raw: Any) -> list[dict[str, Any]]:
    """Atlas `$meta: scoreDetails` returns an object:
        {value, description, details: [{inputPipelineName, rank, weight, value, ...}]}
    The mock path and the UI expect a flat list of per-pipeline entries, so
    we project to that shape and keep the fields the trace panel renders."""
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict):
        return []
    out = []
    for d in raw.get("details", []) or []:
        if not isinstance(d, dict):
            continue
        out.append({
            "inputPipelineName": d.get("inputPipelineName"),
            "rank": d.get("rank"),
            "weight": d.get("weight"),
            "value": d.get("value"),
            "description": d.get("description"),
        })
    return out


async def _atlas_rankfusion(
    uprns: list[str], utterance: str, limit: int
) -> list[dict[str, Any]]:
    db = get_db()
    pipeline = [
        {
            "$rankFusion": {
                "input": {
                    "pipelines": {
                        "semantic": [
                            {
                                "$vectorSearch": {
                                    "index": "vector_index_interactions",
                                    "path": "interactionSummary",
                                    "query": utterance,
                                    "numCandidates": 100,
                                    "limit": 20,
                                    "filter": {
                                        "uprn": {"$in": uprns},
                                        "createdAt": {"$gte": seven_days_ago()},
                                    },
                                }
                            }
                        ],
                        "keyword": [
                            {
                                "$search": {
                                    "index": "search_index_interactions",
                                    "compound": {
                                        "filter": [
                                            {"in": {"path": "uprn", "value": uprns}},
                                            {"range": {"path": "createdAt",
                                                        "gte": seven_days_ago()}},
                                        ],
                                        "should": [
                                            {"text": {"path": "interactionSummary",
                                                      "query": utterance}},
                                            {"text": {
                                                "path": [
                                                    "entities.orderReferences",
                                                    "entities.workOrderReferences",
                                                    "entities.serviceReferences",
                                                ],
                                                "query": utterance,
                                            }},
                                        ],
                                    }
                                }
                            },
                            {"$limit": 20},
                        ],
                    }
                },
                "combination": {
                    "weights": {"semantic": SEMANTIC_WEIGHT, "keyword": KEYWORD_WEIGHT}
                },
                "scoreDetails": True,
            }
        },
        {"$limit": limit},
        {
            "$addFields": {
                "hybridScore": {"$meta": "score"},
                "scoreDetails": {"$meta": "scoreDetails"},
            }
        },
    ]
    docs = await db.interactions.aggregate(pipeline).to_list(length=limit)
    for d in docs:
        d["scoreDetails"] = _flatten_atlas_score_details(d.get("scoreDetails"))
    return docs


# --- Mock-mode RRF ---------------------------------------------------------

_SEMANTIC_SYNONYMS = {
    "broadband": {"line", "fttp", "fiber", "fibre", "internet", "connection", "los"},
    "engineer": {"technician", "tech", "openreach", "field", "install"},
    "install": {"provision", "setup", "appointment"},
    "late": {"delayed", "overdue", "waiting"},
    "down": {"outage", "fault", "los", "ceased"},
    "router": {"hub", "ont", "smart", "device"},
    "install": {"appointment", "slot", "provision"},
}


def _semantic_score(utterance: str, text: str) -> float:
    """Fake cosine-like score: expands utterance tokens with synonyms and counts
    matches in the candidate summary, normalised by length."""
    u_tokens = set(_tokens(utterance))
    expanded = set(u_tokens)
    for t in u_tokens:
        expanded |= _SEMANTIC_SYNONYMS.get(t, set())
    t_tokens = _tokens(text)
    if not t_tokens:
        return 0.0
    overlap = sum(1 for t in t_tokens if t in expanded)
    return overlap / (len(t_tokens) ** 0.5)


def _keyword_score(utterance: str, candidate: dict[str, Any]) -> float:
    """BM25-ish: strong boost for exact identifier matches in entities."""
    u_tokens = _tokens(utterance)
    text_tokens = _tokens(candidate["interactionSummary"])
    score = 0.0
    for t in u_tokens:
        score += text_tokens.count(t) * 0.5

    ents = candidate.get("entities", {}) or {}
    identifiers: list[str] = []
    for key in ("orderReferences", "workOrderReferences", "serviceReferences"):
        identifiers.extend([x.lower() for x in ents.get(key, []) or []])
    for t in u_tokens:
        if t in identifiers:
            score += 10.0  # verbatim ID is king
    return score


def _mock_rankfusion(
    uprns: list[str], utterance: str, limit: int
) -> list[dict[str, Any]]:
    candidates = mock_store.interactions_by_uprns(uprns, seven_days_ago())

    semantic_ranked = sorted(
        candidates,
        key=lambda c: _semantic_score(utterance, c["interactionSummary"]),
        reverse=True,
    )
    keyword_ranked = sorted(
        candidates, key=lambda c: _keyword_score(utterance, c), reverse=True
    )

    sem_rank = {c["_id"]: i + 1 for i, c in enumerate(semantic_ranked)
                if _semantic_score(utterance, c["interactionSummary"]) > 0}
    kw_rank = {c["_id"]: i + 1 for i, c in enumerate(keyword_ranked)
               if _keyword_score(utterance, c) > 0}

    fused: dict[str, dict[str, Any]] = {}
    for cid, rank in sem_rank.items():
        v = SEMANTIC_WEIGHT * (1.0 / (RANK_CONSTANT + rank))
        fused.setdefault(cid, {"value": 0.0, "details": []})
        fused[cid]["value"] += v
        fused[cid]["details"].append({
            "inputPipelineName": "semantic", "rank": rank,
            "weight": SEMANTIC_WEIGHT, "value": v,
            "description": "cosine similarity on interactionSummary",
        })
    for cid, rank in kw_rank.items():
        v = KEYWORD_WEIGHT * (1.0 / (RANK_CONSTANT + rank))
        fused.setdefault(cid, {"value": 0.0, "details": []})
        fused[cid]["value"] += v
        c = next(c for c in candidates if c["_id"] == cid)
        matched_entity = _identifier_match(utterance, c)
        fused[cid]["details"].append({
            "inputPipelineName": "keyword", "rank": rank,
            "weight": KEYWORD_WEIGHT, "value": v,
            "description": (
                f"BM25 match on {matched_entity}" if matched_entity
                else "BM25 match on interactionSummary"
            ),
        })

    ranked = sorted(fused.items(), key=lambda kv: -kv[1]["value"])[:limit]
    out = []
    for cid, info in ranked:
        c = next(c for c in candidates if c["_id"] == cid)
        out.append({
            **c,
            "hybridScore": round(info["value"], 6),
            "scoreDetails": info["details"],
        })
    return out


def _identifier_match(utterance: str, candidate: dict[str, Any]) -> str | None:
    u_tokens = set(_tokens(utterance))
    ents = candidate.get("entities", {}) or {}
    for field, label in (
        ("workOrderReferences", "entities.workOrderReferences"),
        ("orderReferences", "entities.orderReferences"),
        ("serviceReferences", "entities.serviceReferences"),
    ):
        for ident in ents.get(field, []) or []:
            if ident.lower() in u_tokens:
                return f"{label} = {ident}"
    return None
