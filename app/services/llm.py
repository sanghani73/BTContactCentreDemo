"""LLM adapter for Agent 4.

Single path: POST to Grove's Anthropic Messages passthrough at the full URL
configured in LLM_BASE_URL. If no base URL / API key is set, falls back to
a deterministic mock briefing that keeps the demo runnable locally.

Exposes two entry points:
- `llm_briefing(...)`  — awaits a complete Briefing (used by non-streaming code paths).
- `stream_llm_briefing(...)` — async-generator; yields `("delta", <str>)` as the
  tool_use JSON streams, then one `("final", Briefing)` when complete."""
from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator

import httpx

from app.config import settings
from app.models import Briefing, Citations, NextBestAction

_BRIEFING_SYSTEM_PROMPT = """You are the briefing agent for a BT contact-centre
operator. You are given structured context (identity, household, retrieved
interactions, live field-service state). You must call the emit_briefing tool
with a single JSON object matching the BriefingSchema. Every claim must cite a
specific interaction, order or work-order ID from the context. Never invent IDs.
Confidence must reflect the strength of the evidence. The recommendedScript is
the first sentence the human agent will read verbatim — be warm, specific, and
brief (max 2 sentences)."""


BRIEFING_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": [
        "callDriver", "confidence", "recommendedScript",
        "nextBestAction", "citations",
    ],
    "properties": {
        "callDriver": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "recommendedScript": {"type": "string"},
        "nextBestAction": {
            "type": "object",
            "required": ["code", "description"],
            "properties": {
                "code": {
                    "type": "string",
                    "enum": [
                        "APOLOGISE_AND_UPDATE_ETA",
                        "DISPATCH_REPLACEMENT_HARDWARE",
                        "OFFER_EE_ONE_CONVERGENCE",
                        "ISSUE_SLA_CREDIT",
                        "ESCALATE_TO_FIELD_SERVICE",
                        "WARM_TRANSFER_TO_BRAND_SPECIALIST",
                    ],
                },
                "description": {"type": "string"},
            },
        },
        "citations": {
            "type": "object",
            "properties": {
                "interactionIds": {"type": "array", "items": {"type": "string"}},
                "workOrderRefs": {"type": "array", "items": {"type": "string"}},
                "orderIds": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
}


# --- Mock briefing --------------------------------------------------------

def _mock_briefing(ivr, identity, household, context) -> Briefing:
    """Deterministic briefing for the three demo scenarios."""
    caller = identity.get("caller") or {}
    caller_name = f"{caller.get('firstName','')} {caller.get('lastName','')}".strip()
    household_uprn = household.get("uprn")
    dossier = context.get("dossier") or {}
    hybrid = context.get("hybridHits") or []
    live_ops = context.get("liveOps") or []
    active_ops = dossier.get("activeOperations") or []
    address = dossier.get("address") or {}
    thoroughfare = address.get("thoroughfareName")

    all_ops = {op.get("reference"): op for op in (*active_ops, *live_ops)
               if op.get("reference")}
    cited = Citations(
        interactionIds=[h["_id"] for h in hybrid][:3],
        workOrderRefs=sorted(all_ops.keys()),
        orderIds=[o["_id"] for o in dossier.get("openOrders", [])],
    )

    if household_uprn == "100021489211":
        delay_ops = [op for op in all_ops.values() if op.get("status") == "DELAYED"]
        if delay_ops:
            op = delay_ops[0]
            eta = op.get("statusDetails", "")
            return Briefing(
                callDriver=(
                    "Delayed Openreach repair for the household's BT broadband; "
                    "caller's partner raised the fault yesterday."
                ),
                confidence=0.97,
                recommendedScript=(
                    f"Hi {caller.get('firstName','there')} — are you calling about the "
                    f"broadband outage at {thoroughfare}? I can see Mark raised this with "
                    f"us yesterday. The engineer was delayed on an earlier job — {eta}"
                ),
                nextBestAction=NextBestAction(
                    code="APOLOGISE_AND_UPDATE_ETA",
                    description=(
                        "Lead with revised Openreach ETA, then pitch the EE One bundle "
                        "to this converged household."
                    ),
                ),
                citations=cited,
                householdUprn=household_uprn,
                callerProfileId=caller.get("_id"),
                brand=caller.get("brand"),
            )

    if household_uprn == "200039182341":
        return Briefing(
            callDriver=(
                "Plusnet-to-EE migration offline because the DPD-shipped Smart Hub "
                "was marked DAMAGED_IN_TRANSIT; replacement not yet dispatched."
            ),
            confidence=0.93,
            recommendedScript=(
                f"Hi {caller.get('firstName','there')} — I'm so sorry about the courier "
                f"damage. I can see your migration order; let me get a replacement router "
                f"out on priority delivery and set you up with an Emergency Data Booster "
                f"right now so you're not offline overnight."
            ),
            nextBestAction=NextBestAction(
                code="DISPATCH_REPLACEMENT_HARDWARE",
                description=(
                    "Trigger same-day replacement router dispatch and provision an EE "
                    "Emergency Data Booster to bridge the gap."
                ),
            ),
            citations=cited,
            householdUprn=household_uprn,
            callerProfileId=caller.get("_id"),
            brand=caller.get("brand"),
        )

    if household_uprn == "350018447722":
        in_transit = next(
            (op for op in all_ops.values() if op.get("status") == "IN_TRANSIT"), None
        )
        eta_line = in_transit.get("statusDetails") if in_transit else "Engineer en route."
        return Briefing(
            callDriver=(
                "Enquiring about new-provide FTTP install reference OR-2026-10044; "
                "wants ETA confirmation."
            ),
            confidence=0.95,
            recommendedScript=(
                f"Hi {caller.get('firstName','there')} — I can see your install order "
                f"OR-2026-10044. Good news: {eta_line}"
            ),
            nextBestAction=NextBestAction(
                code="APOLOGISE_AND_UPDATE_ETA",
                description=(
                    "Confirm engineer ETA, offer the £10 missed-appointment credit "
                    "proactively if the SLA window slips."
                ),
            ),
            citations=cited,
            householdUprn=household_uprn,
            callerProfileId=caller.get("_id"),
            brand=caller.get("brand"),
        )

    return Briefing(
        callDriver="Unable to confidently infer the call driver from available context.",
        confidence=0.4,
        recommendedScript=f"Hi {caller_name or 'there'} — how can I help you today?",
        nextBestAction=NextBestAction(
            code="WARM_TRANSFER_TO_BRAND_SPECIALIST",
            description="Route to brand specialist for standard triage.",
        ),
        citations=cited,
        householdUprn=household_uprn,
        callerProfileId=caller.get("_id"),
        brand=caller.get("brand"),
    )


# --- Grove (Anthropic Messages passthrough) -------------------------------

async def _call_grove(user_payload: dict[str, Any]) -> dict[str, Any]:
    """POST to the full Grove endpoint configured in LLM_BASE_URL.

    Sends both auth header styles so Grove can use whichever its ingress
    expects (gateway Bearer or Anthropic x-api-key). Request/response shape
    follows the Anthropic Messages API."""
    body = {
        "model": settings.llm_model,
        "max_tokens": 600,
        "system": _BRIEFING_SYSTEM_PROMPT,
        "tools": [{
            "name": "emit_briefing",
            "description": "Return the BriefingSchema-conforming briefing.",
            "input_schema": BRIEFING_INPUT_SCHEMA,
        }],
        "tool_choice": {"type": "tool", "name": "emit_briefing"},
        "messages": [{
            "role": "user",
            "content": json.dumps(user_payload, default=str),
        }],
    }
    headers = {
        "content-type": "application/json",
        "anthropic-version": "2023-06-01",
        "authorization": f"Bearer {settings.llm_api_key}",
        "x-api-key": settings.llm_api_key,
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(settings.llm_base_url, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()

    for block in data.get("content", []):
        if block.get("type") == "tool_use" and block.get("name") == "emit_briefing":
            return block["input"]

    raise RuntimeError(
        f"Grove response did not include an emit_briefing tool_use block: {data!r}"
    )


# --- Public API -----------------------------------------------------------

async def llm_briefing(
    *, ivr, identity: dict[str, Any], household: dict[str, Any], context: dict[str, Any]
) -> Briefing:
    if settings.mock_llm:
        return _mock_briefing(ivr, identity, household, context)

    user_payload = {
        "ivr": ivr.model_dump() if hasattr(ivr, "model_dump") else dict(ivr),
        "identity": _sanitise(identity),
        "household": _sanitise(household),
        "context": _sanitise(context),
    }
    data = await _call_grove(user_payload)

    data.setdefault("householdUprn", household.get("uprn"))
    caller = identity.get("caller") or {}
    data.setdefault("callerProfileId", caller.get("_id"))
    data.setdefault("brand", caller.get("brand"))
    return Briefing(**data)


def _sanitise(obj: Any) -> Any:
    """Strip rawTranscript and non-JSON types for the LLM payload."""
    if isinstance(obj, dict):
        return {k: _sanitise(v) for k, v in obj.items() if k != "rawTranscript"}
    if isinstance(obj, list):
        return [_sanitise(x) for x in obj]
    return obj


# --- Streaming adapter ----------------------------------------------------

def _build_user_payload(ivr, identity, household, context) -> dict[str, Any]:
    return {
        "ivr": ivr.model_dump() if hasattr(ivr, "model_dump") else dict(ivr),
        "identity": _sanitise(identity),
        "household": _sanitise(household),
        "context": _sanitise(context),
    }


def _finalise_briefing(data: dict[str, Any], identity: dict[str, Any],
                       household: dict[str, Any]) -> Briefing:
    data.setdefault("householdUprn", household.get("uprn"))
    caller = identity.get("caller") or {}
    data.setdefault("callerProfileId", caller.get("_id"))
    data.setdefault("brand", caller.get("brand"))
    return Briefing(**data)


async def _stream_grove(payload: dict[str, Any]) -> AsyncIterator[tuple[str, Any]]:
    """Yield ("delta", partial_json_str) for each input_json_delta from the
    model, then ("final", full_json_str) when the tool_use block completes."""
    body = {
        "model": settings.llm_model,
        "max_tokens": 600,
        "system": _BRIEFING_SYSTEM_PROMPT,
        "tools": [{
            "name": "emit_briefing",
            "description": "Return the BriefingSchema-conforming briefing.",
            "input_schema": BRIEFING_INPUT_SCHEMA,
        }],
        "tool_choice": {"type": "tool", "name": "emit_briefing"},
        "messages": [{"role": "user", "content": json.dumps(payload, default=str)}],
        "stream": True,
    }
    headers = {
        "content-type": "application/json",
        "anthropic-version": "2023-06-01",
        "authorization": f"Bearer {settings.llm_api_key}",
        "x-api-key": settings.llm_api_key,
    }
    accumulated = ""
    async with httpx.AsyncClient(timeout=60) as client:
        async with client.stream("POST", settings.llm_base_url,
                                 headers=headers, json=body) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line or not line.startswith("data: "):
                    continue
                try:
                    event = json.loads(line[6:])
                except json.JSONDecodeError:
                    continue
                if event.get("type") != "content_block_delta":
                    continue
                delta = event.get("delta") or {}
                if delta.get("type") == "input_json_delta":
                    chunk = delta.get("partial_json", "")
                    if not chunk:
                        continue
                    accumulated += chunk
                    yield "delta", chunk
    yield "final", accumulated


async def _mock_stream(briefing: Briefing) -> AsyncIterator[tuple[str, Any]]:
    """Fake incremental streaming of the mock briefing's JSON so the UI
    gets to render partial-JSON progress even in mock mode."""
    payload = briefing.model_dump()
    full = json.dumps(payload, ensure_ascii=False)
    chunk_size = 24
    for i in range(0, len(full), chunk_size):
        chunk = full[i:i + chunk_size]
        yield "delta", chunk
        await asyncio.sleep(0.03)
    yield "final", full


async def stream_llm_briefing(
    *, ivr, identity: dict[str, Any], household: dict[str, Any],
    context: dict[str, Any],
) -> AsyncIterator[tuple[str, Any]]:
    """Primary streaming entry point used by the orchestrator SSE path.
    Yields ("delta", str) events, then one ("final", Briefing) at the end."""
    if settings.mock_llm:
        briefing = _mock_briefing(ivr, identity, household, context)
        async for ev_type, chunk in _mock_stream(briefing):
            if ev_type == "delta":
                yield "delta", chunk
        yield "final", briefing
        return

    payload = _build_user_payload(ivr, identity, household, context)
    accumulated = ""
    async for ev_type, chunk in _stream_grove(payload):
        if ev_type == "delta":
            accumulated += chunk
            yield "delta", chunk
        elif ev_type == "final":
            accumulated = chunk  # authoritative final string
    data = json.loads(accumulated)
    yield "final", _finalise_briefing(data, identity, household)
