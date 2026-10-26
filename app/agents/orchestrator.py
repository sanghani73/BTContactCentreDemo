"""End-to-end orchestration of the four stages, with per-stage latency
measurement for the agent-desktop telemetry panel. Exposes a non-streaming
`process_inbound_call` (returns the full result) and a streaming
`stream_inbound_call` async generator used by the SSE endpoint."""
from __future__ import annotations

import time
import uuid
from typing import Any, AsyncIterator

from app.agents.agent1_identity import agent_1_resolve_identity
from app.agents.agent2_household import agent_2_expand_household
from app.agents.agent3_context import agent_3_fetch_context
from app.models import Briefing, IvrContext
from app.services.llm import llm_briefing, stream_llm_briefing
from app.services.writeback import post_call_write_back


def _elapsed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


async def _resolve_identity_and_household(
    ivr: IvrContext, telemetry: dict[str, float]
) -> tuple[dict[str, Any], dict[str, Any]]:
    t0 = time.perf_counter()
    identity = await agent_1_resolve_identity(ivr)
    telemetry["agent1_identity_ms"] = _elapsed_ms(t0)

    t0 = time.perf_counter()
    household = await agent_2_expand_household(identity.get("caller"))
    telemetry["agent2_household_ms"] = _elapsed_ms(t0)
    return identity, household


async def _fetch_context(
    household: dict[str, Any], utterance: str, telemetry: dict[str, float]
) -> dict[str, Any]:
    if not household.get("uprn"):
        telemetry["agent3_context_ms"] = 0.0
        return {"dossier": None, "hybridHits": [], "liveOps": []}
    t0 = time.perf_counter()
    ctx = await agent_3_fetch_context(
        household_uprn=household["uprn"],
        search_uprns=household["searchUprns"],
        utterance=utterance,
    )
    telemetry["agent3_context_ms"] = _elapsed_ms(t0)
    return ctx


async def process_inbound_call(ivr: IvrContext) -> dict[str, Any]:
    """Non-streaming end-to-end — kept for `POST /api/calls/inbound` and tests."""
    telemetry: dict[str, float] = {}
    call_id = uuid.uuid4().hex[:12]

    identity, household = await _resolve_identity_and_household(ivr, telemetry)
    context = await _fetch_context(household, ivr.utterance, telemetry)

    t0 = time.perf_counter()
    briefing: Briefing = await llm_briefing(
        ivr=ivr, identity=identity, household=household, context=context
    )
    telemetry["agent4_briefing_ms"] = _elapsed_ms(t0)
    telemetry["total_ms"] = round(sum(telemetry.values()), 2)

    writeback_doc = await post_call_write_back(call_id, ivr, briefing)

    return {
        "callId": call_id,
        "identity": identity,
        "household": household,
        "context": context,
        "briefing": briefing.model_dump(),
        "writebackInteractionId": writeback_doc["_id"],
        "telemetry": telemetry,
    }


async def stream_inbound_call(
    ivr: IvrContext,
) -> AsyncIterator[tuple[str, dict[str, Any]]]:
    """Streaming end-to-end used by SSE endpoint.
    Yields (event_type, payload) tuples in this order:
      identity → household → context → briefing_delta* → briefing → complete
    `briefing_delta` carries incremental tool_use JSON chunks; the UI can
    extract fields incrementally without waiting for the final event."""
    telemetry: dict[str, float] = {}
    call_id = uuid.uuid4().hex[:12]

    t0 = time.perf_counter()
    identity = await agent_1_resolve_identity(ivr)
    telemetry["agent1_identity_ms"] = _elapsed_ms(t0)
    yield "identity", {"identity": identity, "telemetry": dict(telemetry)}

    t0 = time.perf_counter()
    household = await agent_2_expand_household(identity.get("caller"))
    telemetry["agent2_household_ms"] = _elapsed_ms(t0)
    yield "household", {"household": household, "telemetry": dict(telemetry)}

    context = await _fetch_context(household, ivr.utterance, telemetry)
    yield "context", {"context": context, "telemetry": dict(telemetry)}

    t0 = time.perf_counter()
    first_token = False
    final_briefing: Briefing | None = None
    async for ev_type, chunk in stream_llm_briefing(
        ivr=ivr, identity=identity, household=household, context=context
    ):
        if ev_type == "delta":
            if not first_token:
                telemetry["agent4_first_token_ms"] = _elapsed_ms(t0)
                first_token = True
            yield "briefing_delta", {"delta": chunk}
        elif ev_type == "final":
            final_briefing = chunk
    telemetry["agent4_briefing_ms"] = _elapsed_ms(t0)

    if final_briefing is None:
        return

    writeback_doc = await post_call_write_back(call_id, ivr, final_briefing)
    telemetry["total_ms"] = round(sum(
        v for k, v in telemetry.items() if k.endswith("_ms")
    ), 2)

    yield "briefing", {
        "callId": call_id,
        "briefing": final_briefing.model_dump(),
        "writebackInteractionId": writeback_doc["_id"],
        "telemetry": dict(telemetry),
    }
