from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.agents.orchestrator import process_inbound_call, stream_inbound_call
from app.config import settings
from app.models import IvrContext

router = APIRouter(prefix="/api")


SCENARIOS: dict[str, dict[str, Any]] = {
    "sarah": {
        "label": "Scenario 1 — Sarah Jenkins (EE/BT household outage)",
        "description": ("EE mobile customer calls about a broadband outage — but "
                        "the broadband is her partner Mark's BT account. Tests "
                        "cross-brand household linkage: the system must connect "
                        "Sarah's EE profile to Mark's BT fault and surface the "
                        "delayed Openreach engineer visit."),
        "cli": "+447700900123",
        "authState": "UNAUTH",
        "utterance": ("Hi, our home broadband is still completely down and the "
                       "engineer was supposed to be here by now."),
        "dtmfPath": ["2", "1"],
        "menuSelections": ["BROADBAND", "FAULT"],
    },
    "david": {
        "label": "Scenario 2 — David Patel (unregistered CLI, damaged migration)",
        "description": ("Caller's phone number is not on file. The system must "
                        "identify David from his spoken name and address using "
                        "Atlas Search, then discover his Plusnet-to-EE migration "
                        "order and the damaged router (DPD parcel marked "
                        "DAMAGED_IN_TRANSIT)."),
        "cli": "+447700900888",
        "authState": "UNAUTH",
        "utterance": ("Hello, my name is David Patel from 27 Victoria Road in Leeds. "
                       "My old connection has gone off and your courier broke the "
                       "new router."),
        "dtmfPath": [],
        "menuSelections": [],
    },
    "priya": {
        "label": "Scenario 3 — Priya Shah (authenticated, cites work-order ref)",
        "description": ("Authenticated caller quotes a specific work-order "
                        "reference. Tests hybrid search with exact-match keyword "
                        "boosting: the cited reference OR-2026-10044 should rank "
                        "highest, and live WFM data shows the engineer is en "
                        "route."),
        "cli": "+447700900777",
        "authState": "AUTH",
        "utterance": ("Hi, I'm calling about my install today, the reference is "
                       "OR-2026-10044, no one has turned up yet."),
        "dtmfPath": ["3"],
        "menuSelections": ["APPOINTMENT_STATUS"],
    },
}


@router.get("/scenarios")
async def list_scenarios() -> dict[str, Any]:
    llm_label = "mock" if settings.mock_llm else f"grove:{settings.llm_model}"
    return {
        "scenarios": [
            {"id": k, **{kk: vv for kk, vv in v.items()}} for k, v in SCENARIOS.items()
        ],
        "mockMode": settings.mock_mode,
        "mockLlm": settings.mock_llm,
        "llmLabel": llm_label,
    }


@router.post("/calls/inbound")
async def inbound_call(ivr: IvrContext) -> dict[str, Any]:
    if not ivr.receivedAt:
        ivr.receivedAt = datetime.now(timezone.utc).isoformat()
    result = await process_inbound_call(ivr)
    return result


@router.post("/scenarios/{scenario_id}/run")
async def run_scenario(scenario_id: str) -> dict[str, Any]:
    s = SCENARIOS[scenario_id]
    ivr = IvrContext(
        cli=s["cli"],
        authState=s["authState"],
        dtmfPath=s["dtmfPath"],
        menuSelections=s["menuSelections"],
        utterance=s["utterance"],
        receivedAt=datetime.now(timezone.utc).isoformat(),
        scenarioId=scenario_id,
    )
    return await process_inbound_call(ivr)


# --- Streaming SSE endpoints ---------------------------------------------

async def _sse(generator) -> AsyncIterator[bytes]:
    async for event_type, payload in generator:
        line = (
            f"event: {event_type}\n"
            f"data: {json.dumps(payload, default=str)}\n\n"
        )
        yield line.encode("utf-8")


def _scenario_to_ivr(scenario_id: str) -> IvrContext:
    s = SCENARIOS[scenario_id]
    return IvrContext(
        cli=s["cli"],
        authState=s["authState"],
        dtmfPath=s["dtmfPath"],
        menuSelections=s["menuSelections"],
        utterance=s["utterance"],
        receivedAt=datetime.now(timezone.utc).isoformat(),
        scenarioId=scenario_id,
    )


@router.post("/calls/inbound/stream")
async def inbound_call_stream(ivr: IvrContext) -> StreamingResponse:
    if not ivr.receivedAt:
        ivr.receivedAt = datetime.now(timezone.utc).isoformat()
    return StreamingResponse(
        _sse(stream_inbound_call(ivr)),
        media_type="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@router.post("/scenarios/{scenario_id}/run/stream")
async def run_scenario_stream(scenario_id: str) -> StreamingResponse:
    ivr = _scenario_to_ivr(scenario_id)
    return StreamingResponse(
        _sse(stream_inbound_call(ivr)),
        media_type="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )
