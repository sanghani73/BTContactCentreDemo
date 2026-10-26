"""Post-call write-back: persist the current call as an interaction document so
future hybrid-search calls can retrieve it. Mock mode appends to the in-memory
store so the demo's 'closed loop' is visible across calls within a session."""
from __future__ import annotations

from typing import Any

from app.config import now_utc, settings
from app.database import get_db
from app.models import Briefing, IvrContext
from app.services import mock_store


async def post_call_write_back(
    call_id: str, ivr: IvrContext, briefing: Briefing, agent_notes: str = ""
) -> dict[str, Any]:
    summary = f"{briefing.recommendedScript} {agent_notes}".strip()
    doc = {
        "_id": f"int_voice_{call_id}",
        "uprn": briefing.householdUprn,
        "profileId": briefing.callerProfileId,
        "brand": briefing.brand,
        "channel": "VOICE",
        "createdAt": now_utc(),
        "rawTranscript": None,
        "interactionSummary": summary,
        "intentCategory": briefing.nextBestAction.code,
        "entities": {
            "serviceReferences": [],
            "orderReferences": briefing.citations.orderIds,
            "workOrderReferences": briefing.citations.workOrderRefs,
        },
        "status": "RESOLVED",
    }
    if settings.mock_mode:
        mock_store.INTERACTIONS.append(doc)
    else:
        await get_db().interactions.insert_one(doc)
    return doc
