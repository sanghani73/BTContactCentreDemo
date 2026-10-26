"""Agent 4 — Predictive Briefing. Thin wrapper around the LLM adapter so the
orchestrator imports a single agent symbol."""
from __future__ import annotations

from typing import Any

from app.models import Briefing, IvrContext
from app.services.llm import llm_briefing


async def agent_4_predictive_briefing(
    ivr: IvrContext,
    identity: dict[str, Any],
    household: dict[str, Any],
    context: dict[str, Any],
) -> Briefing:
    return await llm_briefing(
        ivr=ivr, identity=identity, household=household, context=context
    )
