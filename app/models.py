from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class IvrContext(BaseModel):
    cli: Optional[str] = None
    authState: Literal["AUTH", "UNAUTH"] = "UNAUTH"
    dtmfPath: list[str] = Field(default_factory=list)
    menuSelections: list[str] = Field(default_factory=list)
    utterance: str
    receivedAt: Optional[str] = None
    scenarioId: Optional[str] = None  # demo convenience; drives mock routing


class NextBestAction(BaseModel):
    code: Literal[
        "APOLOGISE_AND_UPDATE_ETA",
        "DISPATCH_REPLACEMENT_HARDWARE",
        "OFFER_EE_ONE_CONVERGENCE",
        "ISSUE_SLA_CREDIT",
        "ESCALATE_TO_FIELD_SERVICE",
        "WARM_TRANSFER_TO_BRAND_SPECIALIST",
    ]
    description: str


class Citations(BaseModel):
    interactionIds: list[str] = Field(default_factory=list)
    workOrderRefs: list[str] = Field(default_factory=list)
    orderIds: list[str] = Field(default_factory=list)


class Briefing(BaseModel):
    callDriver: str
    confidence: float = Field(ge=0, le=1)
    recommendedScript: str
    nextBestAction: NextBestAction
    citations: Citations
    householdUprn: Optional[str] = None
    callerProfileId: Optional[str] = None
    brand: Optional[str] = None


class ScoreDetail(BaseModel):
    inputPipelineName: str
    rank: int
    weight: float
    value: float
    description: Optional[str] = None


class HybridHit(BaseModel):
    _id: str
    interactionSummary: str
    channel: Optional[str] = None
    createdAt: Optional[Any] = None
    uprn: Optional[str] = None
    hybridScore: float
    scoreDetails: list[ScoreDetail] = Field(default_factory=list)
