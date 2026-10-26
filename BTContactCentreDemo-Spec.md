# Technical Specification: BT Next-Gen Contact Centre Multi-Agent Operational Customer 360 Demo

## 1. System Overview & Architecture

This specification defines a prototype demonstrating how BT Group can unify fragmented customer journeys across **BT, EE, and Plusnet** brands using MongoDB Atlas as a unified **Operational Data Layer (ODL)**.

The system evolves BT's existing Profile Hub and Service Hub into an **Operational Household & Customer 360**. A materialized, read-optimized `customer_360` collection is maintained via MongoDB Change Streams. At query time, the system combines:

- Deterministic MQL lookups (CLI → profile → UPRN → household dossier),
- Atlas Search (BM25 keyword/autocomplete over addresses, names, order references),
- Atlas Vector Search with **GA auto-embedding** via **Voyage AI** (`voyage-4-large`), and
- **Native hybrid search** via the `$rankFusion` aggregation operator,

to deliver a sub-second, predictive briefing card to the contact centre agent before the inbound call connects.

**A note on framing.** We refer to the pipeline below as "multi-agent" because that is how BT stakeholders think about contact-centre AI. In fact it is deliberately **four coordinated stages, only one of which is agentic**: Agents 1–3 are deterministic Python functions over MongoDB (fast, auditable, no model hallucinations on identity or retrieval), and Agent 4 is a schema-constrained LLM call that synthesises the briefing. This split is a feature, not a limitation — it gives us sub-second latency and a defensible audit trail.

```
                                 [ INBOUND CALL / IVR ]
                        (CLI + DTMF path + Speech-to-Text utterance
                         + Authentication state + Menu selections)
                                           │
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│                              FastAPI Multi-Agent Core                              │
│                                                                                    │
│  ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐     │
│  │ Agent 1: Triage &   │ -> │ Agent 2: Premise &  │ -> │ Agent 3: Multi-     │     │
│  │ Identity Resolver   │    │ Household Resolver  │    │ Domain Retrieval    │     │
│  └─────────────────────┘    └─────────────────────┘    └──────────┬──────────┘     │
│   CLI MQL + Atlas Search    UPRN graph expansion         Parallel fan-out:         │
│   fallback on utterance     + related-party links        - C360 single-key fetch   │
│                                                          - $rankFusion hybrid      │
│                                                            over interactions       │
│                                                          - Orders & WFM queries    │
│                                                                     │              │
│                                                                     ▼              │
│                                                         ┌─────────────────────┐    │
│                                                         │ Agent 4: Predictive │    │
│                                                         │ Briefing (LLM)      │    │
│                                                         └──────────┬──────────┘    │
└────────────────────────────────────────────────────────────────────┼───────────────┘
                                                                     │ WebSocket push
                                                                     ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│                              BT Agent Desktop UI                                   │
│                                                                                    │
│  ┌────────────────────────────────┐  ┌────────────────────────────────────────┐    │
│  │   Predictive Briefing Card     │  │   Unified Household Journey Timeline   │    │
│  │ - Confirmed caller & role      │  │   (digital + voice + field service)    │    │
│  │ - Inferred call driver         │  ├────────────────────────────────────────┤    │
│  │ - Next best action & script    │  │   Household 360 Dossier                │    │
│  └────────────────────────────────┘  │   - Premise topology & line health     │    │
│                                      │   - Related parties & relationships    │    │
│                                      │   - Active orders, appointments        │    │
│                                      │   - Convergence eligibility            │    │
│                                      └────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────────────┘
```

## 2. Asynchronous Materialization Pipeline (Change Streams)

To sustain contact-centre scale without dynamic cross-collection `$lookup` latency on the hot path, the platform maintains a pre-joined, read-optimized `customer_360` collection anchored by the UK **Unique Property Reference Number (UPRN)**.

```
┌──────────────┐
│  profiles    │──┐
└──────────────┘  │   Collection-level change streams (with resume tokens)
┌──────────────┐  │
│ interactions │──┼──> [ Materializer Worker ] ──> [ customer_360 Collection ]
└──────────────┘  │    (debounced per UPRN,         (single-key fetch: ~3 ms)
┌──────────────┐  │     idempotent upsert,
│ operations_  │──┤     DLQ on failure)
│    wfm       │  │
└──────────────┘  │
┌──────────────┐  │
│   orders     │──┘
└──────────────┘
```

### 2.1 Materialization Worker (`app/services/c360_materializer.py`)

```python
import asyncio
from typing import Optional
from pymongo import ReturnDocument
from app.database import db
from app.config import settings

WATCHED = ["profiles", "interactions", "operations_wfm", "orders"]
DEBOUNCE_SECONDS = 0.25  # coalesce bursts of updates to the same UPRN


async def watch_collection(coll_name: str, inbox: asyncio.Queue):
    """Watch a single collection; resume token persisted so restarts are lossless.
    Handles insert/update/replace via fullDocument; deletes via a pre-image lookup
    so a householder moving out still triggers a C360 rebuild for their UPRN."""
    pipeline = [
        {"$match": {"operationType": {"$in": ["insert", "update", "replace", "delete"]}}}
    ]
    kwargs = {
        "full_document": "updateLookup",
        "full_document_before_change": "whenAvailable",  # for delete handling
    }
    resume_token = await db.cdc_state.find_one({"_id": coll_name})
    if resume_token:
        kwargs["resume_after"] = resume_token["token"]

    async with db[coll_name].watch(pipeline, **kwargs) as stream:
        async for change in stream:
            uprn = None
            if change["operationType"] == "delete":
                before = change.get("fullDocumentBeforeChange") or {}
                uprn = before.get("uprn")
                if not uprn:
                    uprn = await _lookup_uprn_by_doc_key(coll_name, change["documentKey"])
            else:
                uprn = (change.get("fullDocument") or {}).get("uprn")

            if uprn:
                await inbox.put(uprn)
            await db.cdc_state.update_one(
                {"_id": coll_name},
                {"$set": {"token": change["_id"]}},
                upsert=True,
            )


async def _lookup_uprn_by_doc_key(coll_name: str, doc_key: dict) -> Optional[str]:
    """Fallback when pre-image is unavailable: resolve the UPRN of the deleted doc
    from the last known C360 by scanning the materialized household membership."""
    _id = doc_key.get("_id")
    hit = await db.customer_360.find_one(
        {"$or": [{"householdMembers.profileId": _id},
                 {"recentInteractionsSummary.interactionId": _id},
                 {"activeOperations.reference": _id},
                 {"openOrders._id": _id}]},
        {"uprn": 1},
    )
    return hit.get("uprn") if hit else None


async def materializer_loop(inbox: asyncio.Queue):
    """Debounce by UPRN and rebuild idempotently."""
    pending: dict[str, asyncio.Task] = {}

    while True:
        uprn = await inbox.get()
        if uprn in pending and not pending[uprn].done():
            continue
        pending[uprn] = asyncio.create_task(_debounced_rebuild(uprn))


async def _debounced_rebuild(uprn: str):
    await asyncio.sleep(DEBOUNCE_SECONDS)
    try:
        await rebuild_c360_document(uprn)
    except Exception as exc:
        await db.c360_dlq.insert_one({"uprn": uprn, "error": str(exc)})


async def rebuild_c360_document(uprn: str) -> Optional[dict]:
    premise = await db.premises.find_one({"uprn": uprn})
    if not premise:
        return None

    profiles, recent_interactions, active_operations, open_orders = await asyncio.gather(
        db.profiles.find({"uprn": uprn}).to_list(length=20),
        db.interactions.find(
            {"uprn": uprn, "createdAt": {"$gte": settings.recent_window_start()}}
        ).sort("createdAt", -1).limit(10).to_list(length=10),
        db.operations_wfm.find(
            {"uprn": uprn, "status": {"$in": ["DELAYED", "IN_TRANSIT", "PENDING", "DISPATCHED"]}}
        ).to_list(length=10),
        db.orders.find({"uprn": uprn, "status": {"$ne": "COMPLETED"}}).to_list(length=10),
    )

    brands = {p.get("brand") for p in profiles}
    is_converged = {"BT", "EE"}.issubset(brands)

    c360_payload = {
        "_id": f"c360_{uprn}",
        "uprn": uprn,
        "address": premise.get("address"),
        "networkInfrastructure": premise.get("networkInfrastructure"),
        "householdMembers": [
            {
                "profileId": p["_id"],
                "name": f"{p['firstName']} {p['lastName']}",
                "brand": p["brand"],
                "role": p.get("roleAtPremise", "RESIDENT"),
                "phone": p.get("contact", {}).get("phone"),
                "relationships": p.get("relationships", []),
                "services": p.get("products", []),
            }
            for p in profiles
        ],
        "activeOperations": active_operations,
        "openOrders": open_orders,
        "recentInteractionsSummary": [
            {
                "interactionId": i["_id"],
                "channel": i.get("channel"),
                "createdAt": i.get("createdAt"),
                "summary": i.get("interactionSummary"),
                "status": i.get("status"),
                "profileId": i.get("profileId"),
            }
            for i in recent_interactions
        ],
        "commercialInsights": {
            "convergedHousehold": is_converged,
            "nextBestAction": "EE_ONE_CONVERGENCE_BUNDLE" if is_converged else "STANDALONE_RETAIN",
            "eligibleDiscounts": (
                ["£10/mo Broadband Discount", "Unlimited Data Boost on EE Mobile"]
                if is_converged else []
            ),
        },
        "updatedAt": premise.get("updatedAt"),
    }

    return await db.customer_360.find_one_and_replace(
        {"_id": f"c360_{uprn}"},
        c360_payload,
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
```

Design notes:

- **Collection-level** change streams (not cluster-wide) with persisted resume tokens — losses on restart are prevented, matching the architecture diagram.
- **Operation-type filter** scopes the stream to inserts/updates/replaces/deletes; the pre-image flag (`fullDocumentBeforeChange: whenAvailable`) lets us rebuild the C360 when a profile is deleted (e.g. a householder moves out) so stale members don't linger.
- **Debounce per UPRN** coalesces bursts (e.g. a householder transfer that touches four profiles).
- **DLQ** (`c360_dlq`) captures rebuild failures for operator review.
- Rebuild is a full idempotent upsert. In production, hot fields (e.g. `operations_wfm.status`) can switch to incremental `$set` updates for sub-50 ms propagation.
- **Alternative deployment:** The same materialization logic can be hosted inside MongoDB using **Atlas Triggers** (database triggers on each watched collection) or **Atlas Stream Processing** (a managed `$source → $merge` pipeline). For a BT ODL pitch, this means "we don't even need to run the worker." The Python worker shown here is the demo artefact; the production pattern can be Atlas-native.

## 3. MongoDB Data Models

### 3.1 Collection: `customer_360` (Materialized Household Dossier)

The primary document retrieved on inbound calls.

```json
{
  "_id": "c360_100021489211",
  "uprn": "100021489211",
  "address": {
    "subBuildingName": "Flat 2B",
    "buildingNumber": "14",
    "thoroughfareName": "Elmwood Terrace",
    "postTown": "Bristol",
    "postcode": "BS1 5RR"
  },
  "networkInfrastructure": {
    "ontSerial": "ALCLB0912A34",
    "exchangeCode": "E-BRIS-01",
    "technology": "FTTP",
    "maxAvailableSpeed": "1000Mbps",
    "lineStatus": "OUTAGE_DETECTED"
  },
  "householdMembers": [
    {
      "profileId": "prof_ee_98214",
      "name": "Sarah Jenkins",
      "brand": "EE",
      "role": "RESIDENT",
      "phone": "+447700900123",
      "relationships": [
        { "profileId": "prof_bt_44102", "type": "PARTNER", "strength": 0.92 }
      ],
      "services": [
        { "serviceId": "srv_ee_mob_11", "category": "MOBILE",
          "plan": "Unlimited All-Rounder 5G", "status": "ACTIVE" }
      ]
    },
    {
      "profileId": "prof_bt_44102",
      "name": "Mark Davis",
      "brand": "BT",
      "role": "ACCOUNT_HOLDER",
      "phone": "+447700900456",
      "relationships": [
        { "profileId": "prof_ee_98214", "type": "PARTNER", "strength": 0.92 }
      ],
      "services": [
        { "serviceId": "srv_bt_bb_99", "category": "BROADBAND",
          "plan": "Full Fibre 900 Halo 3", "status": "FAULT_REPORTED" }
      ]
    }
  ],
  "activeOperations": [
    {
      "workOrderType": "SPLICING_AND_ONT_REPAIR",
      "provider": "OPENREACH",
      "reference": "OR-2026-9921",
      "status": "DELAYED",
      "statusDetails": "Field tech delayed on overhead fibre splice. Revised ETA: 13:45.",
      "scheduledWindow": {
        "start": "2026-10-23T08:00:00Z",
        "end": "2026-10-23T13:00:00Z"
      }
    }
  ],
  "openOrders": [],
  "recentInteractionsSummary": [
    { "interactionId": "int_ee_app_20261023_099",
      "channel": "MOBILE_APP",
      "createdAt": "2026-10-23T12:45:00Z",
      "summary": "Sarah checked 'Broadband in My Area' on MyEE app. Abandoned at line check.",
      "status": "SELF_SERVICE_DROP",
      "profileId": "prof_ee_98214" },
    { "interactionId": "int_bt_chat_20261022_312",
      "channel": "WEBCHAT",
      "createdAt": "2026-10-22T16:15:00Z",
      "summary": "Mark reported steady purple Smart Hub light. LOS detected. Booked Openreach for 23/10.",
      "status": "ESCALATED_FIELD_SERVICE",
      "profileId": "prof_bt_44102" }
  ],
  "commercialInsights": {
    "convergedHousehold": true,
    "nextBestAction": "EE_ONE_CONVERGENCE_BUNDLE",
    "eligibleDiscounts": [
      "£10/mo Broadband Discount",
      "Unlimited Data Boost on EE Mobile"
    ]
  }
}
```

### 3.2 Collection: `interactions` (Service Hub, with Voyage AI auto-embedded summary)

```json
{
  "_id": "int_ee_app_20261023_099",
  "uprn": "100021489211",
  "profileId": "prof_ee_98214",
  "brand": "EE",
  "channel": "MOBILE_APP",
  "createdAt": "2026-10-23T12:45:00Z",
  "rawTranscript": "Path: MyEE > Help > Broadband Status > Check Line. User dropped off after prompt: Enter BT Landline Number.",
  "interactionSummary": "Customer Sarah Jenkins attempted broadband fault self-service in the MyEE app. Abandoned the journey when prompted for a BT landline credential she does not hold.",
  "intentCategory": "BROADBAND_SELF_SERVICE_FAILURE",
  "entities": {
    "serviceReferences": ["srv_bt_bb_99"],
    "orderReferences": [],
    "workOrderReferences": ["OR-2026-9921"]
  },
  "status": "SELF_SERVICE_ABANDONED"
}
```

Only `interactionSummary` is embedded for search. The `rawTranscript` is retained for compliance but is not indexed into vector space (see Section 10: Security & Compliance).

### 3.3 Collection: `orders`

```json
{
  "_id": "ord_ee_fibre_55120",
  "uprn": "200039182341",
  "profileId": "prof_pn_11871",
  "brand": "EE",
  "orderType": "MIGRATION_PLUSNET_ADSL_TO_EE_FFTP",
  "status": "HARDWARE_IN_TRANSIT_EXCEPTION",
  "statusDetails": "DPD-88219-UK marked DAMAGED_IN_TRANSIT. Replacement not yet dispatched.",
  "createdAt": "2026-10-18T09:00:00Z",
  "goLiveTarget": "2026-10-23T00:00:00Z"
}
```

### 3.4 Collection: `profiles` (Profile Hub — adds relationships graph)

Each profile carries a lightweight relationships array so Agent 2 can expand from the caller to related parties (co-residents, partners, guarantors, carers) in a single query. A related party may live at a *different* UPRN (e.g. a guarantor parent) and must still participate in hybrid search — see Section 6.4.

```json
{
  "_id": "prof_bt_55401",
  "firstName": "Priya",
  "lastName": "Shah",
  "brand": "BT",
  "uprn": "350018447722",
  "roleAtPremise": "ACCOUNT_HOLDER",
  "contact": { "phone": "+447700900777", "email": "p.shah@example.com" },
  "relationships": [
    { "profileId": "prof_bt_55402", "type": "GUARANTOR",
      "strength": 0.88, "atUprn": "350018447899" }
  ],
  "products": [
    { "serviceId": "srv_bt_bb_201", "category": "BROADBAND",
      "plan": "Full Fibre 500", "status": "PROVISIONING" }
  ],
  "consent": {
    "crossBrandLinkage": true,
    "lastReviewedAt": "2026-09-01T00:00:00Z"
  }
}
```

Notes:
- `relationships[].atUprn` lets Agent 2 collect related-party UPRNs for Agent 3's cross-UPRN hybrid filter.
- `consent.crossBrandLinkage` gates the household-level linkage (Section 10 Security & Compliance).

## 4. Atlas Search & Vector Search Index Definitions

Three indexes power the demo.

### 4.1 Vector index with GA auto-embedding — `vector_index_interactions`

Auto-embedding is **generally available** in Atlas. The index declares `type: "autoEmbed"` on the embedded field; summaries are embedded by Atlas on insert/update, and query text is embedded by Atlas at query time — no application-side embedding pipeline required. The Voyage AI API key is configured once at Atlas (mongot) deployment, not in the index definition.

```json
{
  "fields": [
    {
      "type": "autoEmbed",
      "modality": "text",
      "path": "interactionSummary",
      "model": "voyage-4-large",
      "numDimensions": 1024,
      "similarity": "cosine",
      "quantization": "scalar",
      "indexingMethod": "hnsw"
    },
    { "type": "filter", "path": "uprn" },
    { "type": "filter", "path": "brand" },
    { "type": "filter", "path": "createdAt" },
    { "type": "filter", "path": "intentCategory" }
  ]
}
```

Model choice:
- **`voyage-4-large`** (chosen): best retrieval quality on short conversational summaries; supports 256 / 512 / 1024 / 2048 dims (1024 default).
- `voyage-4` — balanced default; ~1.5x throughput of `voyage-4-large` at a small quality cost.
- `voyage-4-lite` — latency-first; ~2x throughput, best for p95 pressure.
- `voyage-code-4` — only if we extend indexing to engineer notes containing CLI/config fragments.

Query-time: for `autoEmbed` indexes, `$vectorSearch` accepts the utterance as the `query` field — see Section 5.2.

### 4.2 Atlas Search index (BM25) — `search_index_interactions`

Keyword/phrase retrieval over summaries and extracted entities. Essential for exact matches on order references, work-order IDs, and service IDs that spoken utterances often include verbatim.

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "interactionSummary": [
        { "type": "string", "analyzer": "lucene.english" }
      ],
      "entities.orderReferences": { "type": "token" },
      "entities.workOrderReferences": { "type": "token" },
      "entities.serviceReferences": { "type": "token" },
      "uprn": { "type": "token" },
      "brand": { "type": "token" },
      "createdAt": { "type": "date" }
    }
  }
}
```

### 4.3 Atlas Search index (autocomplete) — `search_index_premises`

Fallback for the unregistered-CLI path, letting Agent 1 resolve a UPRN from a spoken name + address.

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "address.thoroughfareName": [
        { "type": "autocomplete", "tokenization": "edgeGram", "minGrams": 3, "maxGrams": 15 }
      ],
      "address.postTown":   { "type": "string" },
      "address.postcode":   { "type": "token" },
      "uprn":               { "type": "token" }
    }
  }
}
```

## 5. Hybrid Search with Native `$rankFusion`

The IVR utterance is executed as a **single** aggregation that combines Atlas Search (BM25) and Atlas Vector Search via `$rankFusion`. Atlas runs the sub-pipelines, applies Reciprocal Rank Fusion (`rank_constant = 60`), applies the configured weights, and emits a unified ranking — no application-side blending code to maintain.

### 5.1 Why hybrid matters here

A real caller utterance mixes both styles of content:

> "Yeah, our broadband's still down, engineer was meant to be here for order **OR-2026-9921**."

- The exact token `OR-2026-9921` must land via **BM25** (vector search is weak at verbatim identifiers).
- The phrase "engineer was meant to be here" must land via **semantic** retrieval against prior summaries that said "field technician dispatched" or "Openreach booked for 23/10".

`$rankFusion` returns both in one call, deduplicated and ranked.

### 5.2 Pipeline

The hybrid filter accepts a **list of UPRNs** (household + related parties) so a related party's prior interactions are retrieved even when they live at a different premise. Sub-pipeline names are short (`semantic`, `keyword`) so they render legibly in `scoreDetails` traces shown to the agent.

```python
async def hybrid_interaction_search(
    uprns: list[str], utterance: str, limit: int = 5
):
    """Native MongoDB hybrid search via $rankFusion across one or more UPRNs."""
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
                                    "query": utterance,   # auto-embedded by Atlas (GA)
                                    "numCandidates": 100,
                                    "limit": 20,
                                    "filter": {
                                        "uprn": {"$in": uprns},
                                        "createdAt": {"$gte": seven_days_ago()}
                                    }
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
                                            {"range": {
                                                "path": "createdAt",
                                                "gte": seven_days_ago()
                                            }}
                                        ],
                                        "should": [
                                            {"text": {
                                                "path": "interactionSummary",
                                                "query": utterance
                                            }},
                                            {"text": {
                                                "path": [
                                                    "entities.orderReferences",
                                                    "entities.workOrderReferences",
                                                    "entities.serviceReferences"
                                                ],
                                                "query": utterance
                                            }}
                                        ]
                                    }
                                }
                            },
                            {"$limit": 20}
                        ]
                    }
                },
                "combination": {
                    "weights": {"semantic": 0.6, "keyword": 0.4}
                },
                "scoreDetails": True
            }
        },
        {"$limit": limit},
        {
            "$addFields": {
                "hybridScore": {"$meta": "score"},
                "scoreDetails": {"$meta": "scoreDetails"}
            }
        }
    ]
    return await db.interactions.aggregate(pipeline).to_list(length=limit)
```

Docs note: the `$rankFusion` reference examples place `{ "$limit": N }` after `$search` inside a sub-pipeline, as above. The companion "allowed sub-pipeline stages" page omits `$limit`; the reference example is the authoritative pattern. If a future server version rejects it, we move the cap into the outer `$limit`.

### 5.3 `scoreDetails` sample (Scenario 3)

With `scoreDetails: true`, each returned document carries the per-sub-pipeline rank and weight. The UI uses this to answer "why did this interaction appear?" — a credibility beat for technical reviewers.

```json
{
  "_id": "int_bt_voice_20261024_010",
  "interactionSummary": "Priya worried about install slot slipping; asked to be called back.",
  "hybridScore": 0.0612,
  "scoreDetails": {
    "value": 0.0612,
    "description": "value output by reciprocal rank fusion algorithm, computed as sum of weight * (1 / (60 + rank)) across input pipelines from which this document is output, from:",
    "details": [
      { "inputPipelineName": "keyword", "rank": 1, "weight": 0.4,
        "value": 0.00656, "description": "BM25 match on entities.workOrderReferences = OR-2026-10044" },
      { "inputPipelineName": "semantic", "rank": 2, "weight": 0.6,
        "value": 0.00968, "description": "cosine similarity on interactionSummary" }
    ]
  }
}
```

The agent desktop surfaces this as a two-line trace next to each hit (shown in Section 8).

## 6. Multi-Agent Coordination Engine

### 6.1 Structured IVR input

```python
from pydantic import BaseModel
from typing import Literal, Optional

class IvrContext(BaseModel):
    cli: Optional[str]                            # e.g. "+447700900123" (None if withheld)
    authState: Literal["AUTH", "UNAUTH"] = "UNAUTH"
    dtmfPath: list[str] = []                      # e.g. ["2","1","4"] (Broadband > Fault > Engineer)
    menuSelections: list[str] = []                # e.g. ["BROADBAND", "FAULT"]
    utterance: str                                # STT output
    receivedAt: str                               # ISO timestamp
```

### 6.2 Agent 1 — Triage & Identity Resolver

The AUTH-fast-path (verified in the IVR before transfer) skips the Atlas Search fallback entirely. Unauthenticated or withheld-CLI calls route through the fallback: an LLM extracts structured identity tokens from the utterance, which drive a boosted Atlas Search compound query against `search_index_premises`.

```python
async def agent_1_resolve_identity(ivr: IvrContext) -> dict:
    """CLI/AUTH match first; otherwise BM25 over names + addresses from utterance."""
    if ivr.cli and ivr.authState == "AUTH":
        caller = await db.profiles.find_one({"contact.phone": ivr.cli})
        if caller:
            return {"caller": caller, "confidence": 0.99, "method": "AUTH_CLI_MATCH"}

    if ivr.cli:  # unverified CLI — treat as a hint, not a trust anchor
        caller = await db.profiles.find_one({"contact.phone": ivr.cli})
        if caller:
            return {"caller": caller, "confidence": 0.85, "method": "CLI_HINT"}

    extracted = await llm_extract_identity(ivr.utterance)
    # extracted schema: {firstName, lastName, thoroughfare, postTown, postcode, brandHint}
    hits = await db.premises.aggregate([
        {"$search": {
            "index": "search_index_premises",
            "compound": {
                "should": [
                    {"autocomplete": {
                        "path": "address.thoroughfareName",
                        "query": extracted["thoroughfare"],
                        "score": {"boost": {"value": 3}}
                    }},
                    {"text": {
                        "path": "address.postcode",
                        "query": extracted["postcode"],
                        "score": {"boost": {"value": 5}}  # postcode fragments are gold
                    }},
                    {"text": {"path": "address.postTown",
                              "query": extracted["postTown"]}}
                ],
                "minimumShouldMatch": 1
            }
        }},
        {"$limit": 5}
    ]).to_list(5)

    return await disambiguate_profile(
        hits,
        full_name=f"{extracted['firstName']} {extracted['lastName']}",
        brand_hint=extracted.get("brandHint"),
    )


async def disambiguate_profile(
    premise_hits: list[dict], full_name: str, brand_hint: Optional[str]
) -> dict:
    """From a short list of premise candidates, resolve to a single profile+UPRN
    by name match (weighted 0.7) and brand hint (weighted 0.3). Returns
    {caller, confidence, method} with method='SEARCH_FALLBACK'."""
    ...
```

Scenario 2 (ambiguous address) resolves through this path: "27 Victoria Road, Leeds" matches multiple premises; the postcode fragment "LS1" and the brand hint "Plusnet" (extracted from "my old connection") boost UPRN `200039182341` to rank 1, and `disambiguate_profile` confirms via first-name match on "David".

### 6.3 Agent 2 — Premise & Household Resolver

```python
async def agent_2_expand_household(caller_profile: dict) -> dict:
    """Expand from caller → UPRN → co-residents → related parties.
    Returns both the household UPRN and the set of related-party UPRNs so
    Agent 3's hybrid search can retrieve interactions across all of them."""
    uprn = caller_profile["uprn"]
    co_residents = await db.profiles.find({"uprn": uprn}).to_list(length=20)

    related_ids = {
        rel["profileId"]
        for p in co_residents
        for rel in p.get("relationships", [])
        if rel.get("type") in {"PARTNER", "GUARANTOR", "CARER"}
    }
    co_resident_ids = {p["_id"] for p in co_residents}
    related_parties = await db.profiles.find(
        {"_id": {"$in": list(related_ids - co_resident_ids)}}
    ).to_list(length=10)

    related_uprns = {rp["uprn"] for rp in related_parties if rp.get("uprn")}

    return {
        "uprn": uprn,
        "household": co_residents,
        "relatedParties": related_parties,
        "searchUprns": [uprn, *sorted(related_uprns)],  # fed into Agent 3
    }
```

### 6.4 Agent 3 — Multi-Domain Context Retrieval

```python
async def agent_3_fetch_context(
    household_uprn: str, search_uprns: list[str], utterance: str
) -> dict:
    """Parallel fan-out: materialized dossier (household UPRN only) + hybrid
    search across household + related-party UPRNs + live ops."""
    dossier_task = db.customer_360.find_one({"_id": f"c360_{household_uprn}"})
    hybrid_task = hybrid_interaction_search(search_uprns, utterance, limit=5)
    live_ops_task = db.operations_wfm.find(
        {"uprn": {"$in": search_uprns}, "updatedAt": {"$gte": one_hour_ago()}}
    ).to_list(10)

    dossier, hybrid_hits, live_ops = await asyncio.gather(
        dossier_task, hybrid_task, live_ops_task
    )
    return {"dossier": dossier, "hybridHits": hybrid_hits, "liveOps": live_ops}
```

### 6.5 Agent 4 — Predictive Briefing (LLM)

Agent 4 is the only agentic stage. It receives the structured output of Agents 1–3 and emits a schema-constrained briefing. The LLM is given **no raw PII beyond what is already materialized in the dossier**; `rawTranscript` is never passed in. Every field in the output must cite the source interaction, work-order, or order ID, so the result is auditable.

```python
async def agent_4_predictive_briefing(ivr, identity, household, context) -> dict:
    """Grounded synthesis: driver, confidence, script, next-best-action, citations."""
    return await llm_briefing(
        ivr=ivr, identity=identity, household=household, context=context,
        schema=BriefingSchema,
    )
```

**`BriefingSchema` — the output contract.** The LLM must return a JSON object that validates against this schema; non-conforming output is rejected and retried once.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["callDriver", "confidence", "recommendedScript",
               "nextBestAction", "citations"],
  "properties": {
    "callDriver": {
      "type": "string",
      "description": "One-sentence inferred reason for the call."
    },
    "confidence": {
      "type": "number", "minimum": 0, "maximum": 1
    },
    "recommendedScript": {
      "type": "string",
      "description": "Opening line the agent can read verbatim."
    },
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
            "WARM_TRANSFER_TO_BRAND_SPECIALIST"
          ]
        },
        "description": {"type": "string"}
      }
    },
    "citations": {
      "type": "object",
      "properties": {
        "interactionIds": {"type": "array", "items": {"type": "string"}},
        "workOrderRefs": {"type": "array", "items": {"type": "string"}},
        "orderIds":      {"type": "array", "items": {"type": "string"}}
      }
    }
  }
}
```

### 6.6 Post-call write-back (closing the loop)

The brief emphasises "contacted BT the previous day." For the system to compound in value, the **current** call must also become a retrievable interaction the next time. After the agent wraps the call, a lightweight write-back persists a new `interactions` document; Atlas auto-embeds the summary and it participates in the next call's `$rankFusion`.

```python
async def post_call_write_back(call_id: str, ivr: IvrContext,
                               briefing: dict, agent_notes: str) -> None:
    await db.interactions.insert_one({
        "_id": f"int_voice_{call_id}",
        "uprn": briefing["householdUprn"],
        "profileId": briefing["callerProfileId"],
        "brand": briefing["brand"],
        "channel": "VOICE",
        "createdAt": now_utc(),
        "rawTranscript": None,   # stored separately under supervisor-only access
        "interactionSummary": briefing["recommendedScript"] + " " + agent_notes,
        "intentCategory": briefing["nextBestAction"]["code"],
        "entities": briefing["citations"],
        "status": "RESOLVED" if briefing.get("resolved") else "OPEN",
    })
```

This single write — plus `autoEmbed` — is what turns the system from a point-in-time pop into an **asset that grows with every call**.

### 6.7 Orchestration

```python
async def process_inbound_call(ivr: IvrContext) -> dict:
    identity  = await agent_1_resolve_identity(ivr)
    household = await agent_2_expand_household(identity["caller"])
    context   = await agent_3_fetch_context(
        household_uprn=household["uprn"],
        search_uprns=household["searchUprns"],
        utterance=ivr.utterance,
    )
    briefing  = await agent_4_predictive_briefing(ivr, identity, household, context)
    return {"identity": identity, "household": household, "context": context, "briefing": briefing}
```

LLM provider: pluggable; for BT's enterprise fit, Azure OpenAI or Anthropic on AWS Bedrock are both viable targets — the demo abstracts this behind a thin `llm_briefing` adapter.

## 7. End-to-End Demonstration Scenarios

### Scenario 1 — Multi-Brand Household Outage with Digital Drop-off

- **Caller:** Sarah Jenkins (EE Mobile `+447700900123`).
- **Premise:** Flat 2B, 14 Elmwood Terrace, Bristol BS1 5RR (UPRN `100021489211`).
- **Household:** Partner Mark Davis holds the BT Broadband account.
- **Pre-call:** 12:45 — Sarah dropped off the MyEE self-service flow at the landline-number prompt.
- **IVR utterance:** *"Hi, our home broadband is still completely down and the engineer was supposed to be here by now."*
- **Flow:**
  1. Agent 1: CLI resolves Sarah's EE profile in 1 ms.
  2. Agent 2: UPRN `100021489211` expands to Sarah + Mark via `relationships: PARTNER`.
  3. Agent 3 (parallel): C360 dossier (3 ms) + `$rankFusion` hybrid (semantic hit on Mark's webchat yesterday; keyword path idle — no identifiers in utterance) + live WFM check (Openreach `OR-2026-9921` revised ETA 13:45).
  4. Agent 4: synthesises the pop — "Call driver: delayed Openreach repair for partner's BT broadband. Confidence 97%."

### Scenario 2 — Legacy Migration Exception with Damaged Hardware (Unregistered CLI)

- **Caller:** David Patel (unregistered corporate `+447700900888`).
- **Premise:** 27 Victoria Road, Leeds LS1 4DP (UPRN `200039182341`).
- **Context:** Mid-migration Plusnet ADSL → EE Full Fibre; DPD-88219-UK marked `DAMAGED_IN_TRANSIT`.
- **IVR utterance:** *"Hello, my name is David Patel from 27 Victoria Road in Leeds. My old connection has gone off and your courier broke the new router."*
- **Flow:**
  1. Agent 1: CLI miss (no profile for `+447700900888`). LLM extracts `{firstName: "David", lastName: "Patel", thoroughfare: "Victoria Road", postTown: "Leeds", postcode: "LS1", brandHint: "PLUSNET"}` from "my old connection". Atlas Search `search_index_premises` returns six Victoria Road premises; the postcode fragment `LS1` boost (×5) lifts UPRN `200039182341` to rank 1. `disambiguate_profile` confirms via first-name match on "David" and brand hint "PLUSNET". Method `SEARCH_FALLBACK`, confidence 0.91.
  2. Agent 2: single-profile household; no related parties; `searchUprns = [200039182341]`.
  3. Agent 3: C360 dossier shows the open `orders` entry with the DPD `HARDWARE_IN_TRANSIT_EXCEPTION`; hybrid search surfaces the prior migration-window webchat (semantic hit on "install going live tomorrow").
  4. Agent 4: emits `nextBestAction.code = DISPATCH_REPLACEMENT_HARDWARE`, citing `orderIds: [ord_ee_fibre_55120]` and `interactionIds: [...]`. Script apologises for the courier damage and offers an EE Emergency Data Booster to bridge the gap.

### Scenario 3 — Appointment-Centric Call Citing an Order Reference (showcases `$rankFusion` end-to-end)

- **Caller:** Priya Shah (authenticated BT customer `+447700900777`).
- **Premise:** 48 Hanover Street, Edinburgh EH2 1EL (UPRN `350018447722`).
- **Context:** New-provide FTTP install scheduled this morning; parent (guarantor) also on file.
- **IVR utterance:** *"Hi, I'm calling about my install today, the reference is **OR-2026-10044**, no one has turned up yet."*
- **Flow:**
  1. Agent 1: CLI match in 1 ms.
  2. Agent 2: UPRN expands to Priya + guarantor relationship (parent).
  3. Agent 3: `$rankFusion` — the keyword sub-pipeline (BM25) lands `OR-2026-10044` as rank 1; the semantic sub-pipeline lands a prior "worried about install" interaction. Both are fused into a single ranked list; `scoreDetails` lets the UI explain *why* each hit appeared.
  4. Agent 4: pop confirms live WFM slot ("Engineer en route, ETA 11:20") and offers the £10 missed-appointment credit if the SLA slips.

## 8. Web-Based Agent Desktop UI

Split-screen console with a WebSocket channel for live WFM/interaction updates mid-call.

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  BT GROUP UNIFIED AGENT ASSISTANT                                        Agent: Exeter Contact Ctr │
├─────────────────────────────────────────┬──────────────────────────────────────────────────────────┤
│  IVR SIMULATOR & CONTROL PANEL          │  PREDICTIVE SCREEN POP  (Call connected: Inbound)        │
│                                         │                                                          │
│  Scenario:                              │  [ CALLER IDENTIFIED ]                                   │
│  [ Scenario 1: Sarah Jenkins (EE/BT) v ]│  Sarah Jenkins | +44 7700 900123 (EE Mobile: Active)     │
│                                         │  Premise: Flat 2B, 14 Elmwood Terrace, Bristol BS1 5RR   │
│  Utterance:                             ├──────────────────────────────────────────────────────────┤
│  "Our home broadband is still down..."  │  [ PREDICTIVE CALL DRIVER ]  (Confidence 97%)            │
│                                         │  Delayed Openreach repair for partner's BT broadband     │
│  [ Trigger Inbound Call ]               ├──────────────────────────────────────────────────────────┤
│                                         │  [ UNIFIED HOUSEHOLD TIMELINE (last 7 days) ]            │
│  Latency Telemetry:          p50 / p95  │  22 Oct 16:15  Mark · Webchat   LOS fault raised         │
│    CLI + C360 fetch     3  /  7    ms   │  22 Oct 17:40  WFM             Openreach booked 23/10    │
│    $rankFusion hybrid 230  / 380   ms   │  23 Oct 08:00  WFM             Engineer dispatched        │
│    LLM briefing       410  / 720   ms   │  23 Oct 12:45  Sarah · MyEE    Drop-off at line check    │
│    WebSocket push      20  /  35   ms   │  23 Oct 13:00  WFM             DELAYED — new ETA 13:45   │
│  Total                663  / 1142  ms   │  23 Oct 13:00  Call · Sarah    INBOUND (current)         │
│                                         ├──────────────────────────────────────────────────────────┤
│  Hybrid Search Trace:                   │  [ RECOMMENDED SCRIPT ]                                  │
│   semantic rank 1  w=0.60               │  "Hi Sarah — are you calling about the broadband outage │
│   keyword rank  3  w=0.40               │   at Elmwood Terrace? I can see Mark raised this with   │
│   fused score    0.0612                 │   us yesterday. The engineer was delayed on a prior job;│
│                                         │   their revised ETA is 13:45."                           │
│                                         ├──────────────────────────────────────────────────────────┤
│                                         │  [ HOUSEHOLD 360 DOSSIER ]                               │
│                                         │  Occupants:  Mark (BT), Sarah (EE) · PARTNER (0.92)      │
│                                         │  Equipment:  Smart Hub 2 · Optical LOS                   │
│                                         │  Opportunity: Converged household eligible               │
│                                         │  Action:      Offer EE One bundle (£10/mo bb discount)   │
└─────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

The ASCII above is a layout intent only. The running agent desktop lives in `app/static/` (vanilla HTML + CSS + JS, served by FastAPI) and is the authoritative source for the UI — treat it as the design.

## 9. Data Generator & Seeding (`data/seed_database.py`)

- **Volume:** 150 premises, 350 profiles (45% BT, 40% EE, 15% Plusnet), 600 interactions, 80 WFM records, 40 orders.
- **Relationships:** `profiles.relationships` populated with partner / guarantor / carer links at realistic rates (~35% of adult pairs at the same UPRN marked PARTNER).
- **Vector population:** Atlas auto-embeds `interactionSummary` on insert — no application-side embedding code.
- **Deterministic anchors:** scenario characters (Sarah, Mark, David, Priya) and references (OR-2026-9921, OR-2026-10044, DPD-88219-UK) are pinned.

## 10. Security, Privacy & Compliance

Because the demo links identities across BT, EE, and Plusnet brands at a shared premise, GDPR considerations are front-of-mind for a BT audience.

- **Lawful basis:** Household linkage for the agent-assist path operates under legitimate interest (service delivery) and is scoped to in-contact calls; linkage is logged, auditable, and revocable per data subject.
- **PII minimisation:** Only `interactionSummary` is vector-embedded. `rawTranscript` is retained for compliance only and is not in any search index.
- **Encryption:** Atlas encryption at rest and in transit by default; Queryable Encryption is a candidate for phone numbers in `profiles.contact.phone`.
- **Access control:** Atlas RBAC segregates agent, supervisor, and auditor roles. The `rawTranscript` field is excluded from the agent-desktop projection by default and only returned on supervisor escalation.
- **Audit:** every `customer_360` fetch, `$rankFusion` hit, and briefing synthesis is written to an append-only `agent_assist_audit` collection keyed by call ID.

## 11. Evaluation Plan

Credibility hinges on measurable retrieval and briefing quality.

- **Offline retrieval eval:** replay 2,000 labelled historical calls — compute Recall@5 and MRR for the hybrid pipeline, broken down by sub-pipeline (semantic-only, keyword-only, fused) to prove the hybrid lift.
- **Briefing quality:** blind pairwise comparison between the LLM briefing and a 3-agent human baseline, scored on *actionability* and *factual correctness*; schema-validation rate on the raw LLM output (target ≥ 99%).
- **Latency SLO:** p50 ≤ 700 ms, p95 ≤ 1200 ms over 10k synthetic calls at seeded data scale (see Section 8 for the live breakdown).
- **Intent precision:** confusion matrix on the inferred `callDriver` / `nextBestAction.code` label vs the agent-tagged ground-truth disposition code.

### 11.1 Cost sizing (back-of-envelope, demo scale → BT scale)

| Component                     | Demo (seeded)         | BT scale assumption               | Monthly cost indication        |
|-------------------------------|-----------------------|-----------------------------------|--------------------------------|
| Voyage AI embed (write path)  | 600 interactions once | 2 M new interactions / month      | ~£1–2k at `voyage-4-large`     |
| Voyage AI embed (query path)  | one per call          | 1 M calls / month                 | ~£500 (short utterances)       |
| Atlas Search + Vector Search  | M10 shared            | M40–M60 dedicated with search     | ~£3–5k incl. search nodes      |
| LLM briefing (Agent 4)        | one per call          | 1 M calls × ~600 tokens in/out    | ~£3–8k depending on model      |
| **Indicative total**          |                       |                                   | **~£8–15k / month**            |

Figures are rough and purely for stakeholder sizing — final numbers require BT call-volume and retention policy inputs.

## 12. Production Hardening

Beyond demo scope but called out for stakeholder credibility:

- **Change-stream resumability:** resume tokens persisted per collection (shown in Section 2.1); on worker restart, no events are lost.
- **Idempotent writes:** materializer uses `find_one_and_replace(upsert=True)`; replays are safe.
- **Backpressure:** per-UPRN debounce queue (Section 2.1) prevents hot-key storms during migration events.
- **DLQ:** `c360_dlq` captures rebuild failures for operator review.
- **Incremental updates:** hot fields (WFM status) can switch from full rebuild to `$set` for sub-50 ms propagation.
- **Observability:** OpenTelemetry traces from IVR → agents → MongoDB; `scoreDetails` surfaces hybrid-rank explanations in the UI and in logs.

## 13. Why MongoDB — Fuller Comparison

| Capability                 | Elastic + Postgres + Redis   | Graph DB (Neo4j)         | MongoDB Atlas ODL              |
|----------------------------|------------------------------|--------------------------|--------------------------------|
| Household linkage          | Join across stores at query  | First-class, strongest   | Flexible embedded + ref model  |
| Keyword & semantic search  | Two systems, two pipelines   | External search add-on   | Single platform, `$rankFusion` |
| Auto-embedding             | DIY embedding service        | DIY                      | **GA** with Voyage AI          |
| Real-time materialization  | Debezium + Kafka + Redis     | Trigger plugins          | Native change streams          |
| Document flexibility       | Schema per store             | Node/edge rigidity       | Native (brand-specific fields) |
| BT incumbent footprint     | Partial                      | None                     | Profile Hub + Service Hub      |
| Vendor count on hot path   | 3+                           | 2+                       | 1                              |

## 14. Appendix — Helper signatures & artefacts

Function stubs referenced throughout the spec. Full implementations live in the demo repo.

```python
from datetime import datetime, timedelta, timezone

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def seven_days_ago() -> datetime:
    return now_utc() - timedelta(days=7)

def one_hour_ago() -> datetime:
    return now_utc() - timedelta(hours=1)


class Settings:
    recent_window_days: int = 7
    def recent_window_start(self) -> datetime:
        return now_utc() - timedelta(days=self.recent_window_days)

settings = Settings()


async def llm_extract_identity(utterance: str) -> dict:
    """Returns {firstName, lastName, thoroughfare, postTown, postcode, brandHint}
    using a small, schema-constrained LLM call. Any field may be None."""
    ...


async def llm_briefing(*, ivr, identity, household, context, schema) -> dict:
    """Schema-constrained LLM call. Raises on validation failure; caller retries once."""
    ...
```

**Agent desktop.** The split-screen desktop (Section 8) is implemented in `app/static/` and served by FastAPI at `/`. The running page is the design — no separate mock to keep in sync.

---

The executive-facing narrative, speaker tracks, and slide-by-slide walkthrough live in
`BTContactCentreDemo-ExecDeck.md` alongside this spec.
