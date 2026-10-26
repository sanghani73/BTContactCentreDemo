# BT Next-Gen Contact Centre — Multi-Agent Operational Customer 360 Demo

A runnable prototype of the architecture described in
`BTContactCentreDemo-Spec.md`. Narration script for presenters is in
`demo_script.md`; a reveal.js pitch deck lives at
`app/static/deck.html` (served at `/static/deck.html` or via the
**Open pitch deck ↗** link in the agent desktop header).

## What it does

Simulates an inbound call to a BT contact centre and renders an operator
screen pop before the call is answered:

- **Four coordinated stages** — Agent 1 (identity resolver) → Agent 2
  (household expansion incl. related parties) → Agent 3 (parallel context
  fan-out via `$rankFusion`) → Agent 4 (AI-generated briefing). Agents 1–3
  are deterministic MongoDB queries; Agent 4 is a schema-constrained LLM
  call.
- **Progressive streaming UI** — Server-Sent Events deliver each stage's
  result as it completes. The caller card, timeline, dossier and hybrid-
  search trace appear in &lt; 1 s; the recommended script streams in
  token-by-token with a live cursor.
- **Native hybrid search** via `$rankFusion` combining Atlas Vector Search
  (Voyage AI `voyage-4-large` auto-embedding, GA) with Atlas Search (BM25
  over summaries + entity tokens). `scoreDetails` surfaces *why* each hit
  was retrieved.
- **Materialized `customer_360`** maintained by MongoDB change streams
  across `profiles`, `interactions`, `operations_wfm`, `orders`. Single-key
  fetch at call-connect.
- **Post-call write-back closes the loop** — the current call persists as
  a new `interactions` document, auto-embedded by Atlas on insert, so
  tomorrow's call retrieves this one.

## Modes

| Mode              | Trigger                              | What runs                             |
|-------------------|--------------------------------------|---------------------------------------|
| **Mock (zero-dep)** | `.env` has no `MONGO_URI`            | In-memory canned data, local RRF, mock briefing |
| **Live Atlas**    | `.env` has `MONGO_URI` + Grove vars  | Real `$rankFusion`, real auto-embedding, Grove-hosted LLM |

Mock mode lets you demo end-to-end on a laptop with zero credentials. The
code path is identical; the mock just intercepts data access and swaps
the LLM for a deterministic per-scenario briefing.

## Quickstart

```sh
# 1. Virtualenv
python -m venv .venv && source .venv/bin/activate
pip install -e .

# 2. Config (optional — omit = mock mode)
cp .env.example .env
# edit .env — see "Running against Atlas + Grove" below

# 3. Run
./scripts/run_dev.sh
# -> http://localhost:8080
```

## Running against Atlas + Grove

Three LLM env vars. `LLM_BASE_URL` is the **full endpoint**, including
`/v1/messages`:

```sh
# .env
MONGO_URI="mongodb+srv://<user>:<pass>@<cluster>/?retryWrites=true&w=majority"
MONGO_DB="bt_contact_centre"

LLM_BASE_URL="https://ai-gateway.corp.mongodb.com/anthropic/v1/messages"
LLM_API_KEY="<your-grove-key>"
LLM_MODEL="claude-haiku-4-5"
```

The adapter POSTs an Anthropic Messages payload (`tool_use` + `stream:
true`) to that URL. Both `Authorization: Bearer` and `x-api-key` headers
are sent so Grove's ingress can accept either. Streaming responses are
read via `httpx.AsyncClient.stream()` and the partial-JSON deltas are
forwarded to the browser as `briefing_delta` SSE events.

### Seed and index

One command handles everything:

```sh
python -m data.seed_database --drop --volume 500
```

What it does, in order:

1. Drops the six collections (removing their search indexes along with them).
2. Inserts the three scenario anchors + synthetic household volume.
3. Materialises `customer_360` for every seeded premise.
4. Creates the **compound indexes** (B-tree) listed below.
5. Creates the Atlas Search / Vector Search indexes defined in `data/atlas/`.

Then start the server:

```sh
./scripts/run_dev.sh
```

The mode pill in the agent desktop header will switch from
`mock-mongo · mock-llm` to `atlas · grove:<model>`.

### Compound indexes

Added by `app/services/atlas_indexes.py`. All three follow the ESR rule
(Equality, Sort, Range) and are visible in MongoDB Compass under each
collection's Indexes tab.

| Collection       | Index                                             | Supports                                            |
|------------------|---------------------------------------------------|-----------------------------------------------------|
| `interactions`   | `{ uprn: 1, createdAt: -1 }`                      | Materializer's recent-interactions sort             |
| `operations_wfm` | `{ uprn: 1, updatedAt: -1 }`                      | Agent 3's live-ops lookup at call-connect           |
| `orders`         | `{ uprn: 1, status: 1, createdAt: -1 }`           | Materializer's open-orders projection               |

### Atlas Search / Vector Search indexes

Defined as JSON in `data/atlas/`:

| Index                        | Collection      | Type           |
|------------------------------|-----------------|----------------|
| `vector_index_interactions`  | `interactions`  | `vectorSearch` with `type: autoEmbed`, `model: voyage-4-large`, cosine, HNSW |
| `search_index_interactions`  | `interactions`  | `search` — BM25 over `interactionSummary` + token fields on `entities.*` |
| `search_index_premises`      | `premises`      | `search` — autocomplete on `address.thoroughfareName`, token on `postcode`, string on `postTown` |

### Cluster prerequisites

| Capability                            | Required                                            |
|---------------------------------------|-----------------------------------------------------|
| `$rankFusion` aggregation operator    | MongoDB **8.0+**                                    |
| Atlas Vector Search                   | Search nodes enabled on the cluster                 |
| `type: autoEmbed` + `voyage-4-large`  | Voyage AI integration enabled on the Atlas project (Project Settings → Integrations → Voyage AI) |

The Voyage AI key is configured on the Atlas cluster (mongot deployment)
via the Atlas UI — **not** in this app's `.env`. The `autoEmbed` field
type uses it server-side on every write and every `$vectorSearch` query.
If you see "model not available" errors on index build, that integration
is the thing to check.

### Standalone index management

If you want to re-create missing indexes without touching data:

```sh
python scripts/create_indexes.py
```

Or seed without touching indexes:

```sh
python -m data.seed_database --drop --volume 500 --skip-indexes
```

### Verifying each piece

```sh
# Live Mongo reachable?
curl -s localhost:8080/healthz          # mockMode should be false

# Live LLM configured?
curl -s localhost:8080/api/scenarios    # mockLlm should be false

# End-to-end on Scenario 3 (hits both $rankFusion sub-pipelines)
curl -s -X POST localhost:8080/api/scenarios/priya/run | jq .briefing
```

If `$rankFusion` errors on disallowed stages → cluster is below MongoDB 8.0.
If `autoEmbed` fails on index build → Voyage AI integration isn't enabled.

## Agent desktop

Open `http://localhost:8080`. The split-screen console contains:

- **Left**: IVR simulator (scenario dropdown + editable utterance),
  latency telemetry (per-stage ms incl. `agent4_first_token_ms` for
  perceived latency), live hybrid-search trace with `scoreDetails`.
- **Right**: six result cards — caller, predictive call driver + script,
  unified household timeline, dossier, post-call write-back status.
- **Header**: mode pill (atlas / grove / mock), "Open pitch deck ↗" link
  that opens the reveal.js deck in a new tab.

Each result card has a small **?** button in its top-right corner. Click
it to open a speech-bubble callout explaining the operation and the
collection populating that card (incl. the compound index backing it,
where relevant). Only one bubble is open at a time; **×** or Esc
dismisses it. Use these during narration without hiding the result
beneath them.

## Scenarios

Three deterministic scenarios anchor the demo:

1. **Sarah Jenkins (EE/BT household outage)** — CLI match, household
   expansion resolves her BT partner Mark; hybrid search semantically
   lands yesterday's webchat; delayed Openreach work order drives
   `APOLOGISE_AND_UPDATE_ETA` + the EE One convergence recommendation.
2. **David Patel (unregistered CLI, damaged migration)** — CLI miss →
   Atlas Search fallback on `premises` with postcode + brand-hint boost;
   open order with `HARDWARE_IN_TRANSIT_EXCEPTION` drives
   `DISPATCH_REPLACEMENT_HARDWARE`.
3. **Priya Shah (authenticated, cites OR-2026-10044)** — AUTH-fast-path
   identity; `$rankFusion`'s keyword sub-pipeline lands the work-order
   reference verbatim at rank 1 while the semantic sub-pipeline surfaces
   a prior reassurance call.

Change the utterance text in the UI to see the retrieval behaviour
change — the pipeline runs against whatever you type.

## Repo layout

```
app/
  config.py              Settings (mongo + grove + mock flags)
  database.py            Lazy Motor client
  models.py              IvrContext, Briefing (BriefingSchema), Citations
  agents/
    agent1_identity.py   CLI + Atlas Search fallback with disambiguation
    agent2_household.py  UPRN expansion + related-party resolution
    agent3_context.py    Parallel: dossier + $rankFusion + live WFM
    agent4_briefing.py   LLM-driven briefing
    orchestrator.py      Non-streaming + streaming (SSE) end-to-end
  services/
    hybrid_search.py     Native $rankFusion pipeline + local-RRF mock
    llm.py               Grove adapter (streaming + non-streaming) + mock
    writeback.py         Post-call interaction write-back
    atlas_indexes.py     Compound + search index helpers (idempotent)
    mock_store.py        Canned scenario data for zero-dep mode
  api/calls.py           /api/calls/inbound  (streaming + non-streaming)
                         /api/scenarios/{id}/run
  static/
    index.html           Agent desktop (vanilla JS, SSE over fetch)
    style.css            MongoDB-themed dark UI, per-card ? bubbles
    app.js               Streaming SSE client, partial-JSON extractor
    deck.html            reveal.js pitch deck (self-contained)
data/
  atlas/*.json           Atlas Search / Vector Search index definitions
  seed_database.py       Seeder (--drop, --volume, --skip-indexes)
scripts/
  create_indexes.py      Standalone index creator (idempotent)
  run_dev.sh             uvicorn --reload entrypoint
BTContactCentreDemo-Spec.md    Full technical specification
```

## Mock-mode fallback

Just run the server with no `.env`:

```sh
./scripts/run_dev.sh
```

The UI shows a `mock-mongo · mock-llm` pill in the top-right. The three
scenarios return deterministic, schema-conforming briefings so the demo
runs end-to-end — hybrid search, timeline, dossier, writeback and all —
without needing an Atlas cluster or a Grove key.
