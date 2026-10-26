"""Populate MongoDB with scenario anchors + synthetic volume for the demo.

Usage:
    python -m data.seed_database --drop
    python -m data.seed_database --volume 350

Requires MONGO_URI in the environment. For mock mode no seeding is needed —
the in-memory store is already populated.
"""
from __future__ import annotations

import argparse
import asyncio
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import motor.motor_asyncio
except ImportError:
    print("motor is not installed. Run: pip install -e .", file=sys.stderr)
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.config import settings  # noqa: E402
from app.services import mock_store  # noqa: E402
from app.services.atlas_indexes import ensure_indexes  # noqa: E402


BRANDS = ["BT", "EE", "PLUSNET"]
BRAND_WEIGHTS = [0.45, 0.40, 0.15]
STREETS = ["Elm Road", "Victoria Road", "High Street", "Church Lane",
           "Kings Avenue", "Rose Crescent", "Hanover Street", "Mill Way"]
TOWNS = [("Bristol", "BS"), ("Leeds", "LS"), ("Edinburgh", "EH"),
         ("Manchester", "M"), ("Birmingham", "B"), ("Glasgow", "G")]
FIRST_NAMES = ["Alex", "Chris", "Jamie", "Taylor", "Jordan", "Morgan", "Robin",
               "Riley", "Casey", "Drew", "Harper", "Quinn"]
LAST_NAMES = ["Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson",
              "Johnson", "Walker", "Robinson", "Wright"]


def _rand_uprn(seed: int) -> str:
    return str(100000000000 + seed * 10007)


def _rand_phone(seed: int) -> str:
    return f"+4477009{seed:05d}"


def _rand_postcode(prefix: str, seed: int) -> str:
    return f"{prefix}{(seed % 20) + 1} {(seed % 9) + 1}{random.choice('ABDEF')}{random.choice('AEHJ')}"


INTERACTION_SUMMARIES = [
    "Customer reported intermittent broadband drops in evenings.",
    "Customer requested bill explanation; no fault raised.",
    "Customer confirmed engineer visit went ahead; line now stable.",
    "Customer called about mobile data not working after SIM swap.",
    "Caller asked about upgrading from FTTC to FTTP at their address.",
    "Customer wanted to add BT TV package to existing broadband line.",
    "Caller reported dropped mobile calls in a specific postcode area.",
    "Customer flagged an unexpected charge on their last monthly bill.",
    "Caller mentioned their smart hub shows a steady orange light.",
    "Customer confirmed receipt of replacement router; now up and running.",
    "Caller asked about moving services to a new address next month.",
    "Customer requested cancellation of an unused mobile line.",
    "Caller reported speed test well below their guaranteed minimum.",
    "Customer asked about EE One convergence bundle eligibility.",
    "Caller flagged a missed appointment and requested a credit.",
]

WFM_TYPES = [
    "SPLICING_AND_ONT_REPAIR", "NEW_PROVIDE_FTTP", "FAULT_REPAIR",
    "CEASE_LINE", "SPEED_UPGRADE", "ONT_SWAP", "LINE_TEST",
]
WFM_STATUSES = [
    "COMPLETED", "COMPLETED", "COMPLETED",     # weight toward completed
    "DELAYED", "IN_TRANSIT", "DISPATCHED", "PENDING", "SCHEDULED",
]

ORDER_TYPES = [
    "NEW_PROVIDE_FTTP", "MIGRATION_PLUSNET_ADSL_TO_EE_FFTP",
    "SPEED_UPGRADE", "MOBILE_UPGRADE", "TV_ADDON", "MOBILE_PORT_IN",
]
ORDER_STATUSES = [
    "COMPLETED", "COMPLETED",                   # weight toward completed
    "PROVISIONING", "ENGINEER_SCHEDULED", "SHIPPED", "DELIVERED",
    "HARDWARE_IN_TRANSIT_EXCEPTION", "CANCELLED",
]


def build_synthetic(volume: int) -> dict[str, list[dict]]:
    """Generate `volume` premises plus proportional profiles, interactions,
    field-service work orders and customer orders.

    At the default volume of 500 this yields roughly:
      - 500   premises
      - 1200  profiles (avg 2.4 per premise, ~35% paired)
      - 5000  interactions (~10 per premise, last 60 days)
      - 400   operations_wfm rows (~30% of premises)
      - 250   orders (~50% of premises, mix of statuses)
    """
    random.seed(42)
    now = datetime.now(timezone.utc)
    premises, profiles, interactions, operations, orders = [], [], [], [], []

    for i in range(volume):
        uprn = _rand_uprn(100 + i)
        town, pc_prefix = random.choice(TOWNS)
        premises.append({
            "uprn": uprn,
            "address": {
                "buildingNumber": str(random.randint(1, 200)),
                "thoroughfareName": random.choice(STREETS),
                "postTown": town,
                "postcode": _rand_postcode(pc_prefix, i),
            },
            "networkInfrastructure": {
                "exchangeCode": f"E-{town[:4].upper()}-{(i % 20):02d}",
                "technology": random.choice(["FTTP", "FTTC"]),
                "maxAvailableSpeed": "1000Mbps",
                "lineStatus": random.choice(
                    ["HEALTHY", "DEGRADED", "OUTAGE_DETECTED"]
                ),
            },
            "updatedAt": now - timedelta(days=random.randint(0, 30)),
        })

        occupants = random.choices([1, 2, 3, 4], weights=[0.3, 0.45, 0.2, 0.05], k=1)[0]
        occupant_profiles = []
        for j in range(occupants):
            brand = random.choices(BRANDS, weights=BRAND_WEIGHTS, k=1)[0]
            pid = f"prof_{brand.lower()}_{i:04d}_{j}"
            occupant_profiles.append({
                "_id": pid,
                "firstName": random.choice(FIRST_NAMES),
                "lastName": random.choice(LAST_NAMES),
                "brand": brand,
                "uprn": uprn,
                "roleAtPremise": "ACCOUNT_HOLDER" if j == 0 else "RESIDENT",
                "contact": {"phone": _rand_phone(i * 10 + j)},
                "relationships": [],
                "products": [{
                    "serviceId": f"srv_{brand.lower()}_{i:04d}_{j}",
                    "category": random.choice(["BROADBAND", "MOBILE", "TV"]),
                    "plan": "Standard",
                    "status": random.choice(["ACTIVE", "PROVISIONING", "FAULT_REPORTED"]),
                }],
                "consent": {"crossBrandLinkage": True,
                            "lastReviewedAt": now - timedelta(days=90)},
            })
        # Pair the first two occupants as PARTNERs ~35% of the time.
        if len(occupant_profiles) >= 2 and random.random() < 0.35:
            a, b = occupant_profiles[0], occupant_profiles[1]
            a["relationships"].append({"profileId": b["_id"], "type": "PARTNER",
                                       "strength": 0.9, "atUprn": uprn})
            b["relationships"].append({"profileId": a["_id"], "type": "PARTNER",
                                       "strength": 0.9, "atUprn": uprn})
        profiles.extend(occupant_profiles)

        # Interactions — avg ~10 per premise across the last 60 days.
        for _ in range(random.randint(5, 15)):
            prof = random.choice(occupant_profiles)
            created = now - timedelta(days=random.randint(0, 60),
                                      hours=random.randint(0, 23))
            interactions.append({
                "_id": f"int_syn_{i:04d}_{random.randint(1000, 999999)}",
                "uprn": uprn,
                "profileId": prof["_id"],
                "brand": prof["brand"],
                "channel": random.choice(["VOICE", "WEBCHAT", "MOBILE_APP"]),
                "createdAt": created,
                "rawTranscript": None,
                "interactionSummary": random.choice(INTERACTION_SUMMARIES),
                "intentCategory": "GENERAL_QUERY",
                "entities": {"serviceReferences": [], "orderReferences": [],
                             "workOrderReferences": []},
                "status": "RESOLVED",
            })

        # Field-service work orders — ~30% of premises have 1-2 entries.
        if random.random() < 0.3:
            for _ in range(random.randint(1, 2)):
                created = now - timedelta(days=random.randint(0, 30),
                                          hours=random.randint(0, 23))
                updated = created + timedelta(hours=random.randint(0, 72))
                if updated > now:
                    updated = now
                operations.append({
                    "_id": f"wfm_syn_{i:04d}_{random.randint(1000, 999999)}",
                    "uprn": uprn,
                    "workOrderType": random.choice(WFM_TYPES),
                    "provider": "OPENREACH",
                    "reference": f"OR-2026-{random.randint(10000, 99999)}",
                    "status": random.choice(WFM_STATUSES),
                    "statusDetails": "Field technician dispatched.",
                    "scheduledWindow": {
                        "start": created + timedelta(days=1),
                        "end": created + timedelta(days=1, hours=4),
                    },
                    "createdAt": created,
                    "updatedAt": updated,
                })

        # Orders — ~50% of premises have one order, mix of statuses.
        if random.random() < 0.5:
            prof = random.choice(occupant_profiles)
            created = now - timedelta(days=random.randint(0, 90))
            orders.append({
                "_id": f"ord_syn_{i:04d}_{random.randint(1000, 999999)}",
                "uprn": uprn,
                "profileId": prof["_id"],
                "brand": prof["brand"],
                "orderType": random.choice(ORDER_TYPES),
                "status": random.choice(ORDER_STATUSES),
                "statusDetails": "In progress.",
                "createdAt": created,
                "goLiveTarget": created + timedelta(days=random.randint(3, 14)),
            })

    return {
        "premises": premises,
        "profiles": profiles,
        "interactions": interactions,
        "operations_wfm": operations,
        "orders": orders,
    }


ANCHORS = {
    "premises": mock_store.PREMISES,
    "profiles": mock_store.PROFILES,
    "interactions": mock_store.INTERACTIONS,
    "operations_wfm": mock_store.OPERATIONS_WFM,
    "orders": mock_store.ORDERS,
}


async def seed(drop: bool, volume: int, skip_indexes: bool = False) -> None:
    if not settings.mongo_uri:
        print("MONGO_URI is not set — nothing to seed. Mock mode uses the "
              "in-memory store automatically.", file=sys.stderr)
        sys.exit(1)

    client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongo_uri)
    db = client[settings.mongo_db]

    collections = ["premises", "profiles", "interactions",
                   "operations_wfm", "orders", "customer_360", "cdc_state"]
    if drop:
        for c in collections:
            await db[c].drop()
            print(f"dropped {c}")

    synth = build_synthetic(volume=volume)

    # Combine synthetic + scenario-anchor docs per collection. Anchors always
    # win on _id/uprn collisions so the pinned scenarios stay deterministic.
    collections = sorted(set(synth.keys()) | set(ANCHORS.keys()))
    combined: dict[str, list[dict]] = {}
    for coll in collections:
        synth_docs = synth.get(coll, []) or []
        anchor_docs = ANCHORS.get(coll, []) or []
        anchor_ids = {d["_id"] for d in anchor_docs if "_id" in d}
        anchor_uprns = {d.get("uprn") for d in anchor_docs if d.get("uprn")}
        filtered_synth = [
            d for d in synth_docs
            if d.get("_id") not in anchor_ids
            and (coll != "premises" or d.get("uprn") not in anchor_uprns)
        ]
        combined[coll] = filtered_synth + anchor_docs

    for coll, docs in combined.items():
        if not docs:
            continue
        # Make reseeds idempotent: wipe any existing anchor docs by _id/uprn.
        anchor_ids = [d["_id"] for d in ANCHORS.get(coll, []) if "_id" in d]
        if anchor_ids:
            await db[coll].delete_many({"_id": {"$in": anchor_ids}})
        if coll == "premises":
            anchor_uprns = [p["uprn"] for p in ANCHORS.get(coll, [])]
            if anchor_uprns:
                await db[coll].delete_many({"uprn": {"$in": anchor_uprns}})

        await db[coll].insert_many(docs)
        print(f"seeded {len(docs):>5} into {coll}")

    # Materialize c360 for all seeded premises (demo-time one-shot).
    uprns = [p["uprn"] for p in await db.premises.find({}, {"uprn": 1}).to_list(10_000)]
    print(f"materializing customer_360 for {len(uprns)} premises...")
    for uprn in uprns:
        doc = mock_store._build_c360(uprn)  # mock builder works on in-memory data
        if doc is None:
            # Build directly from DB for non-anchor premises
            premise = await db.premises.find_one({"uprn": uprn})
            profiles = await db.profiles.find({"uprn": uprn}).to_list(20)
            if not premise:
                continue
            doc = {
                "_id": f"c360_{uprn}",
                "uprn": uprn,
                "address": premise.get("address"),
                "networkInfrastructure": premise.get("networkInfrastructure"),
                "householdMembers": [
                    {"profileId": p["_id"],
                     "name": f"{p['firstName']} {p['lastName']}",
                     "brand": p["brand"],
                     "role": p.get("roleAtPremise", "RESIDENT"),
                     "phone": p.get("contact", {}).get("phone"),
                     "relationships": p.get("relationships", []),
                     "services": p.get("products", [])}
                    for p in profiles
                ],
                "activeOperations": [],
                "openOrders": [],
                "recentInteractionsSummary": [],
                "commercialInsights": {"convergedHousehold": False,
                                       "nextBestAction": "STANDALONE_RETAIN",
                                       "eligibleDiscounts": []},
                "updatedAt": premise.get("updatedAt"),
            }
        await db.customer_360.replace_one({"_id": doc["_id"]}, doc, upsert=True)
    print("done seeding")

    if skip_indexes:
        print("--skip-indexes set; search indexes not touched")
    else:
        print("ensuring Atlas Search / Vector Search indexes...")
        await ensure_indexes(db)

    client.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--drop", action="store_true",
                   help="drop collections first (search indexes are dropped with them)")
    p.add_argument("--volume", type=int, default=500,
                   help="number of synthetic premises (default 500)")
    p.add_argument("--skip-indexes", action="store_true",
                   help="skip the post-seed index-ensure step")
    args = p.parse_args()
    asyncio.run(seed(drop=args.drop, volume=args.volume,
                     skip_indexes=args.skip_indexes))


if __name__ == "__main__":
    main()
