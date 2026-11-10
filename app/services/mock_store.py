"""Canned data for the three demo scenarios so the demo runs end-to-end
without a MongoDB Atlas cluster. Mirrors the shapes in the spec Section 3."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

_NOW = datetime.now(timezone.utc)


def _ts(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _ago(**kwargs) -> datetime:
    return _NOW - timedelta(**kwargs)


PROFILES: list[dict[str, Any]] = [
    {
        "_id": "prof_ee_98214",
        "firstName": "Sarah",
        "lastName": "Jenkins",
        "brand": "EE",
        "uprn": "100021489211",
        "roleAtPremise": "RESIDENT",
        "contact": {"phone": "+447700900123", "email": "s.jenkins@example.com"},
        "relationships": [
            {"profileId": "prof_bt_44102", "type": "PARTNER",
             "strength": 0.92, "atUprn": "100021489211"},
        ],
        "products": [
            {"serviceId": "srv_ee_mob_11", "category": "MOBILE",
             "plan": "Unlimited All-Rounder 5G", "status": "ACTIVE"},
        ],
        "consent": {"crossBrandLinkage": True,
                    "lastReviewedAt": _ago(days=60)},
    },
    {
        "_id": "prof_bt_44102",
        "firstName": "Mark",
        "lastName": "Davis",
        "brand": "BT",
        "uprn": "100021489211",
        "roleAtPremise": "ACCOUNT_HOLDER",
        "contact": {"phone": "+447700900456", "email": "m.davis@example.com"},
        "relationships": [
            {"profileId": "prof_ee_98214", "type": "PARTNER",
             "strength": 0.92, "atUprn": "100021489211"},
        ],
        "products": [
            {"serviceId": "srv_bt_bb_99", "category": "BROADBAND",
             "plan": "Full Fibre 900 Halo 3", "status": "FAULT_REPORTED"},
        ],
        "consent": {"crossBrandLinkage": True,
                    "lastReviewedAt": _ago(days=60)},
    },
    {
        "_id": "prof_pn_11871",
        "firstName": "David",
        "lastName": "Patel",
        "brand": "PLUSNET",
        "uprn": "200039182341",
        "roleAtPremise": "ACCOUNT_HOLDER",
        "contact": {"phone": "+441135550888", "email": "d.patel@example.com"},
        "relationships": [],
        "products": [
            {"serviceId": "srv_pn_adsl_12", "category": "BROADBAND",
             "plan": "Plusnet Unlimited ADSL", "status": "CEASED"},
        ],
        "consent": {"crossBrandLinkage": True,
                    "lastReviewedAt": _ago(days=40)},
    },
    {
        "_id": "prof_bt_55401",
        "firstName": "Priya",
        "lastName": "Shah",
        "brand": "BT",
        "uprn": "350018447722",
        "roleAtPremise": "ACCOUNT_HOLDER",
        "contact": {"phone": "+447700900777", "email": "p.shah@example.com"},
        "relationships": [
            {"profileId": "prof_bt_55402", "type": "GUARANTOR",
             "strength": 0.88, "atUprn": "350018447899"},
        ],
        "products": [
            {"serviceId": "srv_bt_bb_201", "category": "BROADBAND",
             "plan": "Full Fibre 500", "status": "PROVISIONING"},
        ],
        "consent": {"crossBrandLinkage": True,
                    "lastReviewedAt": _ago(days=55)},
    },
    {
        "_id": "prof_bt_55402",
        "firstName": "Anita",
        "lastName": "Shah",
        "brand": "BT",
        "uprn": "350018447899",
        "roleAtPremise": "ACCOUNT_HOLDER",
        "contact": {"phone": "+447700900778"},
        "relationships": [
            {"profileId": "prof_bt_55401", "type": "GUARANTOR",
             "strength": 0.88, "atUprn": "350018447722"},
        ],
        "products": [],
        "consent": {"crossBrandLinkage": True,
                    "lastReviewedAt": _ago(days=55)},
    },
]


PREMISES: list[dict[str, Any]] = [
    {
        "uprn": "100021489211",
        "address": {"subBuildingName": "Flat 2B", "buildingNumber": "14",
                    "thoroughfareName": "Elmwood Terrace",
                    "postTown": "Bristol", "postcode": "BS1 5RR"},
        "networkInfrastructure": {
            "ontSerial": "ALCLB0912A34", "exchangeCode": "E-BRIS-01",
            "technology": "FTTP", "maxAvailableSpeed": "1000Mbps",
            "lineStatus": "OUTAGE_DETECTED",
        },
        "updatedAt": _ago(hours=1),
    },
    {
        "uprn": "200039182341",
        "address": {"buildingNumber": "27",
                    "thoroughfareName": "Victoria Road",
                    "postTown": "Leeds", "postcode": "LS1 4DP"},
        "networkInfrastructure": {
            "exchangeCode": "E-LEED-07", "technology": "FTTP",
            "maxAvailableSpeed": "1000Mbps", "lineStatus": "AWAITING_PROVISION",
        },
        "updatedAt": _ago(hours=4),
    },
    {
        "uprn": "350018447722",
        "address": {"buildingNumber": "48",
                    "thoroughfareName": "Hanover Street",
                    "postTown": "Edinburgh", "postcode": "EH2 1EL"},
        "networkInfrastructure": {
            "exchangeCode": "E-EDIN-02", "technology": "FTTP",
            "maxAvailableSpeed": "1000Mbps", "lineStatus": "PROVISIONING",
        },
        "updatedAt": _ago(hours=2),
    },
    {
        "uprn": "350018447899",
        "address": {"buildingNumber": "12",
                    "thoroughfareName": "Rose Crescent",
                    "postTown": "Edinburgh", "postcode": "EH3 7PP"},
        "networkInfrastructure": {},
        "updatedAt": _ago(days=30),
    },
]


INTERACTIONS: list[dict[str, Any]] = [
    {
        "_id": "int_bt_chat_20261022_312",
        "uprn": "100021489211",
        "profileId": "prof_bt_44102",
        "brand": "BT",
        "channel": "WEBCHAT",
        "createdAt": _ago(hours=20),
        "rawTranscript": "Customer reports steady purple light on Smart Hub ...",
        "interactionSummary": ("Mark reported a steady purple Smart Hub light. "
                               "LOS detected on the FTTP line. Field technician "
                               "booked via Openreach for 23/10 morning window."),
        "intentCategory": "BROADBAND_HARDWARE_LOS",
        "entities": {"serviceReferences": ["srv_bt_bb_99"],
                     "orderReferences": [],
                     "workOrderReferences": ["OR-2026-9921"]},
        "status": "ESCALATED_FIELD_SERVICE",
    },
    {
        "_id": "int_ee_app_20261023_099",
        "uprn": "100021489211",
        "profileId": "prof_ee_98214",
        "brand": "EE",
        "channel": "MOBILE_APP",
        "createdAt": _ago(hours=1, minutes=15),
        "rawTranscript": "Path: MyEE > Help > Broadband Status > Check Line ...",
        "interactionSummary": ("Customer Sarah Jenkins attempted broadband fault "
                               "self-service in the MyEE app. Abandoned the journey "
                               "when prompted for a BT landline credential she does "
                               "not hold."),
        "intentCategory": "BROADBAND_SELF_SERVICE_FAILURE",
        "entities": {"serviceReferences": ["srv_bt_bb_99"],
                     "orderReferences": [],
                     "workOrderReferences": ["OR-2026-9921"]},
        "status": "SELF_SERVICE_ABANDONED",
    },
    {
        "_id": "int_pn_chat_20261019_045",
        "uprn": "200039182341",
        "profileId": "prof_pn_11871",
        "brand": "PLUSNET",
        "channel": "WEBCHAT",
        "createdAt": _ago(days=4),
        "rawTranscript": "...",
        "interactionSummary": ("David Patel confirmed his migration from Plusnet "
                               "ADSL to EE Full Fibre on 23/10. New router dispatched "
                               "via DPD on tracking DPD-88219-UK."),
        "intentCategory": "MIGRATION_CONFIRMATION",
        "entities": {"serviceReferences": ["srv_pn_adsl_12"],
                     "orderReferences": ["ord_ee_fibre_55120"],
                     "workOrderReferences": []},
        "status": "SCHEDULED",
    },
    {
        "_id": "int_bt_voice_20261024_010",
        "uprn": "350018447722",
        "profileId": "prof_bt_55401",
        "brand": "BT",
        "channel": "VOICE",
        "createdAt": _ago(hours=6),
        "rawTranscript": "...",
        "interactionSummary": ("Priya called to confirm her install slot and "
                               "sounded worried the engineer might not arrive on "
                               "time. Reassured her and read back the OR-2026-10044 "
                               "work order reference."),
        "intentCategory": "APPOINTMENT_REASSURANCE",
        "entities": {"serviceReferences": ["srv_bt_bb_201"],
                     "orderReferences": [],
                     "workOrderReferences": ["OR-2026-10044"]},
        "status": "RESOLVED",
    },
]


OPERATIONS_WFM: list[dict[str, Any]] = [
    {
        "_id": "wfm_or_9921",
        "uprn": "100021489211",
        "workOrderType": "SPLICING_AND_ONT_REPAIR",
        "provider": "OPENREACH",
        "reference": "OR-2026-9921",
        "status": "DELAYED",
        "statusDetails": "Field tech delayed on overhead fibre splice. Revised ETA: 13:45.",
        "scheduledWindow": {"start": _ago(hours=5),
                            "end": _ago(minutes=0)},
        "updatedAt": _ago(minutes=5),
    },
    {
        "_id": "wfm_or_10044",
        "uprn": "350018447722",
        "workOrderType": "NEW_PROVIDE_FTTP",
        "provider": "OPENREACH",
        "reference": "OR-2026-10044",
        "status": "IN_TRANSIT",
        "statusDetails": "Engineer en route. ETA 11:20.",
        "scheduledWindow": {"start": _ago(hours=2),
                            "end": _ago(minutes=0)},
        "updatedAt": _ago(minutes=15),
    },
]


ORDERS: list[dict[str, Any]] = [
    {
        "_id": "ord_ee_fibre_55120",
        "uprn": "200039182341",
        "profileId": "prof_pn_11871",
        "brand": "EE",
        "orderType": "MIGRATION_PLUSNET_ADSL_TO_EE_FFTP",
        "status": "HARDWARE_IN_TRANSIT_EXCEPTION",
        "statusDetails": ("DPD-88219-UK marked DAMAGED_IN_TRANSIT. Replacement "
                          "not yet dispatched."),
        "createdAt": _ago(days=5),
        "goLiveTarget": _ago(hours=2),
    },
    {
        "_id": "ord_bt_bb_60011",
        "uprn": "350018447722",
        "profileId": "prof_bt_55401",
        "brand": "BT",
        "orderType": "NEW_PROVIDE_FTTP",
        "status": "ENGINEER_SCHEDULED",
        "statusDetails": "Install scheduled today 10:00-13:00.",
        "createdAt": _ago(days=5),
        "goLiveTarget": _ago(minutes=0),
    },
]


def _build_c360(uprn: str) -> Optional[dict[str, Any]]:
    premise = next((p for p in PREMISES if p["uprn"] == uprn), None)
    if premise is None:
        return None
    profiles = [p for p in PROFILES if p["uprn"] == uprn]
    brands = {p["brand"] for p in profiles}
    is_converged = {"BT", "EE"}.issubset(brands)
    return {
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
        "activeOperations": [
            op for op in OPERATIONS_WFM
            if op["uprn"] == uprn and op["status"] in {
                "DELAYED", "IN_TRANSIT", "PENDING", "DISPATCHED"
            }
        ],
        "openOrders": [o for o in ORDERS
                       if o["uprn"] == uprn and o["status"] != "COMPLETED"],
        "recentInteractionsSummary": sorted(
            [
                {"interactionId": i["_id"], "channel": i["channel"],
                 "createdAt": i["createdAt"], "summary": i["interactionSummary"],
                 "status": i["status"], "profileId": i["profileId"]}
                for i in INTERACTIONS if i["uprn"] == uprn
            ],
            key=lambda d: d["createdAt"],
            reverse=True,
        )[:10],
        "commercialInsights": {
            "convergedHousehold": is_converged,
            "nextBestAction": ("EE_ONE_CONVERGENCE_BUNDLE" if is_converged
                               else "STANDALONE_RETAIN"),
            "eligibleDiscounts": (
                ["£10/mo Broadband Discount", "Unlimited Data Boost on EE Mobile"]
                if is_converged else []
            ),
        },
        "updatedAt": premise.get("updatedAt"),
    }


def c360(uprn: str) -> Optional[dict[str, Any]]:
    doc = _build_c360(uprn)
    return deepcopy(doc) if doc else None


def profile_by_phone(phone: str) -> Optional[dict[str, Any]]:
    return deepcopy(next((p for p in PROFILES if p.get("contact", {}).get("phone") == phone), None))


def profile_by_id(profile_id: str) -> Optional[dict[str, Any]]:
    return deepcopy(next((p for p in PROFILES if p["_id"] == profile_id), None))


def profiles_at_uprn(uprn: str) -> list[dict[str, Any]]:
    return deepcopy([p for p in PROFILES if p["uprn"] == uprn])


def premise_search(tokens: str) -> list[dict[str, Any]]:
    tokens_l = tokens.lower()
    results = []
    for pr in PREMISES:
        hay = " ".join(filter(None, [
            pr["address"].get("thoroughfareName"),
            pr["address"].get("postTown"),
            pr["address"].get("postcode"),
            pr["address"].get("buildingNumber"),
            pr["uprn"],
        ])).lower()
        score = sum(1 for t in tokens_l.split() if t and t in hay)
        if score:
            results.append((score, pr))
    results.sort(key=lambda x: -x[0])
    return [deepcopy(r[1]) for r in results[:5]]


def interactions_by_uprns(uprns: list[str], since) -> list[dict[str, Any]]:
    rows = [i for i in INTERACTIONS if i["uprn"] in set(uprns) and i["createdAt"] >= since]
    return deepcopy(rows)


def wfm_recent(uprns: list[str], since) -> list[dict[str, Any]]:
    rows = [o for o in OPERATIONS_WFM
            if o["uprn"] in set(uprns) and o["updatedAt"] >= since]
    return deepcopy(rows)
