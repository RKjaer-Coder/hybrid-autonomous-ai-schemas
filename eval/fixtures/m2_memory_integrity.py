"""Fixture generator for M2 memory-integrity milestone evaluation."""

from __future__ import annotations

from .common import DeterministicFactory, INCOME_MECHANISMS, REALISTIC_BRIEFS, REALISTIC_THESES


_NODE_DISTRIBUTION = [
    ("OpportunityRecord", 8),
    ("CouncilVerdict", 6),
    ("IntelligenceBrief", 10),
    ("ResearchTask", 8),
    ("IdeaRecord", 4),
    ("MarketSignal", 4),
    ("CapabilityGap", 3),
    ("SourceReputation", 3),
    ("CalibrationRecord", 4),
]


def generate_memory_roundtrips(n: int = 50, seed: int = 42) -> list[dict]:
    if n != 50:
        raise ValueError("M2 roundtrip set must contain 50 fixtures")
    f = DeterministicFactory(seed)
    out: list[dict] = []
    statuses = ["DETECTED", "SCREENED", "QUALIFIED", "ACTIVE", "CLOSED"]
    for node_type, count in _NODE_DISTRIBUTION:
        for i in range(count):
            rid = f.uuid_v7()
            payload = {
                "node_id": rid,
                "node_type": node_type,
                "title": f"{node_type} {i+1}",
                "domain": (i % 5) + 1,
                "status": statuses[i % len(statuses)],
                "confidence": round(0.45 + (i % 5) * 0.1, 2),
                "summary": REALISTIC_BRIEFS[(i + len(out)) % len(REALISTIC_BRIEFS)],
                "thesis": REALISTIC_THESES[(i + len(out)) % len(REALISTIC_THESES)],
                "created_at": f.random_past(90),
                "updated_at": f.offset(),
                "provenance_links": [f.uuid_v7() for _ in range(i % 3)],
                "trust_tier": (i % 4) + 1,
            }
            if node_type == "SourceReputation":
                payload["reputation_score"] = [0.9, 0.1, 0.55][i]
                payload["on_trusted_list"] = 1 if i == 0 else 0
            if node_type == "CalibrationRecord":
                payload["role_weights_used"] = {"strategist": 0.4, "operator": 0.3, "da": 0.3}
                payload["tie_break"] = 1 if i == 0 else 0
            out.append(
                {
                    "roundtrip_id": rid,
                    "node_type": node_type,
                    "write_payload": payload,
                    "read_query": f"Find {node_type} about {payload['title']}",
                    "expected_match": True,
                    "provenance_links": payload["provenance_links"],
                    "trust_tier": payload["trust_tier"],
                    "training_eligible": payload["trust_tier"] <= 3,
                }
            )
    return out


def generate_relevance_queries(n: int = 10, seed: int = 42) -> list[dict]:
    fixtures = generate_memory_roundtrips(50, seed)
    qf = DeterministicFactory(seed + 100)
    queries = []
    for i in range(n):
        matches = [x["roundtrip_id"] for x in fixtures if (x["write_payload"].get("domain") == (i % 5) + 1)][:3]
        non_matches = [x["roundtrip_id"] for x in fixtures if x["roundtrip_id"] not in matches][:3]
        queries.append(
            {
                "query_id": qf.uuid_v7(),
                "query_text": f"Retrieve Domain {(i % 5) + 1} records with active relevance and recent timestamps",
                "query_filters": {
                    "node_type": None if i % 2 else fixtures[i]["node_type"],
                    "domain": (i % 5) + 1,
                    "status": None if i % 3 else "ACTIVE",
                    "date_range": {"start": qf.offset(days=-30), "end": qf.offset(days=0)},
                },
                "expected_match_ids": matches,
                "expected_non_match_ids": non_matches,
                "description": f"Relevance query {i+1}",
            }
        )
    return queries


def generate_wal_recovery_test(n: int = 10, seed: int = 42) -> list[dict]:
    f = DeterministicFactory(seed + 200)
    out = []
    for i in range(n):
        nid = f.uuid_v7()
        out.append(
            {
                "node_id": nid,
                "node_type": "IntelligenceBrief",
                "write_payload": {
                    "brief_id": nid,
                    "task_id": f.uuid_v7(),
                    "domain": (i % 5) + 1,
                    "title": f"WAL test brief {i+1}",
                    "summary": REALISTIC_BRIEFS[i],
                    "confidence": 0.7,
                    "actionability": "WATCH",
                    "urgency": "ROUTINE",
                    "depth_tier": "QUICK",
                    "created_at": f.offset(minutes=-i),
                },
                "write_order": i + 1,
                "description": "WAL recovery test node — verify present after unclean shutdown",
            }
        )
    return out


def generate_m2_test_set(seed: int = 42) -> dict:
    return {
        "memory_roundtrips": generate_memory_roundtrips(50, seed),
        "relevance_queries": generate_relevance_queries(10, seed),
        "wal_recovery_nodes": generate_wal_recovery_test(10, seed),
        "eval_criteria": {
            "max_provenance_deviations": 0,
            "max_wal_corruption_count": 0,
            "max_relevance_false_positive_rate": 0.10,
            "max_relevance_false_negative_rate": 0.10,
            "latency_p95_ms": 500,
        },
    }
