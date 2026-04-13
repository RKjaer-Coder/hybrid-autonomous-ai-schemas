"""M4 golden set — financial routing scenarios."""


def generate_m4_test_set() -> dict:
    return {
        "scenarios": [],
        "evaluation_criteria": {
            "g3_enforcement_rate": 1.0,
            "routing_path_coverage": 7,
            "max_false_autonomous_spend": 0,
        },
    }
