"""Hermes v0.8.0 bootstrap entrypoint for immune patch integration."""

from __future__ import annotations

import os

from immune.bootstrap_patch import apply_immune_patch
from immune.config import load_config
from immune.verdict_logger import VerdictLogger


def bootstrap_immune_patch(db_path: str | None = None) -> bool:
    """Attach immune wrappers before starting Hermes agent sessions."""
    cfg = load_config()
    logger = VerdictLogger(db_path or os.getenv("IMMUNE_DB_PATH", "immune.db"), cfg)
    return apply_immune_patch(config=cfg, verdict_logger=logger)


if __name__ == "__main__":
    ok = bootstrap_immune_patch()
    print("immune patch applied" if ok else "immune patch not applied")
