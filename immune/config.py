from __future__ import annotations

from dataclasses import fields
import logging
import os

from immune.types import ImmuneConfig

LOGGER = logging.getLogger(__name__)


def load_config(overrides: dict | None = None) -> ImmuneConfig:
    """Load immutable config from defaults plus env and optional overrides."""
    values: dict[str, object] = {}
    for f in fields(ImmuneConfig):
        env_name = f"IMMUNE_{f.name.upper()}"
        if env_name in os.environ:
            raw = os.environ[env_name]
            if isinstance(f.default, bool):
                values[f.name] = raw.lower() in {"1", "true", "yes"}
            elif isinstance(f.default, int):
                values[f.name] = int(raw)
            elif isinstance(f.default, float):
                values[f.name] = float(raw)
            else:
                values[f.name] = raw
    if overrides:
        valid = {f.name for f in fields(ImmuneConfig)}
        for key, value in overrides.items():
            if key in valid:
                values[key] = value
            else:
                LOGGER.warning("Unknown immune config key ignored: %s", key)
    return ImmuneConfig(**values)


if __name__ == "__main__":
    print(load_config())
