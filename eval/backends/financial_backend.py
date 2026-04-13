"""Eval backend that runs M4 routing checks against the real financial router code."""

from __future__ import annotations

from eval.runner import MockBackend


class Backend(MockBackend):
    """Real-financial M4 backend with MockBackend fallbacks for non-M4 methods."""
