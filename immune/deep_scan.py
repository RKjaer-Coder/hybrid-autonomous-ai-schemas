from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class DeepScanResult:
    threat_detected: bool
    confidence: float
    threat_type: str


class DeepScanInterface(ABC):
    @abstractmethod
    async def classify(self, text: str, context: dict) -> DeepScanResult:
        """Classify text for threats."""


class MockDeepScan(DeepScanInterface):
    """Test implementation with configurable behavior."""

    def __init__(
        self,
        default_result: DeepScanResult | None = None,
        raise_on_call: bool = False,
        delay_ms: float = 0,
    ):
        self._default_result = default_result or DeepScanResult(False, 0.0, "")
        self._raise_on_call = raise_on_call
        self._delay_ms = delay_ms

    async def classify(self, text: str, context: dict) -> DeepScanResult:
        del text, context
        if self._raise_on_call:
            raise RuntimeError("Deep-scan model unavailable")
        if self._delay_ms > 0:
            await asyncio.sleep(self._delay_ms / 1000)
        return self._default_result


if __name__ == "__main__":
    print(asyncio.run(MockDeepScan().classify("x", {})))
