from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
import os
from typing import Any


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


class LocalTransformersDeepScan(DeepScanInterface):
    """Local Hugging Face sequence-classification deep-scan model."""

    def __init__(self, model_path: str):
        self._model_path = model_path
        self._pipeline = self._load_pipeline(model_path)

    @staticmethod
    def _load_pipeline(model_path: str):
        try:
            from transformers import pipeline
        except Exception as exc:  # pragma: no cover - optional dependency path
            raise RuntimeError(
                "transformers is required for LocalTransformersDeepScan"
            ) from exc
        return pipeline("text-classification", model=model_path, tokenizer=model_path)

    @staticmethod
    def _interpret_label(label: str) -> tuple[bool, str]:
        norm = label.strip().upper()
        if any(tok in norm for tok in ("INJECTION", "JAILBREAK", "THREAT", "MALICIOUS", "UNSAFE", "TOXIC")):
            return True, norm.lower()
        if norm in {"LABEL_1", "POSITIVE"}:
            return True, "model_positive"
        return False, ""

    def _classify_sync(self, text: str, context: dict[str, Any]) -> DeepScanResult:
        payload = text
        if context:
            payload = f"{text}\ncontext={context}"
        res = self._pipeline(payload, truncation=True, max_length=512)
        pred = res[0] if isinstance(res, list) else res
        label = str(pred.get("label", ""))
        score = float(pred.get("score", 0.0))
        threat_detected, threat_type = self._interpret_label(label)
        return DeepScanResult(threat_detected=threat_detected, confidence=score, threat_type=threat_type)

    async def classify(self, text: str, context: dict) -> DeepScanResult:
        return await asyncio.to_thread(self._classify_sync, text, context)


def build_deep_scan_model(model_path: str | None = None) -> DeepScanInterface:
    """Build a local deep-scan model when configured, otherwise use MockDeepScan."""
    selected = model_path or os.getenv("IMMUNE_DEEP_SCAN_MODEL_PATH")
    if selected:
        return LocalTransformersDeepScan(selected)
    return MockDeepScan()


if __name__ == "__main__":
    print(asyncio.run(MockDeepScan().classify("x", {})))
