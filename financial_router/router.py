from __future__ import annotations

import datetime
import dataclasses
import logging
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Optional

from .types import (
    BudgetState,
    G3Path,
    G3Status,
    JWTClaims,
    ModelInfo,
    RoutingDecision,
    RoutingTier,
    SystemPhase,
    TaskMetadata,
)

_DEFAULT_ESTIMATED_TOKENS = 2000
_ROI_EPSILON = 1e-12
_RESERVATION_TTL_SECONDS = 3600
LOGGER = logging.getLogger(__name__)


def _as_utc(ts: datetime.datetime) -> datetime.datetime:
    """Normalize datetime values to timezone-aware UTC."""
    if ts.tzinfo is None:
        # Defensive assumption: naive timestamps are interpreted as UTC.
        return ts.replace(tzinfo=datetime.timezone.utc)
    return ts.astimezone(datetime.timezone.utc)


class SpendReservationRegistry:
    """
    In-memory atomic reservation registry for paid-route idempotency/concurrency safety.
    Callers may supply a stronger shared/distributed implementation with the same method.
    """

    def reserve(self, session_id: str, request_id: str, current_spend: float, cap: float, amount: float) -> bool:
        raise NotImplementedError

    def commit(self, session_id: str, request_id: str) -> bool:
        raise NotImplementedError

    def release(self, session_id: str, request_id: str) -> bool:
        raise NotImplementedError


class SqliteSpendReservationRegistry(SpendReservationRegistry):
    """Durable, cross-process reservation + idempotency registry."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5.0, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS spend_reservations (
                  request_id TEXT PRIMARY KEY,
                  session_id TEXT NOT NULL,
                  amount_usd REAL NOT NULL,
                  status TEXT NOT NULL CHECK (status IN ('reserved','committed','released')),
                  created_at TEXT NOT NULL,
                  expires_at TEXT NOT NULL,
                  closed_at TEXT
                ) STRICT
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_spend_reservations_session ON spend_reservations(session_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_spend_reservations_expiry ON spend_reservations(expires_at)")
            cols = {row[1] for row in conn.execute("PRAGMA table_info('spend_reservations')").fetchall()}
            if "status" not in cols:
                conn.execute("ALTER TABLE spend_reservations ADD COLUMN status TEXT DEFAULT 'reserved'")
                conn.execute("UPDATE spend_reservations SET status='reserved' WHERE status IS NULL")
            if "closed_at" not in cols:
                conn.execute("ALTER TABLE spend_reservations ADD COLUMN closed_at TEXT")

    def reserve(self, session_id: str, request_id: str, current_spend: float, cap: float, amount: float) -> bool:
        if current_spend < 0 or cap < 0 or amount < 0:
            LOGGER.warning(
                "reservation_invalid_inputs",
                extra={
                    "session_id": session_id,
                    "request_id": request_id,
                    "current_spend": current_spend,
                    "cap": cap,
                    "amount": amount,
                },
            )
            return False
        now = datetime.datetime.now(datetime.timezone.utc)
        expires = now + datetime.timedelta(seconds=_RESERVATION_TTL_SECONDS)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            # TTL cleanup prevents unbounded growth and stale cap pressure.
            conn.execute(
                """
                DELETE FROM spend_reservations
                WHERE status = 'reserved' AND expires_at <= ?
                """,
                (now.isoformat(),),
            )

            existing = conn.execute(
                "SELECT session_id, amount_usd, status FROM spend_reservations WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing:
                existing_session, existing_amount, existing_status = existing
                conn.execute("COMMIT")
                if existing_status == "released":
                    return False
                return (
                    existing_session == session_id
                    and abs(float(existing_amount) - float(amount)) < 1e-9
                )

            reserved_total = conn.execute(
                """
                SELECT COALESCE(SUM(amount_usd), 0.0)
                FROM spend_reservations
                WHERE session_id = ? AND status = 'reserved'
                """,
                (session_id,),
            ).fetchone()[0]
            if current_spend + float(reserved_total) + amount > cap:
                conn.execute("COMMIT")
                return False

            conn.execute(
                """
                INSERT INTO spend_reservations(request_id, session_id, amount_usd, status, created_at, expires_at)
                VALUES (?, ?, ?, 'reserved', ?, ?)
                """,
                (request_id, session_id, amount, now.isoformat(), expires.isoformat()),
            )
            conn.execute("COMMIT")
            return True

    def commit(self, session_id: str, request_id: str) -> bool:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE spend_reservations
                SET status='committed', closed_at=?
                WHERE request_id=? AND session_id=? AND status='reserved'
                """,
                (now, request_id, session_id),
            )
            if cursor.rowcount == 0:
                LOGGER.warning(
                    "reservation_commit_noop",
                    extra={"session_id": session_id, "request_id": request_id},
                )
                return False
            return True

    def release(self, session_id: str, request_id: str) -> bool:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE spend_reservations
                SET status='released', closed_at=?
                WHERE request_id=? AND session_id=? AND status='reserved'
                """,
                (now, request_id, session_id),
            )
            if cursor.rowcount == 0:
                LOGGER.warning(
                    "reservation_release_noop",
                    extra={"session_id": session_id, "request_id": request_id},
                )
                return False
            return True


def _build_default_registry() -> SpendReservationRegistry:
    configured = os.getenv("HERMES_RESERVATION_DB_PATH")
    try:
        default_state_dir = Path.home() / ".local" / "state" / "hybrid_router"
        default_state_dir.mkdir(parents=True, exist_ok=True)
        db_path = configured or str(default_state_dir / "reservations_v2.db")
        return SqliteSpendReservationRegistry(db_path)
    except Exception:  # noqa: BLE001
        fallback = f"{tempfile.gettempdir()}/hybrid_router_reservations_fallback.db"
        LOGGER.exception(
            "reservation_store_init_failed",
            extra={"db_path": configured or "<default>", "fallback": fallback},
        )
        return SqliteSpendReservationRegistry(fallback)


_DEFAULT_RESERVATIONS = _build_default_registry()


def commit_paid_reservation(session_id: str, request_id: str, registry: Optional[SpendReservationRegistry] = None) -> bool:
    return (registry or _DEFAULT_RESERVATIONS).commit(session_id, request_id)


def release_paid_reservation(session_id: str, request_id: str, registry: Optional[SpendReservationRegistry] = None) -> bool:
    return (registry or _DEFAULT_RESERVATIONS).release(session_id, request_id)


def finalize_paid_reservation(
    session_id: str,
    request_id: str,
    success: bool,
    registry: Optional[SpendReservationRegistry] = None,
) -> bool:
    registry_impl = registry or _DEFAULT_RESERVATIONS
    return registry_impl.commit(session_id, request_id) if success else registry_impl.release(session_id, request_id)


def _filter_commercial(models: list[ModelInfo]) -> tuple[list[ModelInfo], dict[str, str]]:
    """Apply commercial use gate. Returns (permitted_models, {model_id: skip_reason})."""
    permitted: list[ModelInfo] = []
    skipped: dict[str, str] = {}
    for model in models:
        if model.tier == "local" or model.commercial_use_permitted:
            permitted.append(model)
        else:
            skipped[model.model_id] = (
                f"Model {model.model_id} excluded by commercial use gate for tier {model.tier}."
            )
    return permitted, skipped


def _best_model_for_tier(
    models: list[ModelInfo],
    tier: str,
    min_quality: float,
    check_quota: bool = False,
    check_rate_limit: bool = False,
) -> tuple[Optional[ModelInfo], str]:
    """Find best model at a given tier meeting quality + availability constraints."""
    tier_models = [m for m in models if m.tier == tier]
    if not tier_models:
        return None, f"no {tier} model available"

    candidates = []
    reasons = []
    for model in sorted(tier_models, key=lambda m: (m.model_id,)):
        if model.quality_score < min_quality:
            reasons.append(
                f"{model.model_id} quality {model.quality_score:.4f} below threshold {min_quality:.4f}"
            )
            continue
        if check_quota and model.quota_remaining is not None and model.quota_remaining <= 0:
            reasons.append(f"{model.model_id} quota exhausted")
            continue
        if check_rate_limit and model.rate_limit_remaining is not None and model.rate_limit_remaining <= 0:
            reasons.append(f"{model.model_id} rate limit exhausted")
            continue
        candidates.append(model)

    if not candidates:
        return None, "; ".join(reasons) if reasons else f"no {tier} model qualified"

    return sorted(candidates, key=lambda m: (-m.quality_score, m.model_id))[0], ""


def _compute_roi(task: TaskMetadata, budget: BudgetState, estimated_cost: float) -> float:
    """Compute ROI for a paid routing decision. Returns -1.0 if not computable."""
    if estimated_cost < 0:
        return -1.0

    if task.estimated_task_value_usd is not None:
        estimated_value = task.estimated_task_value_usd
    elif budget.project_cashflow_target_usd is not None:
        estimated_value = budget.project_cashflow_target_usd * budget.task_contribution_pct
    else:
        return -1.0

    if estimated_cost == 0:
        return float("inf") if estimated_value > 0 else -1.0

    return (estimated_value - estimated_cost) / estimated_cost


def _check_g3_timeout(budget: BudgetState, current_time: datetime.datetime) -> bool:
    """Returns True if G3 has timed out (>=6h since request)."""
    if budget.g3_status != G3Status.PENDING or budget.g3_requested_at is None:
        return False
    timeout_hours = max(0.0, budget.g3_timeout_hours)
    now = _as_utc(current_time)
    requested_at = _as_utc(budget.g3_requested_at)
    return (now - requested_at) >= datetime.timedelta(hours=timeout_hours)


def _build_justification(selected_tier: RoutingTier, skipped: dict[str, str]) -> str:
    """Build human-readable justification from skipped reasons."""
    ordered = ["local", "free_cloud", "subscription", "paid_cloud"]
    priority_parts = [f"{k}: {skipped[k]}" for k in ordered if k in skipped]
    model_parts = [f"{k}: {v}" for k, v in sorted(skipped.items()) if k not in ordered]
    pieces = priority_parts + model_parts
    return f"Selected {selected_tier.value}. {' | '.join(pieces) if pieces else 'no tiers skipped'}."


def route_task(
    task: TaskMetadata,
    available_models: list[ModelInfo],
    budget: BudgetState,
    jwt: JWTClaims,
    current_time: Optional[datetime.datetime] = None,
    request_id: Optional[str] = None,
    reservation_registry: Optional[SpendReservationRegistry] = None,
) -> RoutingDecision:
    """Pure decision logic for financial routing waterfall."""
    now = _as_utc(current_time) if current_time else datetime.datetime.now(datetime.timezone.utc)
    skipped: dict[str, str] = {}

    effective_quality_threshold = task.quality_threshold
    if task.quality_threshold < 0:
        skipped["validation"] = "quality threshold below 0 normalized to 0."
        effective_quality_threshold = 0.0
    elif task.quality_threshold > 1:
        skipped["validation"] = "quality threshold above 1 normalized to 1."
        effective_quality_threshold = 1.0

    models = sorted(available_models, key=lambda m: (m.tier, m.model_id))
    permitted_models, commercial_skips = _filter_commercial(models)
    skipped.update(commercial_skips)

    if jwt.max_api_spend_usd < 0 or jwt.current_session_spend_usd < 0:
        skipped["jwt"] = "invalid negative spend claims; paid routing disabled."
    jwt_spend_valid = jwt.max_api_spend_usd >= 0 and jwt.current_session_spend_usd >= 0 and jwt.current_session_spend_usd <= jwt.max_api_spend_usd
    effective_request_id = request_id or task.idempotency_key

    g3_expired = _check_g3_timeout(budget, now)
    if g3_expired:
        skipped["paid_cloud"] = "G3 request expired (timeout reached), paid routing skipped and fallback applied."

    local_model, local_reason = _best_model_for_tier(permitted_models, "local", effective_quality_threshold)
    if local_model is not None:
        return RoutingDecision(RoutingTier.LOCAL, local_model.model_id, G3Path.NOT_APPLICABLE, 0.0, False, _build_justification(RoutingTier.LOCAL, skipped), skipped, False, False)
    skipped["local"] = local_reason

    free_model, free_reason = _best_model_for_tier(permitted_models, "free_cloud", effective_quality_threshold, check_quota=True)
    if free_model is not None:
        return RoutingDecision(RoutingTier.FREE_CLOUD, free_model.model_id, G3Path.NOT_APPLICABLE, 0.0, False, _build_justification(RoutingTier.FREE_CLOUD, skipped), skipped, False, False)
    skipped["free_cloud"] = free_reason

    sub_model, sub_reason = _best_model_for_tier(permitted_models, "subscription", effective_quality_threshold, check_rate_limit=True)
    if sub_model is not None:
        return RoutingDecision(RoutingTier.SUBSCRIPTION, sub_model.model_id, G3Path.NOT_APPLICABLE, 0.0, False, _build_justification(RoutingTier.SUBSCRIPTION, skipped), skipped, False, False)
    skipped["subscription"] = sub_reason

    if task.is_council_tier1_preassessment:
        return RoutingDecision(
            RoutingTier.COMPUTE_STARVED,
            None,
            G3Path.NOT_APPLICABLE,
            0.0,
            False,
            _build_justification(RoutingTier.COMPUTE_STARVED, {**skipped, "tier1": "tier 1 preassessment requires free compute only"}),
            {**skipped, "tier1": "tier 1 preassessment requires free compute only"},
            False,
            True,
        )

    if not g3_expired:
        if budget.system_phase == SystemPhase.CONSTRUCTION:
            skipped["paid_cloud"] = "construction phase — paid routing prohibited."
        else:
            paid_candidates = [m for m in permitted_models if m.tier == "paid"]
            if not paid_candidates:
                skipped["paid_cloud"] = "no paid model available"
            elif not effective_request_id:
                skipped["paid_cloud"] = "missing idempotency key for paid routing"
            else:
                qualified_paid: list[tuple[ModelInfo, float, G3Path, bool]] = []
                paid_fail_reasons: list[str] = []
                for model in sorted(paid_candidates, key=lambda m: (m.model_id,)):
                    if model.quality_score < effective_quality_threshold:
                        paid_fail_reasons.append(f"{model.model_id} quality {model.quality_score:.4f} below threshold {effective_quality_threshold:.4f}")
                        continue
                    if model.cost_per_1k_tokens < 0:
                        paid_fail_reasons.append(f"{model.model_id} blocked: invalid negative model cost")
                        continue
                    if not jwt_spend_valid:
                        paid_fail_reasons.append(f"{model.model_id} blocked: invalid JWT spend claims")
                        continue
                    estimated_cost = model.cost_per_1k_tokens * _DEFAULT_ESTIMATED_TOKENS / 1000.0
                    if jwt.max_api_spend_usd == 0.0:
                        paid_fail_reasons.append(f"{model.model_id} blocked: JWT spend cap is $0.00 (construction phase default)")
                        continue
                    if jwt.current_session_spend_usd + estimated_cost > jwt.max_api_spend_usd:
                        paid_fail_reasons.append(
                            f"{model.model_id} blocked: JWT session spend cap would be exceeded ({jwt.current_session_spend_usd:.4f} + {estimated_cost:.4f} > {jwt.max_api_spend_usd:.4f})."
                        )
                        continue
                    roi = _compute_roi(task, budget, estimated_cost)
                    if roi + _ROI_EPSILON < 5.0:
                        paid_fail_reasons.append(f"{model.model_id} rejected: ROI {roi:.4f} below 5.0")
                        continue

                    if budget.project_cloud_spend_cap_usd is not None:
                        headroom = budget.project_cloud_spend_cap_usd - budget.project_cloud_spend_current_usd
                        if headroom <= 0 or estimated_cost > headroom:
                            paid_fail_reasons.append(f"{model.model_id} rejected: insufficient budget headroom ({estimated_cost:.4f} > {headroom:.4f})")
                            continue
                        g3_path, requires_approval = G3Path.WITHIN_BUDGET, False
                    else:
                        g3_path, requires_approval = G3Path.OUTSIDE_BUDGET, True
                    qualified_paid.append((model, estimated_cost, g3_path, requires_approval))

                if qualified_paid:
                    for selected_model, estimated_cost, g3_path, requires_approval in sorted(
                        qualified_paid, key=lambda entry: (-entry[0].quality_score, entry[0].model_id)
                    ):
                        if reservation_registry is not None:
                            reserved = reservation_registry.reserve(
                                session_id=jwt.session_id,
                                request_id=effective_request_id,
                                current_spend=jwt.current_session_spend_usd,
                                cap=jwt.max_api_spend_usd,
                                amount=estimated_cost,
                            )
                        else:
                            reserved = _DEFAULT_RESERVATIONS.reserve(
                                session_id=jwt.session_id,
                                request_id=effective_request_id,
                                current_spend=jwt.current_session_spend_usd,
                                cap=jwt.max_api_spend_usd,
                                amount=estimated_cost,
                            )
                        if reserved:
                            return RoutingDecision(RoutingTier.PAID_CLOUD, selected_model.model_id, g3_path, estimated_cost, False, _build_justification(RoutingTier.PAID_CLOUD, skipped), skipped, requires_approval, False, effective_request_id)
                        LOGGER.warning("paid_reservation_rejected", extra={"session_id": jwt.session_id, "request_id": effective_request_id})
                        paid_fail_reasons.append(f"{selected_model.model_id} blocked: atomic reservation rejected (cap race)")
                skipped["paid_cloud"] = "; ".join(paid_fail_reasons)

    sub_fallback_models = [
        m for m in permitted_models
        if m.tier == "subscription" and (m.rate_limit_remaining is None or m.rate_limit_remaining > 0)
    ]
    if sub_fallback_models:
        fallback = sorted(sub_fallback_models, key=lambda m: (-m.quality_score, m.model_id))[0]
        return RoutingDecision(RoutingTier.DEFAULT_FALLBACK, fallback.model_id, G3Path.NOT_APPLICABLE, 0.0, True, _build_justification(RoutingTier.DEFAULT_FALLBACK, skipped), skipped, False, False)

    if "subscription" not in skipped:
        skipped["subscription"] = "no subscription model available for default fallback"

    return RoutingDecision(RoutingTier.COMPUTE_STARVED, None, G3Path.NOT_APPLICABLE, 0.0, False, _build_justification(RoutingTier.COMPUTE_STARVED, skipped), skipped, False, True)


def route_fallback(
    task: TaskMetadata,
    available_models: list[ModelInfo],
    budget: BudgetState,
    jwt: JWTClaims,
    failed_model_id: str,
    failure_reason: str,
    switch_count: int,
    current_time: Optional[datetime.datetime] = None,
    request_id: Optional[str] = None,
    reservation_registry: Optional[SpendReservationRegistry] = None,
) -> RoutingDecision:
    if switch_count >= 2:
        return RoutingDecision(
            RoutingTier.COMPUTE_STARVED,
            None,
            G3Path.NOT_APPLICABLE,
            0.0,
            True,
            f"Max live switches (2) exceeded. Last failure: {failure_reason}",
            {"fallback": "max_switches_exceeded"},
            False,
            True,
        )
    filtered = [model for model in available_models if model.model_id != failed_model_id]
    decision = route_task(
        task=task,
        available_models=filtered,
        budget=budget,
        jwt=jwt,
        current_time=current_time,
        request_id=request_id,
        reservation_registry=reservation_registry,
    )
    return dataclasses.replace(
        decision,
        quality_warning=True,
        justification=f"Fallback from {failed_model_id} ({failure_reason}). {decision.justification}",
    )
