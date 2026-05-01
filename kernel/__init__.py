"""v3.1 foundation kernel authority primitives.

Legacy domain modules remain useful adapters and projections. This package is
the first v3.1 authoritative command/event/capability/budget/side-effect spine.
"""

from .records import (
    ArtifactRef,
    Budget,
    CapabilityGrant,
    Command,
    Event,
    SideEffectIntent,
    SideEffectReceipt,
)
from .store import (
    LEGACY_BOUNDARIES,
    KernelStore,
    KernelTransaction,
    ReplayState,
    create_kernel_database,
)

__all__ = [
    "ArtifactRef",
    "Budget",
    "CapabilityGrant",
    "Command",
    "Event",
    "KernelStore",
    "KernelTransaction",
    "LEGACY_BOUNDARIES",
    "ReplayState",
    "SideEffectIntent",
    "SideEffectReceipt",
    "create_kernel_database",
]
