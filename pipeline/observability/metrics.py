"""Pipeline metrics — counts, rates, and quality aggregates per run."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RunMetrics:
    run_id: int | None = None
    reference_date: str | None = None
    status: str = "IN_PROGRESS"
    started_at: str | None = None
    completed_at: str | None = None

    total_processed: int = 0
    matched: int = 0
    mismatched: int = 0
    unreconciled_processor: int = 0
    unreconciled_internal: int = 0

    total_internal_volume: float = 0.0
    total_processor_volume: float = 0.0
    total_difference: float = 0.0

    hard_checks_failed: list[str] = field(default_factory=list)
    soft_checks_failed: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "reference_date": self.reference_date,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "total_processed": self.total_processed,
            "matched": self.matched,
            "mismatched": self.mismatched,
            "unreconciled_processor": self.unreconciled_processor,
            "unreconciled_internal": self.unreconciled_internal,
            "total_internal_volume": self.total_internal_volume,
            "total_processor_volume": self.total_processor_volume,
            "total_difference": self.total_difference,
            "hard_checks_failed": self.hard_checks_failed,
            "soft_checks_failed": self.soft_checks_failed,
        }

    @property
    def discrepancy_rate(self) -> float:
        total = self.matched + self.mismatched + self.unreconciled_processor + self.unreconciled_internal
        if total == 0:
            return 0.0
        return self.mismatched / total
