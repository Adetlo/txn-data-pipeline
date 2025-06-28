"""In-process metrics for a single batch run (no metrics server required).

The summary is embedded in the batch result and emitted as a structured log event, so
operators get ``records_per_second``, ``quality_pass_rate`` and per-stage durations
without any extra infrastructure.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class MetricsCollector:
    counters: dict[str, int] = field(default_factory=dict)
    stage_durations_ms: dict[str, float] = field(default_factory=dict)
    _started: float = field(default_factory=time.perf_counter)
    _finished: float | None = None

    def increment(self, name: str, value: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + value

    def set(self, name: str, value: int) -> None:
        self.counters[name] = value

    @contextmanager
    def timed(self, stage: str) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            self.stage_durations_ms[stage] = round((time.perf_counter() - start) * 1000, 3)

    def finish(self) -> None:
        self._finished = time.perf_counter()

    @property
    def duration_seconds(self) -> float:
        end = self._finished if self._finished is not None else time.perf_counter()
        return round(end - self._started, 3)

    def summary(self) -> dict[str, Any]:
        processed = self.counters.get("records_processed", 0)
        rejected = self.counters.get("records_rejected", 0)
        duplicates = self.counters.get("records_duplicate", 0)
        accepted = self.counters.get("records_accepted", 0)
        duration = max(self.duration_seconds, 1e-9)
        pass_rate = round(accepted / processed, 4) if processed else 1.0
        return {
            "records_processed": processed,
            "records_accepted": accepted,
            "records_rejected": rejected,
            "records_duplicate": duplicates,
            "records_conflict": self.counters.get("records_conflict", 0),
            "records_warning": self.counters.get("records_warning", 0),
            "quality_pass_rate": pass_rate,
            "duration_seconds": self.duration_seconds,
            "records_per_second": round(processed / duration, 1),
            "stage_durations_ms": dict(self.stage_durations_ms),
        }
