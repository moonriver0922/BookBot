"""Run-level tracking: per-step timing (runtime) and failure reasons (feedback).

Two JSONL log files are produced under ``logs/``:
  - ``runtime.jsonl``  — one JSON object per run with per-step durations.
    In rush mode the output splits into ``prep_steps`` (before 08:30) and
    ``rush_steps`` (after 08:30) so the critical-path is easy to read.
  - ``feedback.jsonl`` — one JSON object per run with structured failure reasons.
"""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

LOGS_DIR = Path("logs")


class _Tracker:
    """Singleton that collects runtime timings and feedback for a single run."""

    def __init__(self) -> None:
        self._prep_steps: list[dict[str, Any]] = []
        self._rush_steps: list[dict[str, Any]] = []
        self._feedbacks: list[dict[str, Any]] = []
        self._metrics: dict[str, Any] = {}
        self._run_start: float = 0.0
        self._rush_start: float = 0.0
        self._mode: str = ""
        self._run_date: str = ""
        self._in_rush_phase: bool = False

    # ── lifecycle ──

    def start_run(self, *, mode: str = "normal") -> None:
        self._prep_steps.clear()
        self._rush_steps.clear()
        self._feedbacks.clear()
        self._metrics.clear()
        self._run_start = time.monotonic()
        self._rush_start = 0.0
        self._mode = mode
        self._run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._in_rush_phase = False
        logger.debug("[tracker] run started (mode={})", mode)

    def mark_rush_start(self) -> None:
        """Call this the instant rush_time is reached (08:30)."""
        self._rush_start = time.monotonic()
        self._in_rush_phase = True

    def finish_run(self, *, success: bool) -> None:
        total = time.monotonic() - self._run_start if self._run_start else 0.0
        rush_total = (time.monotonic() - self._rush_start) if self._rush_start else None
        LOGS_DIR.mkdir(exist_ok=True)
        self._write_runtime(total, rush_total, success)
        self._write_feedback(success)
        logger.debug("[tracker] run finished — total {:.1f}s, success={}", total, success)

    # ── step timing ──

    @contextmanager
    def step(self, name: str):
        """Context manager that times a named step."""
        t0 = time.monotonic()
        try:
            yield
        finally:
            elapsed = time.monotonic() - t0
            target = self._rush_steps if self._in_rush_phase else self._prep_steps
            target.append({"step": name, "duration_s": round(elapsed, 3)})
            logger.debug("[tracker] step '{}' took {:.3f}s", name, elapsed)

    def record_step(self, name: str, duration: float) -> None:
        """Manually record a step that was timed externally."""
        target = self._rush_steps if self._in_rush_phase else self._prep_steps
        target.append({"step": name, "duration_s": round(duration, 3)})
        logger.debug("[tracker] step '{}' recorded {:.3f}s", name, duration)

    # ── feedback ──

    def add_feedback(self, reason: str, **details: Any) -> None:
        entry: dict[str, Any] = {"reason": reason, **details}
        self._feedbacks.append(entry)
        logger.debug("[tracker] feedback: {}", entry)

    def set_metric(self, name: str, value: Any) -> None:
        self._metrics[name] = value
        logger.debug("[tracker] metric {}={}", name, value)

    def incr_metric(self, name: str, delta: int = 1) -> None:
        current = self._metrics.get(name, 0)
        try:
            current = int(current)
        except Exception:
            current = 0
        self._metrics[name] = current + delta
        logger.debug("[tracker] metric {}={}", name, self._metrics[name])

    # ── persistence ──

    def _write_runtime(self, total: float, rush_total: float | None, success: bool) -> None:
        record: dict[str, Any] = {
            "timestamp": self._run_date,
            "mode": self._mode,
            "total_duration_s": round(total, 3),
            "success": success,
        }
        if self._mode == "rush" and self._rush_start:
            record["prep_steps"] = self._prep_steps
            record["rush_total_s"] = round(rush_total, 3) if rush_total else 0.0
            record["rush_steps"] = self._rush_steps
        else:
            record["steps"] = self._prep_steps + self._rush_steps
        if self._metrics:
            record["metrics"] = self._metrics
        path = LOGS_DIR / "runtime.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _write_feedback(self, success: bool) -> None:
        record = {
            "timestamp": self._run_date,
            "mode": self._mode,
            "success": success,
            "events": self._feedbacks,
        }
        if not success and not self._feedbacks:
            record["events"] = [{"reason": "unknown", "detail": "No specific failure reason captured"}]
        path = LOGS_DIR / "feedback.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


tracker = _Tracker()
