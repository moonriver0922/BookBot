"""Run-level tracking: timing, candidates, network, failure class, reports.

JSONL outputs under ``logs/``:
  - ``runtime.jsonl`` — one object per run (join key: ``run_id``)
  - ``feedback.jsonl`` — failure/success events for the same ``run_id``
  - ``candidate_events.jsonl`` — one object per attempted candidate
  - ``network_events.jsonl`` — critical-path request timings
  - ``reports/<run_id>.txt`` — human-readable rush war report
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from loguru import logger

from bookbot.failures import classify_run

LOGS_DIR = Path("logs")
REPORTS_DIR = LOGS_DIR / "reports"


def _git_commit() -> str:
    """Best-effort short git commit hash for experiment provenance."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return out.strip() or "unknown"
    except Exception:
        return "unknown"


def _config_hash(payload: dict[str, Any] | None) -> str:
    if not payload:
        return ""
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def new_run_id(when: datetime | None = None) -> str:
    """Build a compact unique run id, e.g. ``20260911-083000-a83f``."""
    dt = when or datetime.now()
    return f"{dt.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"


class _Tracker:
    """Singleton that collects runtime timings and feedback for a single run."""

    def __init__(self) -> None:
        self._prep_steps: list[dict[str, Any]] = []
        self._rush_steps: list[dict[str, Any]] = []
        self._feedbacks: list[dict[str, Any]] = []
        self._metrics: dict[str, Any] = {}
        self._timeline: list[dict[str, Any]] = []
        self._candidates: list[dict[str, Any]] = []
        self._network: list[dict[str, Any]] = []
        self._run_start: float = 0.0
        self._rush_start_mono: float = 0.0
        self._rush_start_wall: str = ""
        self._mode: str = ""
        self._run_date: str = ""
        self._run_id: str = ""
        self._in_rush_phase: bool = False
        self._experiment_id: str = ""
        self._strategy_version: str = ""
        self._git_commit: str = ""
        self._config_hash: str = ""
        self._failure_class: str = ""
        self._primary_reason: str = ""
        self._last_report_path: Path | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def last_report_path(self) -> Path | None:
        return self._last_report_path

    # ── lifecycle ──

    def start_run(
        self,
        *,
        mode: str = "normal",
        experiment_id: str = "",
        strategy_version: str = "",
        config_snapshot: dict[str, Any] | None = None,
    ) -> str:
        """Start a tracked run and return its ``run_id``."""
        self._prep_steps.clear()
        self._rush_steps.clear()
        self._feedbacks.clear()
        self._metrics.clear()
        self._timeline.clear()
        self._candidates.clear()
        self._network.clear()
        self._run_start = time.monotonic()
        self._rush_start_mono = 0.0
        self._rush_start_wall = ""
        self._mode = mode
        now = datetime.now()
        self._run_date = now.strftime("%Y-%m-%d %H:%M:%S")
        self._run_id = new_run_id(now)
        self._in_rush_phase = False
        self._experiment_id = experiment_id
        self._strategy_version = strategy_version
        self._git_commit = _git_commit()
        self._config_hash = _config_hash(config_snapshot)
        self._failure_class = ""
        self._primary_reason = ""
        self._last_report_path = None
        self._metrics["experiment_id"] = experiment_id or "baseline"
        self._metrics["strategy_version"] = strategy_version or "v0"
        self._metrics["git_commit"] = self._git_commit
        if self._config_hash:
            self._metrics["config_hash"] = self._config_hash
        logger.debug("[tracker] run started run_id={} mode={}", self._run_id, mode)
        return self._run_id

    def mark_rush_start(self) -> None:
        """Call this the instant rush_time is reached (08:30)."""
        self._rush_start_mono = time.monotonic()
        self._rush_start_wall = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        self._in_rush_phase = True
        self.mark_event("rush_open")

    def mark_rush_start_aligned(self, *, open_in_seconds: float) -> None:
        """Align T0 to the official open instant, even if first fire is early/late.

        ``open_in_seconds`` is seconds from now until open under the
        server-adjusted clock (negative if open already passed).
        """
        self._rush_start_mono = time.monotonic() + float(open_in_seconds)
        open_wall = datetime.now() + timedelta(seconds=float(open_in_seconds))
        self._rush_start_wall = open_wall.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        self._in_rush_phase = True
        self.mark_event("rush_open", open_in_seconds=round(float(open_in_seconds), 3))

    def finish_run(self, *, success: bool) -> Path | None:
        """Persist all logs and optionally write a rush war report."""
        total = time.monotonic() - self._run_start if self._run_start else 0.0
        rush_total = (
            (time.monotonic() - self._rush_start_mono) if self._rush_start_mono else None
        )
        failure_class, primary_reason = classify_run(
            success=success,
            events=self._feedbacks,
            metrics=self._metrics,
        )
        self._failure_class = failure_class
        self._primary_reason = primary_reason
        self._metrics["failure_class"] = failure_class
        self._metrics["primary_reason"] = primary_reason

        LOGS_DIR.mkdir(exist_ok=True)
        self._write_runtime(total, rush_total, success)
        self._write_feedback(success)
        self._write_candidates()
        self._write_network()
        report_path = None
        if self._mode == "rush":
            report_path = self.write_run_report(success=success)
            self._last_report_path = report_path
        logger.debug(
            "[tracker] run finished run_id={} total={:.1f}s success={} class={}",
            self._run_id,
            total,
            success,
            failure_class,
        )
        return report_path

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

    # ── timeline / candidates / network ──

    def ms_since_rush(self) -> float | None:
        """Return milliseconds since rush open, or None before rush phase."""
        if not self._rush_start_mono:
            return None
        return round((time.monotonic() - self._rush_start_mono) * 1000.0, 1)

    def mark_event(self, name: str, **fields: Any) -> dict[str, Any]:
        """Record a named critical-path event relative to rush T0."""
        entry: dict[str, Any] = {
            "event": name,
            "t_ms": self.ms_since_rush(),
            "wall": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        }
        entry.update(fields)
        self._timeline.append(entry)
        logger.debug("[tracker] event {} {}", name, entry.get("t_ms"))
        return entry

    def start_candidate(
        self,
        *,
        center: str,
        date: str,
        start: str,
        end: str,
        court: str = "",
    ) -> dict[str, Any]:
        """Open a candidate event row when an acceptable slot is first seen."""
        row: dict[str, Any] = {
            "run_id": self._run_id,
            "center": center,
            "date": date,
            "start": start,
            "end": end,
            "court": court,
            "first_seen_ms": self.ms_since_rush(),
            "click_started_ms": None,
            "selection_registered_ms": None,
            "next_started_ms": None,
            "confirm_started_ms": None,
            "result_ms": None,
            "result": "pending",
        }
        self._candidates.append(row)
        self.mark_event(
            "first_acceptable_slot_seen",
            center=center,
            date=date,
            start=start,
            end=end,
        )
        return row

    def update_candidate(self, candidate: dict[str, Any], field: str) -> None:
        """Stamp a candidate lifecycle field with current rush offset."""
        candidate[field] = self.ms_since_rush()
        event_map = {
            "click_started_ms": "slot_click_started",
            "selection_registered_ms": "slot_selection_registered",
            "next_started_ms": "next_click_started",
            "confirm_started_ms": "confirm_click_started",
        }
        event_name = event_map.get(field)
        if event_name:
            self.mark_event(
                event_name,
                center=candidate.get("center"),
                start=candidate.get("start"),
                end=candidate.get("end"),
            )

    def finish_candidate(self, candidate: dict[str, Any], result: str) -> None:
        """Close a candidate with a final result label."""
        candidate["result"] = result
        candidate["result_ms"] = self.ms_since_rush()
        self.mark_event(
            "booking_result_received",
            result=result,
            center=candidate.get("center"),
            start=candidate.get("start"),
            end=candidate.get("end"),
        )

    def record_network(
        self,
        *,
        request_type: str,
        status_code: int | None = None,
        request_started_ms: float | None = None,
        response_headers_ms: float | None = None,
        response_finished_ms: float | None = None,
        response_size: int | None = None,
        center: str = "",
        attempt: int = 0,
        url: str = "",
    ) -> None:
        """Record one critical-path network observation."""
        started = request_started_ms
        finished = response_finished_ms
        rtt = None
        if isinstance(started, (int, float)) and isinstance(finished, (int, float)):
            rtt = round(float(finished) - float(started), 1)
        entry = {
            "run_id": self._run_id,
            "request_type": request_type,
            "status_code": status_code,
            "request_started_ms": started,
            "response_headers_ms": response_headers_ms,
            "response_finished_ms": finished,
            "response_size": response_size,
            "rtt_ms": rtt,
            "center": center,
            "attempt": attempt,
            "url": url,
        }
        self._network.append(entry)
        if rtt is not None:
            key = f"{request_type}_rtt_ms"
            prev = self._metrics.get(key)
            if not isinstance(prev, (int, float)) or rtt < float(prev):
                self._metrics[key] = rtt

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

    # ── report ──

    def write_run_report(self, *, success: bool) -> Path:
        """Write a short human-readable war report for the current run."""
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORTS_DIR / f"{self._run_id}.txt"
        timeline_lines = []
        for ev in self._timeline:
            t_ms = ev.get("t_ms")
            label = str(ev.get("event", ""))
            if t_ms is None:
                timeline_lines.append(f"{label:<28} (prep)")
            else:
                timeline_lines.append(f"{label:<28} +{t_ms:.0f} ms")

        m = self._metrics
        critical = [
            ("refresh_to_candidate", m.get("refresh_to_first_candidate_ms")),
            ("candidate_to_click", m.get("candidate_to_click_ms")),
            ("click_to_selection", m.get("click_to_selection_ms")),
            ("selection_to_confirm", m.get("selection_to_confirm_ms")),
            ("candidate_to_confirm", m.get("candidate_to_confirm_ms")),
            ("search_rtt", m.get("search_rtt_ms")),
            ("submit_rtt", m.get("submit_rtt_ms")),
        ]
        critical_lines = []
        for name, value in critical:
            if isinstance(value, (int, float)):
                critical_lines.append(f"{name:<28} {value:.0f} ms")

        diagnosis = self._diagnose(success=success)
        body = "\n".join(
            [
                "BOOKBOT RUN REPORT",
                "",
                f"run_id: {self._run_id}",
                f"wall_start: {self._run_date}",
                f"rush_open_wall: {self._rush_start_wall or 'n/a'}",
                f"experiment_id: {self._experiment_id or 'baseline'}",
                f"strategy_version: {self._strategy_version or 'v0'}",
                f"git_commit: {self._git_commit}",
                "",
                "Result:",
                f"{'SUCCESS' if success else 'FAIL'} — {self._failure_class} ({self._primary_reason})",
                "",
                "Timeline:",
                *(timeline_lines or ["(no rush timeline events)"]),
                "",
                "Critical metrics:",
                *(critical_lines or ["(none)"]),
                "",
                "Diagnosis:",
                diagnosis,
                "",
            ]
        )
        path.write_text(body, encoding="utf-8")
        logger.info("Run report written: {}", path)
        return path

    def _diagnose(self, *, success: bool) -> str:
        if success:
            return "Booking succeeded."
        cls = self._failure_class
        if cls == "COMPETITION_LOSS":
            return (
                "Candidate was visible, but reservation conflicted before final confirmation.\n"
                "Likely bottleneck: UI confirmation path or late submit.\n"
                "Recommended experiment: shorten slot→Next path / test hybrid API submit."
            )
        if cls == "NO_INVENTORY":
            return (
                "No acceptable slots were observed in this window.\n"
                "Recommended experiment: verify fire offset and center coverage."
            )
        if cls == "BOT_LATENCY_LOSS":
            return (
                "Slot was visible but automation lagged on click/Next/confirm.\n"
                "Recommended experiment: reduce rush timeouts and enable faster fallback."
            )
        if cls == "AUTOMATION_FAILURE":
            return (
                "Automation failed to complete a UI step.\n"
                "Recommended experiment: inspect candidate_events and screenshots."
            )
        if cls == "TIMING_FAILURE":
            return (
                "Request timing likely missed the inventory open window.\n"
                "Recommended experiment: A/B rush_pre_fire_ms offsets."
            )
        if cls == "PREP_FAILURE":
            return "Prep/login/form readiness failed before the competition window."
        if cls == "SERVER_FAILURE":
            return "Upstream server rejected or blocked the session."
        if cls == "SEARCH_FAILURE":
            return "Search/timetable path failed before candidates could be evaluated."
        return "Insufficient signal to diagnose; inspect feedback events."

    # ── persistence ──

    def _base_record(self, *, success: bool) -> dict[str, Any]:
        return {
            "run_id": self._run_id,
            "timestamp": self._run_date,
            "mode": self._mode,
            "success": success,
            "failure_class": self._failure_class,
            "primary_reason": self._primary_reason,
            "experiment_id": self._experiment_id or "baseline",
            "strategy_version": self._strategy_version or "v0",
            "git_commit": self._git_commit,
            "config_hash": self._config_hash,
        }

    def _write_runtime(self, total: float, rush_total: float | None, success: bool) -> None:
        record = self._base_record(success=success)
        record["total_duration_s"] = round(total, 3)
        if self._mode == "rush" and self._rush_start_mono:
            record["prep_steps"] = self._prep_steps
            record["rush_total_s"] = round(rush_total, 3) if rush_total else 0.0
            record["rush_steps"] = self._rush_steps
            record["timeline"] = self._timeline
            record["rush_open_wall"] = self._rush_start_wall
        else:
            record["steps"] = self._prep_steps + self._rush_steps
            if self._timeline:
                record["timeline"] = self._timeline
        if self._metrics:
            record["metrics"] = self._metrics
        path = LOGS_DIR / "runtime.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _write_feedback(self, success: bool) -> None:
        record = self._base_record(success=success)
        record["events"] = self._feedbacks
        if not success and not self._feedbacks:
            record["events"] = [
                {"reason": "unknown", "detail": "No specific failure reason captured"}
            ]
        path = LOGS_DIR / "feedback.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _write_candidates(self) -> None:
        if not self._candidates:
            return
        path = LOGS_DIR / "candidate_events.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            for row in self._candidates:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _write_network(self) -> None:
        if not self._network:
            return
        path = LOGS_DIR / "network_events.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            for row in self._network:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


tracker = _Tracker()
