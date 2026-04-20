from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass
class AnalysisSummary:
    runs: int
    success: int
    failed: int
    success_rate: float
    technical_fail_rate: float
    visible_slots_unbooked_rate: float
    p90_refresh_to_candidate_ms: float | None
    p90_candidate_to_submit_ms: float | None
    reason_counts: Counter[str]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _to_dt(text: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _p90(values: list[float]) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    idx = int(round(0.9 * (len(xs) - 1)))
    return round(xs[idx], 1)


def _window_filter(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if days <= 0:
        return rows
    cutoff = datetime.now() - timedelta(days=days)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        ts = _to_dt(str(row.get("timestamp", "")))
        if ts and ts >= cutoff:
            filtered.append(row)
    return filtered


def summarize(runtime_rows: list[dict[str, Any]], feedback_rows: list[dict[str, Any]]) -> AnalysisSummary:
    runs = len(runtime_rows)
    success = sum(1 for row in runtime_rows if row.get("success"))
    failed = runs - success
    success_rate = (success / runs * 100.0) if runs else 0.0

    reason_counts: Counter[str] = Counter()
    technical_fail = 0
    visible_slots_unbooked = 0
    failed_with_feedback = 0

    for row in feedback_rows:
        if row.get("success"):
            continue
        failed_with_feedback += 1
        events = row.get("events") or []
        reasons = {str(e.get("reason", "unknown")) for e in events if isinstance(e, dict)}
        reason_counts.update(reasons)
        if any(r in reasons for r in ("form_not_ready", "exception", "all_attempts_exhausted", "login_failed", "navigation_failed")):
            technical_fail += 1
        has_visible_no_slots = any(
            isinstance(e, dict)
            and e.get("reason") == "no_slots"
            and int(e.get("total_slots", 0) or 0) > 0
            for e in events
        )
        if has_visible_no_slots and "no_bookings_made" in reasons:
            visible_slots_unbooked += 1

    technical_fail_rate = (technical_fail / failed_with_feedback * 100.0) if failed_with_feedback else 0.0
    visible_slots_unbooked_rate = (visible_slots_unbooked / failed_with_feedback * 100.0) if failed_with_feedback else 0.0

    refresh_to_candidate_vals: list[float] = []
    candidate_to_submit_vals: list[float] = []
    for row in runtime_rows:
        metrics = row.get("metrics") or {}
        if not isinstance(metrics, dict):
            continue
        r1 = metrics.get("refresh_to_first_candidate_ms")
        r2 = metrics.get("first_candidate_to_submit_ms")
        if isinstance(r1, (int, float)):
            refresh_to_candidate_vals.append(float(r1))
        if isinstance(r2, (int, float)):
            candidate_to_submit_vals.append(float(r2))

    return AnalysisSummary(
        runs=runs,
        success=success,
        failed=failed,
        success_rate=round(success_rate, 1),
        technical_fail_rate=round(technical_fail_rate, 1),
        visible_slots_unbooked_rate=round(visible_slots_unbooked_rate, 1),
        p90_refresh_to_candidate_ms=_p90(refresh_to_candidate_vals),
        p90_candidate_to_submit_ms=_p90(candidate_to_submit_vals),
        reason_counts=reason_counts,
    )


def analyze_logs(
    runtime_path: Path,
    feedback_path: Path,
    *,
    days: int = 14,
    compare_days: int = 14,
) -> str:
    runtime_rows_all = _read_jsonl(runtime_path)
    feedback_rows_all = _read_jsonl(feedback_path)

    current_runtime = _window_filter(runtime_rows_all, days)
    current_feedback = _window_filter(feedback_rows_all, days)
    current = summarize(current_runtime, current_feedback)

    prev_text = ""
    if compare_days > 0:
        now = datetime.now()
        prev_start = now - timedelta(days=days + compare_days)
        prev_end = now - timedelta(days=days)

        def in_prev(row: dict[str, Any]) -> bool:
            ts = _to_dt(str(row.get("timestamp", "")))
            return bool(ts and prev_start <= ts < prev_end)

        prev_runtime = [r for r in runtime_rows_all if in_prev(r)]
        prev_feedback = [r for r in feedback_rows_all if in_prev(r)]
        prev = summarize(prev_runtime, prev_feedback)
        if prev.runs > 0:
            delta = round(current.success_rate - prev.success_rate, 1)
            prev_text = (
                f"\nPrevious {compare_days}d: runs={prev.runs}, success_rate={prev.success_rate:.1f}%"
                f", technical_fail_rate={prev.technical_fail_rate:.1f}%,"
                f" visible_slots_unbooked_rate={prev.visible_slots_unbooked_rate:.1f}%"
                f", delta_success_rate={delta:+.1f}pp"
            )

    top_reasons = ", ".join(
        f"{k}:{v}" for k, v in current.reason_counts.most_common(8)
    ) or "none"

    p90_refresh = (
        f"{current.p90_refresh_to_candidate_ms:.1f}ms"
        if current.p90_refresh_to_candidate_ms is not None else "n/a"
    )
    p90_submit = (
        f"{current.p90_candidate_to_submit_ms:.1f}ms"
        if current.p90_candidate_to_submit_ms is not None else "n/a"
    )

    return (
        f"Window: last {days}d\n"
        f"Runs: {current.runs} (success={current.success}, failed={current.failed})\n"
        f"Success rate: {current.success_rate:.1f}%\n"
        f"Technical fail rate: {current.technical_fail_rate:.1f}%\n"
        f"Visible-slots-unbooked rate: {current.visible_slots_unbooked_rate:.1f}%\n"
        f"P90 refresh_to_first_candidate_ms: {p90_refresh}\n"
        f"P90 first_candidate_to_submit_ms: {p90_submit}\n"
        f"Top failure reasons: {top_reasons}"
        f"{prev_text}\n"
    )
