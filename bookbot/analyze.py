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
    competition_loss_rate: float
    no_inventory_rate: float
    automation_failure_rate: float
    p90_refresh_to_candidate_ms: float | None
    p90_candidate_to_submit_ms: float | None
    p50_search_rtt_ms: float | None
    p90_search_rtt_ms: float | None
    p95_search_rtt_ms: float | None
    p50_open_to_candidate_ms: float | None
    p90_open_to_candidate_ms: float | None
    p95_open_to_candidate_ms: float | None
    p50_candidate_to_click_ms: float | None
    p90_candidate_to_click_ms: float | None
    p50_click_to_next_ms: float | None
    p90_click_to_next_ms: float | None
    p50_candidate_to_confirm_ms: float | None
    p90_candidate_to_confirm_ms: float | None
    p95_candidate_to_confirm_ms: float | None
    timetable_load_p50_s: float | None
    timetable_load_p90_s: float | None
    timetable_load_p99_s: float | None
    timetable_load_gt_8s_rate: float
    reason_counts: Counter[str]
    class_counts: Counter[str]


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
    return _percentile(values, 90.0)


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    idx = int(round((p / 100.0) * (len(xs) - 1)))
    idx = max(0, min(idx, len(xs) - 1))
    return round(xs[idx], 3)


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


def _metric_vals(runtime_rows: list[dict[str, Any]], key: str) -> list[float]:
    vals: list[float] = []
    for row in runtime_rows:
        metrics = row.get("metrics") or {}
        if not isinstance(metrics, dict):
            continue
        value = metrics.get(key)
        if isinstance(value, (int, float)):
            vals.append(float(value))
    return vals


def summarize(runtime_rows: list[dict[str, Any]], feedback_rows: list[dict[str, Any]]) -> AnalysisSummary:
    runs = len(runtime_rows)
    success = sum(1 for row in runtime_rows if row.get("success"))
    failed = runs - success
    success_rate = (success / runs * 100.0) if runs else 0.0

    reason_counts: Counter[str] = Counter()
    class_counts: Counter[str] = Counter()
    technical_fail = 0
    visible_slots_unbooked = 0
    competition_loss = 0
    no_inventory = 0
    automation_failure = 0
    failed_with_feedback = 0

    feedback_by_id = {
        str(row.get("run_id")): row
        for row in feedback_rows
        if row.get("run_id")
    }

    for row in feedback_rows:
        if row.get("success"):
            continue
        failed_with_feedback += 1
        events = row.get("events") or []
        reasons = {str(e.get("reason", "unknown")) for e in events if isinstance(e, dict)}
        reason_counts.update(reasons)
        failure_class = str(row.get("failure_class") or "UNKNOWN")
        class_counts[failure_class] += 1
        if failure_class == "COMPETITION_LOSS":
            competition_loss += 1
        elif failure_class == "NO_INVENTORY":
            no_inventory += 1
        elif failure_class == "AUTOMATION_FAILURE":
            automation_failure += 1
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

    # Prefer runtime failure_class when feedback lacks it (legacy rows).
    for row in runtime_rows:
        if row.get("success"):
            continue
        fc = row.get("failure_class")
        if fc and str(row.get("run_id")) not in feedback_by_id:
            class_counts[str(fc)] += 1

    technical_fail_rate = (technical_fail / failed_with_feedback * 100.0) if failed_with_feedback else 0.0
    visible_slots_unbooked_rate = (visible_slots_unbooked / failed_with_feedback * 100.0) if failed_with_feedback else 0.0
    competition_loss_rate = (competition_loss / failed_with_feedback * 100.0) if failed_with_feedback else 0.0
    no_inventory_rate = (no_inventory / failed_with_feedback * 100.0) if failed_with_feedback else 0.0
    automation_failure_rate = (automation_failure / failed_with_feedback * 100.0) if failed_with_feedback else 0.0

    refresh_to_candidate_vals = _metric_vals(runtime_rows, "refresh_to_first_candidate_ms")
    candidate_to_submit_vals = _metric_vals(runtime_rows, "first_candidate_to_submit_ms")
    search_rtt_vals = _metric_vals(runtime_rows, "search_rtt_ms")
    candidate_to_click_vals = _metric_vals(runtime_rows, "candidate_to_click_ms")
    click_to_selection_vals = _metric_vals(runtime_rows, "click_to_selection_ms")
    candidate_to_confirm_vals = _metric_vals(runtime_rows, "candidate_to_confirm_ms")

    timetable_load_vals_s: list[float] = []
    for row in runtime_rows:
        rush_steps = row.get("rush_steps") or row.get("steps") or []
        if isinstance(rush_steps, list):
            for st in rush_steps:
                if not isinstance(st, dict):
                    continue
                step_name = str(st.get("step", ""))
                if step_name.startswith("timetable_load|"):
                    dur = st.get("duration_s")
                    if isinstance(dur, (int, float)) and dur > 0:
                        timetable_load_vals_s.append(float(dur))

    gt8 = sum(1 for v in timetable_load_vals_s if v > 8.0)
    gt8_rate = (gt8 / len(timetable_load_vals_s) * 100.0) if timetable_load_vals_s else 0.0

    return AnalysisSummary(
        runs=runs,
        success=success,
        failed=failed,
        success_rate=round(success_rate, 1),
        technical_fail_rate=round(technical_fail_rate, 1),
        visible_slots_unbooked_rate=round(visible_slots_unbooked_rate, 1),
        competition_loss_rate=round(competition_loss_rate, 1),
        no_inventory_rate=round(no_inventory_rate, 1),
        automation_failure_rate=round(automation_failure_rate, 1),
        p90_refresh_to_candidate_ms=_p90(refresh_to_candidate_vals),
        p90_candidate_to_submit_ms=_p90(candidate_to_submit_vals),
        p50_search_rtt_ms=_percentile(search_rtt_vals, 50.0),
        p90_search_rtt_ms=_percentile(search_rtt_vals, 90.0),
        p95_search_rtt_ms=_percentile(search_rtt_vals, 95.0),
        p50_open_to_candidate_ms=_percentile(refresh_to_candidate_vals, 50.0),
        p90_open_to_candidate_ms=_percentile(refresh_to_candidate_vals, 90.0),
        p95_open_to_candidate_ms=_percentile(refresh_to_candidate_vals, 95.0),
        p50_candidate_to_click_ms=_percentile(candidate_to_click_vals, 50.0),
        p90_candidate_to_click_ms=_percentile(candidate_to_click_vals, 90.0),
        p50_click_to_next_ms=_percentile(click_to_selection_vals, 50.0),
        p90_click_to_next_ms=_percentile(click_to_selection_vals, 90.0),
        p50_candidate_to_confirm_ms=_percentile(candidate_to_confirm_vals, 50.0),
        p90_candidate_to_confirm_ms=_percentile(candidate_to_confirm_vals, 90.0),
        p95_candidate_to_confirm_ms=_percentile(candidate_to_confirm_vals, 95.0),
        timetable_load_p50_s=_percentile(timetable_load_vals_s, 50.0),
        timetable_load_p90_s=_percentile(timetable_load_vals_s, 90.0),
        timetable_load_p99_s=_percentile(timetable_load_vals_s, 99.0),
        timetable_load_gt_8s_rate=round(gt8_rate, 1),
        reason_counts=reason_counts,
        class_counts=class_counts,
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
                f" competition_loss_rate={prev.competition_loss_rate:.1f}%"
                f", no_inventory_rate={prev.no_inventory_rate:.1f}%"
                f", visible_slots_unbooked_rate={prev.visible_slots_unbooked_rate:.1f}%"
                f", timetable_load_p90={prev.timetable_load_p90_s if prev.timetable_load_p90_s is not None else 'n/a'}s"
                f", timetable_load_gt8s={prev.timetable_load_gt_8s_rate:.1f}%"
                f", delta_success_rate={delta:+.1f}pp"
            )

    top_reasons = ", ".join(
        f"{k}:{v}" for k, v in current.reason_counts.most_common(8)
    ) or "none"
    top_classes = ", ".join(
        f"{k}:{v}" for k, v in current.class_counts.most_common(8)
    ) or "none"

    def fmt_ms(value: float | None) -> str:
        return f"{value:.1f}ms" if value is not None else "n/a"

    p50_load = f"{current.timetable_load_p50_s:.3f}s" if current.timetable_load_p50_s is not None else "n/a"
    p90_load = f"{current.timetable_load_p90_s:.3f}s" if current.timetable_load_p90_s is not None else "n/a"
    p99_load = f"{current.timetable_load_p99_s:.3f}s" if current.timetable_load_p99_s is not None else "n/a"

    return (
        f"Window: last {days}d\n"
        f"Runs: {current.runs} (success={current.success}, failed={current.failed})\n"
        f"Success rate: {current.success_rate:.1f}%\n"
        f"Technical fail rate: {current.technical_fail_rate:.1f}%\n"
        f"Competition-loss rate: {current.competition_loss_rate:.1f}%\n"
        f"No-inventory rate: {current.no_inventory_rate:.1f}%\n"
        f"Automation-failure rate: {current.automation_failure_rate:.1f}%\n"
        f"Visible-slots-unbooked rate: {current.visible_slots_unbooked_rate:.1f}%\n"
        f"P50/P90/P95 open_to_candidate: "
        f"{fmt_ms(current.p50_open_to_candidate_ms)} / "
        f"{fmt_ms(current.p90_open_to_candidate_ms)} / "
        f"{fmt_ms(current.p95_open_to_candidate_ms)}\n"
        f"P50/P90 candidate_to_click: "
        f"{fmt_ms(current.p50_candidate_to_click_ms)} / "
        f"{fmt_ms(current.p90_candidate_to_click_ms)}\n"
        f"P50/P90 click_to_selection: "
        f"{fmt_ms(current.p50_click_to_next_ms)} / "
        f"{fmt_ms(current.p90_click_to_next_ms)}\n"
        f"P50/P90/P95 candidate_to_confirm: "
        f"{fmt_ms(current.p50_candidate_to_confirm_ms)} / "
        f"{fmt_ms(current.p90_candidate_to_confirm_ms)} / "
        f"{fmt_ms(current.p95_candidate_to_confirm_ms)}\n"
        f"P50/P90/P95 search_rtt: "
        f"{fmt_ms(current.p50_search_rtt_ms)} / "
        f"{fmt_ms(current.p90_search_rtt_ms)} / "
        f"{fmt_ms(current.p95_search_rtt_ms)}\n"
        f"P90 first_candidate_to_submit_ms: {fmt_ms(current.p90_candidate_to_submit_ms)}\n"
        f"Timetable load P50/P90/P99: {p50_load} / {p90_load} / {p99_load}\n"
        f"Timetable load >8s rate: {current.timetable_load_gt_8s_rate:.1f}%\n"
        f"Top failure classes: {top_classes}\n"
        f"Top failure reasons: {top_reasons}"
        f"{prev_text}\n"
    )
