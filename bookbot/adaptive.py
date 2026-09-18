"""Bounded adaptive recommendations for rush timing and budgets.

Hard caps prevent request flooding or unbounded timeout growth.
Recommendations are evidence-driven from runtime/feedback JSONL.
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from bookbot.timing import (
    DEFAULT_BOUNDARY_OFFSETS_MS,
    MAX_BOUNDARY_PROBES,
    PRE_FIRE_MAX_MS,
    PRE_FIRE_MIN_MS,
    normalize_boundary_offsets,
    recommend_pre_fire_ms,
    summarize_fire_offsets,
)

MIN_RUNS_FOR_ADAPT = 5
ALLOWED_BOUNDARY_CANDIDATES = (-300, -200, -100, 0, 100, 200, 500)

TIMEOUT_CAPS = {
    "rush_slot_select_timeout_ms": (100, 500),
    "rush_confirm_page_timeout_ms": (300, 1500),
    "rush_confirm_result_timeout_ms": (500, 6000),
}


@dataclass
class AdaptiveRecommendation:
    ready: bool
    reason: str
    runs: int = 0
    recommended_pre_fire_ms: int = 0
    recommended_signed_offset_ms: int = 0
    recommended_boundary_offsets_ms: list[int] = field(
        default_factory=lambda: list(DEFAULT_BOUNDARY_OFFSETS_MS)
    )
    recommended_slot_select_timeout_ms: int | None = None
    recommended_confirm_page_timeout_ms: int | None = None
    recommended_confirm_result_timeout_ms: int | None = None
    recommended_centers: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "reason": self.reason,
            "runs": self.runs,
            "recommended_pre_fire_ms": self.recommended_pre_fire_ms,
            "recommended_signed_offset_ms": self.recommended_signed_offset_ms,
            "recommended_boundary_offsets_ms": list(self.recommended_boundary_offsets_ms),
            "recommended_slot_select_timeout_ms": self.recommended_slot_select_timeout_ms,
            "recommended_confirm_page_timeout_ms": self.recommended_confirm_page_timeout_ms,
            "recommended_confirm_result_timeout_ms": self.recommended_confirm_result_timeout_ms,
            "recommended_centers": list(self.recommended_centers),
            "notes": list(self.notes),
        }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _to_dt(text: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _window(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if days <= 0:
        return rows
    cutoff = datetime.now() - timedelta(days=days)
    out: list[dict[str, Any]] = []
    for row in rows:
        ts = _to_dt(str(row.get("timestamp", "")))
        if ts and ts >= cutoff:
            out.append(row)
    return out


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    idx = int(round((p / 100.0) * (len(xs) - 1)))
    idx = max(0, min(idx, len(xs) - 1))
    return float(xs[idx])


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, int(value)))


def _metric_vals(rows: list[dict[str, Any]], key: str) -> list[float]:
    vals: list[float] = []
    for row in rows:
        metrics = row.get("metrics") or {}
        if not isinstance(metrics, dict):
            continue
        value = metrics.get(key)
        if isinstance(value, (int, float)):
            vals.append(float(value))
    return vals


def _recommend_timeout(
    values: list[float],
    *,
    low: int,
    high: int,
    pad_ms: int,
    fallback: int,
) -> int:
    p90 = _percentile(values, 90.0)
    if p90 is None:
        return _clamp(fallback, low, high)
    # Budget = P90 + small pad, then clamp.
    return _clamp(int(round(p90 + pad_ms)), low, high)


def _recommend_boundary_offsets(runtime_path: Path, *, days: int) -> list[int]:
    stats = summarize_fire_offsets(runtime_path, days=days)
    scored: list[tuple[float, int]] = []
    for offset in ALLOWED_BOUNDARY_CANDIDATES:
        st = stats.get(offset)
        if st is None or st.runs <= 0:
            # Keep a mild prior toward the default set.
            prior = 1.0 if offset in DEFAULT_BOUNDARY_OFFSETS_MS else 0.0
            scored.append((prior, offset))
            continue
        p50 = _percentile(st.open_to_candidate_vals or [], 50.0)
        speed = 0.0 if p50 is None else max(0.0, 3000.0 - p50) / 3000.0
        score = st.success_rate * 2.0 + st.inventory_open_rate + speed * 15.0 + min(st.runs, 8)
        scored.append((score, offset))
    scored.sort(key=lambda x: (-x[0], abs(x[1]), x[1]))

    chosen: list[int] = [0]  # always include open
    for _, offset in scored:
        if offset not in chosen:
            chosen.append(offset)
        if len(chosen) >= MAX_BOUNDARY_PROBES:
            break
    return normalize_boundary_offsets(chosen, enabled=True, max_probes=MAX_BOUNDARY_PROBES)


def _recommend_centers(feedback_rows: list[dict[str, Any]]) -> list[str]:
    booked: Counter[str] = Counter()
    conflicts: Counter[str] = Counter()
    for row in feedback_rows:
        events = row.get("events") or []
        for event in events:
            if not isinstance(event, dict):
                continue
            center = str(event.get("center") or "").strip()
            if not center:
                continue
            reason = str(event.get("reason") or "")
            if reason in {"booked", "api_booked"}:
                booked[center] += 1
            elif reason in {"booking_conflict", "api_step_failed"}:
                conflicts[center] += 1
    scores: dict[str, float] = {}
    for center in set(booked) | set(conflicts):
        scores[center] = booked[center] * 2.0 - conflicts[center] * 0.5
    return [c for c, _ in sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))]


def compute_adaptive_recommendations(
    runtime_path: Path,
    feedback_path: Path | None = None,
    *,
    days: int = 14,
    min_runs: int = MIN_RUNS_FOR_ADAPT,
) -> AdaptiveRecommendation:
    """Compute capped adaptive recommendations from recent rush history."""
    runtime_rows = [
        r for r in _window(_read_jsonl(runtime_path), days)
        if str(r.get("mode")) == "rush"
    ]
    feedback_rows = _window(_read_jsonl(feedback_path), days) if feedback_path else []

    if len(runtime_rows) < min_runs:
        return AdaptiveRecommendation(
            ready=False,
            reason=f"need>={min_runs} rush runs, have={len(runtime_rows)}",
            runs=len(runtime_rows),
        )

    fire = recommend_pre_fire_ms(runtime_path, days=days)
    pre_fire = _clamp(int(fire["recommended_pre_fire_ms"]), PRE_FIRE_MIN_MS, PRE_FIRE_MAX_MS)
    signed = int(fire["recommended_signed_offset_ms"])
    offsets = _recommend_boundary_offsets(runtime_path, days=days)

    click_vals = _metric_vals(runtime_rows, "click_to_selection_ms")
    confirm_page_proxy = _metric_vals(runtime_rows, "selection_to_confirm_ms")
    confirm_vals = _metric_vals(runtime_rows, "candidate_to_confirm_ms")

    slot_timeout = _recommend_timeout(
        click_vals,
        low=TIMEOUT_CAPS["rush_slot_select_timeout_ms"][0],
        high=TIMEOUT_CAPS["rush_slot_select_timeout_ms"][1],
        pad_ms=50,
        fallback=200,
    )
    confirm_page_timeout = _recommend_timeout(
        confirm_page_proxy,
        low=TIMEOUT_CAPS["rush_confirm_page_timeout_ms"][0],
        high=TIMEOUT_CAPS["rush_confirm_page_timeout_ms"][1],
        pad_ms=100,
        fallback=800,
    )
    confirm_result_timeout = _recommend_timeout(
        confirm_vals,
        low=TIMEOUT_CAPS["rush_confirm_result_timeout_ms"][0],
        high=TIMEOUT_CAPS["rush_confirm_result_timeout_ms"][1],
        pad_ms=150,
        fallback=1500,
    )

    centers = _recommend_centers(feedback_rows)
    notes = [
        f"pre_fire from signed offset {signed}ms",
        f"boundary probes capped at {MAX_BOUNDARY_PROBES}",
        "timeouts derived from P90 latency + pad, then clamped",
    ]
    if fire.get("runs", 0) < min_runs:
        notes.append("offset recommendation has limited support; keep defaults if unstable")

    return AdaptiveRecommendation(
        ready=True,
        reason="ok",
        runs=len(runtime_rows),
        recommended_pre_fire_ms=pre_fire,
        recommended_signed_offset_ms=signed,
        recommended_boundary_offsets_ms=offsets,
        recommended_slot_select_timeout_ms=slot_timeout,
        recommended_confirm_page_timeout_ms=confirm_page_timeout,
        recommended_confirm_result_timeout_ms=confirm_result_timeout,
        recommended_centers=centers,
        notes=notes,
    )


def recommendations_to_tuning_actions(
    rec: AdaptiveRecommendation,
    *,
    max_actions: int = 4,
) -> list[tuple[str, Any, str]]:
    """Convert recommendations into whitelist auto_tuning actions."""
    if not rec.ready:
        return []
    actions: list[tuple[str, Any, str]] = [
        (
            "settings.rush_pre_fire_ms",
            ("set", rec.recommended_pre_fire_ms),
            "Adaptive primary pre-fire from historical offset performance",
        ),
        (
            "settings.rush_boundary_offsets_ms",
            ("set", list(rec.recommended_boundary_offsets_ms)),
            "Adaptive bounded open-window probe offsets",
        ),
    ]
    if rec.recommended_slot_select_timeout_ms is not None:
        actions.append(
            (
                "settings.rush_slot_select_timeout_ms",
                ("set", rec.recommended_slot_select_timeout_ms),
                "Adaptive slot→Next timeout from P90 click_to_selection",
            )
        )
    if rec.recommended_confirm_page_timeout_ms is not None:
        actions.append(
            (
                "settings.rush_confirm_page_timeout_ms",
                ("set", rec.recommended_confirm_page_timeout_ms),
                "Adaptive Next→confirm timeout from P90 selection_to_confirm",
            )
        )
    if rec.recommended_confirm_result_timeout_ms is not None:
        actions.append(
            (
                "settings.rush_confirm_result_timeout_ms",
                ("set", rec.recommended_confirm_result_timeout_ms),
                "Adaptive confirm-result timeout from P90 candidate_to_confirm",
            )
        )
    if rec.recommended_centers:
        actions.append(
            (
                "preferences.centers",
                ("set", list(rec.recommended_centers)),
                "Adaptive center priority from booked vs conflict outcomes",
            )
        )
    return actions[:max_actions]


def format_adaptive_report(
    runtime_path: Path,
    feedback_path: Path | None = None,
    *,
    days: int = 14,
) -> str:
    """Render a human-readable adaptive recommendation report."""
    rec = compute_adaptive_recommendations(runtime_path, feedback_path, days=days)
    lines = [
        f"Adaptive window: last {days}d",
        f"Ready: {rec.ready} ({rec.reason})",
        f"Rush runs: {rec.runs}",
        "",
    ]
    if not rec.ready:
        lines.append("Not enough samples for safe auto-tuning yet.")
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            f"recommended_pre_fire_ms: {rec.recommended_pre_fire_ms}",
            f"recommended_signed_offset_ms: {rec.recommended_signed_offset_ms}",
            f"recommended_boundary_offsets_ms: {rec.recommended_boundary_offsets_ms}",
            f"recommended_slot_select_timeout_ms: {rec.recommended_slot_select_timeout_ms}",
            f"recommended_confirm_page_timeout_ms: {rec.recommended_confirm_page_timeout_ms}",
            f"recommended_confirm_result_timeout_ms: {rec.recommended_confirm_result_timeout_ms}",
            f"recommended_centers: {rec.recommended_centers or ['(unchanged)']}",
            "",
            "Notes:",
            *[f"- {n}" for n in rec.notes],
            "",
            "Caps:",
            f"- pre_fire: {PRE_FIRE_MIN_MS}..{PRE_FIRE_MAX_MS} ms",
            f"- boundary probes: <= {MAX_BOUNDARY_PROBES}",
            f"- slot_select: {TIMEOUT_CAPS['rush_slot_select_timeout_ms']}",
            f"- confirm_page: {TIMEOUT_CAPS['rush_confirm_page_timeout_ms']}",
            f"- confirm_result: {TIMEOUT_CAPS['rush_confirm_result_timeout_ms']}",
        ]
    )
    return "\n".join(lines) + "\n"
