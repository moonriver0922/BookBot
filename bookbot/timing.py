"""Rush open-window timing helpers and offset recommendation.

Boundary offsets are signed relative to the official open instant (T0):
  - negative => fire before open (e.g. -200 == T-200ms)
  - zero     => fire at open
  - positive => fire after open (e.g. 200 == T+200ms)
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


DEFAULT_BOUNDARY_OFFSETS_MS = (-200, 0, 200, 500)
MAX_BOUNDARY_PROBES = 5
PRE_FIRE_MIN_MS = 0
PRE_FIRE_MAX_MS = 500


@dataclass
class OffsetStats:
    offset_ms: int
    runs: int = 0
    success: int = 0
    inventory_open: int = 0
    open_to_candidate_vals: list[float] | None = None

    def __post_init__(self) -> None:
        if self.open_to_candidate_vals is None:
            self.open_to_candidate_vals = []

    @property
    def success_rate(self) -> float:
        return (self.success / self.runs * 100.0) if self.runs else 0.0

    @property
    def inventory_open_rate(self) -> float:
        return (self.inventory_open / self.runs * 100.0) if self.runs else 0.0


def normalize_boundary_offsets(
    offsets: list[int] | tuple[int, ...] | None,
    *,
    fallback_pre_fire_ms: int = 0,
    enabled: bool = True,
    max_probes: int = MAX_BOUNDARY_PROBES,
) -> list[int]:
    """Return a sorted, de-duplicated, capped list of signed fire offsets."""
    if not enabled:
        # Legacy rush_pre_fire_ms is "ms before open" (positive => early).
        early = max(0, int(fallback_pre_fire_ms))
        return [-early] if early else [0]

    raw = list(offsets) if offsets else list(DEFAULT_BOUNDARY_OFFSETS_MS)
    cleaned: list[int] = []
    seen: set[int] = set()
    for value in raw:
        try:
            offset = int(value)
        except Exception:
            continue
        if offset in seen:
            continue
        seen.add(offset)
        cleaned.append(offset)

    if not cleaned:
        cleaned = list(DEFAULT_BOUNDARY_OFFSETS_MS)

    cleaned.sort()
    if len(cleaned) > max_probes:
        # Keep earliest, open, and latest representatives when trimming.
        head = cleaned[: max_probes - 1]
        if cleaned[-1] not in head:
            head.append(cleaned[-1])
        cleaned = sorted(set(head))[:max_probes]
    return cleaned


def open_datetime(
    rush_time: tuple[int, int, int],
    *,
    now: datetime | None = None,
    server_delta_ms: float = 0.0,
) -> datetime:
    """Build the server-adjusted open datetime for today's rush window."""
    base = (now or datetime.now()) + timedelta(milliseconds=server_delta_ms)
    return base.replace(
        hour=rush_time[0],
        minute=rush_time[1],
        second=rush_time[2],
        microsecond=0,
    )


def seconds_until_offset(
    rush_time: tuple[int, int, int],
    offset_ms: int,
    *,
    now: datetime | None = None,
    server_delta_ms: float = 0.0,
) -> float:
    """Seconds from now until open+offset under server-adjusted clock."""
    adjusted_now = (now or datetime.now()) + timedelta(milliseconds=server_delta_ms)
    target = open_datetime(rush_time, now=now, server_delta_ms=server_delta_ms)
    target = target + timedelta(milliseconds=int(offset_ms))
    return (target - adjusted_now).total_seconds()


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


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    idx = int(round((p / 100.0) * (len(xs) - 1)))
    idx = max(0, min(idx, len(xs) - 1))
    return round(xs[idx], 1)


def summarize_fire_offsets(
    runtime_path: Path,
    *,
    days: int = 14,
) -> dict[int, OffsetStats]:
    """Aggregate rush runs by the offset that first observed inventory/candidate."""
    rows = _read_jsonl(runtime_path)
    if days > 0:
        cutoff = datetime.now() - timedelta(days=days)
        rows = [
            r for r in rows
            if (ts := _to_dt(str(r.get("timestamp", "")))) is not None and ts >= cutoff
        ]

    by_offset: dict[int, OffsetStats] = {}
    for row in rows:
        if str(row.get("mode")) != "rush":
            continue
        metrics = row.get("metrics") or {}
        if not isinstance(metrics, dict):
            metrics = {}
        hit = metrics.get("boundary_hit_offset_ms")
        if not isinstance(hit, (int, float)):
            # Fall back to configured primary offset / legacy pre-fire.
            configured = metrics.get("configured_fire_offset_ms")
            if isinstance(configured, (int, float)):
                # Legacy positive pre-fire => signed negative early offset.
                hit = -int(configured) if int(configured) > 0 else int(configured)
            else:
                hit = 0
        offset = int(hit)
        stats = by_offset.setdefault(offset, OffsetStats(offset_ms=offset))
        stats.runs += 1
        if row.get("success"):
            stats.success += 1
        candidate = metrics.get("refresh_to_first_candidate_ms")
        if isinstance(candidate, (int, float)):
            stats.inventory_open += 1
            assert stats.open_to_candidate_vals is not None
            stats.open_to_candidate_vals.append(float(candidate))
        elif metrics.get("visible_slots_unbooked") or metrics.get("slots_seen_total"):
            slots = metrics.get("slots_seen_total")
            if isinstance(slots, (int, float)) and slots > 0:
                stats.inventory_open += 1
    return dict(sorted(by_offset.items(), key=lambda kv: kv[0]))


def recommend_pre_fire_ms(
    runtime_path: Path,
    *,
    days: int = 14,
    candidates: list[int] | None = None,
) -> dict[str, Any]:
    """Recommend a primary early offset from historical rush outcomes.

    Returns a dict with ``recommended_pre_fire_ms`` using the legacy convention
    (positive = fire before open), plus supporting stats.
    """
    stats = summarize_fire_offsets(runtime_path, days=days)
    allowed = candidates or [-300, -200, -100, 0, 100, 200]
    scored: list[tuple[float, int, OffsetStats]] = []
    for offset in allowed:
        st = stats.get(offset) or OffsetStats(offset_ms=offset)
        # Prefer higher success, then inventory hit rate, then faster candidate.
        p50 = _percentile(st.open_to_candidate_vals or [], 50.0)
        speed_score = 0.0 if p50 is None else max(0.0, 3000.0 - p50) / 3000.0
        score = (
            st.success_rate * 2.0
            + st.inventory_open_rate
            + speed_score * 20.0
            + min(st.runs, 10)  # small sample-size bonus
        )
        scored.append((score, offset, st))

    scored.sort(key=lambda x: (-x[0], abs(x[1]), x[1]))
    best_score, best_offset, best_stats = scored[0]
    # Convert signed offset -> legacy pre_fire (early only, clamped).
    recommended_pre_fire = 0 if best_offset >= 0 else min(PRE_FIRE_MAX_MS, -best_offset)
    recommended_pre_fire = max(PRE_FIRE_MIN_MS, recommended_pre_fire)

    return {
        "recommended_pre_fire_ms": recommended_pre_fire,
        "recommended_signed_offset_ms": best_offset,
        "score": round(best_score, 2),
        "runs": best_stats.runs,
        "success_rate": round(best_stats.success_rate, 1),
        "inventory_open_rate": round(best_stats.inventory_open_rate, 1),
        "p50_open_to_candidate_ms": _percentile(best_stats.open_to_candidate_vals or [], 50.0),
        "by_offset": {
            str(offset): {
                "runs": st.runs,
                "success_rate": round(st.success_rate, 1),
                "inventory_open_rate": round(st.inventory_open_rate, 1),
                "p50_open_to_candidate_ms": _percentile(st.open_to_candidate_vals or [], 50.0),
            }
            for offset, st in stats.items()
        },
    }


def format_timing_report(runtime_path: Path, *, days: int = 14) -> str:
    """Render a human-readable timing/offset recommendation report."""
    recommendation = recommend_pre_fire_ms(runtime_path, days=days)
    lines = [
        f"Timing window: last {days}d",
        "",
        f"Recommended rush_pre_fire_ms: {recommendation['recommended_pre_fire_ms']}",
        f"Best signed offset: {recommendation['recommended_signed_offset_ms']} ms",
        f"Support: runs={recommendation['runs']}, "
        f"success_rate={recommendation['success_rate']}%, "
        f"inventory_open_rate={recommendation['inventory_open_rate']}%",
        "",
        "Offset | Runs | SuccessRate | InventoryOpen | P50 open→candidate",
        "--- | ---: | ---: | ---: | ---:",
    ]
    by_offset = recommendation.get("by_offset") or {}
    if not by_offset:
        lines.append("(no rush offset samples yet)")
    else:
        for key in sorted(by_offset.keys(), key=lambda k: int(k)):
            row = by_offset[key]
            p50 = row.get("p50_open_to_candidate_ms")
            p50_text = f"{p50:.1f}ms" if isinstance(p50, (int, float)) else "n/a"
            lines.append(
                f"{key} | {row['runs']} | {row['success_rate']:.1f}% | "
                f"{row['inventory_open_rate']:.1f}% | {p50_text}"
            )
    lines.append("")
    lines.append(
        "Use one primary experiment_id per offset change; keep boundary probes small "
        f"(<= {MAX_BOUNDARY_PROBES})."
    )
    return "\n".join(lines) + "\n"


def group_probe_events(events: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    """Group timeline probe events by offset_ms."""
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        if not isinstance(event, dict):
            continue
        if event.get("event") != "boundary_probe_fired":
            continue
        offset = event.get("offset_ms")
        if isinstance(offset, (int, float)):
            grouped[int(offset)].append(event)
    return dict(grouped)
