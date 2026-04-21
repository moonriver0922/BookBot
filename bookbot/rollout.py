from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass
class ModeSummary:
    runs: int = 0
    success: int = 0
    p95_total_s: float = 0.0
    p95_submit_ms: float = 0.0


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def _to_dt(value: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _p(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    idx = int(round((percentile / 100.0) * (len(xs) - 1)))
    idx = max(0, min(len(xs) - 1, idx))
    return float(xs[idx])


def summarize_rollout(runtime_path: Path, *, days: int = 14) -> str:
    rows = _read_jsonl(runtime_path)
    if days > 0:
        cutoff = datetime.now() - timedelta(days=days)
        rows = [
            r for r in rows
            if (ts := _to_dt(str(r.get("timestamp", "")))) is not None and ts >= cutoff
        ]

    by_mode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        metrics = row.get("metrics") or {}
        if not isinstance(metrics, dict):
            metrics = {}
        mode = str(metrics.get("booking_mode") or row.get("mode") or "unknown")
        by_mode[mode].append(row)

    lines = [f"Rollout window: last {days}d", ""]
    if not by_mode:
        lines.append("No runtime rows found for the selected window.")
        return "\n".join(lines) + "\n"

    lines.append("Mode | Runs | SuccessRate | P95 total(s) | P95 first_candidate_to_submit(ms)")
    lines.append("--- | ---: | ---: | ---: | ---:")
    for mode in sorted(by_mode.keys()):
        mode_rows = by_mode[mode]
        total_vals: list[float] = []
        submit_vals: list[float] = []
        success = 0
        for row in mode_rows:
            if row.get("success"):
                success += 1
            td = row.get("total_duration_s")
            if isinstance(td, (int, float)):
                total_vals.append(float(td))
            metrics = row.get("metrics") or {}
            if isinstance(metrics, dict):
                submit_ms = metrics.get("first_candidate_to_submit_ms")
                if isinstance(submit_ms, (int, float)):
                    submit_vals.append(float(submit_ms))
        runs = len(mode_rows)
        success_rate = (success / runs * 100.0) if runs else 0.0
        lines.append(
            f"{mode} | {runs} | {success_rate:.1f}% | {_p(total_vals, 95):.2f} | {_p(submit_vals, 95):.1f}"
        )

    lines.append("")
    lines.append("Gray rollout suggestion:")
    lines.append("- Keep `booking_mode: ui` as baseline.")
    lines.append("- Enable `booking_mode: hybrid` for canary runs and compare this report daily.")
    lines.append("- Promote to `booking_mode: api` only when hybrid success rate is stable and fallback frequency is low.")
    return "\n".join(lines) + "\n"
