"""Tests for bounded adaptive recommendations."""
from __future__ import annotations

import json
from pathlib import Path

from bookbot.adaptive import (
    compute_adaptive_recommendations,
    recommendations_to_tuning_actions,
)
from bookbot.timing import MAX_BOUNDARY_PROBES, PRE_FIRE_MAX_MS


def _write_rows(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


def test_adaptive_not_ready_with_few_runs(tmp_path: Path):
    runtime = tmp_path / "runtime.jsonl"
    _write_rows(
        runtime,
        [
            {
                "timestamp": "2026-09-10 08:30:01",
                "mode": "rush",
                "success": False,
                "metrics": {"boundary_hit_offset_ms": -200},
            }
        ],
    )
    rec = compute_adaptive_recommendations(runtime, days=30, min_runs=5)
    assert rec.ready is False
    assert recommendations_to_tuning_actions(rec) == []


def test_adaptive_ready_and_capped(tmp_path: Path):
    runtime = tmp_path / "runtime.jsonl"
    feedback = tmp_path / "feedback.jsonl"
    rows = []
    for i in range(6):
        rows.append(
            {
                "timestamp": f"2026-09-{10 - i:02d} 08:30:01",
                "mode": "rush",
                "success": i % 2 == 0,
                "metrics": {
                    "boundary_hit_offset_ms": -200 if i % 2 == 0 else 200,
                    "refresh_to_first_candidate_ms": 700 + i * 10,
                    "click_to_selection_ms": 120,
                    "selection_to_confirm_ms": 400,
                    "candidate_to_confirm_ms": 900,
                },
            }
        )
    _write_rows(runtime, rows)
    _write_rows(
        feedback,
        [
            {
                "timestamp": "2026-09-10 08:30:01",
                "success": True,
                "events": [{"reason": "booked", "center": "Shaw Sports Complex"}],
            },
            {
                "timestamp": "2026-09-09 08:30:01",
                "success": False,
                "events": [{"reason": "booking_conflict", "center": "Other Center"}],
            },
        ],
    )

    rec = compute_adaptive_recommendations(runtime, feedback, days=30, min_runs=5)
    assert rec.ready is True
    assert 0 <= rec.recommended_pre_fire_ms <= PRE_FIRE_MAX_MS
    assert len(rec.recommended_boundary_offsets_ms) <= MAX_BOUNDARY_PROBES
    assert 0 in rec.recommended_boundary_offsets_ms
    assert rec.recommended_centers[0] == "Shaw Sports Complex"

    actions = recommendations_to_tuning_actions(rec, max_actions=4)
    keys = [a[0] for a in actions]
    assert "settings.rush_pre_fire_ms" in keys
    assert "settings.rush_boundary_offsets_ms" in keys
    assert all(a[1][0] == "set" for a in actions)
