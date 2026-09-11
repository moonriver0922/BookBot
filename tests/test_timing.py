"""Tests for rush boundary timing helpers."""
from __future__ import annotations

import json
from pathlib import Path

from bookbot.timing import (
    format_timing_report,
    normalize_boundary_offsets,
    recommend_pre_fire_ms,
    seconds_until_offset,
)


def test_normalize_boundary_offsets_caps_and_sorts():
    offsets = normalize_boundary_offsets([500, -200, 0, 200, -200, 900], max_probes=5)
    assert offsets == sorted(set(offsets))
    assert len(offsets) <= 5
    assert -200 in offsets
    assert 0 in offsets


def test_normalize_boundary_disabled_uses_legacy_pre_fire():
    assert normalize_boundary_offsets([-200, 0], enabled=False, fallback_pre_fire_ms=150) == [-150]
    assert normalize_boundary_offsets(None, enabled=False, fallback_pre_fire_ms=0) == [0]


def test_seconds_until_offset_sign():
    from datetime import datetime

    now = datetime(2026, 9, 11, 8, 29, 59, 800_000)
    early = seconds_until_offset((8, 30, 0), -200, now=now, server_delta_ms=0)
    late = seconds_until_offset((8, 30, 0), 200, now=now, server_delta_ms=0)
    assert early < late
    assert abs(early - 0.0) < 0.05  # ~200ms before open from 8:29:59.800


def test_recommend_pre_fire_prefers_successful_offset(tmp_path: Path):
    runtime = tmp_path / "runtime.jsonl"
    rows = [
        {
            "timestamp": "2026-09-10 08:30:01",
            "mode": "rush",
            "success": True,
            "metrics": {
                "boundary_hit_offset_ms": -200,
                "refresh_to_first_candidate_ms": 700,
            },
        },
        {
            "timestamp": "2026-09-09 08:30:01",
            "mode": "rush",
            "success": False,
            "metrics": {
                "boundary_hit_offset_ms": 200,
                "refresh_to_first_candidate_ms": 1200,
            },
        },
        {
            "timestamp": "2026-09-08 08:30:01",
            "mode": "rush",
            "success": True,
            "metrics": {
                "boundary_hit_offset_ms": -200,
                "refresh_to_first_candidate_ms": 650,
            },
        },
    ]
    runtime.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    rec = recommend_pre_fire_ms(runtime, days=30, candidates=[-200, 0, 200])
    assert rec["recommended_signed_offset_ms"] == -200
    assert rec["recommended_pre_fire_ms"] == 200
    report = format_timing_report(runtime, days=30)
    assert "Recommended rush_pre_fire_ms: 200" in report
