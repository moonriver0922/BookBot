"""Unit tests for rush selection, failure taxonomy, and tracker join keys."""
from __future__ import annotations

import json
from pathlib import Path

from bookbot.booker import TimeSlot, find_best_booking, find_rush_booking
from bookbot.config import AppConfig
from bookbot.failures import classify_run
from bookbot.tracker import _Tracker


def _slots() -> list[TimeSlot]:
    return [
        TimeSlot("08:30", "09:30", "Shaw Sports Complex", available=True),
        TimeSlot("09:30", "10:30", "Shaw Sports Complex", available=True),
        TimeSlot("10:30", "11:30", "Shaw Sports Complex", available=True),
        TimeSlot("15:30", "16:30", "Shaw Sports Complex", available=True),
    ]


def test_rush_rejects_before_min_start_and_picks_first_acceptable():
    config = AppConfig()
    config.preferences.time_range.start = "00:00"
    config.preferences.time_range.end = "23:59"
    config.preferences.min_slot_start = "09:30"
    config.settings.rush_prefer_consecutive = 1

    chosen = find_rush_booking(_slots(), remaining_quota=4, config=config)
    assert len(chosen) == 1
    assert chosen[0].start == "09:30"


def test_rush_does_not_prefer_afternoon_over_earlier_acceptable():
    config = AppConfig()
    config.preferences.time_range.start = "00:00"
    config.preferences.time_range.end = "23:59"
    config.settings.rush_prefer_consecutive = 1

    chosen = find_best_booking(_slots(), remaining_quota=4, config=config, rush=True)
    assert chosen[0].start == "09:30"


def test_rush_consecutive_only_when_configured():
    config = AppConfig()
    config.preferences.time_range.start = "00:00"
    config.preferences.time_range.end = "23:59"
    config.settings.rush_prefer_consecutive = 2

    chosen = find_rush_booking(_slots(), remaining_quota=4, config=config)
    assert len(chosen) == 2
    assert chosen[0].start == "09:30"
    assert chosen[1].start == "10:30"


def test_classify_competition_vs_no_inventory():
    cls, reason = classify_run(
        success=False,
        events=[{"reason": "booking_conflict"}, {"reason": "no_bookings_made"}],
        metrics={"refresh_to_first_candidate_ms": 800},
    )
    assert cls == "COMPETITION_LOSS"
    assert reason == "booking_conflict"

    cls2, reason2 = classify_run(
        success=False,
        events=[{"reason": "no_slots"}, {"reason": "no_bookings_made"}],
        metrics={},
    )
    assert cls2 == "NO_INVENTORY"
    assert reason2 == "no_slots"


def test_tracker_writes_run_id_across_logs(tmp_path: Path, monkeypatch):
    tracker = _Tracker()
    monkeypatch.setattr("bookbot.tracker.LOGS_DIR", tmp_path)
    monkeypatch.setattr("bookbot.tracker.REPORTS_DIR", tmp_path / "reports")

    run_id = tracker.start_run(mode="rush", experiment_id="exp-a", strategy_version="v-test")
    tracker.mark_rush_start()
    tracker.mark_event("search_request_fired")
    cand = tracker.start_candidate(
        center="Shaw",
        date="2026-09-18",
        start="09:30",
        end="10:30",
    )
    tracker.update_candidate(cand, "click_started_ms")
    tracker.finish_candidate(cand, "conflict")
    tracker.add_feedback("booking_conflict", center="Shaw")
    report = tracker.finish_run(success=False)

    runtime = json.loads((tmp_path / "runtime.jsonl").read_text(encoding="utf-8").strip())
    feedback = json.loads((tmp_path / "feedback.jsonl").read_text(encoding="utf-8").strip())
    candidate = json.loads((tmp_path / "candidate_events.jsonl").read_text(encoding="utf-8").strip())

    assert run_id
    assert runtime["run_id"] == run_id
    assert feedback["run_id"] == run_id
    assert candidate["run_id"] == run_id
    assert runtime["failure_class"] == "COMPETITION_LOSS"
    assert feedback["failure_class"] == "COMPETITION_LOSS"
    assert report is not None
    assert report.exists()
    assert "COMPETITION_LOSS" in report.read_text(encoding="utf-8")
