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


def test_monday_weekday_window_blocks_afternoon_in_rush():
    from datetime import date

    from bookbot.config import TimeRange

    config = AppConfig()
    config.preferences.min_slot_start = "09:30"
    config.preferences.weekday_time_ranges = {
        0: TimeRange(start="09:30", end="12:30"),
    }
    config.settings.rush_prefer_consecutive = 1
    monday = date(2026, 9, 14)
    assert monday.weekday() == 0

    slots = [
        TimeSlot("09:30", "10:30", "Shaw Sports Complex", available=True),
        TimeSlot("15:30", "16:30", "Shaw Sports Complex", available=True),
    ]
    chosen = find_rush_booking(slots, remaining_quota=4, config=config, target=monday)
    assert len(chosen) == 1
    assert chosen[0].start == "09:30"

    saturday = date(2026, 9, 19)
    chosen_sat = find_rush_booking(slots, remaining_quota=4, config=config, target=saturday)
    assert chosen_sat[0].start == "09:30"  # still first-acceptable, not afternoon preference

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


def test_classify_automation_beats_ambiguous_competition():
    cls, reason = classify_run(
        success=False,
        events=[
            {"reason": "js_click_not_registered"},
            {"reason": "no_bookings_made"},
        ],
        metrics={"refresh_to_first_candidate_ms": 450},
    )
    assert cls == "AUTOMATION_FAILURE"
    assert reason == "js_click_not_registered"

    cls2, reason2 = classify_run(
        success=False,
        events=[{"reason": "confirm_not_found"}, {"reason": "no_bookings_made"}],
        metrics={"refresh_to_first_candidate_ms": 300},
    )
    assert cls2 == "AUTOMATION_FAILURE"
    assert reason2 == "confirm_not_found"


def test_classify_ambiguous_candidate_is_possible_competition():
    cls, reason = classify_run(
        success=False,
        events=[{"reason": "no_bookings_made"}],
        metrics={"refresh_to_first_candidate_ms": 600},
    )
    assert cls == "POSSIBLE_COMPETITION_LOSS"
    assert reason == "candidate_seen_no_booking"


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
