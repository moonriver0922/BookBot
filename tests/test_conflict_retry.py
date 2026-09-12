"""Unit tests for stale-page conflict recovery.

Context (2026-09-12): after a failed booking submit, POSS is left on a
result/error page where the Search button is gone or disabled.  Clicking it
was a silent no-op and the follow-up rescan failed to parse ("Could not parse
timetable structure"), so the conflict-retry lane burned its budget without a
single real attempt.  ``_refire_search_or_rebuild`` detects the stale page and
rebuilds the booking form before rescanning.
"""

from __future__ import annotations

import asyncio
from datetime import date

from bookbot import booker


class FakeSelectors:
    search_button = "#searchButton"
    timetable = "table.tt-timetable"


class FakeSettings:
    same_slot_retry_budget_ms = 3000
    same_slot_retry_limit = 3


class FakeConfig:
    settings = FakeSettings()
    selectors = FakeSelectors()


class FakeTab:
    def __init__(self, *, fired: bool = False, fail_goto: bool = False) -> None:
        self.fired = fired
        self.fail_goto = fail_goto
        self.goto_calls: list[str] = []

    async def evaluate(self, script, *args):
        return self.fired

    async def goto(self, url, **kwargs):
        self.goto_calls.append(url)
        if self.fail_goto:
            raise RuntimeError("navigation failed")

    async def wait_for_selector(self, selector, **kwargs):
        return None


def _slot() -> booker.TimeSlot:
    return booker.TimeSlot(start="09:30", end="10:30", center="C")


def test_refire_uses_direct_click_when_button_alive():
    tab = FakeTab(fired=True)
    status = asyncio.run(
        booker._refire_search_or_rebuild(
            tab, FakeConfig(), ref_date=date(2026, 9, 19), center_name="C",
        )
    )
    assert status == "direct"
    assert tab.goto_calls == []


def test_refire_rebuilds_form_when_search_button_dead(monkeypatch):
    tab = FakeTab(fired=False)
    calls: list[str] = []

    async def fake_ensure(page, config, *, rush=False):
        calls.append("ensure_form")

    async def fake_select(page, ref_date, config, **kwargs):
        calls.append("select_criteria")

    monkeypatch.setattr(booker, "_ensure_booking_form", fake_ensure)
    monkeypatch.setattr(booker, "select_booking_criteria", fake_select)

    status = asyncio.run(
        booker._refire_search_or_rebuild(
            tab, FakeConfig(), ref_date=date(2026, 9, 19), center_name="C",
        )
    )
    assert status == "rebuilt"
    assert tab.goto_calls == [booker.BOOKING_URL]
    assert calls == ["ensure_form", "select_criteria"]


def test_refire_reports_failure_when_navigation_fails(monkeypatch):
    tab = FakeTab(fired=False, fail_goto=True)

    async def fake_ensure(page, config, *, rush=False):
        raise AssertionError("must not refill the form after a failed goto")

    monkeypatch.setattr(booker, "_ensure_booking_form", fake_ensure)

    status = asyncio.run(
        booker._refire_search_or_rebuild(
            tab, FakeConfig(), ref_date=date(2026, 9, 19), center_name="C",
        )
    )
    assert status == "failed"


def test_lane_books_when_page_still_usable(monkeypatch):
    slot = _slot()
    scans = [{date(2026, 9, 19): [slot]}]
    rebuilds: list[str] = []
    booked: dict = {}

    async def fake_scan(tab, config, *, targets, center_name):
        return scans.pop(0) if scans else {}

    async def fake_refire(*args, **kwargs):
        rebuilds.append("rebuild")
        return "rebuilt"

    async def fake_book(tab, choice, target, config, *, rush=False, **kwargs):
        booked["choice"] = list(choice)
        return True

    monkeypatch.setattr(booker, "scan_available_slots_multi", fake_scan)
    monkeypatch.setattr(booker, "_refire_search_or_rebuild", fake_refire)
    monkeypatch.setattr(booker, "book_slots", fake_book)

    result = asyncio.run(
        booker._retry_same_slot_lane(
            object(), FakeConfig(),
            center_name="C", target=date(2026, 9, 19),
            preferred_slots=[slot], remaining=1, ref_date=date(2026, 9, 19),
        )
    )
    assert result == [slot]
    assert rebuilds == []  # healthy page: no rebuild needed
    assert booked["choice"] == [slot]


def test_lane_rebuilds_stale_page_then_books(monkeypatch):
    slot = _slot()
    target = date(2026, 9, 19)
    scans = [{}, {target: [slot]}]  # stale rescan, then inventory after rebuild
    rebuilds: list[str] = []
    booked: dict = {}

    async def fake_scan(tab, config, *, targets, center_name):
        return scans.pop(0) if scans else {}

    async def fake_refire(tab, config, *, ref_date, center_name):
        rebuilds.append("rebuild")
        return "rebuilt"

    async def fake_book(tab, choice, target, config, *, rush=False, **kwargs):
        booked["choice"] = list(choice)
        return True

    monkeypatch.setattr(booker, "scan_available_slots_multi", fake_scan)
    monkeypatch.setattr(booker, "_refire_search_or_rebuild", fake_refire)
    monkeypatch.setattr(booker, "book_slots", fake_book)

    result = asyncio.run(
        booker._retry_same_slot_lane(
            object(), FakeConfig(),
            center_name="C", target=target,
            preferred_slots=[slot], remaining=1, ref_date=target,
        )
    )
    assert result == [slot]
    assert rebuilds == ["rebuild"]
    assert booked["choice"] == [slot]


def test_lane_gives_up_when_inventory_never_returns(monkeypatch):
    target = date(2026, 9, 19)
    rebuilds: list[str] = []

    async def fake_scan(tab, config, *, targets, center_name):
        return {}

    async def fake_refire(tab, config, *, ref_date, center_name):
        rebuilds.append("rebuild")
        return "rebuilt"

    async def fake_book(*args, **kwargs):
        raise AssertionError("must not try to book without inventory")

    monkeypatch.setattr(booker, "scan_available_slots_multi", fake_scan)
    monkeypatch.setattr(booker, "_refire_search_or_rebuild", fake_refire)
    monkeypatch.setattr(booker, "book_slots", fake_book)

    result = asyncio.run(
        booker._retry_same_slot_lane(
            object(), FakeConfig(),
            center_name="C", target=target,
            preferred_slots=[_slot()], remaining=1, ref_date=target,
        )
    )
    assert result == []
    assert rebuilds == ["rebuild"]  # rebuild attempted exactly once
