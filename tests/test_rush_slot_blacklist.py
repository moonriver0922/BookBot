"""Rush failed-slot memory + failure-evidence tests (2026-09-17).

On 09-17 all four rush attempts chased the same 09:30-10:30 cell while the
re-scans kept advertising it as available; nothing remembered the failures,
and a "confirmation page never appeared" loss left no trace of what the
server actually returned.  These tests pin both fixes:

* ``_note_failed_slots`` / ``_prefer_unfailed_slots`` / ``_choose_rush_slots``
  steer later picks to untried cells (falling back to tried cells when no
  acceptable alternative remains - a repeated attempt beats no attempt);
* ``_capture_failure_evidence`` dumps the rendered page (URL + text, optional
  screenshot) so the next such loss is self-explaining.
"""

from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from bookbot import booker

TARGET = date(2026, 9, 24)
PH = "Shaw Sports Complex - Sports Practice Hall"
MAIN = "Shaw Sports Complex"


class FakeTracker:
    """Stand-in for the module-level tracker (records instead of writing logs)."""

    def __init__(self) -> None:
        self._metrics: dict[str, object] = {}
        self.feedback: list[dict] = []

    def set_metric(self, name, value):
        self._metrics[name] = value

    def incr_metric(self, name, delta=1):
        self._metrics[name] = self._metrics.get(name, 0) + delta

    def add_feedback(self, reason, **details):
        self.feedback.append({"reason": reason, **details})


@pytest.fixture()
def fake_tracker(monkeypatch):
    ft = FakeTracker()
    monkeypatch.setattr(booker, "tracker", ft)
    return ft


def _config():
    prefs = SimpleNamespace(
        min_slot_start="09:30",
        weekday_time_ranges={},
        slot_priority_starts=[],
        center=PH,
    )
    settings = SimpleNamespace(rush_prefer_consecutive=1)
    return SimpleNamespace(preferences=prefs, settings=settings)


def _slot(start, end, *, center=PH, available=True):
    return booker.TimeSlot(start=start, end=end, center=center, available=available)


def _memory(*slots, center=PH, target=TARGET):
    return {booker._failed_slot_key(center, target, s): 1.0 for s in slots}


# ---------------------------------------------------------------------------
# _failed_slot_key / _note_failed_slots
# ---------------------------------------------------------------------------

class TestFailedSlotKey:
    def test_shape_includes_center_and_date(self):
        s = _slot("09:30", "10:30")
        assert booker._failed_slot_key(PH, TARGET, s) == (
            PH, str(TARGET), "09:30", "10:30",
        )

    def test_distinguishes_centers_and_dates(self):
        s = _slot("09:30", "10:30")
        assert booker._failed_slot_key(PH, TARGET, s) != booker._failed_slot_key(MAIN, TARGET, s)
        assert booker._failed_slot_key(PH, TARGET, s) != booker._failed_slot_key(
            PH, date(2026, 9, 25), s,
        )


class TestNoteFailedSlots:
    def test_records_each_slot(self, fake_tracker):
        memory: dict = {}
        slots = [_slot("09:30", "10:30"), _slot("10:30", "11:30")]
        booker._note_failed_slots(
            memory, center=PH, target=TARGET, slots=slots, reason="wave1",
        )
        assert set(memory) == {booker._failed_slot_key(PH, TARGET, s) for s in slots}
        assert fake_tracker._metrics["failed_slot_marked_count"] == 1

    def test_empty_slots_noop(self, fake_tracker):
        memory: dict = {}
        booker._note_failed_slots(memory, center=PH, target=TARGET, slots=[], reason="x")
        assert memory == {}
        assert "failed_slot_marked_count" not in fake_tracker._metrics


# ---------------------------------------------------------------------------
# _prefer_unfailed_slots
# ---------------------------------------------------------------------------

class TestPreferUnfailedSlots:
    def test_drops_tried_cell_when_alternative_exists(self, fake_tracker):
        tried, other = _slot("09:30", "10:30"), _slot("10:30", "11:30")
        kept = booker._prefer_unfailed_slots(
            [tried, other], _memory(tried),
            center=PH, target=TARGET, config=_config(),
        )
        assert kept == [other]
        assert fake_tracker._metrics["failed_slot_skip_count"] == 1

    def test_keeps_all_when_every_acceptable_cell_tried(self, fake_tracker):
        tried = _slot("09:30", "10:30")
        slots = [tried]
        kept = booker._prefer_unfailed_slots(
            slots, _memory(tried), center=PH, target=TARGET, config=_config(),
        )
        assert kept == slots

    def test_ignores_unacceptable_untried_cells(self, fake_tracker):
        tried = _slot("09:30", "10:30")
        early = _slot("08:30", "09:30")  # below the 09:30 floor
        slots = [tried, early]
        kept = booker._prefer_unfailed_slots(
            slots, _memory(tried), center=PH, target=TARGET, config=_config(),
        )
        assert kept == slots  # no acceptable alternative -> fall back to retrying

    def test_other_center_not_blocked(self, fake_tracker):
        s = _slot("09:30", "10:30", center=MAIN)
        tried_elsewhere = _memory(_slot("09:30", "10:30"))
        kept = booker._prefer_unfailed_slots(
            [s], tried_elsewhere, center=MAIN, target=TARGET, config=_config(),
        )
        assert kept == [s]

    def test_other_date_not_blocked(self, fake_tracker):
        s = _slot("09:30", "10:30")
        mem = _memory(_slot("09:30", "10:30"), target=date(2026, 9, 25))
        kept = booker._prefer_unfailed_slots(
            [s], mem, center=PH, target=TARGET, config=_config(),
        )
        assert kept == [s]

    def test_vanished_tried_cell_is_harmless(self, fake_tracker):
        ghost = _slot("14:30", "15:30")
        slots = [_slot("09:30", "10:30")]
        kept = booker._prefer_unfailed_slots(
            slots, _memory(ghost), center=PH, target=TARGET, config=_config(),
        )
        assert kept == slots
        assert "failed_slot_skip_count" not in fake_tracker._metrics

    def test_no_memory_is_noop(self, fake_tracker):
        slots = [_slot("09:30", "10:30")]
        kept = booker._prefer_unfailed_slots(
            slots, {}, center=PH, target=TARGET, config=_config(),
        )
        assert kept == slots


# ---------------------------------------------------------------------------
# _choose_rush_slots
# ---------------------------------------------------------------------------

class TestChooseRushSlots:
    def test_steers_to_untried_alternative(self, fake_tracker):
        tried, other = _slot("09:30", "10:30"), _slot("10:30", "11:30")
        choice = booker._choose_rush_slots(
            [tried, other], 1, _config(),
            center=PH, target=TARGET, memory=_memory(tried),
        )
        assert [s.start for s in choice] == ["10:30"]

    def test_falls_back_to_tried_cell_when_alone(self, fake_tracker):
        tried = _slot("09:30", "10:30")
        choice = booker._choose_rush_slots(
            [tried], 1, _config(),
            center=PH, target=TARGET, memory=_memory(tried),
        )
        assert [s.start for s in choice] == ["09:30"]

    def test_no_memory_keeps_earliest_first(self, fake_tracker):
        slots = [_slot("10:30", "11:30"), _slot("09:30", "10:30")]
        choice = booker._choose_rush_slots(
            slots, 1, _config(), center=PH, target=TARGET, memory={},
        )
        assert [s.start for s in choice] == ["09:30"]

    def test_no_acceptable_slots_returns_empty(self, fake_tracker):
        slots = [_slot("08:30", "09:30")]
        choice = booker._choose_rush_slots(
            slots, 1, _config(), center=PH, target=TARGET, memory={},
        )
        assert choice == []


# ---------------------------------------------------------------------------
# _capture_failure_evidence
# ---------------------------------------------------------------------------

class FakeEvidencePage:
    """Page double: scripted evidence probes, records inner_text timeout."""

    def __init__(
        self,
        body="Booking result: slot occupied",
        *,
        fail_inner=False,
        shot_ok=True,
        url=None,
    ) -> None:
        self.body = body
        self.fail_inner = fail_inner
        self.shot_ok = shot_ok
        self.url = url or (
            "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_make_book/make_book_submit.do"
        )
        self.last_inner_timeout = None
        self.shot_requests: list[str] = []

    async def title(self):
        return "POSS Facility Booking"

    async def inner_text(self, selector, timeout=None):
        self.last_inner_timeout = timeout
        if self.fail_inner:
            raise RuntimeError("page closed mid-navigation")
        return self.body

    async def screenshot(self, path=None, timeout=None):
        if not self.shot_ok:
            raise RuntimeError("screenshot denied")
        Path(path).write_bytes(b"PNG")
        self.shot_requests.append(str(path))


class TestFailureEvidenceCapture:
    def test_writes_url_and_body(self, tmp_path, fake_tracker):
        page = FakeEvidencePage(body="09:30-10:30 already booked by others")
        out = asyncio.run(
            booker._capture_failure_evidence(page, "fail-confirm-missing", base_dir=tmp_path)
        )
        assert out is not None and str(out).endswith(".txt")
        text = Path(out).read_text(encoding="utf-8")
        assert page.url in text
        assert "already booked by others" in text
        assert fake_tracker._metrics["failure_evidence_count"] == 1
        assert fake_tracker._metrics["failure_evidence_last"] == str(out)

    def test_inner_text_failure_still_writes(self, tmp_path, fake_tracker):
        page = FakeEvidencePage(fail_inner=True)
        out = asyncio.run(
            booker._capture_failure_evidence(page, "fail-lane-exception", base_dir=tmp_path)
        )
        assert out is not None
        text = Path(out).read_text(encoding="utf-8")
        assert "<inner_text failed:" in text
        assert page.url in text

    def test_inner_text_timeout_is_capped(self, tmp_path, fake_tracker):
        page = FakeEvidencePage()
        asyncio.run(booker._capture_failure_evidence(page, "fail-x", base_dir=tmp_path))
        assert page.last_inner_timeout == 2_000

    def test_screenshot_capture_returns_png(self, tmp_path, fake_tracker):
        page = FakeEvidencePage()
        out = asyncio.run(
            booker._capture_failure_evidence(
                page, "fail-conflict", screenshot=True, base_dir=tmp_path,
            )
        )
        assert out is not None and str(out).endswith(".png")
        assert Path(out).exists()
        assert len(list(tmp_path.glob("*-fail-conflict.txt"))) == 1

    def test_screenshot_failure_falls_back_to_txt(self, tmp_path, fake_tracker):
        page = FakeEvidencePage(shot_ok=False)
        out = asyncio.run(
            booker._capture_failure_evidence(
                page, "fail-conflict", screenshot=True, base_dir=tmp_path,
            )
        )
        assert out is not None and str(out).endswith(".txt")

    def test_default_dir_uses_module_evidence_dir(self, tmp_path, fake_tracker, monkeypatch):
        monkeypatch.setattr(booker, "_EVIDENCE_DIR", tmp_path)
        page = FakeEvidencePage()
        out = asyncio.run(booker._capture_failure_evidence(page, "fail-default"))
        assert out is not None
        assert str(tmp_path) in str(out)
