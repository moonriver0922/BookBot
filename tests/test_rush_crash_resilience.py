"""Rush crash-resilience tests (2026-09-15).

During the 09-15 rush a page navigation destroyed the JS execution context
under the confirm-page checkbox evaluate.  The exception bubbled from
``book_slots`` up through the wave loop and aborted the entire attempt —
taking the late recovery waves (the 09-14 safety net) with it.  These tests
pin the fixes:

* ``_safe_evaluate`` retries when it loses the context to a navigation;
* the checkbox tick falls back to locator clicks and never kills the lane;
* ``_book_slots_guarded`` isolates a dying lane — the run continues;
* the confirm-page wait gets a grace window under slow server renders;
* first-occurrence metrics are sticky across retry attempts.
"""

from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace

import pytest

from bookbot import booker


class ContextLostError(Exception):
    """Mimics playwright: Page.evaluate: Execution context was destroyed …"""


def _ctx_lost() -> ContextLostError:
    return ContextLostError(
        "Page.evaluate: Execution context was destroyed, most likely because of a navigation"
    )


class FakeTracker:
    """Stand-in for the module-level tracker (records instead of writing logs)."""

    def __init__(self) -> None:
        self._metrics: dict[str, object] = {}
        self.feedback: list[dict] = []
        self.events: list[tuple[str, dict]] = []

    def set_metric(self, name, value):
        self._metrics[name] = value

    def incr_metric(self, name, delta=1):
        self._metrics[name] = self._metrics.get(name, 0) + delta

    def add_feedback(self, reason, **details):
        self.feedback.append({"reason": reason, **details})

    def mark_event(self, name, **fields):
        self.events.append((name, fields))

    def update_candidate(self, candidate, field):
        candidate[field] = 1.0

    def finish_candidate(self, candidate, result):
        candidate["result"] = result


@pytest.fixture()
def fake_tracker(monkeypatch):
    ft = FakeTracker()
    monkeypatch.setattr(booker, "tracker", ft)
    return ft


# ---------------------------------------------------------------------------
# _safe_evaluate
# ---------------------------------------------------------------------------

class EvalScriptPage:
    def __init__(self, results):
        self._results = list(results)
        self.evaluate_calls = 0
        self.settle_calls = 0
        self.url = "https://x/make_book.do"

    async def evaluate(self, script, arg=None):
        self.evaluate_calls += 1
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def wait_for_load_state(self, state, timeout=None):
        self.settle_calls += 1


class TestSafeEvaluate:
    def test_retries_after_context_loss(self, fake_tracker):
        page = EvalScriptPage([_ctx_lost(), 7])
        out = asyncio.run(booker._safe_evaluate(page, "() => 7"))
        assert out == 7
        assert page.evaluate_calls == 2
        assert page.settle_calls == 1

    def test_exhausted_returns_default(self, fake_tracker):
        page = EvalScriptPage([_ctx_lost(), _ctx_lost(), _ctx_lost()])
        out = asyncio.run(booker._safe_evaluate(page, "x", default=0, retries=3))
        assert out == 0
        assert page.evaluate_calls == 3

    def test_non_navigation_errors_raise(self, fake_tracker):
        page = EvalScriptPage([RuntimeError("syntax error in injected script")])
        with pytest.raises(RuntimeError):
            asyncio.run(booker._safe_evaluate(page, "x"))

    def test_target_closed_marker_recognised(self, fake_tracker):
        assert booker._is_context_lost(
            RuntimeError("Target page, context or browser has been closed")
        )
        assert not booker._is_context_lost(RuntimeError("some unrelated bug"))


# ---------------------------------------------------------------------------
# _tick_confirm_checkboxes
# ---------------------------------------------------------------------------

class CheckboxPage:
    def __init__(self, eval_results, *, locator_count=2, locator_raises=False):
        self._eval_results = list(eval_results)
        self.locator_count = locator_count
        self.locator_raises = locator_raises
        self.checked: list[int] = []

    async def evaluate(self, script, arg=None):
        result = self._eval_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def wait_for_load_state(self, state, timeout=None):
        return None

    def locator(self, selector):
        page = self

        if page.locator_raises:
            raise RuntimeError("locator unavailable")

        class _CB:
            async def count(self):
                return page.locator_count

            def nth(self, i):
                class _Nth:
                    async def check(self, timeout=None):
                        page.checked.append(i)

                return _Nth()

        return _CB()


class TestTickConfirmCheckboxes:
    def test_js_path_when_context_alive(self, fake_tracker):
        page = CheckboxPage([3])
        assert asyncio.run(booker._tick_confirm_checkboxes(page)) == 3
        assert page.checked == []

    def test_locator_fallback_when_context_lost(self, fake_tracker):
        page = CheckboxPage([_ctx_lost(), _ctx_lost(), _ctx_lost()])
        assert asyncio.run(booker._tick_confirm_checkboxes(page)) == 2
        assert page.checked == [0, 1]

    def test_everything_failing_returns_zero_not_crash(self, fake_tracker):
        page = CheckboxPage([_ctx_lost()] * 3, locator_raises=True)
        assert asyncio.run(booker._tick_confirm_checkboxes(page)) == 0


# ---------------------------------------------------------------------------
# _await_confirmation_page
# ---------------------------------------------------------------------------

class ConfirmPage:
    def __init__(self, selector_results, *, load_state_ok=True):
        self._selector_results = list(selector_results)
        self._load_state_ok = load_state_ok
        self.selector_timeouts: list[int] = []

    async def wait_for_selector(self, selector, timeout=None, state=None):
        self.selector_timeouts.append(timeout)
        result = self._selector_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def wait_for_load_state(self, state, timeout=None):
        if self._load_state_ok:
            return None
        raise RuntimeError("still navigating")


class TestAwaitConfirmationPage:
    def test_fast_path(self, fake_tracker):
        page = ConfirmPage([True])
        ok = asyncio.run(
            booker._await_confirmation_page(page, page_timeout_ms=800, grace_timeout_ms=1500)
        )
        assert ok is True
        assert ("confirmation_page_seen", {}) in fake_tracker.events
        assert not fake_tracker.feedback

    def test_grace_window_saves_slow_render(self, fake_tracker):
        page = ConfirmPage([RuntimeError("timeout"), True])
        ok = asyncio.run(
            booker._await_confirmation_page(page, page_timeout_ms=800, grace_timeout_ms=1500)
        )
        assert ok is True
        assert any(f["reason"] == "confirmation_page_too_slow" for f in fake_tracker.feedback)
        assert any(name == "confirmation_page_seen" for name, _ in fake_tracker.events)

    def test_load_state_fallback(self, fake_tracker):
        page = ConfirmPage([RuntimeError("timeout"), RuntimeError("timeout")], load_state_ok=True)
        ok = asyncio.run(
            booker._await_confirmation_page(page, page_timeout_ms=800, grace_timeout_ms=1500)
        )
        assert ok is True

    def test_all_failing_returns_false(self, fake_tracker):
        page = ConfirmPage(
            [RuntimeError("timeout"), RuntimeError("timeout")], load_state_ok=False
        )
        ok = asyncio.run(
            booker._await_confirmation_page(page, page_timeout_ms=800, grace_timeout_ms=1500)
        )
        assert ok is False


# ---------------------------------------------------------------------------
# _book_slots_guarded
# ---------------------------------------------------------------------------

class TestLaneGuard:
    def test_lane_exception_isolated(self, fake_tracker, monkeypatch):
        async def boom(*args, **kwargs):
            raise _ctx_lost()

        monkeypatch.setattr(booker, "book_slots", boom)
        candidate: dict = {}
        ok = asyncio.run(
            booker._book_slots_guarded(
                None, [], date(2026, 9, 22), None,
                candidate=candidate, center_name="Shaw Sports Complex",
            )
        )
        assert ok is False
        assert candidate["result"] == "automation_failure"
        assert any(f["reason"] == "lane_exception" for f in fake_tracker.feedback)

    def test_lane_success_passthrough(self, fake_tracker, monkeypatch):
        async def ok_(*args, **kwargs):
            return True

        monkeypatch.setattr(booker, "book_slots", ok_)
        assert asyncio.run(
            booker._book_slots_guarded(None, [], date(2026, 9, 22), None)
        ) is True

    def test_session_errors_still_propagate(self, fake_tracker, monkeypatch):
        from bookbot.auth import MaintenanceError

        async def boom(*args, **kwargs):
            raise MaintenanceError("maintenance window")

        monkeypatch.setattr(booker, "book_slots", boom)
        with pytest.raises(MaintenanceError):
            asyncio.run(booker._book_slots_guarded(None, [], date(2026, 9, 22), None))


# ---------------------------------------------------------------------------
# Sticky first-occurrence metrics
# ---------------------------------------------------------------------------

class TestStickyMetrics:
    def test_first_value_wins(self, fake_tracker):
        booker._set_metric_once("actual_fire_delay_ms", 1.5)
        booker._set_metric_once("actual_fire_delay_ms", 104875.6)
        assert fake_tracker._metrics["actual_fire_delay_ms"] == 1.5

    def test_unset_metric_is_recorded(self, fake_tracker):
        booker._set_metric_once("refresh_to_first_candidate_ms", 18842.5)
        assert fake_tracker._metrics["refresh_to_first_candidate_ms"] == 18842.5


# ---------------------------------------------------------------------------
# Booking evidence archival
# ---------------------------------------------------------------------------

class EvidencePage:
    """Fake page for _save_booking_evidence: writes a real PNG stub file."""

    def __init__(self, *, screenshot_raises=False):
        self.screenshot_raises = screenshot_raises
        self.screenshots: list[str] = []

    async def screenshot(self, path=None, timeout=None, **kwargs):
        if self.screenshot_raises:
            raise RuntimeError("screenshot unavailable")
        self.screenshots.append(path)
        if path:
            with open(path, "wb") as fh:
                fh.write(b"\x89PNG\r\n")

    async def inner_text(self, selector):
        return "Booking confirmed\nShaw Sports Complex\n09:30 - 10:30"


class TestBookingEvidence:
    def test_evidence_files_and_metric_written(self, fake_tracker, tmp_path):
        page = EvidencePage()
        out = asyncio.run(booker._save_booking_evidence(page, base_dir=tmp_path))
        assert out is not None and out.exists()
        assert out.parent == tmp_path
        texts = list(tmp_path.glob("*-confirm.txt"))
        assert len(texts) == 1
        assert "Booking confirmed" in texts[0].read_text(encoding="utf-8")
        assert "booking_evidence" in fake_tracker._metrics

    def test_evidence_survives_screenshot_failure(self, fake_tracker, tmp_path):
        page = EvidencePage(screenshot_raises=True)
        out = asyncio.run(booker._save_booking_evidence(page, base_dir=tmp_path))
        assert out is None
        assert len(list(tmp_path.glob("*-confirm.txt"))) == 1


# ---------------------------------------------------------------------------
# End-to-end: the exact 09-15 crash path must not kill book_slots any more
# ---------------------------------------------------------------------------

class BookSlotsStubPage:
    """Just enough page for ``book_slots(rush=True)``; the tick loses its context."""

    def __init__(self) -> None:
        self.url = "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_make_book/make_book.do"
        self.tick_evaluate_attempts = 0
        self.locator_checks = 0
        self.confirm_clicks = 0

    async def evaluate(self, script, arg=None):
        if "checkbox" in script:
            self.tick_evaluate_attempts += 1
            raise _ctx_lost()
        if "tables" in script:
            return 2  # _click_slots_js "clicked" two cells
        raise AssertionError(f"unexpected evaluate: {script[:80]}")

    async def wait_for_selector(self, selector, timeout=None, state=None):
        if selector == "#nextButton:not([disabled])":
            return True
        raise RuntimeError("no match")  # confirm page deliberately slow

    async def wait_for_load_state(self, *args, **kwargs):
        return None

    async def inner_text(self, selector):
        return "Booking confirmed"

    def locator(self, selector):
        page = self

        if "checkbox" in selector:
            class _CB:
                async def count(self):
                    return 2

                def nth(self, i):
                    class _Nth:
                        async def check(self, timeout=None):
                            page.locator_checks += 1

                    return _Nth()

            return _CB()

        if "OK" in selector or "Yes" in selector:
            class _NoDialog:
                async def count(self):
                    return 0

            return _NoDialog()

        class _Confirm:
            def __init__(self):
                self.first = self

            async def count(self):
                return 1

            async def click(self, timeout=None):
                page.confirm_clicks += 1

        return _Confirm()


def _stub_config():
    return SimpleNamespace(
        selectors=SimpleNamespace(timetable="table#t", next_button="#nextButton"),
        settings=SimpleNamespace(
            rush_slot_select_timeout_ms=200,
            rush_confirm_page_timeout_ms=800,
            rush_confirm_result_timeout_ms=1500,
            next_click_backoff_ms=[150, 300, 500],
            rush_next_click_timeout_ms=4000,
        ),
    )


class TestCrashPathRegression:
    def test_navigation_race_on_tick_no_longer_kills_booking_flow(
        self, fake_tracker, monkeypatch
    ):
        async def fake_next_click(*args, **kwargs):
            return True

        monkeypatch.setattr(booker, "_click_next_fast", fake_next_click)

        evidence_calls: list = []

        async def fake_evidence(*args, **kwargs):
            evidence_calls.append(1)

        monkeypatch.setattr(booker, "_save_booking_evidence", fake_evidence)

        page = BookSlotsStubPage()
        slots = [
            SimpleNamespace(start="09:30", end="10:30"),
            SimpleNamespace(start="10:30", end="11:30"),
        ]
        candidate: dict = {}

        ok = asyncio.run(
            booker.book_slots(
                page, slots, date(2026, 9, 22), _stub_config(),
                rush=True, candidate=candidate,
            )
        )

        assert ok is True  # 09-15 this raised and killed the whole attempt
        assert page.tick_evaluate_attempts == 3  # retried, then fell back
        assert page.locator_checks == 2  # both checkboxes ticked via locators
        assert page.confirm_clicks == 1  # booking still confirmed
        assert candidate["result"] == "booked"
        assert evidence_calls == [1]  # receipt archived on success
