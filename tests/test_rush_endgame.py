"""Endgame (last-100m) tests: armed Next click + network event classification.

Context (2026-09-12): after a slot was claimed the submission stalled ~3.7s and
the run ended with a conflict.  The wire timeline showed the site itself spends
~3s re-validating the slot selection before re-enabling Next; on our side, the
poll-based click retries wasted that window and the JS/keyboard fallbacks could
report success without ever dispatching a real click.  Next is now clicked by
an in-page MutationObserver the moment it is stably enabled, the click is
verified against a submit-path effect, and the fallbacks are honest.
"""

from __future__ import annotations

import asyncio

from bookbot import booker


class FakeArmPage:
    """Page double: scripted arm results, failing native fallbacks, str replies."""

    def __init__(self, arm_results, *, str_results=None, url="https://x/make_book.do"):
        self._arm_results = list(arm_results)
        self._str_results = list(str_results or [])
        self.url = url
        self.arm_calls = 0
        self.last_resort_calls = 0

    async def evaluate(self, script, arg=None):
        if isinstance(arg, dict) and "selector" in arg:
            self.arm_calls += 1
            if self._arm_results:
                result = self._arm_results.pop(0)
            else:
                result = {
                    "clicked": False,
                    "via": "timeout",
                    "found": False,
                    "enables": 0,
                    "waitedMs": 0.0,
                }
            if isinstance(result, dict) and result.get("clicked") and result.get("_incr_on_click"):
                booker.tracker.incr_metric("submit_request_seen_count")
            return result
        self.last_resort_calls += 1
        return self._str_results.pop(0) if self._str_results else False

    async def wait_for_selector(self, selector, timeout=None):
        raise RuntimeError("no match")

    def locator(self, selector):
        raise RuntimeError("no locator")


def _arm(clicked, *, incr=False, via="armed", enables=1, waited=150.0):
    return {
        "clicked": clicked,
        "via": via,
        "found": True,
        "enables": enables,
        "waitedMs": waited,
        "_incr_on_click": incr,
    }


class TestArmedNextClick:
    def test_armed_click_with_submit_effect_returns_true(self):
        page = FakeArmPage([_arm(True, incr=True)])
        ok = asyncio.run(booker._click_next_fast(page, "#nextButton", [150, 300, 500]))
        assert ok is True
        assert page.arm_calls == 1
        assert page.last_resort_calls == 0

    def test_armed_click_without_effect_rearms_once(self):
        page = FakeArmPage(
            [_arm(True, incr=False), _arm(True, incr=True)],
            str_results=[True],  # button still enabled -> re-arm allowed
        )
        ok = asyncio.run(booker._click_next_fast(page, "#nextButton", [150, 300, 500]))
        assert ok is True
        assert page.arm_calls == 2

    def test_armed_timeout_falls_back_to_last_resort_click(self):
        page = FakeArmPage([_arm(False, via="timeout", enables=0)], str_results=[True])
        ok = asyncio.run(booker._click_next_fast(page, "#nextButton", [150, 300, 500]))
        assert ok is True
        assert page.last_resort_calls >= 1

    def test_never_clickable_returns_false(self):
        page = FakeArmPage([_arm(False, via="timeout", enables=0)], str_results=[False])
        ok = asyncio.run(booker._click_next_fast(page, "#nextButton", [150, 300, 500]))
        assert ok is False


class TestNetworkEventKind:
    def test_search_family(self):
        assert (
            booker._network_event_kind(
                "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_make_book/timetable.json?CSRFToken=abc"
            )
            == "search"
        )

    def test_submit_family(self):
        assert (
            booker._network_event_kind(
                "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_make_book/make_book_submit.do"
            )
            == "submit"
        )

    def test_prepare_family(self):
        assert (
            booker._network_event_kind(
                "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_make_book/make_book.do"
            )
            == "prepare"
        )

    def test_unrelated_urls_are_ignored(self):
        assert (
            booker._network_event_kind("https://www40.polyu.edu.hk/poss/secure/login/loginhome.do")
            is None
        )
        assert booker._network_event_kind("") is None
