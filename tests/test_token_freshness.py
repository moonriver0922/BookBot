"""CSRF token freshness tests (2026-09-14).

Context: the POSS site freezes ``CSRFToken: getCSRFToken()`` into the Search
click handler at page load.  With the 08:00 start the form sat ~29.5 min
before the 08:30 fire; the frozen token had aged out, every search came back
403 for ~59s (browser and API alike), and the booking only completed after a
retry wave rebuilt the page and handed out a fresh token.  The form is now
re-rendered at fire-150s (bounded, never delaying the fire), and a 403 still
triggers an immediate rebuild + refire (heal) instead of a ~60s wait.
"""

from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace

import pytest

from bookbot import booker


class TestRefreshPlan:
    def test_normal_lead_sleeps_then_caps_budget(self):
        sleep_s, budget_s = booker._refresh_plan_s(1000.0, before_s=150.0)
        assert sleep_s == pytest.approx(850.0)
        assert budget_s == pytest.approx(60.0)

    def test_close_to_fire_refreshes_now_with_shrunk_budget(self):
        sleep_s, budget_s = booker._refresh_plan_s(60.0, before_s=150.0)
        assert sleep_s == 0.0
        assert budget_s == pytest.approx(30.0)

    def test_skipped_inside_skip_window(self):
        assert booker._refresh_plan_s(40.0, before_s=150.0) is None
        assert booker._refresh_plan_s(45.0, before_s=150.0) is None

    def test_disabled_returns_none(self):
        assert booker._refresh_plan_s(1000.0, before_s=0.0) is None

    def test_plan_never_eats_the_fire_margin(self):
        for remaining in (50.0, 80.0, 170.0, 5000.0):
            plan = booker._refresh_plan_s(remaining, before_s=150.0)
            if plan is None:
                continue
            sleep_s, budget_s = plan
            assert sleep_s >= 0.0
            assert sleep_s + budget_s <= remaining - 30.0 + 1e-9


class FakeGotoTab:
    def __init__(self, events, *, fail_goto=False):
        self.events = events
        self.fail_goto = fail_goto

    async def goto(self, url, wait_until=None, timeout=None):
        self.events.append(("goto", url))
        if self.fail_goto:
            raise RuntimeError("goto boom")


class TestRebuildRushFormTab:
    def test_reloads_then_reselects_criteria(self, monkeypatch):
        events = []
        tab = FakeGotoTab(events)

        async def fake_ensure(tab_, config_, *, rush=False):
            events.append(("ensure", rush))

        async def fake_select(
            tab_, ref_date, config_, *,
            center_override=None, auto_search=True, rush=False,
        ):
            events.append(("select", center_override, auto_search))

        monkeypatch.setattr(booker, "_ensure_booking_form", fake_ensure)
        monkeypatch.setattr(booker, "select_booking_criteria", fake_select)

        asyncio.run(
            booker._rebuild_rush_form_tab(
                tab, None, ref_date=date(2026, 9, 21), center_name="Shaw Sports Complex",
            )
        )
        assert [e[0] for e in events] == ["goto", "ensure", "select"]
        assert events[0][1] == booker.BOOKING_URL
        assert events[2][1] == "Shaw Sports Complex"
        assert events[2][2] is False  # auto_search off; the caller fires search itself

    def test_reload_failure_propagates(self, monkeypatch):
        tab = FakeGotoTab([], fail_goto=True)
        with pytest.raises(RuntimeError, match="goto boom"):
            asyncio.run(
                booker._rebuild_rush_form_tab(
                    tab, None, ref_date=date(2026, 9, 21), center_name="X",
                )
            )


class TestReadTabToken:
    def test_returns_token_value(self):
        class T:
            async def evaluate(self, script, *a):
                return "f0465881-4c37"

        assert asyncio.run(booker._read_tab_csrf_token(T())) == "f0465881-4c37"

    def test_empty_on_error(self):
        class T:
            async def evaluate(self, script, *a):
                raise RuntimeError("context destroyed")

        assert asyncio.run(booker._read_tab_csrf_token(T())) == ""

    def test_empty_on_non_str(self):
        class T:
            async def evaluate(self, script, *a):
                return None

        assert asyncio.run(booker._read_tab_csrf_token(T())) == ""


class TestFetchFreshToken:
    def test_parses_render_token(self):
        class T:
            async def evaluate(self, script, *a):
                return {"status": 200, "token": "abc-123"}

        assert asyncio.run(booker._fetch_fresh_csrf_token(T())) == "abc-123"

    def test_blank_on_render_failure(self):
        class T:
            async def evaluate(self, script, *a):
                return {"status": 0, "token": ""}

        assert asyncio.run(booker._fetch_fresh_csrf_token(T())) == ""

    def test_blank_on_exception(self):
        class T:
            async def evaluate(self, script, *a):
                raise RuntimeError("no")

        assert asyncio.run(booker._fetch_fresh_csrf_token(T())) == ""


CFG = SimpleNamespace(selectors=SimpleNamespace(search_button="#searchButton"))


class FakeClickTab:
    def __init__(self, name):
        self.name = name
        self.clicked = 0

    async def evaluate(self, script, *a):
        self.clicked += 1
        return True


class TestTokenHealRound:
    def test_rebuilds_and_refires_every_tab(self, monkeypatch):
        events = []

        async def fake_rebuild(tab, config, *, ref_date, center_name):
            events.append(center_name)

        monkeypatch.setattr(booker, "_rebuild_rush_form_tab", fake_rebuild)

        tabs = [("A", FakeClickTab("A")), ("B", FakeClickTab("B"))]
        ok, failed = asyncio.run(
            booker._token_heal_round(tabs, CFG, ref_date=date(2026, 9, 21))
        )
        assert (ok, failed) == (2, 0)
        assert events == ["A", "B"]
        assert all(tab.clicked == 1 for _, tab in tabs)

    def test_failure_is_counted_and_does_not_stop_other_tabs(self, monkeypatch):
        async def fake_rebuild(tab, config, *, ref_date, center_name):
            if center_name == "A":
                raise RuntimeError("reload failed")

        monkeypatch.setattr(booker, "_rebuild_rush_form_tab", fake_rebuild)

        tabs = [("A", FakeClickTab("A")), ("B", FakeClickTab("B"))]
        ok, failed = asyncio.run(
            booker._token_heal_round(tabs, CFG, ref_date=date(2026, 9, 21))
        )
        assert (ok, failed) == (1, 1)

    def test_stop_flag_halts_further_rebuilds(self, monkeypatch):
        events = []

        async def fake_rebuild(tab, config, *, ref_date, center_name):
            events.append(center_name)

        monkeypatch.setattr(booker, "_rebuild_rush_form_tab", fake_rebuild)

        tabs = [("A", FakeClickTab("A")), ("B", FakeClickTab("B"))]
        ok, failed = asyncio.run(
            booker._token_heal_round(
                tabs, CFG, ref_date=date(2026, 9, 21), is_stopped=lambda: True,
            )
        )
        assert (ok, failed) == (0, 0)
        assert events == []


class FakeResult:
    def __init__(self, status, *, ok=None, payload=None, text=""):
        self.status_code = status
        self.ok = ok if ok is not None else (200 <= status < 300)
        self.payload = payload
        self.text = text


class FakeClient:
    def __init__(self, results):
        self.results = list(results)
        self.tokens = []

    async def search(self, *, csrf_token, payload):
        self.tokens.append(csrf_token)
        return self.results.pop(0)


class TestApiSearchFreshToken:
    def test_ok_first_try_no_retry(self):
        client = FakeClient([FakeResult(200, payload={"a": 1})])
        result = asyncio.run(
            booker._api_search_with_fresh_token(
                client, None, csrf_token="t1", payload={},
            )
        )
        assert result.status_code == 200
        assert client.tokens == ["t1"]

    def test_403_retries_with_new_dom_token(self):
        client = FakeClient([FakeResult(403), FakeResult(200, payload={"a": 1})])

        class T:
            async def evaluate(self, script, *a):
                return "t2"

        result = asyncio.run(
            booker._api_search_with_fresh_token(client, T(), csrf_token="t1", payload={})
        )
        assert result.status_code == 200
        assert client.tokens == ["t1", "t2"]

    def test_403_falls_back_to_render_token_when_dom_token_is_same(self):
        client = FakeClient([FakeResult(403), FakeResult(200, payload={"a": 1})])

        class T:
            async def evaluate(self, script, *a):
                if "fetch(" in script:
                    return {"status": 200, "token": "t2"}
                return "t1"

        result = asyncio.run(
            booker._api_search_with_fresh_token(client, T(), csrf_token="t1", payload={})
        )
        assert result.status_code == 200
        assert client.tokens == ["t1", "t2"]

    def test_403_without_a_newer_token_stays_failed(self):
        client = FakeClient([FakeResult(403)])

        class T:
            async def evaluate(self, script, *a):
                if "fetch(" in script:
                    return {"status": 200, "token": "t1"}
                return "t1"

        result = asyncio.run(
            booker._api_search_with_fresh_token(client, T(), csrf_token="t1", payload={})
        )
        assert result.status_code == 403
        assert client.tokens == ["t1"]


PANEL_CFG = SimpleNamespace(
    selectors=SimpleNamespace(
        activity="#actvId",
        sports_facility_button='a:has-text("Sports Facility"), button:has-text("Sports Facility")',
    )
)


class FakePanelPage:
    """Page double for _open_sports_facility_panel.

    The Sports Facility toggle only starts working after *opens_after*
    clicks - mirroring the page-JS init race where a click right after load
    silently no-ops (2026-09-14 rebuild failures).
    """

    def __init__(self, *, opens_after=1, has_button=True):
        self.opens_after = opens_after
        self.has_button = has_button
        self.clicks = 0
        self.js_clicks = 0

    def locator(self, selector):
        page = self

        class Loc:
            async def count(self):
                if "Sports Facility" in selector:
                    return 1 if page.has_button else 0
                return 1

            @property
            def first(self):
                return self

            async def is_visible(self, timeout=None):
                if "Sports Facility" in selector:
                    return page.has_button
                return page.clicks >= page.opens_after

            async def click(self, timeout=None):
                page.clicks += 1

        return Loc()

    async def evaluate(self, script, *a):
        if "sports facility" in script.lower():
            self.clicks += 1
            self.js_clicks += 1
        return None

    async def wait_for_selector(self, selector, state=None, timeout=None):
        if self.clicks < self.opens_after:
            raise RuntimeError("activity select not visible yet")


class TestOpenSportsFacilityPanel:
    def test_already_open_skips_clicking(self):
        page = FakePanelPage(opens_after=0)
        ok = asyncio.run(booker._open_sports_facility_panel(page, PANEL_CFG, rush=True))
        assert ok is True
        assert page.clicks == 0

    def test_retries_until_the_toggle_works(self):
        page = FakePanelPage(opens_after=3)
        ok = asyncio.run(booker._open_sports_facility_panel(page, PANEL_CFG, rush=True))
        assert ok is True
        assert page.clicks == 3

    def test_gives_up_after_bounded_attempts(self):
        page = FakePanelPage(opens_after=99)
        ok = asyncio.run(booker._open_sports_facility_panel(page, PANEL_CFG, rush=True))
        assert ok is False
        assert page.clicks == 5

    def test_js_click_fallback_when_button_missing(self):
        page = FakePanelPage(opens_after=1, has_button=False)
        ok = asyncio.run(booker._open_sports_facility_panel(page, PANEL_CFG, rush=True))
        assert ok is True
        assert page.js_clicks == 1
