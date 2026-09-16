from __future__ import annotations

import asyncio
import json
import re
import statistics
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING, Awaitable, Callable, List, Tuple, TypeVar

from loguru import logger

from bookbot.api_client import (
    ApiCallResult,
    BookingApiClient,
    build_api_session_bridge,
    extract_form_fields_from_html,
)
from bookbot.api_timetable import parse_timetable_payload
from bookbot.stealth import human_click, human_delay, save_debug_snapshot
from bookbot.timing import normalize_boundary_offsets, seconds_until_offset
from bookbot.tracker import tracker

if TYPE_CHECKING:
    from playwright.async_api import Page

    from bookbot.config import AppConfig


BOOKING_URL = (
    "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_make_book/make_book.do"
)
RUNTIME_LOG_PATH = Path("logs/runtime.jsonl")

# A first-seen inventory timestamp within this distance of a configured
# boundary offset is attributed to that offset in timing statistics; anything
# later is recorded as out-of-window instead of being snapped to the latest
# probe (which would corrupt `bookbot timing-report` recommendations).
BOUNDARY_SNAP_TOLERANCE_MS = 750

# Warm-up scheduling (2026-09-13): the pre-open warm-up must never be able to
# delay the fire. Aim to finish it well before the first fire offset, and
# hard-abandon it on overrun - a late fire costs the whole rush.
_RUSH_WARMUP_LEAD_S = 10.0
_RUSH_WARMUP_FIRE_MARGIN_S = 1.0

T = TypeVar("T")


def _warmup_schedule_s(
    remaining_s: float,
    *,
    lead_s: float = _RUSH_WARMUP_LEAD_S,
    fire_margin_s: float = _RUSH_WARMUP_FIRE_MARGIN_S,
) -> tuple[float, float]:
    """Return ``(sleep_before_warm_s, warmup_cap_s)`` for the time left.

    ``sleep_before_warm_s`` waits, then starts the warm-up so it gets at most
    ``lead_s`` seconds before the fire target. ``warmup_cap_s`` is the hard
    ceiling for the warm-up itself: it is always abandoned early enough to
    keep ``fire_margin_s`` in hand before the fire target.
    """
    sleep_before_warm_s = max(0.0, float(remaining_s) - float(lead_s))
    warmup_cap_s = max(0.5, float(remaining_s) - float(fire_margin_s))
    return sleep_before_warm_s, warmup_cap_s


async def _retry_tab_prep(
    make_tab: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    delay_s: float = 1.5,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> T:
    """Retry tab preparation on transient failures; caller builds a fresh tab."""
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await make_tab()
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            if on_retry is not None:
                on_retry(attempt, exc)
            await asyncio.sleep(delay_s)
    assert last_exc is not None
    raise last_exc

# CSRF freshness (2026-09-14): the site freezes ``CSRFToken: getCSRFToken()``
# into the Search click handler at page load, so a form rendered ~30 min before
# the rush carries a token the server no longer accepts - every search then
# returns 403 until the page is re-rendered (browser and API alike).  The form
# is re-rendered shortly before the fire, and a 403 that still slips through
# triggers an immediate rebuild + refire instead of waiting on retry waves.
_RUSH_FORM_REFRESH_FIRE_MARGIN_S = 30.0
_RUSH_FORM_REFRESH_MIN_BUDGET_S = 5.0
_RUSH_FORM_REFRESH_MAX_BUDGET_S = 60.0
_RUSH_FORM_REFRESH_SKIP_BELOW_S = 45.0
_RUSH_FORM_REBUILD_TIMEOUT_MS = 12_000
_RUSH_TOKEN_HEAL_MAX_ROUNDS = 2
_RUSH_TOKEN_HEAL_COOLDOWN_S = 8.0


def _refresh_plan_s(
    remaining_s: float,
    *,
    before_s: float,
    min_margin_s: float = _RUSH_FORM_REFRESH_FIRE_MARGIN_S,
    max_budget_s: float = _RUSH_FORM_REFRESH_MAX_BUDGET_S,
    skip_below_s: float = _RUSH_FORM_REFRESH_SKIP_BELOW_S,
) -> tuple[float, float] | None:
    """Return ``(sleep_before_refresh_s, refresh_budget_s)`` or None to skip.

    The refresh re-renders each booking tab so the site re-binds a fresh
    CSRFToken.  It must finish well before the fire: the budget is capped and
    always keeps ``min_margin_s`` in hand.  Too close to the fire (or disabled
    with ``before_s <= 0``) the refresh is skipped - the 403 heal path then
    covers recovery.
    """
    if before_s <= 0 or remaining_s <= skip_below_s:
        return None
    sleep_s = max(0.0, float(remaining_s) - float(before_s))
    budget_s = min(
        float(max_budget_s), float(remaining_s) - sleep_s - float(min_margin_s)
    )
    if budget_s < _RUSH_FORM_REFRESH_MIN_BUDGET_S:
        return None
    return sleep_s, budget_s


async def _read_tab_csrf_token(tab: Page) -> str:
    """Read the booking form's current CSRFToken value from a tab."""
    try:
        value = await tab.evaluate(
            """() => document.querySelector('input[name="CSRFToken"], input[name="csrfToken"]')?.value || ''"""
        )
    except Exception:
        return ""
    return value if isinstance(value, str) else ""


async def _fetch_fresh_csrf_token(tab: Page) -> str:
    """GET the booking form in-page and parse the current CSRFToken value.

    Cheap token refresh for the API search channel (no page reload needed -
    only the token value matters there).  Returns "" on failure.
    """
    try:
        res = await tab.evaluate(
            """async () => {
                try {
                    const r = await fetch(window.location.href, {method: 'GET', credentials: 'same-origin', cache: 'no-store'});
                    const t = await r.text();
                    const m = t.match(/name=["']CSRFToken["'][^>]*value=["']([^"']+)/i)
                           || t.match(/value=["']([^"']+)["'][^>]*name=["']CSRFToken["']/i);
                    return {status: r.status, token: m ? m[1] : ''};
                } catch (e) { return {status: 0, token: ''}; }
            }"""
        )
    except Exception:
        return ""
    if not isinstance(res, dict):
        return ""
    token = res.get("token")
    return token if isinstance(token, str) else ""


async def _rebuild_rush_form_tab(
    tab: Page,
    config: AppConfig,
    *,
    ref_date: date,
    center_name: str,
) -> None:
    """Re-render the booking form so the site re-binds a fresh CSRFToken.

    The POSS Search click handler captures ``CSRFToken: getCSRFToken()`` at
    page load; once that token ages out every search returns 403 until the
    page is reloaded.  Reloading resets the form, so the criteria are
    re-selected afterwards.
    """
    await tab.goto(
        BOOKING_URL,
        wait_until="domcontentloaded",
        timeout=_RUSH_FORM_REBUILD_TIMEOUT_MS,
    )
    await _ensure_booking_form(tab, config, rush=True)
    await select_booking_criteria(
        tab, ref_date, config,
        center_override=center_name, auto_search=False, rush=True,
    )


async def _token_heal_round(
    center_tabs: list[tuple[str, Page]],
    config: AppConfig,
    *,
    ref_date: date,
    is_stopped: Callable[[], bool] | None = None,
) -> tuple[int, int]:
    """Rebuild + refire every tab after a stale-token 403. Returns (ok, failed)."""
    ok_count = 0
    fail_count = 0
    for center_name, tab in center_tabs:
        if is_stopped is not None and is_stopped():
            break
        try:
            await _rebuild_rush_form_tab(
                tab, config, ref_date=ref_date, center_name=center_name,
            )
            search_id = _selector_id(config.selectors.search_button)
            await tab.evaluate(f"document.getElementById('{search_id}')?.click()")
            ok_count += 1
        except Exception as exc:
            fail_count += 1
            logger.warning("Token heal rebuild failed for {}: {}", center_name, exc)
    return ok_count, fail_count


async def _api_search_with_fresh_token(
    client,
    tab: Page | None,
    *,
    csrf_token: str,
    payload: dict,
):
    """API search that retries once with a freshly read / minted token on 403."""
    result = await client.search(csrf_token=csrf_token, payload=payload)
    if result.status_code == 403:
        tracker.incr_metric("api_search_403_count")
        if tab is not None:
            fresh = await _read_tab_csrf_token(tab)
            if not fresh or fresh == csrf_token:
                fresh = await _fetch_fresh_csrf_token(tab)
            if fresh and fresh != csrf_token:
                result = await client.search(csrf_token=fresh, payload=payload)
                if result.ok and result.payload is not None:
                    tracker.incr_metric("api_search_token_retry_ok_count")
    return result


# Keepalive for long pre-open waits (2026-09-13: start moved to 08:00): ping
# each tab's session every few minutes so a ~30-min idle window cannot expire
# the POSS session before the rush.
_KEEPALIVE_CHUNK_S = 240.0
_KEEPALIVE_TAIL_GUARD_S = 30.0
_KEEPALIVE_PING_TIMEOUT_S = 10.0

_KEEPALIVE_PING_JS = """async () => {
    try {
        const r = await fetch(window.location.href, {
            method: 'HEAD', credentials: 'same-origin', cache: 'no-store',
        });
        return {ok: true, status: r.status, redirected: r.redirected, url: r.url};
    } catch (e) {
        return {ok: false, status: 0, redirected: false, url: ''};
    }
}"""


def _keepalive_next_step(
    remaining_s: float,
    *,
    chunk_s: float = _KEEPALIVE_CHUNK_S,
    tail_guard_s: float = _KEEPALIVE_TAIL_GUARD_S,
) -> float:
    """Seconds to sleep before the next keepalive check (0 = go straight to tail)."""
    return min(float(chunk_s), max(0.0, float(remaining_s) - float(tail_guard_s)))


async def _keepalive_ping(center_tabs: list[tuple[str, Page]]) -> int:
    """One lightweight HEAD ping per tab so the POSS session does not idle out.

    Returns the number of tabs that looked redirected to login (session risk).
    """
    tracker.incr_metric("keepalive_ping_count")
    suspicious = 0
    for center_name, tab in center_tabs:
        try:
            result = await asyncio.wait_for(
                tab.evaluate(_KEEPALIVE_PING_JS),
                timeout=_KEEPALIVE_PING_TIMEOUT_S,
            )
        except Exception as exc:
            logger.debug("Keepalive ping failed for {}: {}", center_name, exc)
            continue
        if not isinstance(result, dict):
            continue
        url = str(result.get("url") or "")
        if bool(result.get("redirected")) or "login" in url.lower():
            suspicious += 1
            tracker.incr_metric("keepalive_session_suspect_count")
            logger.warning(
                "Keepalive ping for {} may have lost the session (redirected={}, url={})",
                center_name, result.get("redirected"), url,
            )
    return suspicious


async def _sleep_with_keepalive(center_tabs: list[tuple[str, Page]], total_s: float) -> None:
    """Sleep out the pre-rush wait in chunks, pinging tabs to keep the session warm."""
    deadline = time.monotonic() + max(0.0, float(total_s))
    while True:
        step = _keepalive_next_step(deadline - time.monotonic())
        if step <= 0.0:
            break
        await asyncio.sleep(step)
        if deadline - time.monotonic() > _KEEPALIVE_TAIL_GUARD_S:
            await _keepalive_ping(center_tabs)
    remaining = deadline - time.monotonic()
    if remaining > 0:
        await asyncio.sleep(remaining)


def _selector_id(selector: str) -> str:
    return selector[1:] if selector.startswith("#") else selector


class FormNotReadyError(Exception):
    """Raised when the booking form is present but key elements are missing."""
    pass

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class TimeSlot:
    start: str          # e.g. "14:30"
    end: str            # e.g. "15:30"
    center: str
    court: str = ""
    available: bool = True
    element_selector: str = ""
    facility_id: str = ""

    @property
    def start_hour(self) -> float:
        h, m = self.start.split(":")
        return int(h) + int(m) / 60

    @property
    def end_hour(self) -> float:
        h, m = self.end.split(":")
        return int(h) + int(m) / 60


# ---------------------------------------------------------------------------
# Weekly quota
# ---------------------------------------------------------------------------

async def check_weekly_quota(page: Page, config: AppConfig) -> int:
    """Return the number of slots already booked this week (Mon–Sun).

    Opens a new tab to check booking history so we don't lose the current page.
    """
    logger.info("Checking weekly booking quota …")

    try:
        context = page.context
        history_page = await context.new_page()
        try:
            my_record_url = "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_my_record/my_record.do"
            await history_page.goto(my_record_url, wait_until="domcontentloaded")
            await human_delay(1.5, 3.0)
            await save_debug_snapshot(history_page, "05a_booking_history")

            today = date.today()
            monday = today - timedelta(days=today.weekday())
            sunday = monday + timedelta(days=6)

            rows = await history_page.query_selector_all("table tr, .booking-item, .list-group-item")
            booked = 0
            for row in rows:
                text = (await row.inner_text()).strip()
                dates = re.findall(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text)
                for d in dates:
                    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
                        try:
                            dt = datetime.strptime(d, fmt).date()
                            if monday <= dt <= sunday:
                                booked += 1
                            break
                        except ValueError:
                            continue

            logger.info("Slots booked this week: {}/{}", booked, config.preferences.weekly_max_slots)
            return booked
        finally:
            await history_page.close()

    except Exception as exc:
        logger.warning("Could not determine weekly quota ({}), assuming 0", exc)
        return 0


# ---------------------------------------------------------------------------
# Target date calculation
# ---------------------------------------------------------------------------

def compute_target_dates(config: AppConfig) -> list[date]:
    """Return all bookable preferred days within the booking window,
    sorted by priority: furthest date first (newly released slots)."""
    today = date.today()
    max_ahead = config.preferences.book_days_ahead
    candidates = []
    for offset in range(1, max_ahead + 1):
        d = today + timedelta(days=offset)
        if d.weekday() in config.preferences.preferred_days:
            candidates.append(d)
    # Furthest date first — those are the freshly opened slots at 8:30
    candidates.sort(reverse=True)
    return candidates


# ---------------------------------------------------------------------------
# Slot scanning
# ---------------------------------------------------------------------------

async def _dump_form_structure(page: Page) -> None:
    """Log the form structure for debugging."""
    info = await page.evaluate("""() => {
        const selects = [...document.querySelectorAll('select')].map(s => ({
            name: s.name, id: s.id,
            options: [...s.options].slice(0, 10).map(o => o.text.trim())
        }));
        const inputs = [...document.querySelectorAll('input:not([type="hidden"])')].map(i => ({
            name: i.name, id: i.id, type: i.type, value: i.value,
            visible: i.offsetParent !== null
        }));
        const buttons = [...document.querySelectorAll('input[type="submit"], input[type="button"], button')]
            .map(b => ({ tag: b.tagName, value: b.value || b.textContent?.trim(), name: b.name }));
        return { selects, inputs, buttons };
    }""")
    logger.debug("Form selects: {}", info.get("selects"))
    logger.debug("Form inputs: {}", info.get("inputs"))
    logger.debug("Form buttons: {}", info.get("buttons"))


async def _select_dropdown_option(page: Page, select_el, target_label: str) -> bool:
    """Select an option from a <select> by partial label match."""
    try:
        await select_el.select_option(label=target_label)
        return True
    except Exception:
        pass
    options = await select_el.query_selector_all("option")
    for opt in options:
        txt = (await opt.inner_text()).strip()
        if target_label.lower() in txt.lower():
            val = await opt.get_attribute("value") or ""
            await select_el.select_option(value=val)
            return True
    return False


async def _wait_for_select_options(page: Page, select_id: str, timeout: int = 10_000) -> list[str]:
    """Wait until a <select> has more than just the placeholder option."""
    try:
        await page.wait_for_function(
            f"""() => {{
                const sel = document.getElementById('{select_id}');
                return sel && sel.options.length > 1;
            }}""",
            timeout=timeout,
        )
    except Exception:
        pass
    opts = await page.evaluate(
        f"[...document.getElementById('{select_id}')?.options || []].map(o => ({{value: o.value, text: o.text.trim()}}))"
    )
    return opts


async def get_available_centers(page: Page, config: AppConfig) -> list[dict]:
    """Read the center dropdown options currently available."""
    center_id = _selector_id(config.selectors.center)
    try:
        opts = await page.evaluate(
            f"[...document.getElementById('{center_id}')?.options || []]"
            ".filter(o => o.value).map(o => ({value: o.value, text: o.text.trim()}))"
        )
        return opts or []
    except Exception:
        return []


async def select_booking_criteria(
    page: Page, target: date, config: AppConfig, *,
    center_override: str | None = None,
    auto_search: bool = True,
    rush: bool = False,
) -> None:
    """Fill in Date, Activity, and Center on the booking form, then optionally click Search.

    The POSS booking form uses cascading AJAX dropdowns:
      Date -> Activity options load -> Center options load
    Element IDs: #searchDate, #actvId, #ctrId, #searchButton

    When *auto_search* is False the form is populated but Search is not clicked,
    allowing the caller to wait for a precise moment before triggering the search.

    When *rush* is True, skip human delays, screenshots, and form dumps for speed.
    """
    center_name = center_override or config.preferences.center
    search_date_sel = config.selectors.search_date
    activity_sel = config.selectors.activity
    center_sel = config.selectors.center
    search_date_id = _selector_id(search_date_sel)
    activity_id = _selector_id(activity_sel)
    center_id = _selector_id(center_sel)
    logger.info("Selecting booking criteria (date={}, activity={}, center={}) …",
                target, config.preferences.activity, center_name)

    if rush:
        await page.wait_for_load_state("domcontentloaded")
    else:
        await page.wait_for_load_state("networkidle")
        await _dump_form_structure(page)

    date_str = target.strftime("%d/%m/%Y")

    # --- Date ---
    date_input = page.locator(search_date_sel)
    if await date_input.count() > 0:
        if not rush:
            date_trigger = page.locator(
                ".datepicker-trigger, .ui-datepicker-trigger, "
                "img[class*='calendar'], img[class*='date'], "
                "span.input-group-addon, .input-group-append, "
                "img[src*='calendar']"
            )
            if await date_trigger.count() > 0:
                logger.debug("Clicking date picker trigger …")
                await date_trigger.first.click()
                await human_delay(0.5, 1.0)

        await page.evaluate(
            f"""() => {{
                const el = document.getElementById('{search_date_id}');
                if (el) {{
                    el.value = '{date_str}';
                    el.dispatchEvent(new Event('change', {{bubbles: true}}));
                    el.dispatchEvent(new Event('input', {{bubbles: true}}));
                }}
                if (typeof jQuery !== 'undefined' && jQuery.datepicker) {{
                    jQuery('{search_date_sel}').datepicker('hide');
                }}
            }}"""
        )
        if not rush:
            await human_delay(0.5, 1.0)
            await page.keyboard.press("Escape")
            await human_delay(0.3, 0.5)

        logger.info("Date set: {}", date_str)
        if not rush:
            await human_delay(1.0, 2.0)
    else:
        logger.warning("Date input {} not found", search_date_sel)
        raise FormNotReadyError(f"Date input {search_date_sel} not found — form may not have loaded")

    # --- Activity ---
    logger.debug("Waiting for activity options to load …")
    actv_opts = await _wait_for_select_options(page, activity_id, timeout=5_000 if rush else 10_000)
    logger.debug("Activity options: {}", actv_opts)

    if len(actv_opts) <= 1:
        actv_el = page.locator(activity_sel)
        if await actv_el.count() > 0:
            try:
                await actv_el.click(timeout=3_000)
            except Exception:
                logger.debug("Activity dropdown not clickable, triggering via JS")
                await page.evaluate(f"document.getElementById('{activity_id}')?.click()")
            if not rush:
                await human_delay(0.5, 1.0)
            actv_opts = await _wait_for_select_options(page, activity_id, timeout=5_000)
            logger.debug("Activity options after click: {}", actv_opts)

    activity_done = False
    for opt in actv_opts:
        if config.preferences.activity.lower() in opt.get("text", "").lower():
            await page.select_option(activity_sel, value=opt["value"])
            activity_done = True
            logger.info("Activity selected: {} (value={})", opt["text"], opt["value"])
            if rush:
                try:
                    await page.wait_for_function(
                        f"() => document.getElementById('{center_id}')?.options.length > 1",
                        timeout=5_000,
                    )
                except Exception:
                    pass
            else:
                await human_delay(1.0, 2.0)
                await page.wait_for_load_state("networkidle")
            break

    if not activity_done and len(actv_opts) > 1:
        await page.select_option(activity_sel, value=actv_opts[1]["value"])
        logger.warning("Target activity not found, selected: {}", actv_opts[1]["text"])
        if not rush:
            await human_delay(1.0, 2.0)
            await page.wait_for_load_state("networkidle")
    elif not activity_done:
        raise FormNotReadyError("No activity options loaded — form may not be ready")

    # --- Center ---
    logger.debug("Waiting for center options to load …")
    ctr_opts = await _wait_for_select_options(page, center_id, timeout=5_000 if rush else 10_000)
    logger.debug("Center options: {}", ctr_opts)

    if len(ctr_opts) <= 1:
        ctr_el = page.locator(center_sel)
        if await ctr_el.count() > 0:
            try:
                await ctr_el.click(timeout=3_000)
            except Exception:
                logger.debug("Center dropdown not clickable, triggering via JS")
                await page.evaluate(f"document.getElementById('{center_id}')?.click()")
            if not rush:
                await human_delay(0.5, 1.0)
            ctr_opts = await _wait_for_select_options(page, center_id, timeout=5_000)
            logger.debug("Center options after click: {}", ctr_opts)

    center_done = False
    for opt in ctr_opts:
        if center_name.lower() in opt.get("text", "").lower():
            await page.select_option(center_sel, value=opt["value"])
            center_done = True
            logger.info("Center selected: {} (value={})", opt["text"], opt["value"])
            if not rush:
                await human_delay(1.0, 2.0)
                await page.wait_for_load_state("networkidle")
            break

    if not center_done and len(ctr_opts) > 1:
        await page.select_option(center_sel, value=ctr_opts[1]["value"])
        logger.warning("Target center '{}' not found, selected: {}", center_name, ctr_opts[1]["text"])
        if not rush:
            await human_delay(1.0, 2.0)
            await page.wait_for_load_state("networkidle")
    elif not center_done:
        logger.warning("No center options available")

    if not rush:
        await save_debug_snapshot(page, "06_criteria_selected")

    if auto_search:
        await click_search_button(page, config, rush=rush)


async def _click_search_raw(page: Page, config: AppConfig) -> None:
    """Click the Search button without waiting for results."""
    search_sel = config.selectors.search_button
    search_id = _selector_id(search_sel)
    search_btn = page.locator(search_sel)
    if await search_btn.count() > 0:
        try:
            await search_btn.click(timeout=2_000)
        except Exception:
            await page.evaluate(f"document.getElementById('{search_id}')?.click()")
    else:
        fallback = page.locator(
            'button:has-text("Search"), input[value="Search" i], input[type="submit"]'
        )
        if await fallback.count() > 0:
            await fallback.first.click()
        else:
            logger.warning("No Search button found")


def _metric_center_key(center_name: str) -> str:
    return (
        center_name.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
    )


def _build_probe_schedule(total_ms: int, probes: list[int]) -> list[int]:
    base = sorted({p for p in probes if p > 0})
    if not base:
        base = [max(500, total_ms // 3), max(1000, (total_ms * 2) // 3)]
    schedule: list[int] = []
    for p in base:
        if p < total_ms:
            schedule.append(p)
    schedule.append(total_ms)
    # Preserve ordering and de-dup after clamping.
    final: list[int] = []
    for p in schedule:
        if p <= 0:
            continue
        if final and p == final[-1]:
            continue
        final.append(p)
    return final


def _percentile(values: list[float], p: float) -> float:
    xs = sorted(values)
    if not xs:
        return 0.0
    idx = int(round((p / 100.0) * (len(xs) - 1)))
    idx = max(0, min(idx, len(xs) - 1))
    return xs[idx]


def _load_center_timetable_history_s(center_name: str, limit: int = 30) -> list[float]:
    if not RUNTIME_LOG_PATH.exists():
        return []
    out: list[float] = []
    target_step = f"timetable_load|{center_name}"
    try:
        lines = RUNTIME_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    for line in reversed(lines):
        if len(out) >= limit:
            break
        try:
            row = json.loads(line)
        except Exception:
            continue
        steps = row.get("rush_steps") or row.get("steps") or []
        if not isinstance(steps, list):
            continue
        for st in steps:
            if not isinstance(st, dict):
                continue
            if st.get("step") == target_step:
                dur = st.get("duration_s")
                if isinstance(dur, (int, float)) and dur > 0:
                    out.append(float(dur))
    return out


def _derive_wait_budget_for_center(
    center_name: str,
    config: AppConfig,
    *,
    mode: str,
) -> tuple[int, list[int]]:
    if mode == "first":
        default_total = int(config.settings.rush_timetable_first_wait_ms)
    else:
        default_total = int(config.settings.rush_timetable_retry_wait_ms)
    total_ms = max(2_000, default_total)
    probes = list(config.settings.rush_timetable_probe_ms)

    history = _load_center_timetable_history_s(center_name)
    if history:
        p90_ms = _percentile(history, 90.0) * 1000.0
        # Add buffer but cap to avoid over-waiting.
        adaptive = int(min(max(p90_ms * 1.15, 3_000), 35_000))
        if mode == "retry":
            adaptive = int(max(2_500, adaptive * 0.7))
        total_ms = adaptive

    schedule = _build_probe_schedule(total_ms, probes)
    return total_ms, schedule


async def _probe_timetable_state(tab: Page, config: AppConfig) -> dict:
    timetable_sel = config.selectors.timetable
    try:
        return await tab.evaluate(
            """(selector) => {
                const tables = document.querySelectorAll(selector);
                const tableCount = tables.length;
                const firstRows = tableCount >= 1 ? tables[0].querySelectorAll('tr').length : 0;
                const secondRows = tableCount >= 2 ? tables[1].querySelectorAll('tr').length : 0;
                const totalRows = firstRows + secondRows;
                const hasGridLikeRows = totalRows >= 6;
                return {
                    table_count: tableCount,
                    first_rows: firstRows,
                    second_rows: secondRows,
                    has_grid_like_rows: hasGridLikeRows
                };
            }""",
            timetable_sel,
        )
    except Exception:
        return {
            "table_count": 0,
            "first_rows": 0,
            "second_rows": 0,
            "has_grid_like_rows": False,
        }


async def _wait_for_rush_timetable_ready(
    tab: Page,
    config: AppConfig,
    *,
    probe_schedule_ms: list[int],
    reclick_guard_ms: int,
    phase: str,
) -> tuple[bool, dict]:
    """Stage-based rush wait with guarded re-clicks and probe metrics."""
    start = time.monotonic()
    first_table_ms: float | None = None
    two_tables_ms: float | None = None
    reclick_count = 0
    timeout_path = "none"
    checkpoint_idx = 0
    last_reclick_elapsed = -10_000.0
    total_budget_ms = probe_schedule_ms[-1] if probe_schedule_ms else 0

    while True:
        elapsed_ms = (time.monotonic() - start) * 1000.0
        state = await _probe_timetable_state(tab, config)
        table_count = int(state.get("table_count", 0) or 0)

        if table_count >= 1 and first_table_ms is None:
            first_table_ms = elapsed_ms
        if table_count >= 2 and bool(state.get("has_grid_like_rows")):
            two_tables_ms = elapsed_ms
            return True, {
                "search_to_first_table_ms": round(first_table_ms or elapsed_ms, 1),
                "search_to_two_tables_ms": round(two_tables_ms, 1),
                "reclick_count": reclick_count,
                "timeout_path": "none",
                "phase": phase,
            }

        while checkpoint_idx < len(probe_schedule_ms) and elapsed_ms >= probe_schedule_ms[checkpoint_idx]:
            # After the first short probe, allow guarded re-clicks at later checkpoints.
            if checkpoint_idx >= 1 and (elapsed_ms - last_reclick_elapsed) >= reclick_guard_ms:
                try:
                    search_id = _selector_id(config.selectors.search_button)
                    # A disabled button (search already in flight) silently
                    # swallows .click(); count only real dispatches.
                    fired = bool(
                        await tab.evaluate(
                            """(searchId) => {
                                const btn = document.getElementById(searchId);
                                if (!btn || btn.disabled) return false;
                                btn.click();
                                return true;
                            }""",
                            search_id,
                        )
                    )
                    if fired:
                        reclick_count += 1
                    last_reclick_elapsed = elapsed_ms
                except Exception:
                    pass
            checkpoint_idx += 1

        if elapsed_ms >= total_budget_ms:
            if first_table_ms is None:
                timeout_path = f"{phase}_first_table_timeout"
            else:
                timeout_path = f"{phase}_two_tables_timeout"
            break
        await asyncio.sleep(0.08)

    return False, {
        "search_to_first_table_ms": round(first_table_ms, 1) if first_table_ms is not None else None,
        "search_to_two_tables_ms": round(two_tables_ms, 1) if two_tables_ms is not None else None,
        "reclick_count": reclick_count,
        "timeout_path": timeout_path,
        "phase": phase,
    }


async def _wait_for_timetable(
    page: Page,
    config: AppConfig,
    *,
    timeout_ms: int = 15_000,
    retries: int = 1,
) -> bool:
    """Wait for the timetable to render. Re-clicks Search on timeout. Returns True if found."""
    timetable_sel = config.selectors.timetable
    search_id = _selector_id(config.selectors.search_button)
    for attempt in range(1 + retries):
        try:
            await page.wait_for_selector(
                timetable_sel, state="attached", timeout=timeout_ms,
            )
            return True
        except Exception:
            if attempt < retries:
                logger.debug("Timetable not found after {}ms, retrying search …", timeout_ms)
                try:
                    await page.evaluate(f"document.getElementById('{search_id}')?.click()")
                except Exception:
                    pass
            else:
                logger.debug("Timetable selector not found, falling back to networkidle")
                try:
                    await page.wait_for_load_state("networkidle", timeout=5_000)
                except Exception:
                    pass
    return False


async def click_search_button(page: Page, config: AppConfig, *, rush: bool = False) -> None:
    """Click the Search button on the booking form and wait for timetable results."""
    logger.info("Clicking Search …")
    await _click_search_raw(page, config)
    await _wait_for_timetable(
        page,
        config,
        timeout_ms=15_000,
        retries=1,
    )

    if not rush:
        await human_delay(2.0, 4.0)
        await save_debug_snapshot(page, "07_search_results")


def _extract_timetable_data_js(config: AppConfig) -> tuple[str, dict]:
    """Return the JS code that extracts the full timetable grid.

    Factored out so it can be reused by both single-date and multi-date scans.
    """
    return (
        """({ tableSelector, unavailableMarkers }) => {
        const tables = document.querySelectorAll(tableSelector);
        if (tables.length < 2) return null;

        let timeTable = null, dateTable = null;
        for (const t of tables) {
            const firstRowText = t.querySelector('tr')?.innerText?.trim() || '';
            if (/\\d{1,2}\\s+\\w{3}/.test(firstRowText)) {
                dateTable = t;
            } else {
                timeTable = t;
            }
        }
        if (!timeTable || !dateTable) {
            const t0Cols = tables[0].querySelector('tr')?.querySelectorAll('td,th')?.length || 0;
            const t1Cols = tables[1].querySelector('tr')?.querySelectorAll('td,th')?.length || 0;
            if (t0Cols > t1Cols) { dateTable = tables[0]; timeTable = tables[1]; }
            else { dateTable = tables[1]; timeTable = tables[0]; }
        }

        const timeRows = timeTable.querySelectorAll('tr');
        const times = [];
        for (const row of timeRows) {
            const text = row.innerText.trim();
            const m = text.match(/(\\d{1,2}:\\d{2})\\s*[-–\\n]\\s*(\\d{1,2}:\\d{2})/);
            times.push(m ? { start: m[1], end: m[2] } : null);
        }

        const dateRows = dateTable.querySelectorAll('tr');
        const headerCells = dateRows[0]?.querySelectorAll('td, th') || [];
        const dates = [...headerCells].map(c => c.innerText.trim());

        function getEffectiveBg(el) {
            let bg = getComputedStyle(el).backgroundColor;
            if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') return bg;
            for (const child of el.children) {
                bg = getComputedStyle(child).backgroundColor;
                if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') return bg;
                for (const gc of child.children) {
                    bg = getComputedStyle(gc).backgroundColor;
                    if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') return bg;
                }
            }
            return 'rgba(0, 0, 0, 0)';
        }

        const grid = [];
        for (let r = 1; r < dateRows.length; r++) {
            const cells = dateRows[r].querySelectorAll('td');
            const rowData = [];
            for (const cell of cells) {
                const bgColor = getEffectiveBg(cell);
                const classes = cell.className;
                const childClasses = [...cell.querySelectorAll('*')].map(e => e.className).join(' ');
                const allClasses = classes + ' ' + childClasses;
                const text = cell.innerText.trim();
                const hasOnclick = !!cell.getAttribute('onclick') ||
                                   !!cell.querySelector('[onclick]');
                const hasLink = !!cell.querySelector('a, input, button');
                const html = cell.innerHTML.trim();

                let isGray = false;
                const m = bgColor.match(/rgb\\((\\d+),\\s*(\\d+),\\s*(\\d+)\\)/);
                if (m) {
                    const [r, g, b] = [parseInt(m[1]), parseInt(m[2]), parseInt(m[3])];
                    isGray = Math.abs(r - g) < 20 && Math.abs(g - b) < 20 && r > 150;
                }
                isGray = isGray || unavailableMarkers.some((marker) => allClasses.includes(marker));

                const isEmpty = bgColor === 'rgba(0, 0, 0, 0)' && html.length < 5;

                rowData.push({
                    bg: bgColor, classes: allClasses.substring(0, 100),
                    text: text, html: html.substring(0, 200),
                    clickable: hasOnclick || hasLink,
                    isGray: isGray, isEmpty: isEmpty,
                    colIndex: rowData.length
                });
            }
            grid.push(rowData);
        }

        return { times, dates, grid };
    }""",
        {
            "tableSelector": config.selectors.timetable,
            "unavailableMarkers": config.selectors.unavailable_class_markers,
        },
    )


def _slots_from_column(
    grid, times, target_col: int, target: date, center_name: str, verbose: bool = True,
) -> List[TimeSlot]:
    """Extract available slots from a single column of the timetable grid."""
    slots: List[TimeSlot] = []
    for row_idx, row_data in enumerate(grid):
        time_idx = row_idx + 1
        if time_idx >= len(times) or not times[time_idx]:
            continue
        time_info = times[time_idx]
        if target_col >= len(row_data):
            continue
        cell = row_data[target_col]

        is_available = not cell["isGray"] and not cell["isEmpty"]
        if verbose:
            logger.debug(
                "  Row {} ({}-{}): bg={}, gray={}, empty={}, clickable={}, text='{}'",
                row_idx, time_info["start"], time_info["end"],
                cell["bg"], cell["isGray"], cell["isEmpty"],
                cell["clickable"], cell["text"],
            )

        if is_available:
            slots.append(TimeSlot(
                start=time_info["start"],
                end=time_info["end"],
                center=center_name,
                court="",
                available=True,
            ))
    return slots


async def scan_available_slots(
    page: Page, config: AppConfig, target: date | None = None, *, center_name: str | None = None,
    rush: bool = False,
) -> List[TimeSlot]:
    """Parse the POSS timetable grid and return available time slots for one date.

    Legacy single-date interface; see scan_available_slots_multi for rush mode.
    """
    logger.info("Scanning available time slots …")

    await page.wait_for_load_state("networkidle")
    if not rush:
        await human_delay(1.0, 2.0)
        await save_debug_snapshot(page, "08_slot_grid")

    script, args = _extract_timetable_data_js(config)
    timetable_data = await page.evaluate(script, args)

    if not timetable_data:
        logger.warning("Could not parse timetable structure")
        return []

    times = timetable_data["times"]
    dates = timetable_data["dates"]
    grid = timetable_data["grid"]

    logger.debug("Timetable dates: {}", dates)
    logger.debug("Timetable times: {}", [t for t in times if t])

    if target is None:
        candidates = compute_target_dates(config)
        target = candidates[0] if candidates else date.today() + timedelta(days=7)
    target_day = target.strftime("%d %b")
    target_col = -1
    for i, date_header in enumerate(dates):
        if target_day in date_header:
            target_col = i
            break

    if target_col < 0:
        logger.warning("Target date {} not found in timetable headers: {}", target_day, dates)
        return []

    logger.info("Target date column: {} ('{}')", target_col, dates[target_col])
    cname = center_name or config.preferences.center
    slots = _slots_from_column(grid, times, target_col, target, cname, verbose=not rush)

    logger.info("Found {} available slots on {}", len(slots), target)
    for s in slots:
        logger.debug("  {} – {}", s.start, s.end)
    return slots


async def scan_available_slots_multi(
    page: Page, config: AppConfig, targets: list[date], *, center_name: str,
) -> dict[date, List[TimeSlot]]:
    """Scan the timetable grid for ALL target dates in a single pass.

    This avoids re-filling the form and re-searching for each date,
    since the timetable already displays ~2 weeks of columns.
    """
    logger.info("Scanning timetable for {} dates at {} …", len(targets), center_name)

    script, args = _extract_timetable_data_js(config)
    try:
        timetable_data = await page.evaluate(script, args)
    except Exception as exc:
        # A concurrent rebuild/heal can navigate the tab mid-scan.
        logger.debug("Timetable scan skipped (page busy): {}", exc)
        return {}

    if not timetable_data:
        logger.warning("Could not parse timetable structure")
        return {}

    times = timetable_data["times"]
    dates = timetable_data["dates"]
    grid = timetable_data["grid"]

    logger.debug("Timetable dates: {}", dates)

    result: dict[date, List[TimeSlot]] = {}
    for target in targets:
        target_day = target.strftime("%d %b")
        target_col = -1
        for i, date_header in enumerate(dates):
            if target_day in date_header:
                target_col = i
                break

        if target_col < 0:
            logger.debug("Date {} not visible in timetable headers", target_day)
            result[target] = []
            continue

        slots = _slots_from_column(grid, times, target_col, target, center_name, verbose=False)
        result[target] = slots
        logger.info("  {} (col {}): {} available slots", target, target_col, len(slots))

    return result


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

def in_time_range(slot: TimeSlot, config: AppConfig, *, target: date | None = None) -> bool:
    tr = _effective_time_range(config, target)
    return slot.start_hour >= tr.start_hour and slot.end_hour <= tr.end_hour


def _effective_time_range(config: AppConfig, target: date | None) -> "TimeRange":
    """Return weekday-specific window when configured, else global time_range."""
    from bookbot.config import TimeRange  # local import for typing clarity

    if target is not None:
        overrides = getattr(config.preferences, "weekday_time_ranges", None) or {}
        override = overrides.get(target.weekday())
        if isinstance(override, TimeRange):
            return override
    return config.preferences.time_range


def _parse_hhmm(value: str) -> float:
    h, m = value.split(":")
    return int(h) + int(m) / 60


def meets_min_slot_start(slot: TimeSlot, config: AppConfig) -> bool:
    """Return True when slot start is at/after configured rush floor."""
    floor = getattr(config.preferences, "min_slot_start", "") or ""
    if not floor:
        return True
    try:
        return slot.start_hour >= _parse_hhmm(floor)
    except Exception:
        return True


def is_acceptable_rush_slot(
    slot: TimeSlot,
    config: AppConfig,
    *,
    relaxed: bool = False,
    target: date | None = None,
) -> bool:
    """Rush acceptance: available + >= min_slot_start + weekday window if set.

    Global afternoon ``time_range`` is ignored in rush unless a
    ``weekday_time_ranges`` override exists for the target weekday (e.g. Monday
    morning-only). ``relaxed`` is reserved for future fallback windows.
    """
    if not slot.available:
        return False
    if not meets_min_slot_start(slot, config):
        return False
    _ = relaxed
    overrides = getattr(config.preferences, "weekday_time_ranges", None) or {}
    if target is not None and target.weekday() in overrides:
        tr = overrides[target.weekday()]
        if not (slot.start_hour >= tr.start_hour and slot.end_hour <= tr.end_hour):
            return False
    return True


def rank_slot(slot: TimeSlot, config: AppConfig) -> float:
    score: float = 0
    if slot.center.lower() == config.preferences.center.lower():
        score += 100
    # Prefer mid-afternoon: peak at 15:30 (normal mode only)
    score += 50 - abs(slot.start_hour - 15.5) * 20
    return score


def _slot_priority_index(slot: TimeSlot, config: AppConfig) -> int:
    """Return explicit user priority index for slot start time (lower is better)."""
    priorities = config.preferences.slot_priority_starts
    if not priorities:
        return 10_000
    try:
        return priorities.index(slot.start)
    except ValueError:
        return 10_000


def _slot_minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def _ordered_slots(slots: List[TimeSlot], config: AppConfig) -> List[TimeSlot]:
    """Stable, deterministic ordering for evening fixed-slot strategy."""
    return sorted(
        slots,
        key=lambda s: (
            _slot_priority_index(s, config),
            _slot_minutes(s.start),
            -rank_slot(s, config),
        ),
    )


def _ordered_rush_slots(slots: List[TimeSlot], config: AppConfig) -> List[TimeSlot]:
    """Rush ordering: explicit priorities first, then earliest start.

    Afternoon quality scoring is intentionally disabled so the first
    acceptable slot can win without waiting for a "better" one.
    """
    return sorted(
        slots,
        key=lambda s: (
            _slot_priority_index(s, config),
            _slot_minutes(s.start),
            0 if s.center.lower() == config.preferences.center.lower() else 1,
        ),
    )


def rank_pair(pair: Tuple[TimeSlot, TimeSlot], config: AppConfig) -> float:
    return rank_slot(pair[0], config) + rank_slot(pair[1], config) + 200


def find_consecutive_pairs(slots: List[TimeSlot]) -> List[Tuple[TimeSlot, TimeSlot]]:
    pairs: List[Tuple[TimeSlot, TimeSlot]] = []
    for i, s1 in enumerate(slots):
        for s2 in slots[i + 1:]:
            if s1.end == s2.start and s1.center == s2.center:
                pairs.append((s1, s2))
    return pairs


def find_best_booking(
    slots: List[TimeSlot],
    remaining_quota: int,
    config: AppConfig,
    *,
    relaxed: bool = False,
    rush: bool = False,
    target: date | None = None,
) -> List[TimeSlot]:
    """Select booking candidate(s).

    In rush mode, defaults to first-acceptable single slot (>= min_slot_start)
    unless ``rush_prefer_consecutive`` explicitly requests pairs.
    """
    if rush:
        return find_rush_booking(
            slots,
            remaining_quota,
            config,
            relaxed=relaxed,
            target=target,
        )

    if relaxed:
        ftr = config.preferences.fallback_time_range
        if ftr:
            candidates = [
                s for s in slots if s.available
                and s.start_hour >= ftr.start_hour and s.end_hour <= ftr.end_hour
            ]
        else:
            candidates = [s for s in slots if s.available]
    else:
        candidates = [
            s for s in slots if s.available and in_time_range(s, config, target=target)
        ]

    if not candidates:
        label = "fallback" if relaxed else "preferred-range"
        logger.warning("No available {} slots found", label)
        return []

    candidates = _ordered_slots(candidates, config)

    # Try consecutive pairs first
    if remaining_quota >= 2 and config.preferences.prefer_consecutive >= 2:
        pairs = find_consecutive_pairs(candidates)
        if pairs:
            best_pair = max(
                pairs,
                key=lambda p: (
                    -min(_slot_priority_index(p[0], config), _slot_priority_index(p[1], config)),
                    rank_pair(p, config),
                ),
            )
            logger.info(
                "Best consecutive pair: {} – {} & {} – {}",
                best_pair[0].start, best_pair[0].end,
                best_pair[1].start, best_pair[1].end,
            )
            return list(best_pair)

    # Fallback: single best slot
    best = candidates[0]
    logger.info("Best single slot: {} – {}", best.start, best.end)
    return [best]


def find_rush_booking(
    slots: List[TimeSlot],
    remaining_quota: int,
    config: AppConfig,
    *,
    relaxed: bool = False,
    target: date | None = None,
) -> List[TimeSlot]:
    """Rush selection: first acceptable slot wins."""
    candidates = [
        s
        for s in slots
        if is_acceptable_rush_slot(s, config, relaxed=relaxed, target=target)
    ]
    if not candidates:
        logger.warning("No acceptable rush slots found (>= {})", config.preferences.min_slot_start)
        return []

    candidates = _ordered_rush_slots(candidates, config)
    prefer_n = int(getattr(config.settings, "rush_prefer_consecutive", 1) or 1)

    if remaining_quota >= 2 and prefer_n >= 2:
        pairs = find_consecutive_pairs(candidates)
        if pairs:
            # Still pick the earliest/highest-priority pair, not afternoon-scored.
            best_pair = min(
                pairs,
                key=lambda p: (
                    min(_slot_priority_index(p[0], config), _slot_priority_index(p[1], config)),
                    _slot_minutes(p[0].start),
                ),
            )
            logger.info(
                "Rush consecutive pair: {} – {} & {} – {}",
                best_pair[0].start, best_pair[0].end,
                best_pair[1].start, best_pair[1].end,
            )
            return list(best_pair)

    best = candidates[0]
    logger.info("Rush first-acceptable slot: {} – {}", best.start, best.end)
    return [best]


# ---------------------------------------------------------------------------
# Book execution
# ---------------------------------------------------------------------------

async def _find_slot_cell(page: Page, slot: TimeSlot, target: date, config: AppConfig):
    """Locate the timetable cell element for a given time slot on the target date."""
    target_day = target.strftime("%d %b")

    table_selector = config.selectors.timetable
    cell = await page.evaluate_handle(
        """({ targetDay, startTime, endTime, tableSelector }) => {
            const tables = document.querySelectorAll(tableSelector);
            if (tables.length < 2) return null;

            let timeTable = null, dateTable = null;
            for (const t of tables) {
                const firstRowText = t.querySelector('tr')?.innerText?.trim() || '';
                if (/\\d{1,2}\\s+\\w{3}/.test(firstRowText)) dateTable = t;
                else timeTable = t;
            }
            if (!timeTable || !dateTable) return null;

            // Find target column
            const headers = dateTable.querySelectorAll('tr')[0]?.querySelectorAll('td, th') || [];
            let colIdx = -1;
            for (let i = 0; i < headers.length; i++) {
                if (headers[i].innerText.trim().includes(targetDay)) { colIdx = i; break; }
            }
            if (colIdx < 0) return null;

            // Find target row by matching time
            const timeRows = timeTable.querySelectorAll('tr');
            let rowIdx = -1;
            const timeRe = /(\\d{1,2}:\\d{2})\\s*[-–\\n]\\s*(\\d{1,2}:\\d{2})/;
            for (let r = 1; r < timeRows.length; r++) {
                const m = timeRows[r].innerText.trim().match(timeRe);
                if (m && m[1] === startTime && m[2] === endTime) { rowIdx = r; break; }
            }
            if (rowIdx < 0) return null;

            // Get the cell at (rowIdx, colIdx) in the date table
            const dataRow = dateTable.querySelectorAll('tr')[rowIdx];
            if (!dataRow) return null;
            const cells = dataRow.querySelectorAll('td');
            return colIdx < cells.length ? cells[colIdx] : null;
        }""",
        {
            "targetDay": target_day,
            "startTime": slot.start,
            "endTime": slot.end,
            "tableSelector": table_selector,
        },
    )
    return cell


BOOKING_CONFLICT_MARKERS = [
    "facility is occupied",
    "is occupied",
    "already been booked",
    "already booked",
    "no longer available",
    "slot is taken",
    "not available",
    "booking failed",
    "conflict",
]


async def _is_booking_conflict(page: Page) -> bool:
    """Detect if the current page shows a booking conflict / 'occupied' error."""
    try:
        text = (await page.inner_text("body")).lower()
        return any(m in text for m in BOOKING_CONFLICT_MARKERS)
    except Exception:
        return False


async def _fast_switch_center(page: Page, center_name: str) -> bool:
    """Switch only the center dropdown without touching date/activity.

    Much faster than re-filling the entire form when only the center changes.
    """
    ctr_opts = await page.evaluate(
        "[...document.getElementById('ctrId')?.options || []]"
        ".map(o => ({value: o.value, text: o.text.trim()}))"
    )
    for opt in ctr_opts:
        if center_name.lower() in opt.get("text", "").lower():
            await page.select_option("#ctrId", value=opt["value"])
            logger.info("Fast-switched center to: {} (value={})", opt["text"], opt["value"])
            try:
                await page.wait_for_load_state("networkidle", timeout=5_000)
            except Exception:
                pass
            return True
    logger.warning("Center '{}' not found in dropdown for fast switch", center_name)
    return False


async def _click_slots_js(page: Page, slots: List[TimeSlot], target: date, config: AppConfig) -> int:
    """Click multiple timetable cells in a single JS evaluation.

    The POSS timetable binds click handlers on child elements inside <td> cells
    (e.g. <a>, <div>, <span> with onclick).  We must click the innermost
    interactive element, not the bare <td>, otherwise the selection doesn't
    register and the Next button stays disabled.
    """
    target_day = target.strftime("%d %b")
    slot_data = [{"start": s.start, "end": s.end} for s in slots]
    table_selector = config.selectors.timetable
    return await page.evaluate(
        """({ targetDay, slots, tableSelector }) => {
            const tables = document.querySelectorAll(tableSelector);
            if (tables.length < 2) return 0;

            let timeTable = null, dateTable = null;
            for (const t of tables) {
                const firstRowText = t.querySelector('tr')?.innerText?.trim() || '';
                if (/\\d{1,2}\\s+\\w{3}/.test(firstRowText)) dateTable = t;
                else timeTable = t;
            }
            if (!timeTable || !dateTable) {
                const t0 = tables[0].querySelector('tr')?.querySelectorAll('td,th')?.length || 0;
                const t1 = tables[1].querySelector('tr')?.querySelectorAll('td,th')?.length || 0;
                if (t0 > t1) { dateTable = tables[0]; timeTable = tables[1]; }
                else { dateTable = tables[1]; timeTable = tables[0]; }
            }

            const headers = dateTable.querySelectorAll('tr')[0]?.querySelectorAll('td, th') || [];
            let colIdx = -1;
            for (let i = 0; i < headers.length; i++) {
                if (headers[i].innerText.trim().includes(targetDay)) { colIdx = i; break; }
            }
            if (colIdx < 0) return 0;

            const timeRows = timeTable.querySelectorAll('tr');
            const timeRe = /(\\d{1,2}:\\d{2})\\s*[-–\\n]\\s*(\\d{1,2}:\\d{2})/;
            const rowMap = {};
            for (let r = 1; r < timeRows.length; r++) {
                const m = timeRows[r].innerText.trim().match(timeRe);
                if (m) rowMap[m[1] + '-' + m[2]] = r;
            }

            let clicked = 0;
            for (const { start, end } of slots) {
                const rowIdx = rowMap[start + '-' + end];
                if (rowIdx === undefined) continue;
                const dataRow = dateTable.querySelectorAll('tr')[rowIdx];
                if (!dataRow) continue;
                const cells = dataRow.querySelectorAll('td');
                if (colIdx < cells.length) {
                    const cell = cells[colIdx];
                    // Find the innermost interactive element
                    const inner = cell.querySelector(
                        'a[href], a[onclick], [onclick], input, button, '
                        + 'div[class*="slot"], div[class*="book"], span[class*="slot"]'
                    );
                    const clickTarget = inner || cell;
                    // Dispatch the full pointer/mouse event sequence so frameworks
                    // that listen on mousedown/pointerdown also react.
                    const rect = clickTarget.getBoundingClientRect();
                    const cx = rect.left + rect.width / 2;
                    const cy = rect.top + rect.height / 2;
                    const opts = {bubbles: true, cancelable: true, clientX: cx, clientY: cy};
                    clickTarget.dispatchEvent(new PointerEvent('pointerdown', opts));
                    clickTarget.dispatchEvent(new MouseEvent('mousedown', opts));
                    clickTarget.dispatchEvent(new PointerEvent('pointerup', opts));
                    clickTarget.dispatchEvent(new MouseEvent('mouseup', opts));
                    clickTarget.dispatchEvent(new MouseEvent('click', opts));
                    clicked++;
                }
            }
            return clicked;
        }""",
        {"targetDay": target_day, "slots": slot_data, "tableSelector": table_selector},
    )


# ── Endgame: armed Next click (2026-09-12 post-mortem) ──────────────────────
# The site re-enables Next only after its own selection-validation round trip
# (~3s under open congestion).  Polling from Python wastes that window, and
# the old fallbacks could report success without a real click; the observer
# below clicks within ms of the real enablement and the Python side verifies
# the click produced a submit-path request or a navigation.
_NEXT_ARM_STABILITY_MS = 120

_ARM_NEXT_CLICK_JS = r"""async (args) => {
    const { selector, timeoutMs, stabilityMs } = args;
    const t0 = performance.now();
    let enables = 0;
    let wasEnabled = false;
    let stableTimer = null;
    let done = false;

    const findBtn = () => document.querySelector(selector);
    const isEnabled = (btn) => {
        if (!btn) return false;
        if (btn.disabled === true) return false;
        if (btn.hasAttribute && btn.hasAttribute('disabled')) return false;
        const cls = (btn.className || '').toString();
        if (/(^|\s)(disabled|ui-state-disabled)(\s|$)/.test(cls)) return false;
        if (btn.getAttribute && btn.getAttribute('aria-disabled') === 'true') return false;
        return true;
    };
    const clickBtn = (btn) => {
        try {
            const rect = btn.getBoundingClientRect();
            const cx = rect.left + rect.width / 2;
            const cy = rect.top + rect.height / 2;
            const opts = {bubbles: true, cancelable: true, clientX: cx, clientY: cy, button: 0};
            btn.dispatchEvent(new PointerEvent('pointerdown', opts));
            btn.dispatchEvent(new MouseEvent('mousedown', opts));
            btn.dispatchEvent(new PointerEvent('pointerup', opts));
            btn.dispatchEvent(new MouseEvent('mouseup', opts));
            btn.dispatchEvent(new MouseEvent('click', opts));
            return true;
        } catch (e) {
            try { btn.click(); return true; } catch (e2) { return false; }
        }
    };

    return await new Promise((resolve) => {
        const finish = (payload) => {
            if (done) return;
            done = true;
            if (stableTimer !== null) clearTimeout(stableTimer);
            mo.disconnect();
            clearTimeout(tout);
            resolve(Object.assign({}, payload, {
                found: !!findBtn(),
                enables: enables,
                waitedMs: Math.round(performance.now() - t0),
            }));
        };

        const observe = () => {
            const btn = findBtn();
            const en = isEnabled(btn);
            if (en && !wasEnabled) {
                enables += 1;
                if (stableTimer !== null) clearTimeout(stableTimer);
                stableTimer = setTimeout(() => {
                    stableTimer = null;
                    if (done) return;
                    const b2 = findBtn();
                    if (isEnabled(b2)) {
                        finish({clicked: clickBtn(b2), via: 'armed'});
                    }
                }, stabilityMs);
            } else if (!en && wasEnabled) {
                if (stableTimer !== null) { clearTimeout(stableTimer); stableTimer = null; }
            }
            wasEnabled = en;
        };

        const mo = new MutationObserver(observe);
        mo.observe(document.documentElement, {
            subtree: true, attributes: true,
            attributeFilter: ['disabled', 'class', 'aria-disabled'],
            childList: true,
        });
        observe();
        const tout = setTimeout(() => finish({clicked: false, via: 'timeout'}), timeoutMs);
    });
}"""


async def _arm_next_click(page: Page, next_selector: str, *, timeout_ms: int) -> dict:
    """Click Next within ms of the site (re-)enabling it, via an in-page observer."""
    try:
        result = await page.evaluate(
            _ARM_NEXT_CLICK_JS,
            {
                "selector": next_selector,
                "timeoutMs": int(timeout_ms),
                "stabilityMs": _NEXT_ARM_STABILITY_MS,
            },
        )
    except Exception as exc:
        logger.debug("Arm-next observer failed: {}", exc)
        return {"clicked": False, "via": "error", "found": False, "enables": 0, "waitedMs": 0.0}
    if not isinstance(result, dict):
        return {"clicked": False, "via": "invalid", "found": False, "enables": 0, "waitedMs": 0.0}
    return result


def _submit_requests_seen() -> int:
    """Count prepare/submit requests observed on the wire so far this run."""
    metrics = getattr(tracker, "_metrics", {}) or {}
    return int(metrics.get("prepare_request_seen_count", 0) or 0) + int(
        metrics.get("submit_request_seen_count", 0) or 0
    )


async def _wait_submit_effect(page: Page, baseline: int, start_url: str | None,
                              *, timeout_ms: int = 1200) -> bool:
    """True once a click produced evidence: a submit-path request or a navigation.

    ``start_url`` must be snapshotted BEFORE the click: an effect that lands
    before this polling starts must not look like "no effect" (the 2026-09-13
    smoke showed that causing a needless re-arm plus an extra click).
    """
    deadline = time.monotonic() + timeout_ms / 1000.0
    while time.monotonic() < deadline:
        if _submit_requests_seen() > baseline:
            return True
        try:
            if start_url is None or page.url != start_url:
                return True
        except Exception:
            return True
        await asyncio.sleep(0.05)
    return _submit_requests_seen() > baseline


async def _next_button_enabled(page: Page, next_selector: str) -> bool:
    """True when the Next element exists and is currently enabled."""
    try:
        return bool(
            await page.evaluate(
                """(sel) => {
                    const btn = document.querySelector(sel);
                    if (!btn) return false;
                    if (btn.disabled === true || btn.hasAttribute('disabled')) return false;
                    return true;
                }""",
                next_selector,
            )
        )
    except Exception:
        return False


def _network_event_kind(url: str) -> str | None:
    """Classify a POSS URL into the critical-path request families we track."""
    lower = (url or "").lower()
    if "timetable" in lower:
        return "search"
    if "make_book_submit" in lower:
        return "submit"
    if "make_book" in lower and "submit" not in lower:
        return "prepare"
    return None


async def _click_next_fast(page: Page, next_selector: str, backoff_ms: list[int],
                           *, arm_timeout_ms: int = 4000) -> bool:
    """Click Next the moment the site (re-)enables it — event-driven, not polled.

    An in-page MutationObserver clicks within milliseconds of the real
    enablement; if the click produces no submit-path request / navigation
    within a short window (e.g. it landed on a brief fake-enable flash), the
    observer is re-armed once.  Fallbacks are bounded and never report success
    unless a real click was dispatched.
    """
    deadline = time.monotonic() + max(0.5, float(arm_timeout_ms) / 1000.0)
    baseline = _submit_requests_seen()
    rearmed = False
    try:
        start_url: str | None = page.url
    except Exception:
        start_url = None

    while True:
        remaining_ms = int(max(200.0, (deadline - time.monotonic()) * 1000.0))
        armed = await _arm_next_click(page, next_selector, timeout_ms=remaining_ms)
        via = str(armed.get("via") or "")
        tracker.set_metric("next_click_via", via)
        tracker.set_metric("next_click_wait_ms", float(armed.get("waitedMs") or 0.0))
        tracker.set_metric("next_click_enables", int(armed.get("enables") or 0))

        if armed.get("clicked"):
            tracker.mark_event("next_click_done", via=via, waited_ms=armed.get("waitedMs"))
            if await _wait_submit_effect(page, baseline, start_url):
                return True
            if rearmed or time.monotonic() >= deadline:
                break
            if not await _next_button_enabled(page, next_selector):
                break
            rearmed = True
            tracker.set_metric("next_click_rearmed", 1)
            logger.debug("Armed Next click had no effect — re-arming once")
            continue

        if via == "error" and await _wait_submit_effect(page, baseline, start_url, timeout_ms=600):
            tracker.mark_event("next_click_done", via="navigation")
            return True
        break

    # Fallback 1: bounded native retries on the exact selector.
    ready_sel = f"{next_selector}:not([disabled])"
    for wait_ms in backoff_ms:
        try:
            await page.wait_for_selector(ready_sel, timeout=max(100, wait_ms))
            await page.locator(ready_sel).first.click(timeout=max(100, wait_ms))
            tracker.set_metric("next_click_via", "native_retry")
            tracker.mark_event("next_click_done", via="native_retry")
            return True
        except Exception:
            logger.debug("Next not ready after {}ms, retrying …", wait_ms)

    # Fallback 2: strict JS click — only when the element exists and is enabled.
    try:
        clicked = await page.evaluate(
            """(sel) => {
                const btn = document.querySelector(sel);
                if (!btn) return false;
                if (btn.disabled === true || btn.hasAttribute('disabled')) return false;
                btn.click();
                return true;
            }""",
            next_selector,
        )
        if clicked:
            tracker.set_metric("next_click_via", "js_last_resort")
            tracker.mark_event("next_click_done", via="js_last_resort")
            return True
    except Exception:
        pass

    tracker.set_metric("next_click_via", "failed")
    return False


# ---------------------------------------------------------------------------
# Rush crash resilience (2026-09-15)
# ---------------------------------------------------------------------------
# A page navigation destroyed the JS context under the confirm-page checkbox
# evaluate mid-rush; the exception bubbled up through the wave loop and killed
# the entire attempt — taking the late recovery waves (the 09-14 safety net)
# with it.  On the hot path a lane must fail locally, evaluates must tolerate
# losing their context, and the confirm-page wait gets a grace window under
# slow server renders.

_CTX_LOST_MARKERS = (
    "execution context was destroyed",
    "target page, context or browser has been closed",
    "target closed",
    "frame was detached",
    "navigation interrupted",
)


def _is_context_lost(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(marker in msg for marker in _CTX_LOST_MARKERS)


def _set_metric_once(name: str, value: object) -> None:
    """Record a first-occurrence metric; retry attempts must not overwrite it."""
    try:
        existing = getattr(tracker, "_metrics", None) or {}
    except Exception:
        existing = {}
    if name in existing:
        return
    tracker.set_metric(name, value)


async def _safe_evaluate(
    page: Page,
    script: str,
    arg: object = None,
    *,
    retries: int = 3,
    wait_timeout_ms: int = 1500,
    default: object = None,
) -> object:
    """``page.evaluate`` that tolerates losing the JS context to a navigation.

    Non-navigation errors still raise so real bugs stay visible; when every
    retry loses the race the caller's ``default`` is returned instead.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, max(1, int(retries)) + 1):
        try:
            if arg is None:
                return await page.evaluate(script)
            return await page.evaluate(script, arg)
        except Exception as exc:  # noqa: BLE001 — classified below
            if not _is_context_lost(exc):
                raise
            last_exc = exc
            try:
                await page.wait_for_load_state(
                    "domcontentloaded", timeout=max(100, wait_timeout_ms),
                )
            except Exception:
                pass
            await asyncio.sleep(0.05 * attempt)
    logger.warning("safe evaluate gave up after {} retries: {}", retries, last_exc)
    return default


async def _tick_confirm_checkboxes(page: Page) -> int:
    """Tick confirm-page checkboxes; survives a mid-navigation context loss.

    Primary path is one JS evaluation (fastest).  If the page navigated under
    it, fall back to Playwright locator clicks, which re-resolve after
    navigations.  Returns how many checkboxes were (attempted to be) ticked.
    """
    ticked = await _safe_evaluate(
        page,
        """() => {
            const cbs = document.querySelectorAll('input[type="checkbox"]:not(:checked)');
            cbs.forEach(cb => cb.click());
            return cbs.length;
        }""",
    )
    if ticked is not None:
        return int(ticked) if isinstance(ticked, (int, float)) else 0

    try:
        cbs = page.locator('input[type="checkbox"]:not(:checked)')
        count = await cbs.count()
        for i in range(count):
            try:
                await cbs.nth(i).check(timeout=1000)
            except Exception:
                pass
        logger.info("Ticked {} checkbox(es) via locator fallback", count)
        return count
    except Exception as exc:
        logger.debug("Checkbox tick fallback failed: {}", exc)
        return 0


_CONFIRM_PAGE_SELECTOR = (
    'input[type="checkbox"], button:has-text("Confirm"), '
    'input[value="Confirm"], button:has-text("Submit")'
)


async def _await_confirmation_page(
    page: Page,
    *,
    page_timeout_ms: int,
    grace_timeout_ms: int,
) -> bool:
    """Wait for the confirmation-page controls, with a grace window under load.

    First budget stays tight for speed; when the render is slow we keep
    waiting for the *real* controls for one more grace window before falling
    back to a bare load-state wait.
    """
    try:
        await page.wait_for_selector(
            _CONFIRM_PAGE_SELECTOR, timeout=max(100, int(page_timeout_ms)),
        )
        tracker.mark_event("confirmation_page_seen")
        return True
    except Exception:
        tracker.add_feedback("confirmation_page_too_slow", timeout_ms=page_timeout_ms)

    try:
        await page.wait_for_selector(
            _CONFIRM_PAGE_SELECTOR, timeout=max(100, int(grace_timeout_ms)),
        )
        tracker.mark_event("confirmation_page_seen", grace=True)
        return True
    except Exception:
        pass

    try:
        await page.wait_for_load_state(
            "domcontentloaded", timeout=max(100, int(grace_timeout_ms)),
        )
        return True
    except Exception:
        return False


_EVIDENCE_DIR = Path("logs/booking_evidence")


async def _save_booking_evidence(page: Page, base_dir: Path | None = None) -> Path | None:
    """Archive a screenshot + visible text of the confirmation page.

    2026-09-16: the site's confirmation email can be delayed or missing, and a
    missing email made a successful booking look like a failure.  This local
    receipt (plus the daily morning report) answers "did it actually book?"
    without logging into POSS.  Must never break the booking flow itself.
    """
    out_dir = Path(base_dir) if base_dir is not None else _EVIDENCE_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    shot_path: Path | None = out_dir / f"{stamp}-confirm.png"
    try:
        await page.screenshot(path=str(shot_path), timeout=8_000)
    except Exception as exc:
        logger.debug("Evidence screenshot failed: {}", exc)
        shot_path = None
    try:
        text = await page.inner_text("body")
        (out_dir / f"{stamp}-confirm.txt").write_text(text, encoding="utf-8")
    except Exception as exc:
        logger.debug("Evidence text dump failed: {}", exc)
    tracker.set_metric("booking_evidence", str(shot_path or out_dir))
    return shot_path


async def book_slots(page: Page, slots_to_book: List[TimeSlot], target: date, config: AppConfig,
                     *, rush: bool = False, candidate: dict | None = None) -> bool:
    """Click on the chosen slot(s) in the timetable grid and confirm the booking.

    In rush mode, every millisecond counts:
      - Batch-click all slots via a single JS evaluation (no Playwright round-trips)
      - Skip all screenshots
      - Wait for specific elements instead of generic networkidle
      - Tick checkboxes via JS
      - Use short deadlines + fast fallback instead of multi-second waits
    """
    if not slots_to_book:
        return False

    select_timeout = int(getattr(config.settings, "rush_slot_select_timeout_ms", 200) or 200)
    confirm_page_timeout = int(getattr(config.settings, "rush_confirm_page_timeout_ms", 800) or 800)
    confirm_result_timeout = int(getattr(config.settings, "rush_confirm_result_timeout_ms", 1500) or 1500)

    # ── Step 1: Click slot cells ──
    if rush:
        if candidate is not None:
            tracker.update_candidate(candidate, "click_started_ms")
            seen = candidate.get("first_seen_ms")
            clicked = candidate.get("click_started_ms")
            if isinstance(seen, (int, float)) and isinstance(clicked, (int, float)):
                tracker.set_metric("candidate_to_click_ms", round(float(clicked) - float(seen), 1))

        booked_count = await _click_slots_js(page, slots_to_book, target, config)
        for s in slots_to_book[:booked_count]:
            logger.info("Clicked slot {} – {} (via JS)", s.start, s.end)

        # Verify selection registered quickly; fall back to native clicks.
        if booked_count > 0:
            try:
                await page.wait_for_selector(
                    '#nextButton:not([disabled])', timeout=max(50, select_timeout),
                )
                if candidate is not None:
                    tracker.update_candidate(candidate, "selection_registered_ms")
            except Exception:
                logger.warning("Next still disabled after JS click — native fallback")
                tracker.add_feedback("js_click_not_registered",
                                     slots=[f"{s.start}-{s.end}" for s in slots_to_book])
                booked_count = 0
                for slot in slots_to_book:
                    cell_handle = await _find_slot_cell(page, slot, target, config)
                    el = cell_handle.as_element()
                    if el:
                        await el.click()
                        booked_count += 1
                        logger.info("Clicked slot {} – {} (native fallback)", slot.start, slot.end)
                if booked_count > 0:
                    try:
                        await page.wait_for_selector(
                            '#nextButton:not([disabled])',
                            timeout=max(50, select_timeout * 2),
                        )
                        if candidate is not None:
                            tracker.update_candidate(candidate, "selection_registered_ms")
                    except Exception:
                        logger.warning("Next STILL disabled after native clicks")
                        tracker.add_feedback("native_click_failed",
                                             slots=[f"{s.start}-{s.end}" for s in slots_to_book])
                        if candidate is not None:
                            tracker.finish_candidate(candidate, "automation_failure")
                        return False
                else:
                    tracker.add_feedback("slot_cell_not_found",
                                         slots=[f"{s.start}-{s.end}" for s in slots_to_book])
                    if candidate is not None:
                        tracker.finish_candidate(candidate, "automation_failure")
                    return False

        if candidate is not None and candidate.get("selection_registered_ms") is not None:
            click_ms = candidate.get("click_started_ms")
            sel_ms = candidate.get("selection_registered_ms")
            if isinstance(click_ms, (int, float)) and isinstance(sel_ms, (int, float)):
                tracker.set_metric("click_to_selection_ms", round(float(sel_ms) - float(click_ms), 1))
    else:
        booked_count = 0
        for slot in slots_to_book:
            logger.info("Attempting to book {} – {} …", slot.start, slot.end)
            cell_handle = await _find_slot_cell(page, slot, target, config)
            el = cell_handle.as_element()
            if el:
                await el.scroll_into_view_if_needed()
                await human_delay(0.3, 0.8)
                await el.click()
                await human_delay(1.0, 2.0)
                booked_count += 1
                logger.info("Clicked slot {} – {}", slot.start, slot.end)
            else:
                logger.warning("Could not locate timetable cell for {} – {}", slot.start, slot.end)

    if booked_count == 0:
        return False

    if not rush:
        await save_debug_snapshot(page, "09_slots_selected")

    # ── Step 2: Click Next ──
    next_selector = config.selectors.next_button
    if rush:
        # No precheck: the armed observer waits for the site's validation to
        # re-enable Next and clicks it within ms of that moment.
        logger.info("Clicking Next (armed) …")
        if candidate is not None:
            tracker.update_candidate(candidate, "next_started_ms")
        clicked = await _click_next_fast(
            page,
            next_selector,
            config.settings.next_click_backoff_ms,
            arm_timeout_ms=int(getattr(config.settings, "rush_next_click_timeout_ms", 4000) or 4000),
        )
        if not clicked:
            logger.warning("Next click failed (never became clickable within budget)")
            tracker.add_feedback("next_not_found")
            if candidate is not None:
                tracker.finish_candidate(candidate, "automation_failure")
            return False
        tracker.mark_event("confirmation_page_wait_started")
        if not await _await_confirmation_page(
            page,
            page_timeout_ms=confirm_page_timeout,
            grace_timeout_ms=confirm_result_timeout,
        ):
            if candidate is not None:
                tracker.finish_candidate(candidate, "bot_latency_loss")
            return False
    else:
        next_btn = page.locator(f"{next_selector}:not([disabled])")
        if await next_btn.count() == 0:
            next_btn = page.locator('button:has-text("Next")')
        if await next_btn.count() > 0:
            logger.info("Clicking Next …")
            await next_btn.first.click()
            await page.wait_for_load_state("networkidle")
            await human_delay(2.0, 4.0)
            await save_debug_snapshot(page, "10_next_page")

    # ── Step 3: Tick checkboxes ──
    if rush:
        ticked = await _tick_confirm_checkboxes(page)
        if ticked:
            logger.info("Ticked {} checkbox(es)", ticked)
    else:
        checkboxes = page.locator(
            'input[type="checkbox"]:not(:checked), '
            'input[type="checkbox"]:not([checked])'
        )
        cb_count = await checkboxes.count()
        if cb_count > 0:
            logger.info("Found {} unchecked checkbox(es) on confirm page, ticking …", cb_count)
            for i in range(cb_count):
                await checkboxes.nth(i).check()
                await human_delay(0.2, 0.5)
            await save_debug_snapshot(page, "10b_checkbox_ticked")

    # ── Step 4: Click Confirm ──
    confirm_sel = (
        'button:has-text("Confirm"), button:has-text("Submit"), '
        'input[value="Confirm"], input[value="Submit"], '
        'button:has-text("Book"), input[value="Book"]'
    )
    confirm_btn = page.locator(confirm_sel).first
    if await confirm_btn.count() > 0:
        if not rush:
            await human_delay(0.5, 1.0)
        if rush and candidate is not None:
            tracker.update_candidate(candidate, "confirm_started_ms")
            seen = candidate.get("first_seen_ms")
            conf = candidate.get("confirm_started_ms")
            sel = candidate.get("selection_registered_ms")
            if isinstance(seen, (int, float)) and isinstance(conf, (int, float)):
                tracker.set_metric("candidate_to_confirm_ms", round(float(conf) - float(seen), 1))
            if isinstance(sel, (int, float)) and isinstance(conf, (int, float)):
                tracker.set_metric("selection_to_confirm_ms", round(float(conf) - float(sel), 1))
        await confirm_btn.click()

        if rush:
            try:
                await page.wait_for_load_state(
                    "domcontentloaded",
                    timeout=max(100, confirm_result_timeout),
                )
            except Exception:
                pass
        else:
            await page.wait_for_load_state("networkidle")
            await human_delay(1.0, 2.0)

        if not rush:
            await save_debug_snapshot(page, "11_booking_confirmed")

        if await _is_booking_conflict(page):
            logger.warning("Slot was already taken (occupied)! Will try another slot.")
            if candidate is not None:
                tracker.finish_candidate(candidate, "conflict")
            back_btn = page.locator('a:has-text("Back"), button:has-text("Back")')
            if await back_btn.count() > 0:
                await back_btn.first.click()
                try:
                    await page.wait_for_load_state(
                        "domcontentloaded",
                        timeout=max(100, confirm_result_timeout),
                    )
                except Exception:
                    pass
            return False

        logger.success("Booking confirmed ({} slot(s))", booked_count)
        if candidate is not None:
            tracker.finish_candidate(candidate, "booked")

        try:
            ok_btn = page.locator(
                'button:has-text("OK"):visible, button:has-text("Yes"):visible, '
                'input[value="OK"]:visible, input[value="Yes"]:visible'
            )
            if await ok_btn.count() > 0:
                if not rush:
                    await human_delay(0.3, 0.8)
                await ok_btn.first.click(timeout=5_000)
                if not rush:
                    await human_delay(1.0, 2.0)
                    await save_debug_snapshot(page, "12_final_confirm")
        except Exception as exc:
            logger.debug("OK/Yes button click skipped or timed out: {}", exc)

        # Keep a local receipt of the confirmation page (screenshot + text).
        try:
            await _save_booking_evidence(page)
        except Exception as exc:
            logger.debug("Booking evidence capture failed: {}", exc)
    else:
        logger.warning("No confirm button found – booking may require manual confirmation")
        if rush:
            tracker.add_feedback("confirm_not_found")
            if candidate is not None:
                tracker.finish_candidate(candidate, "automation_failure")
            return False
        await save_debug_snapshot(page, "11_no_confirm_button")

    return True


async def _book_slots_guarded(
    tab: Page,
    best: List[TimeSlot],
    target: date,
    config: AppConfig,
    *,
    candidate: dict | None = None,
    center_name: str = "",
) -> bool:
    """``book_slots`` with lane-local failure isolation on the rush hot path.

    Any unexpected page error inside a booking lane (e.g. a mid-navigation
    context destruction) must fail just that lane — the wave loop continues
    with the remaining centers and the late recovery waves stay alive.
    Session-level errors still propagate to the attempt handler.
    """
    try:
        return await book_slots(
            tab, best, target, config, rush=True, candidate=candidate,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        from bookbot.auth import MaintenanceError

        if isinstance(exc, (MaintenanceError, FormNotReadyError)):
            raise
        logger.warning(
            "book_slots lane failed for {} @ {} ({}): {}",
            center_name or "?", target, type(exc).__name__, exc,
        )
        tracker.add_feedback(
            "lane_exception",
            center=center_name,
            date=str(target),
            error=type(exc).__name__,
            detail=str(exc)[:200],
        )
        if candidate is not None:
            tracker.finish_candidate(candidate, "automation_failure")
        return False


# ---------------------------------------------------------------------------
# Orchestrated booking flow
# ---------------------------------------------------------------------------

async def _ensure_booking_form(page: Page, config: AppConfig, *, rush: bool = False) -> None:
    """Make sure the Sports Facility booking form is loaded AND visible on the page.

    The POSS page contains #actvId in the DOM even when the Sports Facility
    form is collapsed/hidden.  We must check *visibility*, not just presence,
    otherwise a new tab will skip the 'Sports Facility' click and all
    subsequent form interactions will fail on hidden elements.
    """
    activity_sel = config.selectors.activity
    activity_id = _selector_id(activity_sel)
    search_date_id = _selector_id(config.selectors.search_date)
    center_id = _selector_id(config.selectors.center)
    search_button_id = _selector_id(config.selectors.search_button)

    actv = page.locator(activity_sel)
    if await actv.count() > 0:
        try:
            if await actv.first.is_visible(timeout=2_000):
                return
        except Exception:
            pass
        logger.debug("Booking form exists in DOM but is NOT visible — need to click Sports Facility")

    opened = await _open_sports_facility_panel(page, config, rush=rush)
    if not opened:
        logger.debug("Sports Facility panel did not open, re-navigating …")
        await page.goto(BOOKING_URL, wait_until="domcontentloaded")
        if not rush:
            await human_delay(1.0, 2.0)
        opened = await _open_sports_facility_panel(page, config, rush=rush)
        if not opened:
            logger.debug("Sports Facility panel still closed after re-navigation")

    if await page.locator(activity_sel).count() == 0:
        from bookbot.auth import is_maintenance_page, MaintenanceError
        if await is_maintenance_page(page):
            raise MaintenanceError("Booking form page is under maintenance")
        raise FormNotReadyError(f"Booking form ({activity_sel}) not found after navigation")

    # Readiness hardening: verify key controls are visible and enabled before rush flow continues.
    try:
        await page.wait_for_function(
            f"""() => {{
                const dateInput = document.getElementById('{search_date_id}');
                const actv = document.getElementById('{activity_id}');
                const ctr = document.getElementById('{center_id}');
                const search = document.getElementById('{search_button_id}');
                if (!dateInput || !actv || !ctr || !search) return false;
                const visible = (el) => !!(el.offsetParent || el.getClientRects().length);
                return visible(dateInput) && visible(actv) && visible(ctr) && !search.disabled;
            }}""",
            timeout=8_000 if rush else 12_000,
        )
    except Exception as exc:
        raise FormNotReadyError(f"Booking form controls not actionable: {exc}") from exc


async def _element_visible(page: Page, selector: str) -> bool:
    """True when the first match of *selector* is visible."""
    try:
        el = page.locator(selector)
        if await el.count() == 0:
            return False
        return bool(await el.first.is_visible(timeout=1_000))
    except Exception:
        return False


async def _open_sports_facility_panel(page: Page, config: AppConfig, *, rush: bool) -> bool:
    """Click the Sports Facility toggle until the booking form is visible.

    A single click right after a page load can silently no-op while the
    page's JS is still initializing (2026-09-14: form rebuilds spent ~20s
    waiting here and then failed), so retry the click while the activity
    select stays hidden, with a JS-click fallback on any visible matching
    element.
    """
    activity_sel = config.selectors.activity
    sports_sel = config.selectors.sports_facility_button
    if await _element_visible(page, activity_sel):
        return True

    async def _click_once() -> None:
        try:
            btn = page.locator(sports_sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=4_000)
                return
        except Exception:
            pass
        try:
            await page.evaluate(
                """() => {
                    const els = Array.from(document.querySelectorAll('a, button'))
                        .filter(e => (e.textContent || '').trim().toLowerCase().includes('sports facility'));
                    const vis = els.find(e => (e.offsetParent || e.getClientRects().length));
                    if (vis) vis.click();
                }"""
            )
        except Exception:
            pass

    attempts = 5 if rush else 3
    for attempt in range(1, attempts + 1):
        await _click_once()
        try:
            await page.wait_for_selector(
                activity_sel, state="visible", timeout=3_000 if rush else 8_000,
            )
            if not rush:
                await human_delay(1.0, 2.0)
                await page.wait_for_load_state("networkidle")
            return True
        except Exception:
            if attempt < attempts:
                await asyncio.sleep(0.4)
    if not rush:
        try:
            await page.wait_for_load_state("networkidle")
        except Exception:
            pass
    return await _element_visible(page, activity_sel)


async def _build_center_order(
    page: Page,
    config: AppConfig,
    *,
    configured_only: bool = False,
) -> list[str]:
    """Build an ordered list of centers to try.

    Priority:
    1. Centers listed in config.preferences.centers (user-defined order)
    2. Any remaining centers from the dropdown that aren't in the list
    """
    configured = [c for c in config.preferences.centers if c.strip()]
    if not configured and config.preferences.center.strip():
        configured = [config.preferences.center]

    if configured_only:
        return configured

    dropdown_centers = await get_available_centers(page, config)
    dropdown_names = [c["text"] for c in dropdown_centers]
    logger.debug("Centers in dropdown: {}", dropdown_names)

    # Append any dropdown centers not already in the configured list
    seen_lower = {c.lower() for c in configured}
    for name in dropdown_names:
        if name.lower() not in seen_lower and name.strip():
            configured.append(name)
            seen_lower.add(name.lower())

    return configured


async def _async_wait_until(hour: int, minute: int, second: int = 0, *, pre_fire_ms: int = 0) -> None:
    """Sleep until HH:MM:SS (minus pre_fire_ms) with 5ms spin-wait precision."""
    await _async_wait_until_with_offset(
        hour,
        minute,
        second,
        pre_fire_ms=pre_fire_ms,
        server_delta_ms=0.0,
    )


async def _async_wait_until_with_offset(
    hour: int,
    minute: int,
    second: int = 0,
    *,
    pre_fire_ms: int = 0,
    offset_ms: int | None = None,
    server_delta_ms: float = 0.0,
) -> None:
    """Wait using server-compensated wall clock.

    Prefer signed ``offset_ms`` relative to open (negative = early).
    Legacy ``pre_fire_ms`` means milliseconds before open when positive.
    """
    def _server_now() -> datetime:
        return datetime.now() + timedelta(milliseconds=server_delta_ms)

    now = _server_now()
    target = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
    if offset_ms is not None:
        signed_offset = int(offset_ms)
    else:
        signed_offset = -int(pre_fire_ms) if int(pre_fire_ms) > 0 else 0
    target = target + timedelta(milliseconds=signed_offset)
    if now >= target:
        logger.debug(
            "Target time {:02d}:{:02d}:{:02d}{:+d}ms already passed, proceeding immediately",
            hour, minute, second, signed_offset,
        )
        return

    delta = (target - now).total_seconds()
    logger.info(
        "Preparation complete. Waiting {:.1f}s until {:02d}:{:02d}:{:02d}{:+d}ms "
        "(server_delta={:.1f}ms) …",
        delta, hour, minute, second, signed_offset, server_delta_ms,
    )

    if delta > 2:
        await asyncio.sleep(delta - 2)

    while _server_now() < target:
        await asyncio.sleep(0.005)

    logger.info(
        "Fire offset reached: local={} server_adjusted={} offset_ms={:+d}",
        datetime.now().strftime("%H:%M:%S.%f"),
        _server_now().strftime("%H:%M:%S.%f"),
        signed_offset,
    )


async def _estimate_server_time_delta_ms(
    tab: Page,
    config: AppConfig,
    *,
    sample_count: int,
    timeout_ms: int,
) -> float:
    """Estimate server-client wall clock delta using Date header midpoint RTT."""
    deltas: list[float] = []
    statuses: list[int] = []
    for _ in range(max(1, sample_count)):
        start_ms = time.time() * 1000.0
        try:
            response = await tab.context.request.fetch(
                BOOKING_URL,
                method="HEAD",
                timeout=timeout_ms,
            )
            end_ms = time.time() * 1000.0
            status = int(response.status)
            statuses.append(status)
            headers = response.headers
            date_header = headers.get("date") or headers.get("Date")
            if not date_header:
                continue
            server_dt = parsedate_to_datetime(date_header)
            if server_dt is None:
                continue
            server_ms = server_dt.timestamp() * 1000.0
            mid_ms = (start_ms + end_ms) / 2.0
            deltas.append(server_ms - mid_ms)
        except Exception:
            continue
        await asyncio.sleep(0.05)

    if not deltas:
        tracker.set_metric("server_time_sync_ok", False)
        tracker.set_metric("server_time_sync_samples", 0)
        return 0.0

    median_delta = float(statistics.median(deltas))
    tracker.set_metric("server_time_sync_ok", True)
    tracker.set_metric("server_time_sync_samples", len(deltas))
    tracker.set_metric("server_time_delta_ms", round(median_delta, 1))
    if statuses:
        tracker.set_metric("server_time_sync_statuses", statuses[-3:])
    logger.info(
        "Server time aligned: delta={:.1f}ms from {} sample(s)",
        median_delta,
        len(deltas),
    )
    return median_delta


async def _compute_server_time_delta_ms(tab: Page, config: AppConfig) -> float:
    if not config.settings.rush_time_sync_enabled:
        tracker.set_metric("server_time_sync_ok", False)
        tracker.set_metric("server_time_sync_disabled", True)
        return 0.0
    with tracker.step("server_time_sync"):
        return await _estimate_server_time_delta_ms(
            tab,
            config,
            sample_count=config.settings.rush_time_sync_samples,
            timeout_ms=config.settings.rush_time_sync_timeout_ms,
        )


async def _warm_connections(center_tabs: list[tuple[str, Page]], mode: str = "head") -> None:
    """Warm TCP/TLS via lightweight fetch() — no page navigation, forms stay filled.
    Unlike the old approach (clicking Search then refilling), this sends a HEAD
    request from each tab's JS context. The TCP + TLS handshake happens, but
    the page DOM is untouched, so we skip the expensive 2-3s refill step.
    """
    warm_js_head = """async () => {
        try {
            const r = await fetch(window.location.href, {
                method: 'HEAD', credentials: 'same-origin', cache: 'no-store',
            });
            return {ok: true, status: r.status};
        } catch(e) { return {ok: false, status: 0}; }
    }"""
    warm_js_mixed = """async () => {
        const result = {headStatus: 0, getStatus: 0, staticProbe: 0};
        try {
            const h = await fetch(window.location.href, {
                method: 'HEAD', credentials: 'same-origin', cache: 'no-store',
            });
            result.headStatus = h.status;
        } catch (e) {}
        try {
            const g = await fetch(window.location.href, {
                method: 'GET', credentials: 'same-origin', cache: 'no-store',
            });
            result.getStatus = g.status;
            await g.text();
        } catch (e) {}
        try {
            const link = document.querySelector('link[rel="stylesheet"]')?.href;
            if (link) {
                const s = await fetch(link, { method: 'GET', cache: 'no-store' });
                result.staticProbe = s.status;
            }
        } catch (e) {}
        return result;
    }"""

    ok_count = 0
    fail_count = 0

    async def _warm_one(center_name: str, tab: Page) -> None:
        nonlocal ok_count, fail_count
        try:
            if mode == "mixed":
                status = await tab.evaluate(warm_js_mixed)
                logger.debug("Connection warmed for {} (mixed={})", center_name, status)
                if isinstance(status, dict) and any(int(status.get(k, 0) or 0) > 0 for k in ("headStatus", "getStatus", "staticProbe")):
                    ok_count += 1
                else:
                    fail_count += 1
            else:
                status = await tab.evaluate(warm_js_head)
                logger.debug("Connection warmed for {} (HEAD={})", center_name, status)
                if isinstance(status, dict) and bool(status.get("ok")):
                    ok_count += 1
                else:
                    fail_count += 1
        except Exception as exc:
            logger.debug("Warm-up for {} failed (ok): {}", center_name, exc)
            fail_count += 1

    await asyncio.gather(
        *[_warm_one(cn, t) for cn, t in center_tabs],
        return_exceptions=True,
    )
    total = len(center_tabs)
    tracker.set_metric("warmup_total_tabs", total)
    tracker.set_metric("warmup_ok_tabs", ok_count)
    tracker.set_metric("warmup_failed_tabs", fail_count)
    tracker.set_metric("warmup_mode", mode)


def _slot_signature(slot: TimeSlot) -> tuple[str, str]:
    return (slot.start, slot.end)


async def _refire_search_or_rebuild(
    tab: Page,
    config: AppConfig,
    *,
    ref_date: date,
    center_name: str,
) -> str:
    """Fire Search on the current page; rebuild the booking form when stale.

    Returns ``"direct"`` when the existing page dispatched the search,
    ``"rebuilt"`` after a full form rebuild, or ``"failed"``.

    A failed booking submit leaves POSS on a result/error page where the
    Search button is gone or disabled; clicking it is a silent no-op and any
    rescan then fails to parse ("Could not parse timetable structure").
    Rebuilding the form from scratch is the only reliable way back to a
    usable timetable.
    """
    search_id = _selector_id(config.selectors.search_button)
    fired = False
    try:
        fired = bool(
            await tab.evaluate(
                """(searchId) => {
                    const btn = document.getElementById(searchId);
                    if (!btn || btn.disabled) return false;
                    btn.click();
                    return true;
                }""",
                search_id,
            )
        )
    except Exception:
        fired = False
    if not fired:
        try:
            await _rebuild_rush_form_tab(
                tab, config, ref_date=ref_date, center_name=center_name,
            )
            await tab.evaluate(f"document.getElementById('{search_id}')?.click()")
        except Exception as exc:
            logger.debug("Rebuild booking form failed for {}: {}", center_name, exc)
            return "failed"
    try:
        await tab.wait_for_selector(
            config.selectors.timetable, state="attached", timeout=3_000,
        )
    except Exception:
        pass
    return "direct" if fired else "rebuilt"


async def _retry_same_slot_lane(
    tab: Page,
    config: AppConfig,
    *,
    center_name: str,
    target: date,
    preferred_slots: List[TimeSlot],
    remaining: int,
    ref_date: date | None = None,
) -> List[TimeSlot]:
    """Fast conflict recovery loop: retry same evening slot(s) before moving on.

    When the page is left in a stale post-submit state (no usable timetable),
    rebuild the booking form once so the rescan reads real inventory instead
    of failing to parse a result page.
    """
    deadline = time.monotonic() + (config.settings.same_slot_retry_budget_ms / 1000.0)
    retry_limit = max(1, config.settings.same_slot_retry_limit)

    preferred_sig = {_slot_signature(s) for s in preferred_slots}
    rebuilt = False

    for _attempt in range(1, retry_limit + 1):
        if time.monotonic() >= deadline:
            break
        tracker.incr_metric("conflict_retries")

        try:
            scanned = await scan_available_slots_multi(
                tab, config, targets=[target], center_name=center_name,
            )
            slots = scanned.get(target, [])
            if not slots and ref_date is not None and not rebuilt:
                rebuilt = True
                status = await _refire_search_or_rebuild(
                    tab, config, ref_date=ref_date, center_name=center_name,
                )
                tracker.set_metric("conflict_retry_rebuild", status)
                if status != "failed":
                    scanned = await scan_available_slots_multi(
                        tab, config, targets=[target], center_name=center_name,
                    )
                    slots = scanned.get(target, [])
        except Exception as exc:
            logger.debug("Conflict retry scan failed for {}: {}", center_name, exc)
            continue
        if not slots:
            continue

        same_slots = [s for s in slots if _slot_signature(s) in preferred_sig]
        choice = same_slots if same_slots else find_best_booking(
            slots, remaining, config, relaxed=False, target=target,
        )
        if not choice:
            continue

        tracker.incr_metric("submit_attempt_count")
        success = await _book_slots_guarded(
            tab, choice, target, config, center_name=center_name,
        )
        if success:
            return choice

    return []


def _format_target_date_display(target: date) -> str:
    return target.strftime("%d %b %Y")


async def _extract_booking_form_state(page: Page, config: AppConfig) -> dict[str, str]:
    center_id = _selector_id(config.selectors.center)
    activity_id = _selector_id(config.selectors.activity)
    return await page.evaluate(
        """({ centerId, activityId }) => {
            const val = (sel) => document.querySelector(sel)?.value || "";
            const centerEl = document.getElementById(centerId);
            const actvEl = document.getElementById(activityId);
            const centerText = centerEl?.selectedOptions?.[0]?.text?.trim() || "";
            return {
                csrf_token: val('input[name="CSRFToken"], input[name="csrfToken"]'),
                fb_user_id: val('input[name="fbUserId"]'),
                data_set_id: val('input[name="dataSetId"]'),
                actv_id: actvEl?.value || val('input[name="actvId"]'),
                center_id: centerEl?.value || "",
                center_name: centerText,
                book_type: val('input[name="bookType"]') || "INDV",
            };
        }""",
        {"centerId": center_id, "activityId": activity_id},
    )


async def _extract_slot_protocol_meta(
    page: Page,
    slot: TimeSlot,
    target: date,
    config: AppConfig,
) -> dict[str, str]:
    target_day = target.strftime("%d %b")
    table_selector = config.selectors.timetable
    center_id = _selector_id(config.selectors.center)
    return await page.evaluate(
        """({ targetDay, startTime, endTime, tableSelector, centerId }) => {
            const out = {
                facility_id: "",
                facility_name: "",
                center_id: "",
                center_name: "",
            };
            const centerEl = document.getElementById(centerId);
            out.center_id = centerEl?.value || "";
            out.center_name = centerEl?.selectedOptions?.[0]?.text?.trim() || "";

            const tables = document.querySelectorAll(tableSelector);
            if (tables.length < 2) return out;

            let timeTable = null, dateTable = null;
            for (const t of tables) {
                const firstRowText = t.querySelector('tr')?.innerText?.trim() || '';
                if (/\\d{1,2}\\s+\\w{3}/.test(firstRowText)) dateTable = t;
                else timeTable = t;
            }
            if (!timeTable || !dateTable) return out;

            const headers = dateTable.querySelectorAll('tr')[0]?.querySelectorAll('td, th') || [];
            let colIdx = -1;
            for (let i = 0; i < headers.length; i++) {
                if (headers[i].innerText.trim().includes(targetDay)) { colIdx = i; break; }
            }
            if (colIdx < 0) return out;

            const timeRows = timeTable.querySelectorAll('tr');
            const timeRe = /(\\d{1,2}:\\d{2})\\s*[-–\\n]\\s*(\\d{1,2}:\\d{2})/;
            let rowIdx = -1;
            for (let r = 1; r < timeRows.length; r++) {
                const m = timeRows[r].innerText.trim().match(timeRe);
                if (m && m[1] === startTime && m[2] === endTime) { rowIdx = r; break; }
            }
            if (rowIdx < 0) return out;

            const dataRow = dateTable.querySelectorAll('tr')[rowIdx];
            const cell = dataRow?.querySelectorAll('td')?.[colIdx];
            if (!cell) return out;

            const onclickRaw = cell.getAttribute('onclick')
                || cell.querySelector('[onclick]')?.getAttribute('onclick')
                || "";
            const html = cell.innerHTML || "";
            const text = cell.textContent?.trim() || "";
            out.facility_name = text || cell.getAttribute('title') || "";

            const dataId = cell.getAttribute('data-facility-id')
                || cell.querySelector('[data-facility-id]')?.getAttribute('data-facility-id')
                || "";
            let facilityId = dataId;
            if (!facilityId) {
                let m = onclickRaw.match(/facilityId\\s*[=:]\\s*['"]?(\\d+)/i);
                if (!m) m = onclickRaw.match(/['",\\s](\\d{2,})['",\\s]/);
                if (!m) m = html.match(/facilityId\\s*[=:]\\s*['"]?(\\d+)/i);
                if (m) facilityId = m[1];
            }
            out.facility_id = facilityId || "";
            return out;
        }""",
        {
            "targetDay": target_day,
            "startTime": slot.start,
            "endTime": slot.end,
            "tableSelector": table_selector,
            "centerId": center_id,
        },
    )


def _build_search_payload_from_state(state: dict[str, str], target: date) -> dict[str, str]:
    return {
        "CSRFToken": state.get("csrf_token", ""),
        "fbUserId": state.get("fb_user_id", ""),
        "bookType": state.get("book_type", "INDV") or "INDV",
        "dataSetId": state.get("data_set_id", ""),
        "actvId": state.get("actv_id", ""),
        "searchDate": _format_target_date_display(target),
        "ctrId": state.get("center_id", ""),
        "facilityId": "",
        "showCourtAreaDetails": "true",
    }


def _build_prepare_payload(
    *,
    state: dict[str, str],
    slot_meta: dict[str, str],
    slot: TimeSlot,
    target: date,
) -> dict[str, str]:
    search_form = (
        f"fbUserId={state.get('fb_user_id', '')}"
        f"&bookType={state.get('book_type', 'INDV') or 'INDV'}"
        f"&dataSetId={state.get('data_set_id', '')}"
        f"&actvId={state.get('actv_id', '')}"
        f"&searchDate={_format_target_date_display(target)}"
        f"&ctrId={slot_meta.get('center_id') or state.get('center_id', '')}"
        f"&facilityId="
    )
    start_date_time = f"{_format_target_date_display(target)} {slot.start}"
    end_date_time = f"{_format_target_date_display(target)} {slot.end}"
    center_id = slot_meta.get("center_id") or state.get("center_id", "")
    facility_id = slot_meta.get("facility_id", "")
    return {
        "brcdNo": "",
        "phone": "",
        "extlPtyDclrId": "",
        "dataSetId": state.get("data_set_id", ""),
        "actvId": state.get("actv_id", ""),
        "onBehalfOfFbUserId": "",
        "byPassQuota": "false",
        "byPassChrgSchm": "false",
        "byPassBookingDaysLimit": "false",
        "repeatOccurrence": "false",
        "grpFacilityIds": "",
        "searchFormString": search_form,
        "boMakeBookFacilities[0].ctrId": center_id,
        "boMakeBookFacilities[0].facilityId": facility_id,
        "boMakeBookFacilities[0].startDateTime": start_date_time,
        "boMakeBookFacilities[0].endDateTime": end_date_time,
        "CSRFToken": state.get("csrf_token", ""),
    }


def _classify_api_submit_outcome(text: str, final_url: str = "") -> tuple[bool, str]:
    txt = text.lower()
    url = final_url.lower()

    if any(m in txt for m in BOOKING_CONFLICT_MARKERS):
        return False, "slot_conflict"
    if "csrf" in txt and any(k in txt for k in ("invalid", "expired", "mismatch", "token")):
        return False, "csrf_expired"
    if "declare" in txt and any(k in txt for k in ("required", "must", "agree", "accept")):
        return False, "declare_missing"
    if any(k in txt for k in ("quota", "limit reached", "booking days limit", "exceed")):
        return False, "quota_limit"
    if any(k in txt for k in ("under maintenance", "we'll be back soon", "temporarily unavailable")):
        return False, "maintenance"

    success_markers = (
        "booking confirmed",
        "booking successful",
        "booked successfully",
        "booking result",
        "booking no",
        "reservation no",
        "reference no",
    )
    if any(marker in txt for marker in success_markers):
        return True, "success_marker"
    if "make_book_result.do" in url and "error" not in txt and "failed" not in txt:
        return True, "result_page"
    return False, "submit_unknown"


def _classify_api_error(phase: str, result: ApiCallResult) -> str:
    haystack = f"{result.error} {result.text}".lower()
    if result.status_code in {401, 403}:
        return "auth_required"
    if result.status_code in {429}:
        return "rate_limited"
    if result.status_code >= 500:
        return "server_error"
    if result.status_code == 0:
        return "network_error"
    if "missing_facility_id" in haystack or "facility_id_not_found" in haystack:
        return "missing_facility_id"
    if "submit_form_fields_not_found" in haystack:
        return "submit_form_missing_fields"
    if "csrf" in haystack and any(k in haystack for k in ("invalid", "expired", "mismatch", "token")):
        return "csrf_expired"
    if "declare" in haystack and any(k in haystack for k in ("required", "must", "agree", "accept")):
        return "declare_missing"
    if any(m in haystack for m in BOOKING_CONFLICT_MARKERS):
        return "slot_conflict"
    if any(k in haystack for k in ("quota", "limit reached", "booking days limit", "exceed")):
        return "quota_limit"
    if any(k in haystack for k in ("timeout", "timed out")):
        return f"{phase}_timeout"
    return f"{phase}_failed"


def _record_api_failure(reason: str, *, center: str, target: date, detail: str = "") -> None:
    tracker.incr_metric(f"api_fail_reason|{reason}")
    tracker.add_feedback(
        "api_step_failed",
        reason_detail=reason,
        center=center,
        date=str(target),
        detail=detail[:300],
    )


async def _submit_booking_via_protocol(
    page: Page,
    client: BookingApiClient,
    *,
    state: dict[str, str],
    target: date,
    slot: TimeSlot,
    config: AppConfig,
    slot_meta: dict[str, str] | None = None,
) -> ApiCallResult:
    meta = slot_meta
    if meta is None:
        meta = await _extract_slot_protocol_meta(page, slot, target, config)
    if not meta.get("facility_id") and slot.facility_id:
        meta = {
            **(meta or {}),
            "facility_id": slot.facility_id,
            "facility_name": slot.court or (meta or {}).get("facility_name", ""),
            "center_id": (meta or {}).get("center_id") or state.get("center_id", ""),
            "center_name": (meta or {}).get("center_name") or slot.center,
        }
    if not meta.get("facility_id"):
        return ApiCallResult(ok=False, status_code=0, error="missing_facility_id")

    prepare_payload = _build_prepare_payload(
        state=state,
        slot_meta=meta,
        slot=slot,
        target=target,
    )
    prepare_res = await client.prepare_submit(prepare_payload)
    if not prepare_res.ok:
        return prepare_res

    submit_fields = extract_form_fields_from_html(prepare_res.text)
    if not submit_fields:
        return ApiCallResult(ok=False, status_code=0, error="submit_form_fields_not_found")
    if "declare" not in submit_fields:
        tracker.incr_metric("api_warning|declare_not_present_in_form")
    submit_fields["declare"] = "on"
    if state.get("csrf_token"):
        submit_fields["CSRFToken"] = state["csrf_token"]
    return await client.submit(submit_fields)


async def _run_booking_api_first(
    page: Page,
    config: AppConfig,
    *,
    dry_run: bool,
    rush_time: tuple[int, int, int] | None,
) -> bool:
    with tracker.step("ensure_booking_form_api"):
        await _ensure_booking_form(page, config, rush=bool(rush_time))

    center_order = await _build_center_order(page, config)
    target_dates = compute_target_dates(config)
    if not target_dates:
        tracker.add_feedback("api_no_preferred_days")
        return False

    bridge = await build_api_session_bridge(page, config)
    client = BookingApiClient(config, bridge)
    if not client.enabled:
        tracker.add_feedback("api_not_configured")
        logger.info("API mode configured but endpoints are missing, using fallback behavior")
        return False

    if rush_time is not None:
        server_delta_ms = await _compute_server_time_delta_ms(page, config)
        await _async_wait_until_with_offset(
            *rush_time,
            pre_fire_ms=config.settings.rush_pre_fire_ms,
            server_delta_ms=server_delta_ms,
        )

    for center_name in center_order:
        for target in target_dates:
            try:
                await select_booking_criteria(
                    page,
                    target,
                    config,
                    center_override=center_name,
                    auto_search=False,
                    rush=True,
                )
            except Exception as exc:
                _record_api_failure(
                    "select_criteria_failed",
                    center=center_name,
                    target=target,
                    detail=str(exc),
                )
                continue

            state = await _extract_booking_form_state(page, config)
            csrf_token = state.get("csrf_token", "")
            if not csrf_token:
                _record_api_failure(
                    "csrf_expired",
                    center=center_name,
                    target=target,
                    detail="csrf token missing on booking form",
                )
                continue

            search_payload = _build_search_payload_from_state(state, target)
            with tracker.step(f"api_search|{center_name}|{target}"):
                search_res = await client.search(csrf_token=csrf_token, payload=search_payload)
            tracker.incr_metric("api_search_attempt_count")
            if not search_res.ok:
                tracker.incr_metric("api_search_fail_count")
                reason = _classify_api_error("search", search_res)
                _record_api_failure(
                    reason,
                    center=center_name,
                    target=target,
                    detail=f"status={search_res.status_code} error={search_res.error}",
                )
                continue

            slots: List[TimeSlot] = []
            if search_res.payload is not None:
                parsed = parse_timetable_payload(
                    search_res.payload,
                    center_name=center_name,
                    target_dates=[target],
                )
                slots = parsed.get(target, [])
                tracker.set_metric("api_search_parsed_slots", len(slots))

            if not slots:
                await _click_search_raw(page, config)
                await _wait_for_timetable(page, config, timeout_ms=8_000, retries=0)
                slots = await scan_available_slots(
                    page,
                    config,
                    target=target,
                    center_name=center_name,
                    rush=True,
                )
            best = find_best_booking(
                slots,
                config.preferences.weekly_max_slots,
                config,
                rush=bool(rush_time),
                target=target,
            )
            if not best:
                continue

            if dry_run:
                tracker.add_feedback(
                    "api_dry_run_candidate",
                    center=center_name,
                    date=str(target),
                    slots=[f"{s.start}-{s.end}" for s in best],
                )
                return True

            slot = best[0]
            with tracker.step(f"api_submit|{center_name}|{target}"):
                submit_res = await _submit_booking_via_protocol(
                    page,
                    client,
                    state=state,
                    target=target,
                    slot=slot,
                    config=config,
                )
            tracker.incr_metric("api_submit_attempt_count")
            if not submit_res.ok:
                tracker.incr_metric("api_submit_fail_count")
                reason = _classify_api_error("submit", submit_res)
                _record_api_failure(
                    reason,
                    center=center_name,
                    target=target,
                    detail=f"status={submit_res.status_code} error={submit_res.error}",
                )
                continue

            success, outcome = _classify_api_submit_outcome(submit_res.text, submit_res.final_url)
            tracker.incr_metric(f"api_submit_outcome|{outcome}")
            if success:
                tracker.add_feedback(
                    "api_booked",
                    center=center_name,
                    date=str(target),
                    slots=[f"{slot.start}-{slot.end}"],
                    submit_outcome=outcome,
                )
                tracker.set_metric("api_path_success", True)
                return True
            tracker.incr_metric("api_submit_fail_count")
            _record_api_failure(
                outcome,
                center=center_name,
                target=target,
                detail=f"status={submit_res.status_code} final_url={submit_res.final_url}",
            )

    tracker.set_metric("api_path_success", False)
    return False


async def run_booking(
    page: Page, config: AppConfig, *,
    dry_run: bool = False,
    rush_time: tuple[int, int, int] | None = None,
) -> bool:
    """Full booking flow: quota check -> iterate centers -> scan all dates -> rank -> book.

    When *rush_time* is given, uses a parallel multi-tab fast path:
      - Opens one browser tab per center, pre-fills each form before rush_time
      - At rush_time, clicks Search in ALL tabs simultaneously via asyncio.gather
      - Races all tabs: whichever timetable loads first gets used for booking
      - Each tab scans ALL target dates from one search (timetable shows ~2 weeks)
      - Uses minimal delays throughout

    This turns serial center-by-center searches into parallel, reducing
    the critical path from ~50s to ~15s (limited only by server response time).
    """
    rush = rush_time is not None
    mode = (config.settings.booking_mode or "ui").strip().lower()
    tracker.set_metric("booking_mode", mode)

    # Rush hybrid: keep the UI race path, but enable in-rush API Search race when configured.
    if rush and mode == "hybrid":
        tracker.set_metric("api_skipped_in_rush", False)
        tracker.set_metric("api_rush_hybrid_ui_race", True)
        logger.info("Rush + hybrid: using UI rush race with optional API Search race")
        return await _run_booking_rush(page, config, dry_run=dry_run, rush_time=rush_time)

    if mode in {"api", "hybrid"}:
        logger.info("Booking mode={} (API-first path enabled)", mode)
        with tracker.step("api_first_path"):
            api_success = await _run_booking_api_first(
                page,
                config,
                dry_run=dry_run,
                rush_time=rush_time,
            )
        if api_success:
            return True
        if mode == "api":
            return False
        tracker.set_metric("api_fallback_to_ui", True)
        logger.info("Falling back to UI booking flow (hybrid mode)")

    if rush:
        return await _run_booking_rush(page, config, dry_run=dry_run, rush_time=rush_time)
    return await _run_booking_normal(page, config, dry_run=dry_run)


async def _run_booking_rush(
    page: Page, config: AppConfig, *,
    dry_run: bool,
    rush_time: tuple[int, int, int],
) -> bool:
    """Rush mode: parallel multi-tab race for first acceptable slot.

    Strategy:
      1. Open one browser tab per center, pre-fill each form (before rush_time)
      2. At rush_time, click Search in ALL tabs simultaneously
      3. Race: first tab with an acceptable slot claims the booking lock
      4. Remaining tabs are cancelled once booking is claimed
    """

    remaining = config.preferences.weekly_max_slots
    logger.info("Rush mode: skipping quota check, assuming {} slots available", remaining)
    tracker.set_metric("rush_prefer_consecutive", config.settings.rush_prefer_consecutive)
    tracker.set_metric("rush_selection_mode", config.settings.rush_selection_mode)
    tracker.set_metric("min_slot_start", config.preferences.min_slot_start)

    target_dates = compute_target_dates(config)
    if not target_dates:
        logger.warning("No preferred days found in the next {} days", config.preferences.book_days_ahead)
        tracker.add_feedback("no_preferred_days", book_days_ahead=config.preferences.book_days_ahead)
        return False

    logger.info(
        "Rush mode: {} target date(s): {} | quota: {} | first-acceptable >= {}",
        len(target_dates),
        [f"{d} ({d.strftime('%a')})" for d in target_dates],
        remaining,
        config.preferences.min_slot_start,
    )

    with tracker.step("ensure_booking_form"):
        await _ensure_booking_form(page, config, rush=True)
    tracker.mark_event("booking_form_ready")

    center_order = await _build_center_order(page, config, configured_only=True)
    logger.info("Center priority: {}", center_order)

    ref_date = target_dates[0]

    # ── Phase 1: Prepare tabs (first tab sync, extras in parallel) ──
    context = page.context
    center_tabs: list[tuple[str, Page]] = []
    skipped_centers_due_to_deadline = 0

    with tracker.step("rush_prepare_tabs"):
        async def _prep_extra_tab_once(cname: str) -> tuple[str, Page]:
            tab = await context.new_page()
            try:
                await tab.goto(BOOKING_URL, wait_until="domcontentloaded")
                await _ensure_booking_form(tab, config, rush=True)
                await select_booking_criteria(
                    tab, ref_date, config,
                    center_override=cname, auto_search=False, rush=True,
                )
            except Exception:
                try:
                    await tab.close()
                except Exception:
                    pass
                raise
            return cname, tab

        async def _prep_extra_tab(cname: str) -> tuple[str, Page]:
            attempts = 3

            def _note_retry(attempt: int, exc: Exception) -> None:
                tracker.incr_metric("prep_tab_retry_count")
                logger.warning(
                    "Retrying tab prep for {} (attempt {}/{}): {}",
                    cname, attempt + 1, attempts, exc,
                )

            # Transient prep failures used to cost a whole center for the rush
            # (2026-09-13 lost Sports Practice Hall this way); the pre-open
            # window is minutes long, so retry with a fresh tab.
            return await _retry_tab_prep(
                lambda: _prep_extra_tab_once(cname),
                attempts=attempts,
                delay_s=1.5,
                on_retry=_note_retry,
            )

        await select_booking_criteria(
            page, ref_date, config,
            center_override=center_order[0], auto_search=False, rush=True,
        )
        center_tabs.append((center_order[0], page))
        logger.info("Tab 1 ready: {} (pre-filled for {})", center_order[0], ref_date)

        if len(center_order) > 1:
            extra_centers = center_order[1:]
            deadline_dt = datetime.now().replace(
                hour=rush_time[0],
                minute=rush_time[1],
                second=rush_time[2],
                microsecond=0,
            ) - timedelta(seconds=max(0.0, config.settings.rush_extra_tab_deadline_s))
            prepare_budget_s = (deadline_dt - datetime.now()).total_seconds()

            if prepare_budget_s <= 0:
                skipped_centers_due_to_deadline = len(extra_centers)
                logger.warning(
                    "Skipping {} extra center tab(s): too close to rush time",
                    skipped_centers_due_to_deadline,
                )
            else:
                tasks = {
                    asyncio.create_task(_prep_extra_tab(cn)): cn
                    for cn in extra_centers
                }
                done, pending = await asyncio.wait(
                    tasks.keys(),
                    timeout=prepare_budget_s,
                )
                skipped_centers_due_to_deadline = len(pending)
                for task in pending:
                    task.cancel()
                    logger.warning(
                        "Skipping center {}: extra tab missed rush deadline",
                        tasks[task],
                    )
                for task in done:
                    try:
                        r = task.result()
                    except Exception as exc:
                        logger.warning("Failed to prepare tab for {}: {}", tasks[task], exc)
                        continue
                    center_tabs.append(r)
                    logger.info(
                        "Tab {} ready: {} (pre-filled for {})",
                        len(center_tabs),
                        r[0],
                        ref_date,
                    )

    tracker.set_metric("prepared_center_count", len(center_tabs))
    tracker.set_metric("skipped_centers_due_to_deadline", skipped_centers_due_to_deadline)
    tracker.mark_event("criteria_prefilled", centers=[c for c, _ in center_tabs])

    # Optional API Search race (hybrid/ui with api.enabled + rush_search_race).
    api_client: BookingApiClient | None = None
    center_states: dict[str, dict[str, str]] = {}
    tab_by_center = {name: tab for name, tab in center_tabs}
    api_race_enabled = bool(
        config.api.enabled
        and getattr(config.api, "rush_search_race", True)
        and config.api.search_endpoint
    )
    tracker.set_metric("api_rush_search_race", api_race_enabled)
    tracker.set_metric("api_submit_canary", bool(getattr(config.api, "submit_canary", False)))
    if api_race_enabled:
        try:
            bridge = await build_api_session_bridge(page, config)
            api_client = BookingApiClient(config, bridge)
            if not api_client.enabled:
                api_client = None
                api_race_enabled = False
                tracker.set_metric("api_rush_search_race", False)
            else:
                for cname, tab in center_tabs:
                    state = await _extract_booking_form_state(tab, config)
                    center_states[cname] = state
                tracker.mark_event("api_session_bridge_ready", centers=list(center_states.keys()))
        except Exception as exc:
            logger.warning("API rush race disabled: {}", exc)
            api_client = None
            api_race_enabled = False
            tracker.set_metric("api_rush_search_race", False)
            tracker.add_feedback("api_step_failed", reason_detail="bridge_failed", detail=str(exc)[:200])

    # ── Phase 2: Wait with lightweight warm-up (forms stay filled) ──
    now = datetime.now()
    target_dt = now.replace(hour=rush_time[0], minute=rush_time[1], second=rush_time[2], microsecond=0)
    pre_fire_ms = config.settings.rush_pre_fire_ms
    boundary_offsets = normalize_boundary_offsets(
        list(getattr(config.settings, "rush_boundary_offsets_ms", []) or []),
        fallback_pre_fire_ms=pre_fire_ms,
        enabled=bool(getattr(config.settings, "rush_boundary_enabled", True)),
        max_probes=int(getattr(config.settings, "rush_boundary_max_probes", 5) or 5),
    )
    first_offset_ms = boundary_offsets[0]
    server_delta_ms = await _compute_server_time_delta_ms(page, config)
    tracker.set_metric("boundary_offsets_ms", boundary_offsets)
    tracker.set_metric("rush_boundary_enabled", bool(getattr(config.settings, "rush_boundary_enabled", True)))

    # Recompute the remaining time AFTER the server-time sync: the sync can
    # take seconds and used to silently consume the warm-up budget (which was
    # measured before the sync), pushing the 2026-09-13 fire ~4.2s late.
    target_fire_srv = target_dt + timedelta(milliseconds=first_offset_ms)
    now_srv = datetime.now() + timedelta(milliseconds=server_delta_ms)
    remaining_pre_fire_s = (target_fire_srv - now_srv).total_seconds()

    # ── Phase 2a: refresh the form (fresh CSRFToken) shortly before the fire ──
    # The site freezes `CSRFToken: getCSRFToken()` into the Search click handler
    # at page load; a form prepped at 08:00 holds a token the server stops
    # accepting long before 08:30 (2026-09-14: every search 403'd for ~59s).
    refresh_plan = _refresh_plan_s(
        remaining_pre_fire_s,
        before_s=float(getattr(config.settings, "rush_form_refresh_before_s", 0.0) or 0.0),
    )
    if refresh_plan is not None:
        sleep_before_refresh, refresh_budget_s = refresh_plan
        if sleep_before_refresh > 0:
            logger.info("Sleeping {:.0f}s before form refresh …", sleep_before_refresh)
            await _sleep_with_keepalive(center_tabs, sleep_before_refresh)
        logger.info("Refreshing booking form tokens (budget {:.0f}s) …", refresh_budget_s)

        async def _refresh_one(cname: str, tab: Page) -> bool:
            per_try = max(5.0, refresh_budget_s / 2.0)
            for attempt in (1, 2):
                try:
                    await asyncio.wait_for(
                        _rebuild_rush_form_tab(
                            tab, config, ref_date=ref_date, center_name=cname,
                        ),
                        timeout=per_try,
                    )
                    tracker.incr_metric("form_refresh_ok_count")
                    return True
                except Exception as exc:
                    logger.warning(
                        "Form refresh attempt {} failed for {}: {}", attempt, cname, exc,
                    )
            tracker.incr_metric("form_refresh_fail_count")
            return False

        t_refresh = time.monotonic()
        with tracker.step("form_refresh"):
            refreshed = await asyncio.gather(
                *[_refresh_one(cn, t) for cn, t in center_tabs]
            )
        tracker.incr_metric("form_refresh_count")
        tracker.set_metric(
            "form_refresh_ms", round((time.monotonic() - t_refresh) * 1000.0, 1),
        )
        tracker.mark_event(
            "form_refresh_done", ok=sum(1 for r in refreshed if r), tabs=len(center_tabs),
        )
        for (cn, tab), ok in zip(center_tabs, refreshed):
            if not ok:
                continue
            try:
                center_states[cn] = await _extract_booking_form_state(tab, config)
            except Exception:
                pass
        now_srv = datetime.now() + timedelta(milliseconds=server_delta_ms)
        remaining_pre_fire_s = (target_fire_srv - now_srv).total_seconds()
        logger.info("Form refresh done ({:.1f}s left to fire)", remaining_pre_fire_s)

    if remaining_pre_fire_s > 3.0:
        sleep_before_warm, _ = _warmup_schedule_s(remaining_pre_fire_s)
        logger.info("Sleeping {:.0f}s before warm-up …", sleep_before_warm)
        await _sleep_with_keepalive(center_tabs, sleep_before_warm)

        now_srv = datetime.now() + timedelta(milliseconds=server_delta_ms)
        _, warmup_cap_s = _warmup_schedule_s((target_fire_srv - now_srv).total_seconds())
        with tracker.step("warm_connections"):
            try:
                await asyncio.wait_for(
                    _warm_connections(center_tabs, mode=config.settings.rush_warmup_mode),
                    timeout=warmup_cap_s,
                )
            except asyncio.TimeoutError:
                tracker.incr_metric("warmup_timeout_count")
                logger.warning(
                    "Warm-up exceeded its {:.1f}s budget - abandoning it to keep the fire on time",
                    warmup_cap_s,
                )
        tracker.mark_event("connection_warmup_completed")
        warm_done_srv = datetime.now() + timedelta(milliseconds=server_delta_ms)
        tracker.set_metric(
            "warmup_completed_offset_ms",
            round((warm_done_srv - target_dt).total_seconds() * 1000.0, 1),
        )

        await _async_wait_until_with_offset(
            *rush_time,
            offset_ms=first_offset_ms,
            server_delta_ms=server_delta_ms,
        )
    else:
        with tracker.step("rush_wait"):
            await _async_wait_until_with_offset(
                *rush_time,
                offset_ms=first_offset_ms,
                server_delta_ms=server_delta_ms,
            )

    # Align T0 to official open even when the first probe fires early/late.
    open_in_s = seconds_until_offset(
        rush_time,
        0,
        server_delta_ms=server_delta_ms,
    )
    tracker.mark_rush_start_aligned(open_in_seconds=open_in_s)
    rush_started_at = time.monotonic() + open_in_s
    adjusted_now = datetime.now() + timedelta(milliseconds=server_delta_ms)
    _set_metric_once(
        "actual_fire_delay_ms",
        round((adjusted_now - (adjusted_now.replace(
            hour=rush_time[0], minute=rush_time[1], second=rush_time[2], microsecond=0,
        ) + timedelta(milliseconds=first_offset_ms))).total_seconds() * 1000, 1),
    )
    tracker.set_metric("configured_fire_offset_ms", pre_fire_ms)
    _set_metric_once("primary_boundary_offset_ms", first_offset_ms)
    tracker.set_metric("estimated_server_delta_ms", server_delta_ms)
    first_candidate_seen_at: float | None = None
    first_submit_started_at: float | None = None
    slots_seen_total = 0
    tracker.set_metric("submit_attempt_count", 0)
    tracker.set_metric("conflict_retries", 0)
    tracker.set_metric("late_success_wave", -1)
    tracker.set_metric("reclick_count", 0)
    tracker.set_metric("early_scan_attempt_count", 0)
    tracker.set_metric("early_scan_hit_count", 0)
    tracker.set_metric("boundary_probe_count", 0)

    # Global race lock: first acceptable candidate owns booking.
    booking_lock = asyncio.Lock()
    booking_claimed = False
    inventory_seen = asyncio.Event()
    stop_boundary_probes = asyncio.Event()
    search_403_event = asyncio.Event()
    network_attempt = {"n": 0}
    boundary_hit_offset_ms: int | None = None

    def _attach_network_listener(tab: Page, center_name: str) -> None:
        """Capture Search/Submit RTT from Playwright response events."""

        async def _on_response(response) -> None:
            try:
                url = response.url or ""
                req_type = _network_event_kind(url)
                if req_type is None:
                    return
                tracker.mark_event(
                    f"{req_type}_response_seen",
                    center=center_name,
                    status=response.status,
                )
                if req_type == "search" and response.status == 403:
                    tracker.incr_metric("search_403_count")
                    tracker.mark_event("search_403_seen", center=center_name)
                    search_403_event.set()
                network_attempt["n"] += 1
                finished = tracker.ms_since_rush()
                started = finished
                headers_ms = None
                try:
                    timing = response.request.timing
                except Exception:
                    timing = None
                if isinstance(timing, dict):
                    # Playwright timing values are ms relative to navigation start.
                    resp_start = timing.get("responseStart")
                    resp_end = timing.get("responseEnd")
                    req_start = timing.get("requestStart")
                    if (
                        isinstance(finished, (int, float))
                        and isinstance(resp_end, (int, float))
                        and isinstance(req_start, (int, float))
                        and resp_end >= req_start
                    ):
                        rtt = float(resp_end) - float(req_start)
                        started = round(float(finished) - rtt, 1)
                        if isinstance(resp_start, (int, float)) and resp_start >= req_start:
                            headers_ms = round(
                                float(finished) - (float(resp_end) - float(resp_start)),
                                1,
                            )
                size = None
                try:
                    headers = response.headers
                    cl = headers.get("content-length")
                    if cl is not None:
                        size = int(cl)
                except Exception:
                    size = None
                tracker.record_network(
                    request_type=req_type,
                    status_code=response.status,
                    request_started_ms=started,
                    response_headers_ms=headers_ms,
                    response_finished_ms=finished,
                    response_size=size,
                    center=center_name,
                    attempt=network_attempt["n"],
                    url=url,
                )
            except Exception:
                return

        async def _on_request(request) -> None:
            try:
                req_type = _network_event_kind(request.url or "")
                if req_type not in ("prepare", "submit"):
                    return
                tracker.incr_metric(f"{req_type}_request_seen_count")
                tracker.mark_event(f"{req_type}_request_seen", center=center_name)
            except Exception:
                return

        tab.on("response", lambda resp: asyncio.create_task(_on_response(resp)))
        tab.on("request", lambda req: asyncio.create_task(_on_request(req)))

    for cn, tab in center_tabs:
        _attach_network_listener(tab, cn)

    # ── Phase 3+4: Fire search with staged probes + guarded re-clicks ──
    global_reclick_count = 0

    async def _click_search(tab: Page) -> bool:
        search_id = _selector_id(config.selectors.search_button)
        try:
            return bool(
                await tab.evaluate(
                    """(searchId) => {
                        const btn = document.getElementById(searchId);
                        if (!btn || btn.disabled) return false;
                        btn.click();
                        return true;
                    }""",
                    search_id,
                )
            )
        except Exception:
            return False

    async def _boundary_probe_scheduler() -> None:
        """Fire a small set of open-boundary Search probes; stop on inventory."""
        nonlocal boundary_hit_offset_ms
        for idx, offset_ms in enumerate(boundary_offsets):
            if idx == 0:
                # First offset already waited; fire happens inside _fire_and_scan.
                tracker.incr_metric("boundary_probe_count")
                tracker.mark_event("boundary_probe_fired", offset_ms=offset_ms, wave=idx)
                continue
            wait_s = seconds_until_offset(
                rush_time,
                offset_ms,
                server_delta_ms=server_delta_ms,
            )
            if wait_s > 0:
                try:
                    await asyncio.wait_for(
                        stop_boundary_probes.wait(),
                        timeout=wait_s,
                    )
                    return
                except asyncio.TimeoutError:
                    pass
            if stop_boundary_probes.is_set() or inventory_seen.is_set() or booking_claimed:
                return
            fired = 0
            for _cn, tab in center_tabs:
                if await _click_search(tab):
                    fired += 1
            blocked = len(center_tabs) - fired
            tracker.incr_metric("boundary_probe_count")
            if blocked:
                tracker.incr_metric("boundary_probe_blocked_count", blocked)
            tracker.mark_event(
                "boundary_probe_fired",
                offset_ms=offset_ms,
                wave=idx,
                tabs_fired=fired,
                tabs_blocked=blocked,
            )
            logger.info(
                "Boundary probe {:+d}ms fired on {}/{} tabs ({} blocked: button missing/disabled)",
                offset_ms,
                fired,
                len(center_tabs),
                blocked,
            )
            hook = api_wave_hook
            if hook is not None:
                asyncio.create_task(hook(offset_ms))

    probe_task = asyncio.create_task(_boundary_probe_scheduler())
    api_wave_hook = None  # set after claim helpers are ready

    async def _token_heal_task() -> None:
        """Rebuild + refire when a search 403s on a stale CSRFToken.

        The site freezes the CSRFToken into the Search click handler at page
        load, so a 403 means the page must be re-rendered before any further
        search can succeed.  Rebuild immediately instead of waiting ~60s for
        the retry waves to get around to it.
        """
        rounds = 0
        while not stop_boundary_probes.is_set():
            if rounds >= _RUSH_TOKEN_HEAL_MAX_ROUNDS:
                return
            try:
                await asyncio.wait_for(search_403_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return
            search_403_event.clear()
            if stop_boundary_probes.is_set() or booking_claimed:
                return
            rounds += 1
            tracker.incr_metric("search_403_heal_count")
            logger.warning(
                "Search 403 (stale CSRFToken) - rebuilding tabs to refresh (heal {}/{})",
                rounds, _RUSH_TOKEN_HEAL_MAX_ROUNDS,
            )
            ok, failed = await _token_heal_round(
                center_tabs, config, ref_date=ref_date,
                is_stopped=lambda: stop_boundary_probes.is_set() or booking_claimed,
            )
            if ok:
                tracker.incr_metric("search_403_heal_refire_count")
            if failed:
                tracker.incr_metric("search_403_heal_fail_count")
            await asyncio.sleep(_RUSH_TOKEN_HEAL_COOLDOWN_S)

    heal_task = asyncio.create_task(_token_heal_task())

    def _first_acceptable_from_scan(
        all_date_slots: dict[date, List[TimeSlot]],
    ) -> tuple[date, List[TimeSlot]] | None:
        for target in target_dates:
            slots = all_date_slots.get(target, [])
            best = find_best_booking(slots, remaining, config, rush=True, target=target)
            if best:
                return target, best
        return None

    def _note_inventory(offset_hint: int | None = None) -> None:
        nonlocal boundary_hit_offset_ms
        inventory_seen.set()
        stop_boundary_probes.set()
        if boundary_hit_offset_ms is not None:
            return
        if offset_hint is not None:
            # Candidate observed by the API wave that fired at this offset.
            boundary_hit_offset_ms = int(offset_hint)
        else:
            t_ms = tracker.ms_since_rush()
            if isinstance(t_ms, (int, float)):
                tracker.set_metric("inventory_first_seen_ms", round(float(t_ms), 1))
                nearest = min(
                    boundary_offsets,
                    key=lambda o: abs(float(t_ms) - float(o)),
                )
                if abs(float(t_ms) - float(nearest)) <= BOUNDARY_SNAP_TOLERANCE_MS:
                    boundary_hit_offset_ms = int(nearest)
                else:
                    # Inventory surfaced well outside the probe window;
                    # attributing it to the latest probe would corrupt
                    # timing-report statistics, so fall back to the primary.
                    tracker.set_metric("boundary_hit_out_of_window", True)
                    boundary_hit_offset_ms = first_offset_ms
            else:
                boundary_hit_offset_ms = first_offset_ms
        tracker.set_metric("boundary_hit_offset_ms", boundary_hit_offset_ms)

    async def _fire_and_scan(
        center_name: str, tab: Page,
    ) -> tuple[str, Page, dict[date, List[TimeSlot]], float, float]:
        nonlocal global_reclick_count
        t_search = time.monotonic()
        await _click_search(tab)
        tracker.mark_event(
            "search_request_fired",
            center=center_name,
            offset_ms=first_offset_ms,
        )
        logger.info("Search fired: {} (boundary {:+d}ms)", center_name, first_offset_ms)

        first_budget_ms, first_schedule = _derive_wait_budget_for_center(
            center_name,
            config,
            mode="first",
        )
        retry_budget_ms, retry_schedule = _derive_wait_budget_for_center(
            center_name,
            config,
            mode="retry",
        )
        center_key = _metric_center_key(center_name)
        tracker.set_metric(f"first_wait_budget_ms|{center_key}", first_budget_ms)
        tracker.set_metric(f"retry_wait_budget_ms|{center_key}", retry_budget_ms)

        found, first_probe = await _wait_for_rush_timetable_ready(
            tab,
            config,
            probe_schedule_ms=first_schedule,
            reclick_guard_ms=config.settings.rush_reclick_guard_ms,
            phase="first",
        )
        global_reclick_count += int(first_probe.get("reclick_count", 0) or 0)
        tracker.set_metric("reclick_count", global_reclick_count)

        first_table_ms = first_probe.get("search_to_first_table_ms")
        if isinstance(first_table_ms, (int, float)):
            tracker.set_metric(
                f"search_to_first_table_ms|{center_key}",
                first_table_ms,
            )
            tracker.mark_event(
                "first_timetable_dom_seen",
                center=center_name,
                search_to_first_table_ms=first_table_ms,
            )

        early_scan: dict[date, List[TimeSlot]] = {}
        early_hit = False
        early_choice: tuple[date, List[TimeSlot]] | None = None
        if isinstance(first_table_ms, (int, float)):
            tracker.incr_metric("early_scan_attempt_count")
            early_scan = await scan_available_slots_multi(
                tab, config, targets=target_dates, center_name=center_name,
            )
            early_hit = any(bool(v) for v in early_scan.values())
            if early_hit:
                tracker.incr_metric("early_scan_hit_count")
                early_choice = _first_acceptable_from_scan(early_scan)
                if early_choice is not None:
                    # First acceptable slot found — stop waiting for full timetable.
                    _note_inventory()
                    t_timetable_loaded = time.monotonic()
                    load_dur = t_timetable_loaded - t_search
                    tracker.record_step(f"timetable_load|{center_name}", load_dur)
                    tracker.record_step(f"scan_slots|{center_name}", 0.0)
                    tracker.mark_event(
                        "target_date_seen",
                        center=center_name,
                        date=str(early_choice[0]),
                        early=True,
                    )
                    return center_name, tab, early_scan, load_dur, 0.0

        if not found and not early_hit:
            found, retry_probe = await _wait_for_rush_timetable_ready(
                tab,
                config,
                probe_schedule_ms=retry_schedule,
                reclick_guard_ms=config.settings.rush_reclick_guard_ms,
                phase="retry",
            )
            global_reclick_count += int(retry_probe.get("reclick_count", 0) or 0)
            tracker.set_metric("reclick_count", global_reclick_count)
            timeout_path = retry_probe.get("timeout_path")
            if isinstance(timeout_path, str) and timeout_path != "none":
                tracker.set_metric(f"timeout_path|{center_key}", timeout_path)

            two_ms = retry_probe.get("search_to_two_tables_ms")
            if isinstance(two_ms, (int, float)):
                tracker.set_metric(
                    f"search_to_two_tables_ms|{center_key}",
                    two_ms,
                )
        else:
            timeout_path = first_probe.get("timeout_path")
            if isinstance(timeout_path, str) and timeout_path != "none":
                tracker.set_metric(f"timeout_path|{center_key}", timeout_path)

            two_ms = first_probe.get("search_to_two_tables_ms")
            if isinstance(two_ms, (int, float)):
                tracker.set_metric(
                    f"search_to_two_tables_ms|{center_key}",
                    two_ms,
                )

        t_timetable_loaded = time.monotonic()
        load_dur = t_timetable_loaded - t_search

        if not found and not early_hit:
            logger.warning(
                "{}: timetable never reached ready state after {:.1f}s "
                "(first_budget={}ms, retry_budget={}ms)",
                center_name,
                load_dur,
                first_budget_ms,
                retry_budget_ms,
            )

        t_scan_start = time.monotonic()
        result = await scan_available_slots_multi(
            tab, config, targets=target_dates, center_name=center_name,
        )
        if early_scan:
            for d, slots in early_scan.items():
                if slots and not result.get(d):
                    result[d] = slots
        scan_dur = time.monotonic() - t_scan_start

        tracker.record_step(f"timetable_load|{center_name}", load_dur)
        tracker.record_step(f"scan_slots|{center_name}", scan_dur)
        if any(result.values()):
            tracker.mark_event("target_date_seen", center=center_name)
            if _first_acceptable_from_scan(result) is not None:
                _note_inventory()

        return center_name, tab, result, load_dur, scan_dur

    tasks = [
        asyncio.create_task(_fire_and_scan(cn, t))
        for cn, t in center_tabs
    ]

    any_booked = False

    async def _try_claim_booking() -> bool:
        nonlocal booking_claimed
        async with booking_lock:
            if booking_claimed:
                return False
            booking_claimed = True
            stop_boundary_probes.set()
            inventory_seen.set()
            return True

    async def _api_search_wave(offset_ms: int) -> None:
        """Race API Search across centers; book first acceptable candidate."""
        nonlocal any_booked, remaining, first_candidate_seen_at, first_submit_started_at, slots_seen_total
        nonlocal booking_claimed
        if not api_race_enabled or api_client is None:
            return
        if booking_claimed or stop_boundary_probes.is_set():
            return

        async def _search_one(center_name: str) -> tuple[str, dict[date, List[TimeSlot]], float] | None:
            state = center_states.get(center_name) or {}
            tab = tab_by_center.get(center_name)
            # Read the token fresh from the (pre-fire refreshed) form: the
            # prep-time token used to be frozen into state and would 403 once
            # it aged out (2026-09-14: all 8 API searches died that way).
            csrf = await _read_tab_csrf_token(tab) if tab is not None else ""
            if not csrf:
                csrf = state.get("csrf_token", "")
            if not csrf:
                return None
            # Search the earliest target date first (same as UI prep date).
            target = target_dates[0]
            payload = _build_search_payload_from_state(state, target)
            t0 = time.monotonic()
            started_ms = tracker.ms_since_rush()
            tracker.incr_metric("api_search_attempt_count")
            result = await _api_search_with_fresh_token(
                api_client, tab, csrf_token=csrf, payload=payload,
            )
            rtt_ms = round((time.monotonic() - t0) * 1000.0, 1)
            finished_ms = tracker.ms_since_rush()
            tracker.record_network(
                request_type="search",
                status_code=result.status_code,
                request_started_ms=started_ms,
                response_finished_ms=finished_ms,
                response_size=len(result.text or ""),
                center=center_name,
                attempt=int(tracker._metrics.get("api_search_attempt_count", 0) or 0),
                url=config.api.search_endpoint,
            )
            if not result.ok or result.payload is None:
                tracker.incr_metric("api_search_fail_count")
                logger.warning(
                    "API search failed for {} (status={}, rtt={}ms): {}",
                    center_name,
                    result.status_code,
                    rtt_ms,
                    (result.error or "no payload")[:160],
                )
                return None
            tracker.incr_metric("api_search_ok_count")
            parsed = parse_timetable_payload(
                result.payload,
                center_name=center_name,
                target_dates=target_dates,
            )
            # If dates missing in JSON, map under the prepared target.
            if not parsed and target_dates:
                parsed = parse_timetable_payload(
                    result.payload,
                    center_name=center_name,
                    target_dates=[target_dates[0]],
                )
            return center_name, parsed, rtt_ms

        tracker.mark_event("api_search_wave_started", offset_ms=offset_ms)
        gathered = await asyncio.gather(
            *[_search_one(cn) for cn, _ in center_tabs],
            return_exceptions=True,
        )
        for item in gathered:
            if booking_claimed or any_booked or remaining <= 0:
                return
            if item is None or isinstance(item, Exception):
                continue
            center_name, by_date, rtt_ms = item
            tracker.set_metric(f"api_search_rtt_ms|{_metric_center_key(center_name)}", rtt_ms)
            choice = _first_acceptable_from_scan(by_date)
            if choice is None:
                continue
            target, best = choice
            slots_seen_total_local = sum(len(v) for v in by_date.values())
            if slots_seen_total_local:
                slots_seen_total += slots_seen_total_local
                tracker.set_metric("slots_seen_total", slots_seen_total)

            _note_inventory(offset_ms)
            if first_candidate_seen_at is None:
                first_candidate_seen_at = time.monotonic()
                _set_metric_once(
                    "refresh_to_first_candidate_ms",
                    round((first_candidate_seen_at - rush_started_at) * 1000, 1),
                )
                _set_metric_once("first_candidate_source", "api_search")

            if not await _try_claim_booking():
                return

            for t in tasks:
                if not t.done():
                    t.cancel()

            candidate = tracker.start_candidate(
                center=center_name,
                date=str(target),
                start=best[0].start,
                end=best[-1].end,
                court=best[0].court,
            )
            logger.info(
                "API Search race claimed {} @ {} ({}) rtt={}ms",
                best[0].start,
                center_name,
                target,
                rtt_ms,
            )

            if dry_run:
                tracker.finish_candidate(candidate, "dry_run")
                remaining -= len(best)
                any_booked = True
                return

            tab = tab_by_center.get(center_name, page)
            state = center_states.get(center_name, {})
            submit_canary = bool(getattr(config.api, "submit_canary", False))
            booked_ok = False

            if submit_canary and best[0].facility_id:
                tracker.incr_metric("submit_attempt_count")
                tracker.set_metric("submit_path", "api_canary")
                if first_submit_started_at is None:
                    first_submit_started_at = time.monotonic()
                    if first_candidate_seen_at is not None:
                        tracker.set_metric(
                            "first_candidate_to_submit_ms",
                            round((first_submit_started_at - first_candidate_seen_at) * 1000, 1),
                        )
                with tracker.step(f"api_submit|{center_name}|{target}"):
                    submit_res = await _submit_booking_via_protocol(
                        tab,
                        api_client,
                        state=state,
                        target=target,
                        slot=best[0],
                        config=config,
                        slot_meta={
                            "facility_id": best[0].facility_id,
                            "facility_name": best[0].court,
                            "center_id": state.get("center_id", ""),
                            "center_name": center_name,
                        },
                    )
                ok, reason = _classify_api_submit_outcome(submit_res.text, submit_res.final_url)
                if submit_res.ok and ok:
                    booked_ok = True
                    tracker.set_metric("api_submit_outcome", reason)
                    tracker.finish_candidate(candidate, "booked")
                else:
                    tracker.incr_metric("api_submit_canary_fail_count")
                    tracker.add_feedback(
                        "api_step_failed",
                        reason_detail=_classify_api_error("submit", submit_res) if not submit_res.ok else reason,
                        center=center_name,
                        date=str(target),
                    )
                    tracker.finish_candidate(candidate, "api_submit_failed")
                    # Release claim so UI path / retries can continue.
                    async with booking_lock:
                        booking_claimed = False

            if not booked_ok:
                # Safe default: UI confirm path on the winning center tab.
                tracker.set_metric("submit_path", "ui_after_api_search")
                await _click_search(tab)
                try:
                    await tab.wait_for_selector(
                        config.selectors.timetable,
                        state="attached",
                        timeout=max(200, int(config.settings.rush_confirm_page_timeout_ms)),
                    )
                except Exception:
                    pass
                if first_submit_started_at is None:
                    first_submit_started_at = time.monotonic()
                    if first_candidate_seen_at is not None:
                        tracker.set_metric(
                            "first_candidate_to_submit_ms",
                            round((first_submit_started_at - first_candidate_seen_at) * 1000, 1),
                        )
                tracker.incr_metric("submit_attempt_count")
                # Re-claim if canary released it.
                if not booking_claimed:
                    if not await _try_claim_booking():
                        return
                with tracker.step(f"book_slots_api_hit|{center_name}|{target}"):
                    booked_ok = await _book_slots_guarded(
                        tab, best, target, config,
                        candidate=candidate, center_name=center_name,
                    )
                if not booked_ok:
                    tracker.add_feedback(
                        "booking_conflict",
                        center=center_name,
                        date=str(target),
                        slots=[f"{s.start}-{s.end}" for s in best],
                        source="api_search",
                    )
                    async with booking_lock:
                        booking_claimed = False
                    return

            remaining -= len(best)
            any_booked = True
            tracker.add_feedback(
                "booked",
                center=center_name,
                date=str(target),
                slots=[f"{s.start}-{s.end}" for s in best],
                source="api_search",
                submit_path=tracker._metrics.get("submit_path"),
            )
            tracker.set_metric("late_success_wave", 0)
            logger.success(
                "Booked via API Search race: {} slot(s) on {} @ {}",
                len(best),
                target,
                center_name,
            )
            return

    if api_race_enabled:
        async def _set_api_wave_hook(offset_ms: int) -> None:
            await _api_search_wave(offset_ms)

        api_wave_hook = _set_api_wave_hook
        asyncio.create_task(_api_search_wave(first_offset_ms))

    for completed in asyncio.as_completed(tasks):
        if remaining <= 0 or booking_claimed and any_booked:
            break

        try:
            center_name, tab, all_date_slots, _ld, _sd = await completed
        except asyncio.CancelledError:
            # Expected when a sibling scan was cancelled after another center
            # claimed the booking. Keep the flow alive so cleanup and the
            # retry waves still run; only re-raise if this task itself is
            # being cancelled.
            current = asyncio.current_task()
            cancelling = getattr(current, "cancelling", None)
            if cancelling is not None and cancelling() > 0:
                raise
            tracker.incr_metric("sibling_scan_cancelled_count")
            continue
        except Exception as exc:
            logger.debug("Tab scan failed: {}", exc)
            tracker.add_feedback("tab_scan_failed", error=str(exc))
            continue

        if booking_claimed and any_booked:
            continue

        logger.info("Results ready for {} ({})",
                     center_name,
                     {str(d): len(s) for d, s in all_date_slots.items() if s})

        for target in target_dates:
            if remaining <= 0:
                break

            slots = all_date_slots.get(target, [])
            slots_seen_total += len(slots)
            tracker.set_metric("slots_seen_total", slots_seen_total)
            best = find_best_booking(slots, remaining, config, rush=True, target=target)
            if not best:
                tracker.add_feedback(
                    "no_slots",
                    center=center_name,
                    date=str(target),
                    total_slots=len(slots),
                    after_min_start=config.preferences.min_slot_start,
                )
                continue

            if first_candidate_seen_at is None:
                first_candidate_seen_at = time.monotonic()
                _set_metric_once(
                    "refresh_to_first_candidate_ms",
                    round((first_candidate_seen_at - rush_started_at) * 1000, 1),
                )

            if not await _try_claim_booking():
                logger.info("Booking already claimed by another center — skipping {}", center_name)
                break

            # Cancel competitor scans as soon as we claim.
            for t in tasks:
                if not t.done():
                    t.cancel()

            logger.info("=== Claimed first-acceptable on {} @ {} ===", target, center_name)

            candidate = tracker.start_candidate(
                center=center_name,
                date=str(target),
                start=best[0].start,
                end=best[-1].end,
                court=best[0].court,
            )

            if dry_run:
                logger.info("[DRY RUN] Would book on {} @ {}:", target, center_name)
                for s in best:
                    logger.info("  {} – {} (court: {})", s.start, s.end, s.court)
                remaining -= len(best)
                any_booked = True
                tracker.finish_candidate(candidate, "dry_run")
                continue

            if first_submit_started_at is None:
                first_submit_started_at = time.monotonic()
                if first_candidate_seen_at is not None:
                    tracker.set_metric(
                        "first_candidate_to_submit_ms",
                        round((first_submit_started_at - first_candidate_seen_at) * 1000, 1),
                    )

            tracker.incr_metric("submit_attempt_count")
            with tracker.step(f"book_slots|{center_name}|{target}"):
                success = await _book_slots_guarded(
                    tab, best, target, config,
                    candidate=candidate, center_name=center_name,
                )
            if success:
                remaining -= len(best)
                any_booked = True
                tracker.add_feedback("booked", center=center_name, date=str(target),
                                     slots=[f"{s.start}-{s.end}" for s in best])
                tracker.set_metric("late_success_wave", 0)
                logger.success("Booked {} slot(s) on {} @ {}", len(best), target, center_name)
                break
            else:
                tracker.add_feedback("booking_conflict", center=center_name, date=str(target),
                                     slots=[f"{s.start}-{s.end}" for s in best])
                logger.warning("Booking failed for {} on {}. Retrying same slot lane …", center_name, target)
                for s in best:
                    if s in slots:
                        slots.remove(s)
                all_date_slots[target] = slots

                # Allow another claim attempt after conflict.
                async with booking_lock:
                    booking_claimed = False

                with tracker.step(f"conflict_retry|{center_name}|{target}"):
                    retry_booked = await _retry_same_slot_lane(
                        tab,
                        config,
                        center_name=center_name,
                        target=target,
                        preferred_slots=best,
                        remaining=remaining,
                        ref_date=ref_date,
                    )
                if retry_booked:
                    remaining -= len(retry_booked)
                    any_booked = True
                    tracker.set_metric("late_success_wave", 0)
                    tracker.add_feedback(
                        "booked",
                        center=center_name,
                        date=str(target),
                        slots=[f"{s.start}-{s.end}" for s in retry_booked],
                        retried=True,
                    )
                    logger.success("Conflict retry succeeded for {} on {}", center_name, target)
                    async with booking_lock:
                        booking_claimed = True
                    break

        if any_booked:
            break

    # Cancel still-running tasks
    stop_boundary_probes.set()
    if not probe_task.done():
        probe_task.cancel()
        try:
            await probe_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    if not heal_task.done():
        heal_task.cancel()
        try:
            await heal_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    for t in tasks:
        t.cancel()

    # ── Phase 5: Retry waves if no booking made (same evening strategy only) ──
    retry_offsets = list(config.settings.rush_retry_offsets_s)
    if not any_booked and remaining > 0 and retry_offsets:
        logger.info("No bookings in initial scan. Starting retry offsets {}s …", retry_offsets)

        for wave, target_offset in enumerate(retry_offsets, start=1):
            if any_booked or remaining <= 0:
                break
            elapsed = time.monotonic() - rush_started_at
            wait_s = max(0.0, float(target_offset) - elapsed)
            if wait_s > 0:
                logger.info("── Retry wave {} at T+{}s (waiting {:.2f}s) ──", wave, target_offset, wait_s)
                await asyncio.sleep(wait_s)
            else:
                logger.info("── Retry wave {} late trigger (target T+{}s, now T+{:.2f}s) ──",
                            wave, target_offset, elapsed)

            async def _retry_one(
                _cn: str, _tab: Page,
            ) -> tuple[str, Page, dict[date, List[TimeSlot]]]:
                status = await _refire_search_or_rebuild(
                    _tab, config, ref_date=ref_date, center_name=_cn,
                )
                if status == "direct":
                    tracker.incr_metric("retry_direct_search_count")
                else:
                    tracker.incr_metric("retry_fallback_reload_count")
                if status == "failed":
                    return _cn, _tab, {}
                res = await scan_available_slots_multi(
                    _tab, config, targets=target_dates, center_name=_cn,
                )
                return _cn, _tab, res

            retry_results = await asyncio.gather(
                *[_retry_one(cn, t) for cn, t in center_tabs],
                return_exceptions=True,
            )

            for rr in retry_results:
                if any_booked or remaining <= 0:
                    break
                if isinstance(rr, Exception):
                    logger.debug("Retry wave {} failed: {}", wave, rr)
                    continue

                cn, tab, all_date_slots = rr
                logger.info("Retry wave {} results for {}: {}",
                            wave, cn,
                            {str(d): len(s) for d, s in all_date_slots.items() if s})

                for target in target_dates:
                    if remaining <= 0:
                        break
                    slots = all_date_slots.get(target, [])
                    best = find_best_booking(slots, remaining, config, rush=True, target=target)
                    if not best:
                        continue

                    if not await _try_claim_booking():
                        break

                    logger.info("Wave {}: found slots on {} @ {}", wave, cn, target)
                    candidate = tracker.start_candidate(
                        center=cn,
                        date=str(target),
                        start=best[0].start,
                        end=best[-1].end,
                        court=best[0].court,
                    )

                    if dry_run:
                        for s in best:
                            logger.info("  [DRY RUN] {} – {}", s.start, s.end)
                        remaining -= len(best)
                        any_booked = True
                        tracker.finish_candidate(candidate, "dry_run")
                        continue

                    with tracker.step(f"wave{wave}_book|{cn}|{target}"):
                        tracker.incr_metric("submit_attempt_count")
                        success = await _book_slots_guarded(
                            tab, best, target, config,
                            candidate=candidate, center_name=cn,
                        )
                    if success:
                        remaining -= len(best)
                        any_booked = True
                        tracker.set_metric("late_success_wave", wave)
                        tracker.add_feedback(
                            "booked", center=cn, date=str(target),
                            slots=[f"{s.start}-{s.end}" for s in best],
                            wave=wave, relaxed=False,
                        )
                        logger.success("Wave {}: booked {} slot(s) on {} @ {}",
                                       wave, len(best), target, cn)
                        break
                    async with booking_lock:
                        booking_claimed = False

                if any_booked:
                    break

    # Cleanup extra tabs
    for _cn, tab in center_tabs:
        if tab != page:
            try:
                await tab.close()
            except Exception:
                pass

    if slots_seen_total > 0 and not any_booked:
        tracker.set_metric("visible_slots_unbooked", True)
    elif slots_seen_total > 0:
        tracker.set_metric("visible_slots_unbooked", False)

    if not any_booked:
        logger.warning("No bookings made across all preferred dates and centers")
        tracker.add_feedback("no_bookings_made",
                             dates=[str(d) for d in target_dates],
                             centers=center_order)
    return any_booked


async def _run_booking_normal(
    page: Page, config: AppConfig, *,
    dry_run: bool,
) -> bool:
    """Normal (non-rush) booking flow with human-like delays."""
    with tracker.step("check_weekly_quota"):
        booked = await check_weekly_quota(page, config)
    remaining = config.preferences.weekly_max_slots - booked
    if remaining <= 0:
        logger.info("Weekly quota reached ({}/{}). Skipping.", booked, config.preferences.weekly_max_slots)
        tracker.add_feedback("quota_full", booked=booked, max=config.preferences.weekly_max_slots)
        return False

    target_dates = compute_target_dates(config)
    if not target_dates:
        logger.warning("No preferred days found in the next {} days", config.preferences.book_days_ahead)
        tracker.add_feedback("no_preferred_days", book_days_ahead=config.preferences.book_days_ahead)
        return False

    logger.info(
        "Will try {} preferred date(s): {} | remaining quota: {}",
        len(target_dates),
        [f"{d} ({d.strftime('%a')})" for d in target_dates],
        remaining,
    )

    with tracker.step("ensure_booking_form"):
        await _ensure_booking_form(page, config)

    center_order = await _build_center_order(page, config)
    logger.info("Center priority: {}", center_order)

    any_booked = False
    for target in target_dates:
        if remaining <= 0:
            logger.info("Quota exhausted after booking. Stopping.")
            break

        logger.info("=== Trying {} ({}) ===", target, target.strftime("%A"))

        date_booked = False
        for center_name in center_order:
            if remaining <= 0:
                break

            logger.info("--- Center: {} ---", center_name)
            with tracker.step(f"select_criteria|{target}|{center_name}"):
                await select_booking_criteria(page, target, config, center_override=center_name)

            with tracker.step(f"scan_slots|{target}|{center_name}"):
                slots = await scan_available_slots(page, config, target=target, center_name=center_name)

            best = find_best_booking(slots, remaining, config, target=target)
            if not best:
                logger.info("No afternoon slots at {} on {}. Trying next center …", center_name, target)
                tracker.add_feedback("no_slots", center=center_name, date=str(target),
                                     total_slots=len(slots))
                continue

            if dry_run:
                logger.info("[DRY RUN] Would book on {} @ {}:", target, center_name)
                for s in best:
                    logger.info("  {} – {} (court: {})", s.start, s.end, s.court)
                remaining -= len(best)
                any_booked = True
                date_booked = True
                break

            with tracker.step(f"book_slots|{target}|{center_name}"):
                success = await book_slots(page, best, target, config)
            if success:
                remaining -= len(best)
                any_booked = True
                date_booked = True
                tracker.add_feedback("booked", center=center_name, date=str(target),
                                     slots=[f"{s.start}-{s.end}" for s in best])
                logger.success("Booked {} slot(s) on {} @ {}", len(best), target, center_name)
                break
            else:
                tracker.add_feedback("booking_failed", center=center_name, date=str(target),
                                     detail="book_slots returned False (conflict or cell not found)")

        if date_booked and remaining > 0 and target != target_dates[-1]:
            logger.info("Reloading booking form for next date …")
            await page.goto(BOOKING_URL, wait_until="domcontentloaded")
            await human_delay(1.5, 3.0)
            await _ensure_booking_form(page, config)

    if not any_booked:
        logger.warning("No bookings made across all preferred dates and centers")
        tracker.add_feedback("no_bookings_made", dates=[str(d) for d in target_dates],
                             centers=center_order)
    return any_booked
