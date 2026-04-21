from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, List, Tuple

from loguru import logger

from bookbot.stealth import human_click, human_delay, save_debug_snapshot
from bookbot.tracker import tracker

if TYPE_CHECKING:
    from playwright.async_api import Page

    from bookbot.config import AppConfig


BOOKING_URL = (
    "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_make_book/make_book.do"
)
RUNTIME_LOG_PATH = Path("logs/runtime.jsonl")


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


async def get_available_centers(page: Page) -> list[dict]:
    """Read the center dropdown options currently available."""
    try:
        opts = await page.evaluate(
            "[...document.getElementById('ctrId')?.options || []]"
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
    logger.info("Selecting booking criteria (date={}, activity={}, center={}) …",
                target, config.preferences.activity, center_name)

    if rush:
        await page.wait_for_load_state("domcontentloaded")
    else:
        await page.wait_for_load_state("networkidle")
        await _dump_form_structure(page)

    date_str = target.strftime("%d/%m/%Y")

    # --- Date ---
    date_input = page.locator("#searchDate")
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
                const el = document.getElementById('searchDate');
                if (el) {{
                    el.value = '{date_str}';
                    el.dispatchEvent(new Event('change', {{bubbles: true}}));
                    el.dispatchEvent(new Event('input', {{bubbles: true}}));
                }}
                if (typeof jQuery !== 'undefined' && jQuery.datepicker) {{
                    jQuery('#searchDate').datepicker('hide');
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
        logger.warning("Date input #searchDate not found")
        raise FormNotReadyError("Date input #searchDate not found — form may not have loaded")

    # --- Activity ---
    logger.debug("Waiting for activity options to load …")
    actv_opts = await _wait_for_select_options(page, "actvId", timeout=5_000 if rush else 10_000)
    logger.debug("Activity options: {}", actv_opts)

    if len(actv_opts) <= 1:
        actv_el = page.locator("#actvId")
        if await actv_el.count() > 0:
            try:
                await actv_el.click(timeout=3_000)
            except Exception:
                logger.debug("Activity dropdown not clickable, triggering via JS")
                await page.evaluate("document.getElementById('actvId')?.click()")
            if not rush:
                await human_delay(0.5, 1.0)
            actv_opts = await _wait_for_select_options(page, "actvId", timeout=5_000)
            logger.debug("Activity options after click: {}", actv_opts)

    activity_done = False
    for opt in actv_opts:
        if config.preferences.activity.lower() in opt.get("text", "").lower():
            await page.select_option("#actvId", value=opt["value"])
            activity_done = True
            logger.info("Activity selected: {} (value={})", opt["text"], opt["value"])
            if rush:
                try:
                    await page.wait_for_function(
                        "() => document.getElementById('ctrId')?.options.length > 1",
                        timeout=5_000,
                    )
                except Exception:
                    pass
            else:
                await human_delay(1.0, 2.0)
                await page.wait_for_load_state("networkidle")
            break

    if not activity_done and len(actv_opts) > 1:
        await page.select_option("#actvId", value=actv_opts[1]["value"])
        logger.warning("Target activity not found, selected: {}", actv_opts[1]["text"])
        if not rush:
            await human_delay(1.0, 2.0)
            await page.wait_for_load_state("networkidle")
    elif not activity_done:
        raise FormNotReadyError("No activity options loaded — form may not be ready")

    # --- Center ---
    logger.debug("Waiting for center options to load …")
    ctr_opts = await _wait_for_select_options(page, "ctrId", timeout=5_000 if rush else 10_000)
    logger.debug("Center options: {}", ctr_opts)

    if len(ctr_opts) <= 1:
        ctr_el = page.locator("#ctrId")
        if await ctr_el.count() > 0:
            try:
                await ctr_el.click(timeout=3_000)
            except Exception:
                logger.debug("Center dropdown not clickable, triggering via JS")
                await page.evaluate("document.getElementById('ctrId')?.click()")
            if not rush:
                await human_delay(0.5, 1.0)
            ctr_opts = await _wait_for_select_options(page, "ctrId", timeout=5_000)
            logger.debug("Center options after click: {}", ctr_opts)

    center_done = False
    for opt in ctr_opts:
        if center_name.lower() in opt.get("text", "").lower():
            await page.select_option("#ctrId", value=opt["value"])
            center_done = True
            logger.info("Center selected: {} (value={})", opt["text"], opt["value"])
            if not rush:
                await human_delay(1.0, 2.0)
                await page.wait_for_load_state("networkidle")
            break

    if not center_done and len(ctr_opts) > 1:
        await page.select_option("#ctrId", value=ctr_opts[1]["value"])
        logger.warning("Target center '{}' not found, selected: {}", center_name, ctr_opts[1]["text"])
        if not rush:
            await human_delay(1.0, 2.0)
            await page.wait_for_load_state("networkidle")
    elif not center_done:
        logger.warning("No center options available")

    if not rush:
        await save_debug_snapshot(page, "06_criteria_selected")

    if auto_search:
        await click_search_button(page, rush=rush)


async def _click_search_raw(page: Page) -> None:
    """Click the Search button without waiting for results."""
    search_btn = page.locator("#searchButton")
    if await search_btn.count() > 0:
        try:
            await search_btn.click(timeout=2_000)
        except Exception:
            await page.evaluate("document.getElementById('searchButton')?.click()")
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


async def _probe_timetable_state(tab: Page) -> dict:
    try:
        return await tab.evaluate(
            """() => {
                const tables = document.querySelectorAll('table.tt-timetable');
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
            }"""
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
        state = await _probe_timetable_state(tab)
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
                    await tab.evaluate("document.getElementById('searchButton')?.click()")
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


async def _wait_for_timetable(page: Page, *, timeout_ms: int = 15_000, retries: int = 1) -> bool:
    """Wait for the timetable to render. Re-clicks Search on timeout. Returns True if found."""
    for attempt in range(1 + retries):
        try:
            await page.wait_for_selector(
                "table.tt-timetable", state="attached", timeout=timeout_ms,
            )
            return True
        except Exception:
            if attempt < retries:
                logger.debug("Timetable not found after {}ms, retrying search …", timeout_ms)
                try:
                    await page.evaluate("document.getElementById('searchButton')?.click()")
                except Exception:
                    pass
            else:
                logger.debug("Timetable selector not found, falling back to networkidle")
                try:
                    await page.wait_for_load_state("networkidle", timeout=5_000)
                except Exception:
                    pass
    return False


async def click_search_button(page: Page, *, rush: bool = False) -> None:
    """Click the Search button on the booking form and wait for timetable results."""
    logger.info("Clicking Search …")
    await _click_search_raw(page)
    await _wait_for_timetable(page, timeout_ms=15_000, retries=1)

    if not rush:
        await human_delay(2.0, 4.0)
        await save_debug_snapshot(page, "07_search_results")


def _extract_timetable_data_js() -> str:
    """Return the JS code that extracts the full timetable grid.

    Factored out so it can be reused by both single-date and multi-date scans.
    """
    return """() => {
        const tables = document.querySelectorAll('table.tt-timetable');
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
                isGray = isGray || allClasses.includes('not-avail') ||
                         allClasses.includes('unavail') || allClasses.includes('closed');

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
    }"""


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

    timetable_data = await page.evaluate(_extract_timetable_data_js())

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

    timetable_data = await page.evaluate(_extract_timetable_data_js())

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

def in_time_range(slot: TimeSlot, config: AppConfig) -> bool:
    tr = config.preferences.time_range
    return slot.start_hour >= tr.start_hour and slot.end_hour <= tr.end_hour


def rank_slot(slot: TimeSlot, config: AppConfig) -> float:
    score: float = 0
    if slot.center.lower() == config.preferences.center.lower():
        score += 100
    # Prefer mid-afternoon: peak at 15:30
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
) -> List[TimeSlot]:
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
        candidates = [s for s in slots if s.available and in_time_range(s, config)]

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


# ---------------------------------------------------------------------------
# Book execution
# ---------------------------------------------------------------------------

async def _find_slot_cell(page: Page, slot: TimeSlot, target: date, config: AppConfig):
    """Locate the timetable cell element for a given time slot on the target date."""
    target_day = target.strftime("%d %b")

    cell = await page.evaluate_handle(
        """({ targetDay, startTime, endTime }) => {
            const tables = document.querySelectorAll('table.tt-timetable');
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
        {"targetDay": target_day, "startTime": slot.start, "endTime": slot.end},
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


async def _click_slots_js(page: Page, slots: List[TimeSlot], target: date) -> int:
    """Click multiple timetable cells in a single JS evaluation.

    The POSS timetable binds click handlers on child elements inside <td> cells
    (e.g. <a>, <div>, <span> with onclick).  We must click the innermost
    interactive element, not the bare <td>, otherwise the selection doesn't
    register and the Next button stays disabled.
    """
    target_day = target.strftime("%d %b")
    slot_data = [{"start": s.start, "end": s.end} for s in slots]
    return await page.evaluate(
        """({ targetDay, slots }) => {
            const tables = document.querySelectorAll('table.tt-timetable');
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
        {"targetDay": target_day, "slots": slot_data},
    )


async def _click_next_fast(page: Page, backoff_ms: list[int]) -> bool:
    """Click Next with short retries to avoid a single long blocking wait."""
    for wait_ms in backoff_ms:
        try:
            await page.wait_for_selector(
                "#nextButton:not([disabled]), button:has-text('Next')",
                timeout=max(100, wait_ms),
            )
            next_btn = page.locator("#nextButton:not([disabled]), button:has-text('Next')").first
            await next_btn.click(timeout=max(100, wait_ms))
            return True
        except Exception:
            logger.debug("Next not ready after {}ms, retrying …", wait_ms)

    # JS fallback + keyboard submit for edge cases where click target is unstable.
    try:
        clicked = await page.evaluate(
            """() => {
                const btn = document.querySelector('#nextButton:not([disabled])')
                    || [...document.querySelectorAll('button')]
                        .find(b => (b.textContent || '').trim().toLowerCase() === 'next');
                if (!btn) return false;
                btn.click();
                return true;
            }"""
        )
        if clicked:
            return True
    except Exception:
        pass

    try:
        await page.keyboard.press("Enter")
        return True
    except Exception:
        return False


async def book_slots(page: Page, slots_to_book: List[TimeSlot], target: date, config: AppConfig,
                     *, rush: bool = False) -> bool:
    """Click on the chosen slot(s) in the timetable grid and confirm the booking.

    In rush mode, every millisecond counts:
      - Batch-click all slots via a single JS evaluation (no Playwright round-trips)
      - Skip all screenshots
      - Wait for specific elements instead of generic networkidle
      - Tick checkboxes via JS
    """
    if not slots_to_book:
        return False

    # ── Step 1: Click slot cells ──
    if rush:
        booked_count = await _click_slots_js(page, slots_to_book, target)
        for s in slots_to_book[:booked_count]:
            logger.info("Clicked slot {} – {} (via JS)", s.start, s.end)

        # Verify selection registered: Next button should become enabled.
        # If not, JS click didn't work — fall back to Playwright native clicks.
        if booked_count > 0:
            try:
                await page.wait_for_selector(
                    '#nextButton:not([disabled])', timeout=3_000,
                )
            except Exception:
                logger.warning("Next button still disabled after JS click — falling back to native clicks")
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
                            '#nextButton:not([disabled])', timeout=3_000,
                        )
                    except Exception:
                        logger.warning("Next button STILL disabled after native clicks")
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
    next_btn = page.locator('#nextButton:not([disabled])')
    if await next_btn.count() == 0:
        next_btn = page.locator('button:has-text("Next")')
    if await next_btn.count() > 0:
        logger.info("Clicking Next …")
        if rush:
            clicked = await _click_next_fast(page, config.settings.next_click_backoff_ms)
            if not clicked:
                logger.warning("Fast Next click failed")
                return False
        else:
            await next_btn.first.click()
        if rush:
            try:
                await page.wait_for_selector(
                    'input[type="checkbox"], button:has-text("Confirm"), '
                    'input[value="Confirm"], button:has-text("Submit")',
                    timeout=10_000,
                )
            except Exception:
                await page.wait_for_load_state("domcontentloaded", timeout=5_000)
        else:
            await page.wait_for_load_state("networkidle")
            await human_delay(2.0, 4.0)
            await save_debug_snapshot(page, "10_next_page")

    # ── Step 3: Tick checkboxes ──
    if rush:
        ticked = await page.evaluate(
            """() => {
                const cbs = document.querySelectorAll('input[type="checkbox"]:not(:checked)');
                cbs.forEach(cb => cb.click());
                return cbs.length;
            }"""
        )
        if ticked:
            logger.info("Ticked {} checkbox(es) via JS", ticked)
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
        await confirm_btn.click()

        if rush:
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=10_000)
            except Exception:
                pass
        else:
            await page.wait_for_load_state("networkidle")
            await human_delay(1.0, 2.0)

        if not rush:
            await save_debug_snapshot(page, "11_booking_confirmed")

        if await _is_booking_conflict(page):
            logger.warning("Slot was already taken (occupied)! Will try another slot.")
            back_btn = page.locator('a:has-text("Back"), button:has-text("Back")')
            if await back_btn.count() > 0:
                await back_btn.first.click()
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=5_000)
                except Exception:
                    pass
            return False

        logger.success("Booking confirmed ({} slot(s))", booked_count)

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
    else:
        logger.warning("No confirm button found – booking may require manual confirmation")
        if not rush:
            await save_debug_snapshot(page, "11_no_confirm_button")

    return True


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
    actv = page.locator("#actvId")
    if await actv.count() > 0:
        try:
            if await actv.first.is_visible(timeout=2_000):
                return
        except Exception:
            pass
        logger.debug("Booking form exists in DOM but is NOT visible — need to click Sports Facility")

    sports_btn = page.locator(
        'a:has-text("Sports Facility"), button:has-text("Sports Facility")'
    )
    if await sports_btn.count() > 0:
        logger.debug("Clicking Sports Facility button …")
        await sports_btn.first.click()
        if rush:
            try:
                await page.wait_for_selector("#actvId", state="visible", timeout=10_000)
            except Exception:
                await page.wait_for_load_state("domcontentloaded", timeout=5_000)
        else:
            await page.wait_for_load_state("networkidle")
            await human_delay(1.5, 2.5)
    else:
        logger.debug("No Sports Facility button found, re-navigating …")
        await page.goto(BOOKING_URL, wait_until="domcontentloaded")
        if not rush:
            await human_delay(1.0, 2.0)
        sports_btn = page.locator(
            'a:has-text("Sports Facility"), button:has-text("Sports Facility")'
        )
        if await sports_btn.count() > 0:
            await sports_btn.first.click()
            if rush:
                try:
                    await page.wait_for_selector("#actvId", state="visible", timeout=10_000)
                except Exception:
                    await page.wait_for_load_state("domcontentloaded", timeout=5_000)
            else:
                await page.wait_for_load_state("networkidle")
                await human_delay(1.5, 2.5)

    if await page.locator("#actvId").count() == 0:
        from bookbot.auth import is_maintenance_page, MaintenanceError
        if await is_maintenance_page(page):
            raise MaintenanceError("Booking form page is under maintenance")
        raise FormNotReadyError("Booking form (#actvId) not found after navigation")

    # Readiness hardening: verify key controls are visible and enabled before rush flow continues.
    try:
        await page.wait_for_function(
            """() => {
                const dateInput = document.getElementById('searchDate');
                const actv = document.getElementById('actvId');
                const ctr = document.getElementById('ctrId');
                const search = document.getElementById('searchButton');
                if (!dateInput || !actv || !ctr || !search) return false;
                const visible = (el) => !!(el.offsetParent || el.getClientRects().length);
                return visible(dateInput) && visible(actv) && visible(ctr) && !search.disabled;
            }""",
            timeout=5_000 if rush else 10_000,
        )
    except Exception as exc:
        raise FormNotReadyError(f"Booking form controls not actionable: {exc}") from exc


async def _build_center_order(page: Page, config: AppConfig) -> list[str]:
    """Build an ordered list of centers to try.

    Priority:
    1. Centers listed in config.preferences.centers (user-defined order)
    2. Any remaining centers from the dropdown that aren't in the list
    """
    configured = list(config.preferences.centers)

    dropdown_centers = await get_available_centers(page)
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
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
    if pre_fire_ms > 0:
        target = target - timedelta(milliseconds=pre_fire_ms)
    if now >= target:
        logger.debug("Target time {:02d}:{:02d}:{:02d} already passed, proceeding immediately", hour, minute, second)
        return

    delta = (target - now).total_seconds()
    if pre_fire_ms > 0:
        logger.info(
            "Preparation complete. Waiting {:.1f}s until {:02d}:{:02d}:{:02d} (pre-fire {}ms) …",
            delta, hour, minute, second, pre_fire_ms,
        )
    else:
        logger.info(
            "Preparation complete. Waiting {:.1f}s until {:02d}:{:02d}:{:02d} to start booking …",
            delta, hour, minute, second,
        )

    if delta > 2:
        await asyncio.sleep(delta - 2)

    while datetime.now() < target:
        await asyncio.sleep(0.005)

    logger.info("Rush time reached: {}", datetime.now().strftime("%H:%M:%S.%f"))


async def _warm_connections(center_tabs: list[tuple[str, Page]], mode: str = "head") -> None:
    """Warm TCP/TLS via lightweight fetch() — no page navigation, forms stay filled.

    Unlike the old approach (clicking Search then refilling), this sends a HEAD
    request from each tab's JS context.  The TCP + TLS handshake happens, but
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

    async def _warm_one(center_name: str, tab: Page) -> None:
        try:
            if mode == "mixed":
                status = await tab.evaluate(warm_js_mixed)
                logger.debug("Connection warmed for {} (mixed={})", center_name, status)
            else:
                status = await tab.evaluate(warm_js_head)
                logger.debug("Connection warmed for {} (HEAD={})", center_name, status)
        except Exception as exc:
            logger.debug("Warm-up for {} failed (ok): {}", center_name, exc)

    await asyncio.gather(
        *[_warm_one(cn, t) for cn, t in center_tabs],
        return_exceptions=True,
    )


def _slot_signature(slot: TimeSlot) -> tuple[str, str]:
    return (slot.start, slot.end)


async def _retry_same_slot_lane(
    tab: Page,
    config: AppConfig,
    *,
    center_name: str,
    target: date,
    preferred_slots: List[TimeSlot],
    remaining: int,
) -> List[TimeSlot]:
    """Fast conflict recovery loop: retry same evening slot(s) before moving on."""
    deadline = time.monotonic() + (config.settings.same_slot_retry_budget_ms / 1000.0)
    retry_limit = max(1, config.settings.same_slot_retry_limit)

    preferred_sig = {_slot_signature(s) for s in preferred_slots}

    for _attempt in range(1, retry_limit + 1):
        if time.monotonic() >= deadline:
            break
        tracker.incr_metric("conflict_retries")

        try:
            await tab.evaluate("document.getElementById('searchButton')?.click()")
            await _wait_for_rush_timetable_ready(
                tab,
                probe_schedule_ms=[500, 1500],
                reclick_guard_ms=max(300, config.settings.rush_reclick_guard_ms // 2),
                phase="conflict_retry",
            )
        except Exception:
            logger.debug("Conflict retry: search re-fire failed for {}", center_name)

        scanned = await scan_available_slots_multi(
            tab, config, targets=[target], center_name=center_name,
        )
        slots = scanned.get(target, [])
        if not slots:
            continue

        same_slots = [s for s in slots if _slot_signature(s) in preferred_sig]
        choice = same_slots if same_slots else find_best_booking(slots, remaining, config, relaxed=False)
        if not choice:
            continue

        tracker.incr_metric("submit_attempt_count")
        success = await book_slots(tab, choice, target, config, rush=True)
        if success:
            return choice

    return []


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

    if rush:
        return await _run_booking_rush(page, config, dry_run=dry_run, rush_time=rush_time)
    return await _run_booking_normal(page, config, dry_run=dry_run)


async def _run_booking_rush(
    page: Page, config: AppConfig, *,
    dry_run: bool,
    rush_time: tuple[int, int, int],
) -> bool:
    """Rush mode: parallel multi-tab search for maximum speed at 08:30.

    Strategy:
      1. Open one browser tab per center, pre-fill each form (before rush_time)
      2. At rush_time, click Search in ALL tabs simultaneously
      3. Race: whichever tab loads the timetable first gets used for booking
      4. This turns serial 30s+15s = 45s into parallel max(30s,15s) ≈ 15s
    """

    remaining = config.preferences.weekly_max_slots
    logger.info("Rush mode: skipping quota check, assuming {} slots available", remaining)

    target_dates = compute_target_dates(config)
    if not target_dates:
        logger.warning("No preferred days found in the next {} days", config.preferences.book_days_ahead)
        tracker.add_feedback("no_preferred_days", book_days_ahead=config.preferences.book_days_ahead)
        return False

    logger.info(
        "Rush mode: {} target date(s): {} | quota: {}",
        len(target_dates),
        [f"{d} ({d.strftime('%a')})" for d in target_dates],
        remaining,
    )

    with tracker.step("ensure_booking_form"):
        await _ensure_booking_form(page, config, rush=True)

    center_order = await _build_center_order(page, config)
    logger.info("Center priority: {}", center_order)

    ref_date = target_dates[0]

    # ── Phase 1: Prepare tabs (first tab sync, extras in parallel) ──
    context = page.context
    center_tabs: list[tuple[str, Page]] = []

    with tracker.step("rush_prepare_tabs"):
        async def _prep_extra_tab(cname: str) -> tuple[str, Page]:
            tab = await context.new_page()
            await tab.goto(BOOKING_URL, wait_until="domcontentloaded")
            await _ensure_booking_form(tab, config, rush=True)
            await select_booking_criteria(
                tab, ref_date, config,
                center_override=cname, auto_search=False, rush=True,
            )
            return cname, tab

        await select_booking_criteria(
            page, ref_date, config,
            center_override=center_order[0], auto_search=False, rush=True,
        )
        center_tabs.append((center_order[0], page))
        logger.info("Tab 1 ready: {} (pre-filled for {})", center_order[0], ref_date)

        if len(center_order) > 1:
            extra = await asyncio.gather(
                *[_prep_extra_tab(cn) for cn in center_order[1:]],
                return_exceptions=True,
            )
            for idx, r in enumerate(extra):
                if isinstance(r, tuple):
                    center_tabs.append(r)
                    logger.info("Tab {} ready: {} (pre-filled for {})",
                                len(center_tabs), r[0], ref_date)
                else:
                    logger.warning("Failed to prepare tab for {}: {}",
                                   center_order[idx + 1], r)

    # ── Phase 2: Wait with lightweight warm-up (forms stay filled) ──
    now = datetime.now()
    target_dt = now.replace(hour=rush_time[0], minute=rush_time[1], second=rush_time[2], microsecond=0)
    secs_to_rush = (target_dt - now).total_seconds()
    pre_fire_ms = config.settings.rush_pre_fire_ms

    if secs_to_rush > 7:
        sleep_before_warm = secs_to_rush - 5
        logger.info("Sleeping {:.0f}s before warm-up …", sleep_before_warm)
        await asyncio.sleep(sleep_before_warm)

        with tracker.step("warm_connections"):
            await _warm_connections(center_tabs, mode=config.settings.rush_warmup_mode)

        await _async_wait_until(*rush_time, pre_fire_ms=pre_fire_ms)
    else:
        with tracker.step("rush_wait"):
            await _async_wait_until(*rush_time, pre_fire_ms=pre_fire_ms)

    # Mark the critical phase — all steps from now go into rush_steps
    tracker.mark_rush_start()
    rush_started_at = time.monotonic()
    first_candidate_seen_at: float | None = None
    first_submit_started_at: float | None = None
    slots_seen_total = 0
    tracker.set_metric("submit_attempt_count", 0)
    tracker.set_metric("conflict_retries", 0)
    tracker.set_metric("late_success_wave", -1)
    tracker.set_metric("reclick_count", 0)
    tracker.set_metric("early_scan_attempt_count", 0)
    tracker.set_metric("early_scan_hit_count", 0)

    # ── Phase 3+4: Fire search with staged probes + guarded re-clicks ──
    global_reclick_count = 0

    async def _fire_and_scan(
        center_name: str, tab: Page,
    ) -> tuple[str, Page, dict[date, List[TimeSlot]], float, float]:
        nonlocal global_reclick_count
        t_search = time.monotonic()
        await tab.evaluate("document.getElementById('searchButton')?.click()")
        logger.info("Search fired: {}", center_name)

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

        early_scan: dict[date, List[TimeSlot]] = {}
        early_hit = False
        if isinstance(first_table_ms, (int, float)):
            tracker.incr_metric("early_scan_attempt_count")
            early_scan = await scan_available_slots_multi(
                tab, config, targets=target_dates, center_name=center_name,
            )
            early_hit = any(bool(v) for v in early_scan.values())
            if early_hit:
                tracker.incr_metric("early_scan_hit_count")

        if not found and not early_hit:
            found, retry_probe = await _wait_for_rush_timetable_ready(
                tab,
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

        return center_name, tab, result, load_dur, scan_dur

    tasks = [
        asyncio.create_task(_fire_and_scan(cn, t))
        for cn, t in center_tabs
    ]

    any_booked = False

    for completed in asyncio.as_completed(tasks):
        if remaining <= 0:
            break

        try:
            center_name, tab, all_date_slots, _ld, _sd = await completed
        except Exception as exc:
            logger.debug("Tab scan failed: {}", exc)
            tracker.add_feedback("tab_scan_failed", error=str(exc))
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
            best = find_best_booking(slots, remaining, config)
            if not best:
                tracker.add_feedback("no_slots", center=center_name, date=str(target),
                                     total_slots=len(slots))
                continue

            if first_candidate_seen_at is None:
                first_candidate_seen_at = time.monotonic()
                tracker.set_metric(
                    "refresh_to_first_candidate_ms",
                    round((first_candidate_seen_at - rush_started_at) * 1000, 1),
                )

            logger.info("=== Found slots on {} @ {} ===", target, center_name)

            if dry_run:
                logger.info("[DRY RUN] Would book on {} @ {}:", target, center_name)
                for s in best:
                    logger.info("  {} – {} (court: {})", s.start, s.end, s.court)
                remaining -= len(best)
                any_booked = True
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
                success = await book_slots(tab, best, target, config, rush=True)
            if success:
                remaining -= len(best)
                any_booked = True
                tracker.add_feedback("booked", center=center_name, date=str(target),
                                     slots=[f"{s.start}-{s.end}" for s in best])
                tracker.set_metric("late_success_wave", 0)
                logger.success("Booked {} slot(s) on {} @ {}", len(best), target, center_name)

                if remaining > 0:
                    logger.info("Reloading booking form for more bookings …")
                    try:
                        await tab.goto(BOOKING_URL, wait_until="domcontentloaded")
                        await _ensure_booking_form(tab, config, rush=True)
                        remaining_dates = [d for d in target_dates if d > target]
                        if remaining_dates:
                            await select_booking_criteria(
                                tab, remaining_dates[0], config,
                                center_override=center_name, rush=True,
                            )
                            updated = await scan_available_slots_multi(
                                tab, config, targets=remaining_dates, center_name=center_name,
                            )
                            all_date_slots.update(updated)
                    except Exception as exc:
                        logger.debug("Failed to reload for more bookings: {}", exc)
                break
            else:
                tracker.add_feedback("booking_conflict", center=center_name, date=str(target),
                                     slots=[f"{s.start}-{s.end}" for s in best])
                logger.warning("Booking failed for {} on {}. Retrying same slot lane …", center_name, target)
                for s in best:
                    if s in slots:
                        slots.remove(s)
                all_date_slots[target] = slots

                with tracker.step(f"conflict_retry|{center_name}|{target}"):
                    retry_booked = await _retry_same_slot_lane(
                        tab,
                        config,
                        center_name=center_name,
                        target=target,
                        preferred_slots=best,
                        remaining=remaining,
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
                    break

    # Cancel still-running tasks
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
                try:
                    await _tab.reload(wait_until="domcontentloaded", timeout=8_000)
                    await _tab.wait_for_selector(
                        "table.tt-timetable", state="attached", timeout=5_000,
                    )
                except Exception:
                    try:
                        await _tab.goto(BOOKING_URL, wait_until="domcontentloaded", timeout=8_000)
                        await _ensure_booking_form(_tab, config, rush=True)
                        await select_booking_criteria(
                            _tab, ref_date, config,
                            center_override=_cn, auto_search=False, rush=True,
                        )
                        await _tab.evaluate("document.getElementById('searchButton')?.click()")
                        for _ in range(5):
                            try:
                                await _tab.wait_for_selector(
                                    "table.tt-timetable", state="attached", timeout=3_000,
                                )
                                break
                            except Exception:
                                try:
                                    await _tab.evaluate(
                                        "document.getElementById('searchButton')?.click()"
                                    )
                                except Exception:
                                    pass
                    except Exception as exc:
                        logger.debug("Retry re-navigate failed for {}: {}", _cn, exc)
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
                    best = find_best_booking(slots, remaining, config, relaxed=False)
                    if not best:
                        continue

                    logger.info("Wave {}: found slots on {} @ {}", wave, target, cn)

                    if dry_run:
                        for s in best:
                            logger.info("  [DRY RUN] {} – {}", s.start, s.end)
                        remaining -= len(best)
                        any_booked = True
                        continue

                    with tracker.step(f"wave{wave}_book|{cn}|{target}"):
                        tracker.incr_metric("submit_attempt_count")
                        success = await book_slots(tab, best, target, config, rush=True)
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

            best = find_best_booking(slots, remaining, config)
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
