#!/usr/bin/env python3
"""Live probe: failed-slot memory steering + failure-evidence capture (2026-09-17).

Read-only against POSS (searches only, never books):

  A) scan the real timetable for the configured target dates (both centers)
  B) _choose_rush_slots: seed the memory with the first choice and verify the
     next choice steers to another acceptable cell (or falls back honestly)
  C) _capture_failure_evidence on a live page  -> file with real url+text
  D) _capture_failure_evidence on a CLOSED page -> must not raise, still writes

    cd "<proj>" && .venv/bin/python tests/smoke_slot_memory.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime

PROJ = "/Users/wangguosheng/Nutstore Files/.symlinks/Nutstore/BookBot"
sys.path.insert(0, PROJ)

from loguru import logger

logger.remove()
logger.add(sys.stderr, level="WARNING")

from playwright.async_api import async_playwright  # noqa: E402

from bookbot import booker  # noqa: E402
from bookbot.auth import login, navigate_to_booking  # noqa: E402
from bookbot.booker import (  # noqa: E402
    _ensure_booking_form,
    _selector_id,
    scan_available_slots_multi,
    select_booking_criteria,
)
from bookbot.config import load_config  # noqa: E402
from bookbot.stealth import create_stealth_browser  # noqa: E402

LOG = "/tmp/probe_slot_memory.log"
_llf = open(LOG, "a", buffering=1)
EVID = "/tmp/evidence_smoke"


def log(*a):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] " + " ".join(str(x) for x in a)
    print(line, flush=True)
    _llf.write(line + "\n")


class _T:
    """Tracker stub so probe-side helpers don't touch real logs."""

    def __init__(self):
        self._metrics = {}

    def incr_metric(self, name, delta=1):
        self._metrics[name] = self._metrics.get(name, 0) + delta

    def set_metric(self, name, value):
        self._metrics[name] = value

    def add_feedback(self, reason, **kw):
        pass

    def mark_event(self, name, **kw):
        pass


async def scan_center(page, config, center, targets):
    log("=" * 12, f"scan: {center}")
    await _ensure_booking_form(page, config, rush=False)
    await select_booking_criteria(
        page, targets[0], config, center_override=center, auto_search=False, rush=False,
    )
    sid = _selector_id(config.selectors.search_button)
    fired = await page.evaluate(
        "(id) => { const b = document.getElementById(id); if (!b || b.disabled) return false; b.click(); return true; }",
        sid,
    )
    log("search fired:", fired)
    await asyncio.sleep(5)
    result = await scan_available_slots_multi(
        page, config, targets=targets, center_name=center,
    )
    for d, slots in result.items():
        log(f"  [{center}] {d}: {[(s.start, s.end) for s in slots]}")
    return result


async def main():
    config = load_config(os.path.join(PROJ, "config.yaml"))
    booker.tracker = _T()
    targets = booker.compute_target_dates(config)
    log("=" * 22, "slot-memory probe start", "=" * 22)
    log("target dates:", [str(d) for d in targets])

    verdicts = []

    async with async_playwright() as pw:
        browser, context, page = await create_stealth_browser(pw, config, rush=False)
        try:
            log("login:", await login(page, config, rush=False))
            await navigate_to_booking(page, config, rush=False)

            scans = {}
            for center in config.preferences.centers[:2]:
                try:
                    scans[center] = await scan_center(page, config, center, targets)
                except Exception as exc:
                    log(f"[{center}] scan FAILED:", str(exc)[:200])
                    try:
                        await navigate_to_booking(page, config, rush=False)
                        await _ensure_booking_form(page, config, rush=False)
                    except Exception:
                        pass

            # ── B) steering on real data ──
            log("-" * 22, "B) failed-slot memory steering")
            demo = None
            for center, result in scans.items():
                for d, slots in result.items():
                    ok = [s for s in slots if booker.is_acceptable_rush_slot(s, config, target=d)]
                    if len(ok) >= 2:
                        demo = (center, d, slots)
                        break
                if demo:
                    break
            if demo:
                center, d, slots = demo
                mem = {}
                c1 = booker._choose_rush_slots(slots, 1, config, center=center, target=d, memory=mem)
                booker._note_failed_slots(mem, center=center, target=d, slots=c1, reason="probe")
                c2 = booker._choose_rush_slots(slots, 1, config, center=center, target=d, memory=mem)
                log(f"  center={center} date={d}")
                log(f"  choice1 (no memory): {[s.start + '-' + s.end for s in c1]}")
                log(f"  choice2 (after seed) : {[s.start + '-' + s.end for s in c2]}")
                steered = bool(c1 and c2) and [s.start for s in c1] != [s.start for s in c2]
                log("  STEERED:" , steered)
                verdicts.append(("B_steering_real", steered))
            else:
                # No two acceptable cells on the live site right now: still
                # verify mechanics on real center/date keys with synthesized slots.
                log("  no date with >=2 acceptable slots live; using synthesized pair")
                center = config.preferences.centers[0]
                d = targets[0]
                S = booker.TimeSlot
                slots = [S(start="09:30", end="10:30", center=center),
                         S(start="10:30", end="11:30", center=center)]
                mem = {}
                c1 = booker._choose_rush_slots(slots, 1, config, center=center, target=d, memory=mem)
                booker._note_failed_slots(mem, center=center, target=d, slots=c1, reason="probe")
                c2 = booker._choose_rush_slots(slots, 1, config, center=center, target=d, memory=mem)
                log(f"  choice1: {[s.start for s in c1]}  choice2: {[s.start for s in c2]}")
                steered = [s.start for s in c1] != [s.start for s in c2]
                log("  STEERED (synthetic):", steered)
                verdicts.append(("B_steering_synth", steered))

            # fallback honesty: only one acceptable cell -> same choice
            center = config.preferences.centers[0]
            d = targets[0]
            S = booker.TimeSlot
            one = [S(start="09:30", end="10:30", center=center)]
            mem2 = {}
            c1 = booker._choose_rush_slots(one, 1, config, center=center, target=d, memory=mem2)
            booker._note_failed_slots(mem2, center=center, target=d, slots=c1, reason="probe")
            c2 = booker._choose_rush_slots(one, 1, config, center=center, target=d, memory=mem2)
            fallback_ok = bool(c1) and bool(c2) and c1[0].start == c2[0].start
            log("  fallback-on-retry (single cell):", fallback_ok)
            verdicts.append(("B_fallback_retry", fallback_ok))

            # ── C) live-page evidence capture ──
            log("-" * 22, "C) failure evidence on live page")
            out = await booker._capture_failure_evidence(page, "smoke-evidence", base_dir=EVID)
            exists = bool(out) and os.path.exists(out)
            head = open(out, encoding="utf-8").read()[:260] if exists else ""
            log("  file:", out, "exists:", exists)
            log("  head:", head.replace("\n", " | ")[:240])
            verdicts.append(("C_live_capture", exists and "url:" in head))

            # ── D) closed-page evidence capture (must not raise) ──
            log("-" * 22, "D) failure evidence on CLOSED page")
            p2 = await context.new_page()
            await p2.close()
            out2 = await booker._capture_failure_evidence(p2, "smoke-closed", base_dir=EVID)
            text2 = open(out2, encoding="utf-8").read() if out2 and os.path.exists(out2) else ""
            log("  file:", out2, "bytes:", len(text2))
            log("  head:", text2.replace("\n", " | ")[:240])
            verdicts.append(("D_closed_capture_survives", bool(out2) and "<inner_text failed:" in text2))

        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass

    log("=" * 22, "verdicts", "=" * 22)
    all_ok = True
    for name, ok in verdicts:
        log(f"  {'PASS' if ok else 'FAIL'}  {name}")
        all_ok = all_ok and ok
    log("=" * 22, "probe end", "=" * 22, "ALL_OK" if all_ok else "SOME FAILED")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
