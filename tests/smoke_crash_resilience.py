#!/usr/bin/env python3
"""Live probe v2: rush crash-resilience helpers against the real POSS site.

Deterministic reproduction of the 09-15 crash condition: a JS evaluation that
is interrupted by a real navigation (its context is destroyed mid-flight).

  A1) RAW page.evaluate      -> must RAISE "Execution context was destroyed"
  A2) _safe_evaluate (same)  -> must retry and return 'SURVIVED'
  B)  _tick_confirm_checkboxes on a live page            -> int, never raises
  B2) _tick_confirm_checkboxes during a navigation race  -> same
  C)  _await_confirmation_page on a live page            -> bool, never hangs

    cd "<proj>" && .venv/bin/python tests/smoke_crash_resilience.py
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
from bookbot.config import load_config  # noqa: E402
from bookbot.stealth import create_stealth_browser  # noqa: E402

LOG = "/tmp/probe_resilience.log"
_lf = open(LOG, "a", buffering=1)


def log(*a):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] " + " ".join(str(x) for x in a)
    print(line, flush=True)
    _lf.write(line + "\n")


class _T:
    """Lightweight tracker stub so probe-side helpers don't touch real logs."""

    def __init__(self):
        self._metrics = {}
        self.events = []
        self.fb = []

    def mark_event(self, name, **kw):
        self.events.append(name)

    def add_feedback(self, reason, **kw):
        self.fb.append(reason)

    def set_metric(self, name, value):
        self._metrics[name] = value


NAV_JS = (
    "() => {"
    "  if (!sessionStorage.getItem('probe_nav')) {"
    "    sessionStorage.setItem('probe_nav', '1');"
    "    setTimeout(() => { location.href = %r; }, 50);"
    "    return new Promise(r => setTimeout(() => r('FIRST-RUN'), 4000));"
    "  }"
    "  return 'SURVIVED';"
    "}" % booker.BOOKING_URL
)


async def main():
    config = load_config(os.path.join(PROJ, "config.yaml"))
    booker.tracker = _T()

    log("=" * 22, "resilience probe v2 start", "=" * 22)

    async with async_playwright() as pw:
        browser, context, page = await create_stealth_browser(pw, config, rush=True)
        try:
            log("login:", await login(page, config, rush=True))
            await navigate_to_booking(page, config, rush=True)
            await booker._ensure_booking_form(page, config, rush=True)
            log("booking form ready")
            await page.evaluate("() => { try { sessionStorage.clear(); } catch (e) {} }")

            # ── A1) RAW evaluate destroyed by a real navigation ──
            try:
                val = await page.evaluate(NAV_JS)
                log("A1 RAW evaluate -> returned:", repr(val), "(expected RAISE)")
            except Exception as exc:
                log("A1 RAW evaluate -> RAISED:", str(exc)[:160])
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=8000)
            except Exception:
                pass
            await page.evaluate("() => { try { sessionStorage.clear(); } catch (e) {} }")

            # ── A2) _safe_evaluate surviving the same destruction ──
            try:
                val2 = await booker._safe_evaluate(
                    page, NAV_JS, retries=4, wait_timeout_ms=6000,
                )
                log("A2 _safe_evaluate ->", repr(val2), "(retried through the nav)")
            except Exception as exc:
                log("A2 _safe_evaluate -> RAISED:", str(exc)[:160])
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=8000)
            except Exception:
                pass
            await booker._ensure_booking_form(page, config, rush=True)

            # ── B) tick helper on a live page (with diagnostics) ──
            try:
                info = await page.evaluate(
                    "() => [...document.querySelectorAll('input[type=checkbox]:not(:checked)')]"
                    ".map(c => ({id: c.id, name: c.name, cls: String(c.className).slice(0,40)}))"
                )
                log("B pre-tick unchecked boxes:", info)
                n = await booker._tick_confirm_checkboxes(page)
                log("B tick on live page ->", n, "(int, no raise)")
            except Exception as exc:
                log("B tick on live page -> RAISED:", str(exc)[:160])

            # ── B2) tick helper DURING a navigation race ──
            nav3 = asyncio.create_task(
                page.goto(booker.BOOKING_URL, wait_until="commit")
            )
            await asyncio.sleep(0.05)
            try:
                n2 = await booker._tick_confirm_checkboxes(page)
                log("B2 tick during nav race ->", n2, "(no raise)")
            except Exception as exc:
                log("B2 tick during nav race -> RAISED:", str(exc)[:160])
            await nav3
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=8000)
            except Exception:
                pass

            # ── C) confirmation-page wait fallback on a live page ──
            try:
                ok = await booker._await_confirmation_page(
                    page, page_timeout_ms=400, grace_timeout_ms=800,
                )
                log("C confirm-page wait ->", ok, "(bool, no hang)")
            except Exception as exc:
                log("C confirm-page wait -> RAISED:", str(exc)[:160])

            log("tracker events:", booker.tracker.events)
            log("tracker feedback:", booker.tracker.fb)
        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass
            log("=" * 22, "resilience probe v2 end", "=" * 22)


if __name__ == "__main__":
    asyncio.run(main())
