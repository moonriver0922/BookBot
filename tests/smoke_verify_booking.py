#!/usr/bin/env python3
"""Read-only check: does the POSS account really carry a booked slot?

Finds and dumps the Facility Booking "My Record" page (the ground-truth
transaction ledger) so a "booked" verdict can be verified against the site —
and so the user gets a place to look that beats waiting for the confirmation
email.  Never clicks anything; GET only.

    cd "<proj>" && .venv/bin/python tests/smoke_verify_booking.py
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
from datetime import datetime

PROJ = "/Users/wangguosheng/Nutstore Files/.symlinks/Nutstore/BookBot"
sys.path.insert(0, PROJ)

from loguru import logger

logger.remove()
logger.add(sys.stderr, level="WARNING")

from playwright.async_api import async_playwright  # noqa: E402

from bookbot.auth import login  # noqa: E402
from bookbot.config import load_config  # noqa: E402
from bookbot.stealth import create_stealth_browser  # noqa: E402

OUT = "/tmp/verify_booking"
os.makedirs(OUT, exist_ok=True)
_lf = open(f"{OUT}/record.log", "a", buffering=1)


def log(*a):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] " + " ".join(str(x) for x in a)
    print(line, flush=True)
    _lf.write(line + "\n")


async def dump(page, tag):
    try:
        await page.screenshot(path=f"{OUT}/{tag}.png", full_page=True)
    except Exception as exc:
        log("screenshot failed:", str(exc)[:120])
    try:
        txt = await page.inner_text("body")
    except Exception as exc:
        log("inner_text failed:", str(exc)[:120])
        return ""
    with open(f"{OUT}/{tag}.txt", "w", encoding="utf-8") as f:
        f.write(txt)
    compact = re.sub(r"\n{2,}", "\n", txt)
    log(f"===== {tag} | {page.url} =====")
    print(compact[:2200])
    log(f"===== {tag} end ({len(txt)} chars) =====")
    return txt


MY_RECORD_URL = "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_my_record/my_record.do"


async def main():
    config = load_config(os.path.join(PROJ, "config.yaml"))
    log("=" * 20, "verify booking start", "=" * 20)

    async with async_playwright() as pw:
        browser, context, page = await create_stealth_browser(pw, config, rush=False)
        try:
            log("login:", await login(page, config, rush=False))
            await page.wait_for_timeout(1200)

            # Ground truth: Facility Booking > My Record (transaction ledger).
            # NOTE: do NOT look at saoposs/secure/sessions — that is the SAO
            # activity page; facility bookings never appear there.
            await page.goto(MY_RECORD_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)
            txt = await dump(page, "my_record")
            for pat in ["Individual Booking", "Confirmed", "Cancelled", "Shaw", "Sep 2026"]:
                n = len(re.findall(re.escape(pat), txt))
                log(f"   '{pat}': {n}")
            log("=" * 20, "verify booking end", "=" * 20)
        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
