from __future__ import annotations

import asyncio
import traceback
from pathlib import Path

from loguru import logger
from playwright.async_api import async_playwright

from bookbot.auth import MaintenanceError, login, navigate_to_booking
from bookbot.booker import FormNotReadyError, run_booking
from bookbot.config import AppConfig
from bookbot.stealth import create_stealth_browser, save_debug_snapshot
from bookbot.tracker import tracker


async def execute(
    config: AppConfig, *,
    dry_run: bool = False,
    rush_time: tuple[int, int, int] | None = None,
) -> bool:
    """Run the full booking pipeline: launch browser -> login -> book.

    When *rush_time* is (hour, minute, second) the bot completes login and
    form preparation immediately, then waits until the target time to click
    Search.  This lets you launch early (e.g. 08:20) while still booking the
    moment new slots open (e.g. 08:30).
    """
    Path("screenshots").mkdir(exist_ok=True)

    mode = "rush" if rush_time else "normal"
    tracker.start_run(mode=mode)

    async with async_playwright() as pw:
        with tracker.step("browser_launch"):
            browser, context, page = await create_stealth_browser(
                pw, config, rush=rush_time is not None,
            )

        success = False
        try:
            success = await _run_with_retries(page, config, dry_run=dry_run, rush_time=rush_time)
            return success
        finally:
            tracker.finish_run(success=success)
            await context.close()
            await browser.close()


async def _run_with_retries(
    page, config: AppConfig, *, dry_run: bool,
    rush_time: tuple[int, int, int] | None = None,
) -> bool:
    max_retries = config.settings.retry_count
    base_interval = config.settings.retry_interval
    logged_in = False

    rush = rush_time is not None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("=== Attempt {}/{} ===", attempt, max_retries)

            if not logged_in:
                with tracker.step("login"):
                    if not await login(page, config, rush=rush):
                        tracker.add_feedback("login_failed", attempt=attempt)
                        raise RuntimeError("Login failed")
                logged_in = True

            with tracker.step("navigate_to_booking"):
                if not await navigate_to_booking(page, config, rush=rush):
                    tracker.add_feedback("navigation_failed", attempt=attempt, url=page.url)
                    raise RuntimeError("Failed to reach booking page")

            result = await run_booking(page, config, dry_run=dry_run, rush_time=rush_time)

            if result:
                logger.success("Booking completed successfully on attempt {}", attempt)
                return True
            else:
                logger.warning("No booking made on attempt {}", attempt)
                return False

        except MaintenanceError as exc:
            tracker.add_feedback("maintenance", attempt=attempt, detail=str(exc))
            wait = min(15, 5 + attempt * 5)
            logger.warning(
                "Maintenance detected on attempt {}: {} – retrying in {}s …",
                attempt, exc, wait,
            )
            await save_debug_snapshot(page, f"maintenance_attempt_{attempt}")

            if attempt < max_retries:
                await asyncio.sleep(wait)
                # Go back to POSS home and re-navigate
                try:
                    await page.goto(
                        "https://www40.polyu.edu.hk/poss/secure/login/loginhome.do",
                        wait_until="domcontentloaded",
                    )
                    await asyncio.sleep(2)
                except Exception:
                    pass

        except FormNotReadyError as exc:
            tracker.add_feedback("form_not_ready", attempt=attempt, detail=str(exc))
            wait = min(20, 8 + attempt * 5)
            logger.warning(
                "Form not ready on attempt {}: {} – retrying in {}s …",
                attempt, exc, wait,
            )
            await save_debug_snapshot(page, f"form_not_ready_attempt_{attempt}")

            if attempt < max_retries:
                await asyncio.sleep(wait)
                try:
                    await page.goto(
                        "https://www40.polyu.edu.hk/poss/secure/login/loginhome.do",
                        wait_until="domcontentloaded",
                    )
                    await asyncio.sleep(2)
                except Exception:
                    pass

        except Exception as exc:
            tracker.add_feedback("exception", attempt=attempt,
                                 error=type(exc).__name__, detail=str(exc))
            wait = base_interval * (2 ** (attempt - 1))
            logger.error(
                "Attempt {} failed: {} – retrying in {}s …",
                attempt, exc, wait,
            )
            logger.debug(traceback.format_exc())
            await save_debug_snapshot(page, f"error_attempt_{attempt}")

            if attempt < max_retries:
                try:
                    content = await page.content()
                    if _is_blocked(content):
                        wait *= 2
                        logger.warning("Possible anti-bot block detected, doubling wait to {}s", wait)
                except Exception:
                    logger.debug("Could not read page content for anti-bot check (page may still be navigating)")

                await asyncio.sleep(wait)

                try:
                    await page.goto(
                        "https://www40.polyu.edu.hk/poss/secure/login/loginhome.do",
                        wait_until="domcontentloaded",
                        timeout=config.settings.timeout,
                    )
                except Exception:
                    pass
                try:
                    logout_loc = page.locator('a:has-text("Logout")')
                    if await logout_loc.count() == 0:
                        logged_in = False
                except Exception:
                    logged_in = False

    logger.error("All {} attempts exhausted", max_retries)
    tracker.add_feedback("all_attempts_exhausted", max_retries=max_retries)
    return False


def _is_blocked(html: str) -> bool:
    lower = html.lower()
    indicators = [
        "access denied",
        "forbidden",
        "blocked",
        "rate limit",
        "too many requests",
        "captcha",
        "challenge",
        "verify you are human",
    ]
    return any(ind in lower for ind in indicators)
