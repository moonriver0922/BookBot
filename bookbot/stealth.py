from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING

from loguru import logger
from playwright.async_api import Page
from playwright_stealth import Stealth

if TYPE_CHECKING:
    from playwright.async_api import Browser, BrowserContext, Playwright

    from bookbot.config import AppConfig

_stealth = Stealth(
    navigator_platform_override="MacIntel",
    navigator_vendor_override="Google Inc.",
)


async def create_stealth_browser(
    pw: Playwright, config: AppConfig, *, rush: bool = False,
) -> tuple[Browser, BrowserContext, Page]:
    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-gpu",
        "--disable-dev-shm-usage",
    ]

    launch_kwargs: dict = {
        "headless": config.settings.headless,
        "args": launch_args,
    }
    if config.stealth.use_real_chrome:
        launch_kwargs["channel"] = "chrome"

    browser = await pw.chromium.launch(**launch_kwargs)

    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
        timezone_id="Asia/Hong_Kong",
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
    )
    context.set_default_timeout(config.settings.timeout)

    await _stealth.apply_stealth_async(context)

    if rush:
        await _install_resource_blocker(context)

    page = await context.new_page()

    logger.debug("Stealth browser created (headless={}, rush={})", config.settings.headless, rush)
    return browser, context, page


_BLOCKED_RESOURCE_TYPES = {"image", "media", "font"}
_BLOCKED_URL_PATTERNS = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
                         ".woff", ".woff2", ".ttf", ".eot",
                         "google-analytics", "googletagmanager",
                         "facebook.net", "doubleclick.net")


async def _install_resource_blocker(context_or_page) -> None:
    """Block images/fonts/analytics to speed up page loads in rush mode.

    When applied to a BrowserContext, it covers all pages (including new tabs).
    """
    async def _handle_route(route):
        req = route.request
        if req.resource_type in _BLOCKED_RESOURCE_TYPES:
            await route.abort()
            return
        url = req.url.lower()
        if any(p in url for p in _BLOCKED_URL_PATTERNS):
            await route.abort()
            return
        await route.continue_()

    await context_or_page.route("**/*", _handle_route)
    logger.debug("Resource blocker installed (blocking images/fonts/analytics)")


async def human_delay(
    min_s: float | None = None,
    max_s: float | None = None,
    *,
    config: AppConfig | None = None,
    rush: bool = False,
) -> None:
    if rush:
        await asyncio.sleep(random.uniform(0.02, 0.05))
        return
    if config:
        lo = config.stealth.human_delay_min
        hi = config.stealth.human_delay_max
    else:
        lo = min_s or 0.3
        hi = max_s or 1.5
    await asyncio.sleep(random.uniform(lo, hi))


async def human_type(
    page: Page,
    selector: str,
    text: str,
    *,
    config: AppConfig | None = None,
) -> None:
    lo = config.stealth.typing_delay_min if config else 50
    hi = config.stealth.typing_delay_max if config else 150

    await page.click(selector)
    await human_delay(0.2, 0.5)
    await page.type(selector, text, delay=random.randint(lo, hi))


async def human_click(page: Page, selector: str, *, config: AppConfig | None = None) -> None:
    element = await page.wait_for_selector(selector, state="visible")
    if element:
        box = await element.bounding_box()
        if box:
            x = box["x"] + box["width"] / 2 + random.uniform(-3, 3)
            y = box["y"] + box["height"] / 2 + random.uniform(-3, 3)
            await page.mouse.move(x, y)
            await human_delay(0.1, 0.3)
            await page.mouse.click(x, y)
        else:
            await element.click()
    else:
        await page.click(selector)
    await human_delay(config=config)


async def save_debug_snapshot(page: Page, name: str) -> None:
    try:
        await page.screenshot(path=f"screenshots/{name}.png", full_page=True)
        logger.debug("Screenshot saved: screenshots/{}.png", name)
    except Exception as exc:
        logger.warning("Failed to save screenshot {}: {}", name, exc)
