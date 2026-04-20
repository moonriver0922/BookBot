from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from bookbot.stealth import human_click, human_delay, human_type, save_debug_snapshot

if TYPE_CHECKING:
    from playwright.async_api import Page

    from bookbot.config import AppConfig

LOGIN_URL = "https://www40.polyu.edu.hk/poss/secure/login/loginhome.do"
POSS_HOME_URL = "https://www40.polyu.edu.hk/poss/secure"
POST_LOGIN_INDICATOR = "poss/secure"

MAINTENANCE_MARKERS = [
    "we'll be back soon",
    "we will be back",
    "maintenance",
    "under construction",
    "temporarily unavailable",
    "please check back later",
]


async def _is_already_logged_in(page: Page) -> bool:
    logout_loc = page.locator('a:has-text("Logout"), a:has-text("logout"), button:has-text("Logout")')
    return await logout_loc.count() > 0


async def is_maintenance_page(page: Page) -> bool:
    """Detect if the current page is a maintenance/down page."""
    try:
        text = (await page.inner_text("body")).lower()
        return any(m in text for m in MAINTENANCE_MARKERS)
    except Exception:
        return False


async def login(page: Page, config: AppConfig, *, rush: bool = False) -> bool:
    logger.info("Navigating to login page …")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")

    if not rush:
        await human_delay(config=config)
        await save_debug_snapshot(page, "01_login_page")

    if await _is_already_logged_in(page):
        logger.success("Already logged in – skipping credential form")
        return True

    username_sel = 'input[name="j_username"]'
    password_sel = 'input[name="j_password"]'

    try:
        await page.wait_for_selector(username_sel, state="visible", timeout=10_000)
    except Exception:
        if await _is_already_logged_in(page):
            logger.success("Already logged in – skipping credential form")
            return True
        raise

    logger.info("Filling credentials …")
    if rush:
        await page.fill(username_sel, config.credentials.username)
        await page.fill(password_sel, config.credentials.password)
    else:
        await human_type(page, username_sel, config.credentials.username, config=config)
        await human_delay(0.3, 0.8)
        await human_type(page, password_sel, config.credentials.password, config=config)
        await human_delay(0.5, 1.0)
        await save_debug_snapshot(page, "02_credentials_filled")

    submit_sel = 'input[value="SIGN IN"], button:has-text("SIGN IN"), input[name="buttonAction"]'
    logger.info("Submitting login form …")
    if rush:
        await page.locator(submit_sel).first.click()
    else:
        await human_click(page, submit_sel, config=config)

    try:
        await page.wait_for_url(f"**/{POST_LOGIN_INDICATOR}**", timeout=config.settings.timeout)
    except Exception:
        await save_debug_snapshot(page, "03_login_failed")
        logger.error("Login failed – did not reach post-login page. Check credentials.")
        return False

    if not rush:
        await save_debug_snapshot(page, "03_login_success")
    logger.success("Login successful")
    return True


MAKE_BOOKING_URL = (
    "https://www40.polyu.edu.hk/starspossfbstud/secure/ui_make_book/make_book.do"
)


class MaintenanceError(Exception):
    """Raised when the booking system shows a maintenance page."""
    pass


async def navigate_to_booking(page: Page, config: AppConfig, *, rush: bool = False) -> bool:
    """Navigate from the POSS home page to the sports facility booking form.

    Raises MaintenanceError if the booking system is under maintenance.
    In rush mode, goes directly to the booking URL — skipping menu navigation,
    screenshots, and human delays.
    """
    logger.info("Navigating to facility booking page …")
    timeout = config.settings.timeout

    if rush:
        await page.goto(MAKE_BOOKING_URL, wait_until="domcontentloaded", timeout=timeout)
    else:
        # Step 1: navigate via POSS menu
        try:
            facility_link = page.locator('a:has-text("Facility Booking")')
            if await facility_link.count() > 0:
                if not await facility_link.first.is_visible():
                    menu_toggle = page.locator(
                        'a.dropdown-toggle:has-text("Facility"), '
                        'button.navbar-toggle, '
                        '.nav-toggle, '
                        'a:has-text("Services")'
                    )
                    if await menu_toggle.count() > 0:
                        await menu_toggle.first.click()
                        await human_delay(0.5, 1.0)

                if await facility_link.first.is_visible():
                    await facility_link.first.click(timeout=timeout)
                    await human_delay(1.0, 2.0)
                    await page.wait_for_load_state("domcontentloaded")
                else:
                    logger.debug("Facility Booking link still not visible after menu expand attempt")

            make_booking_link = page.locator('a:has-text("Make Booking")')
            if await make_booking_link.count() > 0:
                await make_booking_link.first.click(timeout=timeout)
                await human_delay(1.0, 2.0)
                await page.wait_for_load_state("domcontentloaded")
        except Exception as e:
            logger.debug("Menu click navigation issue: {}", e)

        # Fallback: direct URL if we didn't reach the booking area
        if "make_book" not in page.url and "starspossfb" not in page.url:
            logger.debug("Direct navigation to booking URL")
            await page.goto(MAKE_BOOKING_URL, wait_until="domcontentloaded", timeout=timeout)
            await human_delay(1.5, 3.0)

        await save_debug_snapshot(page, "04_booking_page")

    # Check for maintenance page
    if await is_maintenance_page(page):
        logger.warning("Booking system is under maintenance!")
        raise MaintenanceError("Booking system shows 'We'll be back soon!'")

    if "make_book" not in page.url and "starspossfb" not in page.url:
        logger.error("Failed to reach booking page (current: {})", page.url)
        await save_debug_snapshot(page, "04_booking_page_failed")
        return False

    logger.success("Reached Make Booking page")

    # Step 2: click "Sports Facility" to enter the booking form
    sports_btn = page.locator(
        'a:has-text("Sports Facility"), '
        'button:has-text("Sports Facility"), '
        'input[value*="Sports"], '
        '.btn:has-text("Sports Facility"), '
        'a.btn:has-text("Sports")'
    )
    if await sports_btn.count() > 0:
        logger.info("Clicking 'Sports Facility' …")
        await sports_btn.first.click()
        if rush:
            try:
                await page.wait_for_selector("#actvId", state="visible", timeout=10_000)
            except Exception:
                await page.wait_for_load_state("domcontentloaded", timeout=5_000)
        else:
            await human_delay(2.0, 4.0)
            await page.wait_for_load_state("networkidle")
            await save_debug_snapshot(page, "05_sports_facility_form")

        if await is_maintenance_page(page):
            raise MaintenanceError("Sports Facility page shows maintenance after click")

        logger.success("Entered Sports Facility booking form")
    else:
        form_el = await page.query_selector("#actvId, #searchDate, #ctrId")
        if form_el:
            logger.debug("Already on the booking form")
        else:
            logger.warning("No 'Sports Facility' button and no form found")
            if not rush:
                await save_debug_snapshot(page, "05_no_form_found")

    return True
