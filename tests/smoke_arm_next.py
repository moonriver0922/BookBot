"""Headless-chromium smoke for the armed Next-click observer (NOT a pytest test).

Run from the repo root with the project venv:
    .venv/bin/python tests/smoke_arm_next.py

Cases:
  1. button enables after 1200ms -> armed click lands right after enable
  2. button enables after 200ms  -> measured enable->click delta
  3. first click ignored (fake validation flash) -> re-arm -> second click lands
  4. button never enables -> observer times out, no false success
"""
import asyncio
import pathlib
import sys
import tempfile

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from playwright.async_api import async_playwright  # noqa: E402

from bookbot import booker  # noqa: E402

_SCRIPT_COMMON = """
const b = () => document.getElementById('nextButton');
b().addEventListener('click', () => {
    window.__clicks = (window.__clicks || 0) + 1;
    window.__clickedAt = performance.now();
    if (window.__ignoreBefore && performance.now() < window.__ignoreBefore) {
        window.__ignored = (window.__ignored || 0) + 1;
        return;
    }
    location.hash = 'ok';
});
"""

HTML_DELAYED = """<!doctype html><html><body>
<form><button id="nextButton" type="button" disabled>Next</button></form>
<script>
""" + _SCRIPT_COMMON + """
setTimeout(() => { b().disabled = false; window.__enabledAt = performance.now(); }, 1200);
</script></body></html>"""

HTML_ENABLE_200 = """<!doctype html><html><body>
<form><button id="nextButton" type="button" disabled>Next</button></form>
<script>
""" + _SCRIPT_COMMON + """
setTimeout(() => { b().disabled = false; window.__enabledAt = performance.now(); }, 200);
</script></body></html>"""

HTML_IGNORE_FIRST = """<!doctype html><html><body>
<form><button id="nextButton" type="button" disabled>Next</button></form>
<script>
""" + _SCRIPT_COMMON + """
window.__ignoreBefore = 1500;   // clicks before ~1500ms are ignored (validation flash sim)
setTimeout(() => { b().disabled = false; window.__enabledAt = performance.now(); }, 300);
</script></body></html>"""

HTML_NEVER = """<!doctype html><html><body>
<form><button id="nextButton" type="button" disabled>Next</button></form>
</body></html>"""


async def _stats(page):
    return await page.evaluate(
        "() => ({clicks: window.__clicks || 0, ignored: window.__ignored || 0, "
        "enabledAt: Math.round(window.__enabledAt || 0), clickedAt: Math.round(window.__clickedAt || 0), "
        "url: location.href})"
    )


async def main():
    tmpdir = pathlib.Path(tempfile.mkdtemp())
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        page = await browser.new_page()

        async def run_case(name, html, *, use_fast_click=False, arm_timeout_ms=4000):
            path = tmpdir / f"{name}.html"
            path.write_text(html)
            await page.goto(path.as_uri())
            t0 = asyncio.get_event_loop().time()
            if use_fast_click:
                result = await booker._click_next_fast(
                    page, "#nextButton", [150, 300, 500], arm_timeout_ms=arm_timeout_ms
                )
            else:
                result = await booker._arm_next_click(page, "#nextButton", timeout_ms=arm_timeout_ms)
            wall = (asyncio.get_event_loop().time() - t0) * 1000
            stats = await _stats(page)
            gap = (
                stats["clickedAt"] - stats["enabledAt"]
                if stats["clickedAt"] and stats["enabledAt"]
                else None
            )
            print(f"{name}: result={result} wall={wall:.0f}ms stats={stats} enable_to_click={gap}ms")

        await run_case("case1_delayed", HTML_DELAYED)
        await run_case("case2_enable200", HTML_ENABLE_200)
        await run_case("case3_rearm", HTML_IGNORE_FIRST, use_fast_click=True)
        await run_case("case4_never", HTML_NEVER, use_fast_click=True, arm_timeout_ms=600)

        await browser.close()


asyncio.run(main())
