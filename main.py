import asyncio
import random
import json
from playwright.async_api import async_playwright, Page
from playwright_stealth import Stealth

async def scrape_chrono24_full(url: str, pages_to_run: int,  starting_page: int):
    """
    Orchestrates the multi-page, deep-link extraction process.
    """
    stealth_engine = Stealth()
    async with stealth_engine.use_async(async_playwright()) as playwright:
        browser = await playwright.chromium.launch(headless=False, slow_mo=100)
        page = await browser.new_page()
        await page.goto(url)
        await handle_cookie_consent(page)
        
        await process_pages(page, pages_to_run, starting_page)
        await browser.close()

async def process_pages(page: Page, total_pages: int,  starting_page: int):
    """
    Iterates through pagination and triggers deep-link processing for each page.
    """
    for current_num in range(1, total_pages + 1):
        await human_scroll_to_bottom(page)
        
        if current_num >= starting_page:
            # Get URLs once per page to avoid stale element issues
            urls = await collect_listing_urls(page)
            await process_listing_urls(page, urls)
        
        if current_num < total_pages:
            await navigate_to_page_number(page, current_num + 1)

async def process_listing_urls(page: Page, urls: list):
    """
    Navigates to each watch, extracts table data, and flushes it to storage.
    """
    for url in urls:
        try:
            await page.goto(url)
            watch_data = await extract_table_data(page)
            save_data_to_file(url, watch_data)
        except Exception as error:
            print(f"Error processing {url}: {error}")
        finally:
            # Ensures we return to the listing flow even on failure
            await page.go_back()
            await page.wait_for_selector(".js-listing-item-link")

async def extract_table_data(page: Page) -> list:
    """
    Finds all tables and extracts row text similarly to the BS4 separator logic.
    """
    await page.wait_for_selector("table")
    return await page.evaluate("""() => {
        const tables = Array.from(document.querySelectorAll('table'));
        return tables.flatMap(table => 
            Array.from(table.querySelectorAll('tr')).map(tr => tr.innerText.replace(/\\t/g, ': ').trim())
        );
    }""")

def save_data_to_file(url: str, data: list):
    """
    Flushes data to a JSONL file immediately to prevent data loss.
    """
    entry = {"url": url, "specifications": data}
    with open("scraped_watches.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

async def collect_listing_urls(page: Page) -> list:
    """
    Retrieves all hrefs for the watch listings on the current page.
    """
    selector = "a.js-listing-item-link"
    await page.wait_for_selector(selector)
    return await page.locator(selector).evaluate_all("links => links.map(a => a.href)")

async def handle_cookie_consent(page: Page):
    """Clears the cookie consent layer."""
    try:
        button = await page.wait_for_selector(".js-cookie-accept-all", timeout=5000)
        if button: await button.click()
    except: pass

async def navigate_to_page_number(page: Page, target_num: int):
    """Navigates to the next page in the pagination list."""
    selector = "nav.pagination ul li a"
    await page.wait_for_selector(selector)
    links = await page.query_selector_all(selector)

    for link in links:
        if (await link.inner_text()).strip() == str(target_num):
            await link.click()
            await page.wait_for_load_state("networkidle")
            return

async def human_scroll_to_bottom(page: Page):
    """Mimics human scrolling."""
    for _ in range(4):
        await page.mouse.wheel(0, 800)
        await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(scrape_chrono24_full("https://www.chrono24.com/rolex/index.htm", 27, 16))