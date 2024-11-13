import asyncio
import logging
from playwright.async_api import async_playwright


async def search_google_ads(url: str):
    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        retries = 3
        for attempt in range(retries):
            try:
                await page.goto(
                    "https://adstransparency.google.com/?authuser=0&region=SE"
                )
                logging.info(
                    "Successfully navigated to the Google Ads Transparency page."
                )
                break  # If successful, break out of the loop
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed to load the page: {e}")
                if attempt < retries - 1:  # Don't raise on the last attempt
                    await asyncio.sleep(2)  # Wait before retrying
                else:
                    logging.critical(
                        "Failed to load the Google Ads Transparency page due to network issues."
                    )
                    raise ValueError(
                        "Failed to load the Google Ads Transparency page due to network issues.",
                        e,
                    )

        try:
            await page.wait_for_selector(".input.input-area", state="visible")
        except Exception:
            logging.error("The search input area could not be found on the page.")
            raise ValueError("The search input area could not be found on the page.")

        await page.locator(".input.input-area").type(url)

        # Wait for a short period to allow suggestions to load
        await page.wait_for_timeout(2000)

        result_count = await page.locator("material-select-item.item").count()

        logging.info(f"Found {result_count} search results.")

        if result_count == 0:
            await browser.close()
            logging.error(f"The URL '{url}' does not yield any results in Google Ads.")
            raise ValueError(
                f"The URL '{url}' does not yield any results in Google Ads."
            )

        # Wait for the first search result to be visible
        await page.wait_for_selector("material-select-item.item", state="visible")

        # Click on the first search result
        await page.click("material-select-item.item:first-of-type")

        final_url = page.url

        await browser.close()

        return final_url
