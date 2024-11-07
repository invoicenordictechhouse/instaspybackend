from playwright.async_api import async_playwright
import logging


async def extract_advertiser_id(previous_url: str) -> str:

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Navigate to the previously returned URL
        await page.goto(previous_url)
        logging.info(f"Navigated to the URL: {previous_url}")
        # Wait for the priority-creative-grid to be visible
        await page.wait_for_selector("creative-grid", state="visible")

        # Now wait for the first creative-preview element under creative-grid to be visible
        await page.wait_for_selector("creative-grid creative-preview", state="visible")

        # Check if the targeted <a> element is present
        is_present = await page.locator(
            "creative-grid creative-preview:first-of-type a"
        ).count()
        logging.info(f"Found {is_present} matching elements.")

        if is_present == 0:
            logging.error("The advertiser ID could not be found.")
            raise ValueError("The advertiser ID could not be found.")

        # Get the href value of the first <a> within the first creative-preview
        href_value = await page.eval_on_selector(
            "creative-grid creative-preview:first-of-type a", "a => a.href"
        )
        if href_value is None:
            logging.error("The expected link for the advertiser ID could not be found.")
            raise ValueError("The advertiser ID could not be found.")
        advertiser_id = href_value.split("/")[4]  # Extracting the desired ID
        logging.info(f"Extracted advertiser ID: {advertiser_id}")
        await browser.close()

        return advertiser_id
