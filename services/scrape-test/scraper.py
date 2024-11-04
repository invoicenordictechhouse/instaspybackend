# scraper.py

import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext
from bigquery_utils import get_rows_from_bq, insert_row_to_bq
from utils import convert_embed_to_watch_url
from logging_config import logger
from config_loader import config
import global_vars
from models import BigQueryRow
from pydantic import ValidationError
from typing import Dict, Any, List


async def scrape_youtube_link(url: str, context: BrowserContext) -> str:
    """
    Scrape a given URL to find embedded YouTube links.

    Args:
        url (str): The URL to scrape.
        context (BrowserContext): The Playwright browser context.

    Returns:
        str: The YouTube embed URL if found, "timeout_error" if a timeout occurs, or None if not found.
    """
    page = await context.new_page()

    try:
        await page.goto(url, timeout=20000)
        await page.wait_for_load_state("networkidle", timeout=20000)

        async def find_youtube_in_frames(frames) -> str:
            for frame in frames:
                if "youtube.com" in frame.url:
                    logger.debug(f"Found YouTube iframe with src: {frame.url}")
                    return frame.url

                iframe_elements = await frame.query_selector_all("iframe")
                for iframe in iframe_elements:
                    child_frame = await iframe.content_frame()
                    if child_frame:
                        result = await find_youtube_in_frames([child_frame])
                        if result:
                            return result
            return None

        youtube_url = await find_youtube_in_frames(page.frames)

        if youtube_url:
            return youtube_url
        else:
            logger.debug("No YouTube iframe found.")
            return None

    except Exception as e:
        logger.error(f"An error occurred while scraping {url}: {e}")
        return "timeout_error"
    finally:
        await page.close()


async def process_url(
    row: Dict[str, Any],
    browser: Browser,
    counters: Dict[str, int],
    lock: asyncio.Lock,
    sem: asyncio.Semaphore,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """
    Process a single URL from the row data.

    Args:
        row (Dict[str, Any]): The row data containing the URL and identifiers.
        browser (Browser): The Playwright browser instance.
        counters (Dict[str, int]): Shared counters for tracking progress.
        lock (asyncio.Lock): Lock for synchronizing access to shared counters.
        sem (asyncio.Semaphore): Semaphore for controlling concurrency.
        loop (asyncio.AbstractEventLoop): The event loop.
    """
    async with sem:
        async with lock:
            counters["total_urls_processed"] += 1

        url = row.get("creative_page_url")
        if not url:
            return

        logger.info(
            f"Processing URL {counters['total_urls_processed']}/{counters['total_rows']}: {url}"
        )

        context = await browser.new_context()
        youtube_link = await scrape_youtube_link(url, context)
        await context.close()

        if (
            youtube_link
            and youtube_link != "timeout_error"
            and "youtube.com" in youtube_link
        ):
            logger.info(f"Found YouTube link: {youtube_link}")
            async with lock:
                counters["successful_scrapes"] += 1

            youtube_watch_link = convert_embed_to_watch_url(youtube_link)
            if not youtube_watch_link:
                logger.warning(
                    "Failed to convert embed URL to watch URL. Skipping insertion."
                )
                return

            try:
                row_data = BigQueryRow(
                    advertiser_id=row["advertiser_id"],
                    creative_id=row["creative_id"],
                    creative_page_url=row["creative_page_url"],
                    youtube_video_url=youtube_link,
                    youtube_watch_url=youtube_watch_link,
                )
            except ValidationError as e:
                logger.error(f"Data validation error: {e}")
                return  # Skip insertion if validation fails

            destination_table = config["bigquery"]["tables"]["youtube_links"]
            await loop.run_in_executor(
                None, insert_row_to_bq, row_data, destination_table
            )

            logger.info(f"Total successful scrapes: {counters['successful_scrapes']}")

        elif youtube_link == "timeout_error":
            logger.info("Scraping timed out. Inserting row into timeouts table.")
            async with lock:
                counters["timeouts_inserted"] += 1

            try:
                row_data = BigQueryRow(
                    advertiser_id=row["advertiser_id"],
                    creative_id=row["creative_id"],
                    creative_page_url=row["creative_page_url"],
                    youtube_video_url=None,
                    youtube_watch_url=None,
                )
            except ValidationError as e:
                logger.error(f"Data validation error: {e}")
                return  # Skip insertion if validation fails

            destination_table = config["bigquery"]["tables"]["timeouts"]
            await loop.run_in_executor(
                None, insert_row_to_bq, row_data, destination_table
            )

            logger.info(f"Total timeouts inserted: {counters['timeouts_inserted']}")

        else:
            logger.info("No YouTube link found, moving to next URL.")

        logger.debug(
            f"Total URLs processed so far: {counters['total_urls_processed']}/{counters['total_rows']}"
        )


async def process_job(job_id: str, advertiser_id: str) -> None:
    """
    Process a scraping job for a given advertiser ID.

    Args:
        job_id (str): The unique identifier for the job.
        advertiser_id (str): The advertiser ID to process.
    """
    job_statuses = global_vars.job_statuses
    active_jobs = global_vars.active_jobs

    job_statuses[job_id] = "Running"
    try:
        loop = asyncio.get_running_loop()
        rows = await loop.run_in_executor(None, get_rows_from_bq, advertiser_id)

        counters = {
            "total_rows": len(rows),
            "total_urls_processed": 0,
            "successful_scrapes": 0,
            "timeouts_inserted": 0,
        }
        lock = asyncio.Lock()
        sem = asyncio.Semaphore(config["concurrency"]["max_concurrent_tasks"])

        logger.info(
            f"Starting processing {counters['total_rows']} URLs for advertiser_id {advertiser_id}"
        )

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)

            tasks = [
                process_url(row, browser, counters, lock, sem, loop) for row in rows
            ]

            await asyncio.gather(*tasks)

            await browser.close()

        logger.info(f"Job {job_id} completed.")
        logger.info(f"Total URLs processed: {counters['total_urls_processed']}")
        logger.info(f"Total successful scrapes: {counters['successful_scrapes']}")
        logger.info(f"Total timeouts inserted: {counters['timeouts_inserted']}")

        job_statuses[job_id] = "Completed"
    except Exception as e:
        logger.exception(f"Job {job_id} failed: {str(e)}")
        job_statuses[job_id] = f"Failed: {str(e)}"
    finally:
        active_jobs.discard(job_id)
        if not active_jobs:
            if (
                global_vars.shutdown_timer_task is None
                or global_vars.shutdown_timer_task.done()
            ):
                global_vars.shutdown_timer_task = asyncio.create_task(
                    shutdown_after_delay()
                )
                logger.info("No active jobs. Shutdown timer started.")
        else:
            logger.info(f"Active jobs remaining: {len(active_jobs)}")


async def shutdown_after_delay() -> None:
    """
    Initiates server shutdown after a specified delay.
    """
    try:
        await asyncio.sleep(config["shutdown"]["delay_seconds"])
        logger.info("Shutdown timer expired. Shutting down the server.")
        global_vars.shutdown_event.set()
    except asyncio.CancelledError:
        logger.info("Shutdown timer canceled.")
