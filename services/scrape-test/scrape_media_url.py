from playwright.sync_api import BrowserContext
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def scrape_youtube_link(url: str, context: BrowserContext) -> Optional[str]:
    """
    Scrapes the given URL for an embedded YouTube link by inspecting the frames.

    Args:
        url (str): The URL to scrape.
        context (BrowserContext): The Playwright browser context to use for scraping.

    Returns:
        Optional[str]: The YouTube link if found, "timeout_error" on timeout, or None if no link is found.
    """
    logger.info(f"Starting to scrape for YouTube link at URL: {url}")
    page = context.new_page()
    try:
        page.goto(url, timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)

        def find_youtube_in_frames(frames):
            for frame in frames:
                if "youtube.com" in frame.url:
                    logger.info(f"Found YouTube iframe with src: {frame.url}")
                    return frame.url
                iframe_elements = frame.query_selector_all("iframe")
                for iframe in iframe_elements:
                    child_frame = iframe.content_frame()
                    if child_frame:
                        result = find_youtube_in_frames([child_frame])
                        if result:
                            return result
            return None

        youtube_url = find_youtube_in_frames(page.frames)
        if youtube_url:
            logger.info(f"Successfully found YouTube URL: {youtube_url}")
            return youtube_url
        else:
            logger.warning("No YouTube iframe found.")
            return None

    except Exception as e:
        logger.error(f"Error occurred while scraping {url}: {e}")
        return "timeout_error"
    finally:
        page.close()
