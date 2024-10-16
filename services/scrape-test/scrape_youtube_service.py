# worker.py
from fastapi import FastAPI, Request, HTTPException
from get_rows import get_rows_from_bq
from insert_youtube_url import insert_row_to_bq
from scrape_media_url import scrape_youtube_link
from playwright.sync_api import sync_playwright
import logging

app = FastAPI()
logger = logging.getLogger(__name__)


@app.post("/scrape_media_url")
async def run_task(request: Request):
    """
    Endpoint to process a data scraping task for a given company.

    Args:
        request (Request): The HTTP request containing the task payload.

    Returns:
        dict: A message indicating the task status.

    Raises:
        HTTPException: If company_name is missing from the request.
    """
    request_data = await request.json()
    company_name = request_data.get("company_name")

    if not company_name:
        logger.error("Missing company name in request.")
        raise HTTPException(status_code=400, detail="Missing company name")

    logger.info(f"Processing task for company: {company_name}")

    rows = get_rows_from_bq()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context()

        for row in rows:
            url = row.get("creative_page_url")
            if url:
                logger.info(f"Processing URL: {url}")
                youtube_link = scrape_youtube_link(url, context)

                if (
                    youtube_link
                    and youtube_link != "timeout_error"
                    and "youtube.com" in youtube_link
                ):
                    row["youtube_video_url"] = youtube_link
                    destination_table = (
                        "annular-net-436607-t0.sample_ds.new_table_with_youtube_link"
                    )
                    insert_row_to_bq(row, destination_table)
                elif youtube_link == "timeout_error":
                    destination_table = "annular-net-436607-t0.sample_ds.ads_probably_with_a_youtube_link"
                    insert_row_to_bq(row, destination_table)

        browser.close()

    logger.info(f"Completed task for company: {company_name}")
    return {"message": f"Data processing started for {company_name}"}
