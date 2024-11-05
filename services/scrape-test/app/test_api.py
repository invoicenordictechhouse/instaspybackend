import asyncio
from google.cloud import bigquery
from playwright.async_api import async_playwright
import re
from fastapi import FastAPI, BackgroundTasks
from typing import Dict
import uuid
from config_loader import load_config

config = load_config()
test_consumption_table = config["bigquery"]["tables"]["test_consumption"]
youtube_links_table = config["bigquery"]["tables"]["youtube_links"]

app = FastAPI()

# In-memory storage for job statuses
job_statuses: Dict[str, str] = {}


def get_rows_from_bq():
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Query to retrieve rows from the successful scrapes table
    query = f"""
    SELECT
        advertiser_id,
        creative_id,
        creative_page_url
    FROM
        `{test_consumption_table}`
    WHERE
        advertiser_id IS NOT NULL
        AND creative_id IS NOT NULL
        AND creative_page_url IS NOT NULL
    """

    # Run the query and fetch results
    query_job = client.query(query)
    results = query_job.result()

    # Convert results to a list of dictionaries for easier handling
    rows = [dict(row) for row in results]
    return rows


def insert_row_to_bq(row, destination_table):
    client = bigquery.Client()

    # Define the columns we want to include
    required_columns = [
        "advertiser_id",
        "creative_id",
        "creative_page_url",
        "youtube_video_url",
        "youtube_watch_url",
    ]

    # Filter the row to only include the required columns
    filtered_row = {key: row[key] for key in required_columns if key in row}

    # For debugging: print the row being inserted
    print(f"Inserting row into {destination_table}: {filtered_row}")

    # Insert data into BigQuery
    errors = client.insert_rows_json(
        destination_table, [filtered_row]
    )  # Wrap row in a list
    if errors:
        print(f"Errors occurred while inserting row into {destination_table}: {errors}")
    else:
        print(f"Row inserted into {destination_table} successfully.")


async def scrape_youtube_link(url, context):
    page = await context.new_page()

    try:
        # Navigate to the target URL
        await page.goto(
            url, timeout=20000
        )  # Timeout after 20 seconds if page doesn't load

        # Wait for the page to load completely
        await page.wait_for_load_state("networkidle", timeout=20000)

        # Function to recursively search frames
        async def find_youtube_in_frames(frames):
            for frame in frames:
                # Check the frame's URL
                if "youtube.com" in frame.url:
                    print(f"Found YouTube iframe with src: {frame.url}")
                    return frame.url

                # Search for iframes within the frame
                iframe_elements = await frame.query_selector_all("iframe")
                for iframe in iframe_elements:
                    child_frame = await iframe.content_frame()
                    if child_frame:
                        result = await find_youtube_in_frames([child_frame])
                        if result:
                            return result
            return None

        # Start the search from the main page frames
        youtube_url = await find_youtube_in_frames(page.frames)

        if youtube_url:
            return youtube_url
        else:
            print("No YouTube iframe found.")
            return None

    except Exception as e:
        print(f"An error occurred while scraping {url}: {e}")
        return "timeout_error"
    finally:
        await page.close()


def convert_embed_to_watch_url(embed_url: str) -> str:
    """
    Convert a YouTube embed URL to a standard watch URL.

    Args:
        embed_url (str): The YouTube embed URL.

    Returns:
        str: The standard YouTube watch URL.
    """
    # Regular expression to extract the video ID from the embed URL
    match = re.search(r"/embed/([^?&]+)", embed_url)
    if match:
        video_id = match.group(1)
        watch_url = f"https://www.youtube.com/watch?v={video_id}"
        return watch_url
    else:
        # If the embed URL doesn't match the expected pattern, return None or handle accordingly
        print(f"Invalid embed URL format: {embed_url}")
        return None


async def process_url(row, browser, counters, lock, sem, loop, stop_event):
    if stop_event.is_set():
        return

    async with sem:
        if stop_event.is_set():
            return

        async with lock:
            print(f"Row keys: {list(row.keys())}")
            print(f"Advertiser ID: {row.get('advertiser_id')}")
            print(f"Creative ID: {row.get('creative_id')}")
            counters["total_urls_processed"] += 1

        url = row.get("creative_page_url")
        if not url:
            return

        print(f"\nProcessing URL {counters['total_urls_processed']}: {url}")

        context = await browser.new_context()
        youtube_link = await scrape_youtube_link(url, context)
        await context.close()

        # Now process youtube_link
        async with lock:
            if (
                youtube_link
                and youtube_link != "timeout_error"
                and "youtube.com" in youtube_link
            ):
                if counters["successful_scrapes"] >= 50:
                    print(
                        "Reached 50 successful scrapes. Skipping insertion into original table."
                    )
                else:
                    print(f"Found YouTube link: {youtube_link}")
                    counters["successful_scrapes"] += 1

                    # Convert embed URL to watch URL
                    youtube_watch_link = convert_embed_to_watch_url(youtube_link)
                    if not youtube_watch_link:
                        print(
                            "Failed to convert embed URL to watch URL. Skipping insertion."
                        )
                        return  # Skip this row if conversion fails

                    # Add the YouTube links to the row data
                    row["youtube_video_url"] = youtube_link
                    row["youtube_watch_url"] = youtube_watch_link

                    # Insert the row into the original table
                    destination_table = youtube_links_table
                    await loop.run_in_executor(
                        None, insert_row_to_bq, row, destination_table
                    )

                    print(f"Total successful scrapes: {counters['successful_scrapes']}")

            elif youtube_link == "timeout_error":
                if counters["timeouts_inserted"] >= 50:
                    print(
                        "Reached 50 timeouts inserted. Skipping insertion into new table."
                    )
                else:
                    print(
                        "Scraping timed out. Inserting row into 'ads_probably_with_a_youtube_link' table."
                    )
                    counters["timeouts_inserted"] += 1

                    # Insert the row into the new table without YouTube links
                    destination_table = "annular-net-436607-t0.sample_ds.ads_probably_with_a_youtube_link"
                    await loop.run_in_executor(
                        None, insert_row_to_bq, row, destination_table
                    )

                    print(f"Total timeouts inserted: {counters['timeouts_inserted']}")

            else:
                print("No YouTube link found, moving to next URL.")
                print(f"Total successful scrapes: {counters['successful_scrapes']}")
                print(f"Total timeouts inserted: {counters['timeouts_inserted']}")

            # Check if both limits are reached
            if (
                counters["successful_scrapes"] >= 50
                and counters["timeouts_inserted"] >= 50
            ):
                stop_event.set()

        print(f"Total URLs processed so far: {counters['total_urls_processed']}")


async def process_job(job_id: str):
    job_statuses[job_id] = "Running"
    try:
        # Step 1: Fetch rows from BigQuery
        loop = asyncio.get_running_loop()
        rows = await loop.run_in_executor(None, get_rows_from_bq)

        # Counters and lock
        counters = {
            "total_urls_processed": 0,
            "successful_scrapes": 0,
            "timeouts_inserted": 0,
        }
        lock = asyncio.Lock()
        sem = asyncio.Semaphore(5)  # Limit concurrency to 5
        stop_event = asyncio.Event()

        # Initialize Playwright and the browser
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)

            tasks = [
                process_url(row, browser, counters, lock, sem, loop, stop_event)
                for row in rows
            ]

            await asyncio.gather(*tasks)

            # Close the browser
            await browser.close()

        print("\nScript completed.")
        print(f"Total URLs processed: {counters['total_urls_processed']}")
        print(f"Total successful scrapes: {counters['successful_scrapes']}")
        print(f"Total timeouts inserted: {counters['timeouts_inserted']}")

        job_statuses[job_id] = "Completed"
    except Exception as e:
        job_statuses[job_id] = f"Failed: {str(e)}"


@app.post("/start_job")
async def start_job(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    job_statuses[job_id] = "Pending"
    background_tasks.add_task(process_job, job_id)
    return {"job_id": job_id}


@app.get("/job_status/{job_id}")
async def job_status(job_id: str):
    status = job_statuses.get(job_id, "Not Found")
    return {"job_id": job_id, "status": status}
