from google.cloud import bigquery
from playwright.sync_api import sync_playwright
import time
import datetime


def get_rows_from_bq():
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Query to retrieve URLs and other columns from BigQuery
    query = """
        SELECT *  -- Select all columns
        FROM `dbt.testConsumption`
        LIMIT 500
    """

    # Run the query and fetch results
    query_job = client.query(query)
    results = query_job.result()

    # Convert results to a list of dictionaries for easier handling
    rows = [dict(row) for row in results]
    return rows


def serialize_row(row):
    for key, value in row.items():
        if isinstance(value, datetime.date):
            row[key] = value.isoformat()
    return row


def insert_row_to_bq(row, destination_table):
    client = bigquery.Client()

    # Serialize the row to handle date fields
    row = serialize_row(row)

    # Insert data into BigQuery
    errors = client.insert_rows_json(destination_table, [row])  # Wrap row in a list
    if errors:
        print(f"Errors occurred while inserting row into {destination_table}: {errors}")
    else:
        print(f"Row inserted into {destination_table} successfully.")


def scrape_youtube_link(url, context):
    page = context.new_page()

    try:
        # Navigate to the target URL
        page.goto(url, timeout=60000)  # Timeout after 60 seconds if page doesn't load

        # Wait for the page to load completely
        page.wait_for_load_state("networkidle", timeout=60000)

        # Function to recursively search frames
        def find_youtube_in_frames(frames):
            for frame in frames:
                # Check the frame's URL
                if "youtube.com" in frame.url:
                    print(f"Found YouTube iframe with src: {frame.url}")
                    return frame.url

                # Search for iframes within the frame
                iframe_elements = frame.query_selector_all("iframe")
                for iframe in iframe_elements:
                    child_frame = iframe.content_frame()
                    if child_frame:
                        result = find_youtube_in_frames([child_frame])
                        if result:
                            return result
            return None

        # Start the search from the main page frames
        youtube_url = find_youtube_in_frames(page.frames)

        if youtube_url:
            return youtube_url
        else:
            print("No YouTube iframe found.")
            return None

    except Exception as e:
        print(f"An error occurred while scraping {url}: {e}")
        return "timeout_error"
    finally:
        page.close()


def main():
    # Step 1: Fetch rows from BigQuery
    rows = get_rows_from_bq()

    # Counters
    total_urls_processed = 0
    successful_scrapes = (
        0  # Number of YouTube links found and inserted into original table
    )
    timeouts_inserted = 0  # Number of timeouts inserted into new table

    # Initialize Playwright and the browser
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)  # Set to False for debugging
        context = browser.new_context()

        # Step 3: Scrape each URL for YouTube links
        for row in rows:
            # Stop the script when 50 rows have been inserted into each table
            if successful_scrapes >= 50 and timeouts_inserted >= 50:
                print(
                    "Reached 50 successful scrapes and 50 timeouts inserted. Exiting."
                )
                break

            url = row.get("creative_page_url")
            if url:
                total_urls_processed += 1
                print(f"\nProcessing URL {total_urls_processed}: {url}")
                youtube_link = scrape_youtube_link(url, context)

                if (
                    youtube_link
                    and youtube_link != "timeout_error"
                    and "youtube.com" in youtube_link
                ):
                    if successful_scrapes >= 50:
                        print(
                            "Reached 50 successful scrapes. Skipping insertion into original table."
                        )
                    else:
                        print(f"Found YouTube link: {youtube_link}")
                        successful_scrapes += 1

                        # Add the YouTube link to the row data
                        row["youtube_video_url"] = youtube_link

                        # Insert the row into the original table immediately
                        destination_table = "annular-net-436607-t0.sample_ds.new_table_with_youtube_link"  # Update to your target table
                        insert_row_to_bq(row, destination_table)

                        print(f"Total successful scrapes: {successful_scrapes}")

                elif youtube_link == "timeout_error":
                    if timeouts_inserted >= 50:
                        print(
                            "Reached 50 timeouts inserted. Skipping insertion into new table."
                        )
                    else:
                        print(
                            "Scraping timed out. Inserting row into 'ads_probably_with_a_youtube_link' table."
                        )
                        timeouts_inserted += 1

                        # Insert the row into the new table
                        destination_table = "annular-net-436607-t0.sample_ds.ads_probably_with_a_youtube_link"  # Update to your new table
                        insert_row_to_bq(row, destination_table)

                        print(f"Total timeouts inserted: {timeouts_inserted}")

                else:
                    print("No YouTube link found, moving to next URL.")
                    print(f"Total successful scrapes: {successful_scrapes}")
                    print(f"Total timeouts inserted: {timeouts_inserted}")

                print(f"Total URLs processed so far: {total_urls_processed}")

        # Close the browser
        browser.close()

    print(f"\nScript completed.")
    print(f"Total URLs processed: {total_urls_processed}")
    print(f"Total successful scrapes: {successful_scrapes}")
    print(f"Total timeouts inserted: {timeouts_inserted}")


if __name__ == "__main__":
    main()
