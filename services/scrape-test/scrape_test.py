from google.cloud import bigquery
from playwright.sync_api import sync_playwright
import time
import re


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


def insert_rows_to_bq(data, destination_table):
    client = bigquery.Client()

    # Insert data into BigQuery
    errors = client.insert_rows_json(destination_table, data)
    if errors:
        print("Errors occurred while inserting rows:", errors)
    else:
        print("Data inserted successfully.")


def scrape_youtube_link(url, max_duration=20):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)  # Set to True in production
        context = browser.new_context()
        page = context.new_page()

        youtube_url = None

        try:
            # Navigate to the target URL with a timeout
            page.goto(url, timeout=max_duration * 1000)

            # Wait for the page to load completely
            page.wait_for_load_state("networkidle", timeout=max_duration * 1000)

            # Wait for iframes to be attached (regardless of visibility)
            page.wait_for_selector(
                "iframe", state="attached", timeout=max_duration * 1000
            )

            # Get all iframe elements
            iframe_elements = page.query_selector_all("iframe")

            # Iterate over iframes to find the YouTube video
            for iframe in iframe_elements:
                iframe_src = iframe.get_attribute("src")
                if iframe_src and "youtube.com" in iframe_src:
                    youtube_url = iframe_src
                    break  # Exit the loop once the video is found

            if youtube_url:
                return youtube_url
            else:
                print("No YouTube iframe found on the page.")
                return None

        except Exception as e:
            print(f"An error occurred while scraping {url}: {e}")
            return None
        finally:
            # Ensure the browser is closed
            browser.close()


def main():
    # Prepare a single row with the specific URL
    row = {
        "creative_page_url": "https://adstransparency.google.com/advertiser/AR14182191227838922753/creative/CR06756772009524330497"
    }
    rows_with_youtube = []

    # Scrape the URL for YouTube links
    url = row.get("creative_page_url")
    if url:
        print(f"Scraping URL: {url}")
        youtube_link = scrape_youtube_link(url, max_duration=30)
        if youtube_link and "youtube.com" in youtube_link:
            print(f"Found YouTube link: {youtube_link}")

            # Add the YouTube link to the row data
            row["youtube_video_url"] = youtube_link

            # Append the updated row to the list
            rows_with_youtube.append(row)
        else:
            print("No YouTube link found, moving to next URL.")

    # Optionally insert the row into BigQuery or handle it as needed
    if rows_with_youtube:
        print("YouTube link found and processed.")
    else:
        print("No YouTube link was found.")


if __name__ == "__main__":
    main()
