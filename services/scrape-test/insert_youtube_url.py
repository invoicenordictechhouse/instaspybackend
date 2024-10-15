from google.cloud import bigquery
import re


def get_rows_from_bq():
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Query to retrieve rows from the successful scrapes table
    query = """
        SELECT * FROM `annular-net-436607-t0.sample_ds.new_table_with_youtube_link`
    """

    # Run the query and fetch results
    query_job = client.query(query)
    results = query_job.result()

    # Convert results to a list of dictionaries for easier handling
    rows = [dict(row) for row in results]
    return rows


def extract_video_id(youtube_embed_url):
    # Updated regular expression to extract video ID
    match = re.search(r"youtube\.com/embed/([^/?]+)", youtube_embed_url)
    if match:
        return match.group(1)
    else:
        return None


def update_rows_with_watch_url(rows):
    updated_rows = []
    for row in rows:
        embed_url = row.get("youtube_video_url")
        if embed_url:
            video_id = extract_video_id(embed_url)
            if video_id:
                watch_url = f"https://www.youtube.com/watch?v={video_id}"
                row["youtube_watch_url"] = watch_url
                print(f"Extracted watch URL: {watch_url}")
                updated_rows.append(row)
            else:
                print(f"Could not extract video ID from URL: {embed_url}")
        else:
            print("No embed URL found in row.")
    return updated_rows


def update_bq_table(rows):
    client = bigquery.Client()

    # Define the table ID
    table_id = "annular-net-436607-t0.sample_ds.new_table_with_youtube_link"

    # Check if 'youtube_watch_url' field exists, add if it doesn't
    table = client.get_table(table_id)
    field_names = [field.name for field in table.schema]
    if "youtube_watch_url" not in field_names:
        # Add the new field to the schema
        new_schema = table.schema[:]  # Copy existing schema
        new_schema.append(bigquery.SchemaField("youtube_watch_url", "STRING"))
        table.schema = new_schema
        table = client.update_table(table, ["schema"])
        print("Added 'youtube_watch_url' field to the table schema.")

    for row in rows:
        creative_id = row.get("creative_id")
        youtube_watch_url = row.get("youtube_watch_url")

        if creative_id and youtube_watch_url:
            # Construct the UPDATE query with parameters
            query = """
                UPDATE `annular-net-436607-t0.sample_ds.new_table_with_youtube_link`
                SET youtube_watch_url = @youtube_watch_url
                WHERE creative_id = @creative_id
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "youtube_watch_url", "STRING", youtube_watch_url
                    ),
                    bigquery.ScalarQueryParameter("creative_id", "STRING", creative_id),
                ]
            )

            # Execute the query
            query_job = client.query(query, job_config=job_config)
            query_job.result()  # Wait for the job to complete

            print(f"Updated row with creative_id {creative_id}")

        else:
            print(f"Missing creative_id or youtube_watch_url for row: {row}")


def main():
    # Fetch rows from BigQuery
    rows = get_rows_from_bq()

    # Extract video IDs and construct watch URLs
    updated_rows = update_rows_with_watch_url(rows)

    # Update BigQuery table with new watch URLs
    update_bq_table(updated_rows)


if __name__ == "__main__":
    main()
