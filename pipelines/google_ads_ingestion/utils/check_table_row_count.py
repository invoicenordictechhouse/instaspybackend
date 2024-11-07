from google.cloud import bigquery


def check_table_row_count(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    raw_table_id: str,
) -> int:
    """
    Retrieves the row count of a specified BigQuery table.

    This function executes a query to count the number of rows in a specified table within
    a BigQuery dataset. It can be used to verify data insertion or update success by
    comparing row counts before and after operations.

    Args:
        bigquery_client (bigquery.Client): BigQuery client instance for running queries.
        project_id (str): Google Cloud project ID where the dataset is located.
        dataset_id (str): BigQuery dataset ID containing the target table.
        raw_table_id (str): Table ID in BigQuery for which the row count is needed.

    Returns:
        int: The total number of rows in the specified table.
    """

    query = (
        f"SELECT COUNT(*) as row_count FROM {project_id}.{dataset_id}.{raw_table_id}"
    )
    job = bigquery_client.query(query)
    return list(job.result())[0].row_count
