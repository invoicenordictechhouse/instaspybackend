import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def create_incremental_table_if_not_exists(
    bigquery_client: bigquery.Client, dataset_id: str, table_id: str
) -> bool:
    """
    Check if a BigQuery table exists, and if not, create a partitioned table on `data_modified`.

     Args:
        client (bigquery.Client): A BigQuery client instance.
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.

    Returns:
        bool: True if the table was created, False if it already exists.

    """
    if not dataset_id or not table_id:
        raise ValueError("Dataset ID, and Table ID must be provided.")

    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    try:
        bigquery_client.get_table(table_ref)
        logging.info(f"Table {table_id} already exists.")
        return True
    except NotFound:
        logging.info(f"Table {table_id} does not exist. Creating...")

        schema = [
            bigquery.SchemaField("data_modified", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("metadata_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("raw_data", "STRING", mode="REQUIRED"),
        ]

        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            field="data_modified", type_=bigquery.TimePartitioningType.DAY
        )

        bigquery_client.create_table(table)
        logging.info(
            f"Created partitioned incremental table {table_id} on data_modified."
        )
        return True
    except Exception as e:
        logging.error(f"Failed to create table {table_id}: {e}")
        return False
