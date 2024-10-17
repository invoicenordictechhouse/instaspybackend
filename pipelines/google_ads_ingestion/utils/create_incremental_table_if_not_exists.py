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
        logging.info(f"Table '{table_id}' already exists in dataset '{dataset_id}'.")
        return True
    except NotFound:
        logging.info(f"Table '{table_id}' does not exist in dataset '{dataset_id}'. Creating table...")

        schema = [
            bigquery.SchemaField(
                "data_modified",
                "TIMESTAMP",
                mode="REQUIRED",
                description=(
                    "Timestamp indicating the exact moment when the ad data was last modified or updated. "
                    "This field is used for partitioning the table to optimize query performance and manage data lifecycle."
                )
            ),
            bigquery.SchemaField(
                "metadata_time",
                "TIMESTAMP",
                mode="REQUIRED",
                description=(
                    "Timestamp representing when the metadata for the ad record was recorded. "
                    "It serves as a reference for tracking the ingestion time of each record into the BigQuery table."
                )
            ),
            bigquery.SchemaField(
                "advertiser_id",
                "STRING",
                mode="REQUIRED",
                description=(
                    "Unique identifier assigned to each advertiser. "
                    "This ID is used to associate ads with their respective advertisers and facilitate aggregation and filtering based on advertiser entities."
                )
            ),
            bigquery.SchemaField(
                "creative_id",
                "STRING",
                mode="REQUIRED",
                description=(
                    "Unique identifier for each creative asset associated with an ad. "
                    "This ID distinguishes between different creative versions and is essential for tracking performance metrics at the creative level."
                )
            ),
            bigquery.SchemaField(
                "raw_data",
                "STRING",
                mode="REQUIRED",
                description=(
                    "JSON-formatted string containing the complete raw data of the ad. "
                    "This field encapsulates all relevant details and metadata related to the ad, providing a comprehensive snapshot for downstream analysis and auditing."
                )
            ),
        ]

        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            field="data_modified", type_=bigquery.TimePartitioningType.DAY
        )

        table.clustering_fields = ["advertiser_id", "creative_id"]

        bigquery_client.create_table(table)
        logging.info(
                f"Successfully created partitioned and clustered table '{table_id}' in dataset '{dataset_id}' with detailed schema descriptions."
        )
        return True
    except Exception as e:
        logging.error(f"Failed to create table {table_id}: {e}")
        return False
