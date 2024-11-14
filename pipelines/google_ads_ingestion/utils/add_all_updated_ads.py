from config import ADVERTISERS_TRACKING_TABLE_ID
from google.cloud import bigquery
from enums.IngestionStatus import IngestionStatus
from .query_builder import QueryBuilder
from utils.check_table_row_count import check_table_row_count


def add_all_updated_ads(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    raw_table_id: str,
) -> IngestionStatus:
    """
    Inserts updated ad versions for all advertisers listed in the ADVERTISERS_TRACKING_TABLE_ID.

    This function retrieves the latest ad records for advertisers in the specified tracking table,
    and inserts new or modified ad records into the target raw table, ensuring only unique
    records are added based on raw data changes. This maintains historical versions of each ad.

    Args:
        bigquery_client (bigquery.Client): An instance of BigQuery client to execute queries.
        project_id (str): The Google Cloud project ID where BigQuery datasets reside.
        dataset_id (str): The ID of the dataset containing both the target and tracking tables.
        raw_table_id (str): The ID of the raw table where updated ad data is stored.

    Returns:
        IngestionStatus: An enum value indicating the status of the insertion process:
            - DATA_INSERTED: New rows were successfully added to the raw table.
            - NO_NEW_UPDATES: No new rows were added, as all ads already existed in the table.

    """
    initial_row_count = check_table_row_count(
        bigquery_client, project_id, dataset_id, raw_table_id
    )

    query = QueryBuilder.build_add_updated_ads_query(
        project_id=project_id,
        dataset_id=dataset_id,
        raw_table_id=raw_table_id,
        advertisers_tracking_table=ADVERTISERS_TRACKING_TABLE_ID,
    )

    query_job = bigquery_client.query(query)
    query_job.result()

    final_row_count = check_table_row_count(
        bigquery_client, project_id, dataset_id, raw_table_id
    )

    if final_row_count > initial_row_count:
        return IngestionStatus.DATA_INSERTED
    else:
        return IngestionStatus.NO_NEW_UPDATES
