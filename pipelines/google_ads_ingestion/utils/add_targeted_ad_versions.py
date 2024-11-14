from typing import List
from google.cloud import bigquery
from enums.IngestionStatus import IngestionStatus
from .check_table_row_count import check_table_row_count
from .query_builder import QueryBuilder


def add_targeted_ad_versions(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    raw_table_id: str,
    advertiser_ids: List[str] = None,
    creative_ids: List[str] = None,
) -> None:
    """
    Inserts new versions of ads for specified advertisers or creatives, retaining ad version history.

    This function targets specific ads identified by either `advertiser_ids` or `creative_ids` and inserts
    records into the raw table if there are updates. It prevents duplicate entries by checking for
    uniqueness based on raw data changes.

    Args:
        bigquery_client (bigquery.Client): BigQuery client instance for executing queries.
        project_id (str): Google Cloud project ID where the BigQuery dataset is located.
        dataset_id (str): The BigQuery dataset ID containing both the target and tracking tables.
        raw_table_id (str): The ID of the raw table where ad records are stored.
        advertiser_ids (List[str], optional): List of specific advertiser IDs to filter for updates.
        creative_ids (List[str], optional): List of specific creative IDs to filter for updates.

    Returns:
        IngestionStatus: Enum indicating the insertion status:
            - DATA_INSERTED: New records were added to the raw table.
            - NO_NEW_UPDATES: No new records were added as all ads already existed in the table.
    """

    initial_row_count = check_table_row_count(
        bigquery_client, project_id, dataset_id, raw_table_id
    )

    query = QueryBuilder.build_add_targeted_ad_versions_query(
        project_id=project_id,
        dataset_id=dataset_id,
        raw_table_id=raw_table_id,
        advertiser_ids=advertiser_ids,
        creative_ids=creative_ids,
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
