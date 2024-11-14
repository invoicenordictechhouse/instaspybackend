from typing import List, Optional
from queries import (
    ADVERTISER_IDS_SUBQUERY,
    ADVERTISER_TRACKING_SUBQUERY,
    CHECK_DATA_AVAILABILITY_QUERY,
    INSERT_NEW_GOOGLE_ADS_DATA_QUERY,
    ADD_UPDATED_ADS_QUERY,
    ADD_TARGETED_ADS_QUERY,
)


class QueryBuilder:
    @staticmethod
    def _get_advertiser_ids_subquery(
        backfill: bool, project_id: str, dataset_id: str, advertiser_ids_table: str
    ) -> str:
        """
        Generates a subquery to select advertiser IDs based on the backfill flag.

        Args:
            backfill (bool): If True, select from provided advertiser IDs; if False, select from advertiser tracking table.
            project_id (str): Google Cloud project ID.
            dataset_id (str): BigQuery dataset ID.
            advertiser_ids_table (str): Table ID for advertiser tracking.

        Returns:
            str: SQL subquery for advertiser ID selection.
        """
        return (
            ADVERTISER_IDS_SUBQUERY
            if backfill
            else ADVERTISER_TRACKING_SUBQUERY.format(
                project_id, dataset_id, advertiser_ids_table
            )
        )

    @staticmethod
    def build_check_data_availability_query(
        project_id: str, dataset_id: str, advertiser_ids_table: str, backfill: bool
    ) -> str:
        """
        Constructs the query to check data availability for selected advertiser IDs.

        Args:
            project_id (str): Google Cloud project ID.
            dataset_id (str): BigQuery dataset ID.
            advertiser_ids_table (str): Table ID for advertiser tracking.
            backfill (bool): If True, select from provided advertiser IDs; if False, select from advertiser tracking table.

        Returns:
            str: SQL query for checking data availability.
        """
        advertiser_ids_subquery = QueryBuilder._get_advertiser_ids_subquery(
            backfill=backfill,
            project_id=project_id,
            dataset_id=dataset_id,
            advertiser_ids_table=advertiser_ids_table,
        )
        return CHECK_DATA_AVAILABILITY_QUERY.format(
            advertiser_ids_subquery=advertiser_ids_subquery
        )

    @staticmethod
    def build_add_targeted_ad_versions_query(
        project_id: str,
        dataset_id: str,
        raw_table_id: str,
        advertiser_ids: Optional[List[str]] = None,
        creative_ids: Optional[List[str]] = None,
    ):
        """
        Constructs the query to add specific ad versions for targeted advertisers or creatives.

        Args:
            project_id (str): Google Cloud project ID.
            dataset_id (str): BigQuery dataset ID.
            raw_table_id (str): Target table ID in BigQuery for storing ad data.
            advertiser_ids (Optional[List[str]]): List of specific advertiser IDs to update.
            creative_ids (Optional[List[str]]): List of specific creative IDs to update.

        Returns:
            str: SQL query for adding targeted ad versions.
        """
        conditions = []
        if advertiser_ids:
            advertiser_ids_str = ", ".join(
                [f'"{advertiser_id}"' for advertiser_id in advertiser_ids]
            )
            conditions.append(f"t.advertiser_id IN ({advertiser_ids_str})")

        if creative_ids:
            creative_ids_str = ", ".join(
                [f'"{creative_id}"' for creative_id in creative_ids]
            )
            conditions.append(f"t.creative_id IN ({creative_ids_str})")

        where_clause = " OR ".join(conditions)
        return ADD_TARGETED_ADS_QUERY.format(
            project_id=project_id,
            dataset_id=dataset_id,
            raw_table_id=raw_table_id,
            where_clause=where_clause,
        )

    @staticmethod
    def build_add_updated_ads_query(
        project_id: str, dataset_id: str, raw_table_id: str, tracking_table_id: str
    ):
        """
        Constructs the query to add updated ads for all advertisers in the tracking table.

        Args:
            project_id (str): Google Cloud project ID.
            dataset_id (str): BigQuery dataset ID.
            raw_table_id (str): Target table ID in BigQuery for storing ad data.
            tracking_table_id (str): Table ID for advertiser tracking.

        Returns:
            str: SQL query for adding updated ads.
        """
        return ADD_UPDATED_ADS_QUERY.format(
            project_id=project_id,
            dataset_id=dataset_id,
            raw_table_id=raw_table_id,
            ADVERTISERS_TRACKING_TABLE_ID=tracking_table_id,
        )

    @staticmethod
    def build_insert_new_google_ads_data_query(
        project_id: str,
        dataset_id: str,
        table_id: str,
        advertiser_ids_table: str,
        backfill: bool,
    ):
        """
        Constructs the query for inserting new Google Ads data.

        Args:
            project_id (str): Google Cloud project ID.
            dataset_id (str): BigQuery dataset ID.
            table_id (str): Target table ID in BigQuery for storing ad data.
            advertiser_ids_table (str): Table ID for advertiser tracking.
            backfill (bool): If True, select from provided advertiser IDs; if False, select from advertiser tracking table.

        Returns:
            str: SQL query for inserting new Google Ads data.
        """
        selected_advertisers_query = QueryBuilder._get_advertiser_ids_subquery(
            backfill=backfill,
            project_id=project_id,
            dataset_id=dataset_id,
            advertiser_ids_table=advertiser_ids_table,
        )

        return INSERT_NEW_GOOGLE_ADS_DATA_QUERY.format(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            selected_advertisers_query=selected_advertisers_query,
        )
