import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def  store_active_creative_ids_in_staging(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    clean_table_id: str,
    staging_table_id: str
) -> None:
    """
    Store active creative IDs from the clean table in a temporary staging table.

    Args:
    bigquery_client (bigquery.Client): BigQuery client instance.
    project_id (str): Google Cloud project ID.
    dataset_id (str): BigQuery dataset ID.
    clean_table_id (str): Clean table ID.
    staging_table_id (str): Temporary staging table ID.

    Raises:
    Exception: If the query execution or table creation fails.
    """
    try:
        clean_table_ref = f"{project_id}.{dataset_id}.{clean_table_id}"
        try:
            bigquery_client.get_table(clean_table_ref)
            logging.info(f"Clean table {clean_table_ref} exists.")
            print(f"Clean table {clean_table_ref} exists.")

        except NotFound:
            logging.warning(f"Clean table {clean_table_ref} does not exist.")
            print(f"Clean table {clean_table_ref} does not exist.")
            return  
        
        query = f"""
        CREATE TABLE `{project_id}.{dataset_id}.{staging_table_id}` AS
        SELECT creative_id
        FROM `{project_id}.{dataset_id}.{clean_table_id}`
        WHERE is_active = TRUE
        """
        
        bigquery_client.query(query).result()
        logging.info(f"Active creative IDs stored in staging table {staging_table_id}")
        print(f"Active creative IDs stored in staging table {project_id}.{dataset_id}.{staging_table_id}")

    except Exception as e:
        logging.error(f"Failed to create staging table: {e}")
        print(f"Failed to create staging table: {e}")
        raise
