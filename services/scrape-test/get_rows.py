logger = logging.getLogger(__name__)


def get_rows_from_bq():
    client = bigquery.Client()
    query = """
        SELECT * 
        FROM `dbt.testConsumption`
        LIMIT 500
    """
    try:
        logger.info("Starting BigQuery query to fetch rows.")
        query_job = client.query(query)
        results = query_job.result()
        rows = [dict(row) for row in results]
        logger.info("Successfully fetched rows from BigQuery.")
        return rows
    except Exception as e:
        logger.error(f"Failed to fetch rows from BigQuery: {e}")
        return []
