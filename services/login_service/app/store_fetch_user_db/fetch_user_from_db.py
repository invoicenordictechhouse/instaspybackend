from google.cloud import bigquery
from queries import GET_USER

bigquery_client = bigquery.Client()


def get_user_from_bigquery(email: str):
    """
    Retrieves a user from BigQuery by email.

    Args:
        email (str): The user's email.

    Returns:
        dict or None: The user data if found, or None if not.
    """
    query = GET_USER

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email)]
    )
    query_job = bigquery_client.query(query, job_config=job_config)
    results = list(query_job.result())
    return results[0] if results else None
