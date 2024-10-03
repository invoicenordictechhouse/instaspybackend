from google.cloud import bigquery
from queries import DELETE_USER_FROM_TEMP_TABLE


def delete_verification_code(email: str):
    """
    Deletes the verification code entry for the given email from the verification_codes table.

    Args:
        email (str): The email whose verification code should be deleted.
    """
    client = bigquery.Client()

    query = DELETE_USER_FROM_TEMP_TABLE

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email)]
    )

    query_job = client.query(query, job_config=job_config)

    # Wait for the job to complete
    query_job.result()

    print(f"Verification code for {email} has been deleted.")
