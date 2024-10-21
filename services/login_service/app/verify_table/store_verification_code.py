from google.cloud import bigquery
from datetime import datetime
import logging
from config.settings import Config


def store_verification_code(email, verification_code):
    """Stores the email, verification code, and the current timestamp in the BigQuery table."""
    client = bigquery.Client()
    table_id = f"{Config.PROJECT_ID}.{Config.DATASET_ID}.verification_codes"

    rows_to_insert = [
        {
            "email": email,
            "verification_code": verification_code,
            "created_at": datetime.now().isoformat(),
        }
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        logging.error(f"Encountered errors while inserting rows: {errors}")
        return False
    return True
