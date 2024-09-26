from google.cloud import bigquery
from config.settings import Config
import logging
from datetime import datetime, timezone

bigquery_client = bigquery.Client()


def insert_user_into_bigquery(email: str, hashed_password: str):
    """
    Inserts a new user into BigQuery.

    Args:
        email (str): The user's email.
        hashed_password (str): The hashed password.
    
    Returns:
        bool: True if the insertion is successful, False otherwise.
    """
    try:
        table = f"{Config.PROJECT_ID}.{Config.DATASET_ID}.{Config.TABLE_ID}"
        rows_to_insert = [
            {"email": email, 
             "password": hashed_password,
             "date_created": datetime.now(timezone.utc).isoformat()
             }
        ]
        errors = bigquery_client.insert_rows_json(table, rows_to_insert)
        if errors:
            logging.error(f"Error inserting rows: {errors}")
            return False
        return True
    except Exception as e:
        logging.error(f"Failed to insert user: {e}")
        return False
