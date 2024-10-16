from google.cloud import bigquery
from typing import Dict, Any
import datetime
import logging

logger = logging.getLogger(__name__)


def serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Serializes a row dictionary by converting date fields to ISO format strings.

    Args:
        row (Dict[str, Any]): The row data to serialize.

    Returns:
        Dict[str, Any]: The serialized row with date fields as ISO strings.
    """
    logger.debug("Serializing row data.")
    for key, value in row.items():
        if isinstance(value, datetime.date):
            row[key] = value.isoformat()
    logger.debug("Row data serialization complete.")
    return row


def insert_row_to_bq(row: Dict[str, Any], destination_table: str) -> None:
    """
    Inserts a single row into a specified BigQuery table.

    Args:
        row (Dict[str, Any]): The data to insert, as a dictionary.
        destination_table (str): The BigQuery table to insert the row into.

    Raises:
        RuntimeError: If the row insertion encounters an error.
    """
    client = bigquery.Client()
    row = serialize_row(row)

    try:
        logger.info(f"Inserting row into BigQuery table: {destination_table}")
        errors = client.insert_rows_json(destination_table, [row])
        if errors:
            logger.error(f"Errors occurred while inserting row: {errors}")
        else:
            logger.info(f"Row successfully inserted into {destination_table}.")
    except Exception as e:
        logger.error(f"Failed to insert row into {destination_table}: {e}")
