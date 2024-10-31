import logging

from fastapi import HTTPException
from enums.IngestionStatus import IngestionStatus


def handle_ingestion_result(result: IngestionStatus, process_name: str, is_backfill: bool = False) -> None:
    """
    Handles logging and raises HTTP exceptions based on the ingestion status.

    This function logs the status of the ingestion or table creation process and raises HTTP exceptions 
    when necessary. It adapts its response based on whether the process is a backfill or a daily ingestion.

    Args:
        result (IngestionStatus): The status result of the ingestion or table creation operation. 
            Accepted values include:
            - IngestionStatus.NO_DATA_AVAILABLE: No relevant data found for ingestion.
            - IngestionStatus.DATA_INSERTED: Data was successfully inserted.
            - IngestionStatus.INCOMPLETE_INSERTION: Insertion completed but no new rows were added.
            - IngestionStatus.NO_NEW_UPDATES: No updates were found; all data is up-to-date.
            - IngestionStatus.TABLE_EXISTS: The specified table already exists.
            - IngestionStatus.TABLE_CREATED: A new table was successfully created.
            - IngestionStatus.TABLE_CREATION_FAILED: An error occurred while creating the table.
        process_name (str): A descriptive name for the process, such as "Daily Ingestion" or "Backfill Ingestion". 
            This is used in log messages and exceptions for context.
        is_backfill (bool): Indicates whether the process is a backfill. This parameter affects how 
            certain statuses, like `INCOMPLETE_INSERTION`, are handled. Defaults to False.

    Raises:
        HTTPException: Raised for specific ingestion statuses:
            - HTTP 204 if no data was available or no new updates were found (`NO_DATA_AVAILABLE` or `NO_NEW_UPDATES`).
            - HTTP 200 if a backfill was attempted but no new rows were added (`INCOMPLETE_INSERTION`).
            - HTTP 500 if table creation failed or an unexpected error occurred (`TABLE_CREATION_FAILED`).
            - HTTP 500 for any other unexpected exceptions.
    """
    try:
        if result in [IngestionStatus.NO_DATA_AVAILABLE, IngestionStatus.NO_NEW_UPDATES]:
            logging.info(result.value)
            raise HTTPException(status_code=204, detail=result.value)
        
        if result == IngestionStatus.DATA_INSERTED:
            logging.info(f"{process_name} completed successfully.")
            return

        if result == IngestionStatus.INCOMPLETE_INSERTION:
            if is_backfill:
                logging.warning(f"{process_name}: {result.value}")
                raise HTTPException(status_code=200, detail=f"{process_name}: {result.value}")
            else:
                logging.info(f"{process_name}: {result.value}")
        
        if result == IngestionStatus.TABLE_EXISTS:
            logging.info(f"Table check for {process_name}: {result.value}.")

        if result == IngestionStatus.TABLE_CREATED:
            logging.info(f"{result.value} for {process_name}.")

        if result == IngestionStatus.TABLE_CREATION_FAILED:
            logging.error(result.value)
            raise HTTPException(status_code=500, detail=result.value)

        raise ValueError("Unexpected ingestion status encountered.")

    except HTTPException as http_exc:
        raise http_exc
    
    except Exception as e:
        logging.error(f"Unexpected error in {process_name}: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred during ingestion.")
