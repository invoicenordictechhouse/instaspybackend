from fastapi import HTTPException
from utils.logging_config import logger
from fastapi.responses import JSONResponse
from enums.IngestionStatus import IngestionStatus


def handle_ingestion_result(
    result: IngestionStatus, process_name: str, is_backfill: bool = False
) -> JSONResponse:
    """
    Handles logging and raises HTTP exceptions based on the ingestion status.

    This function logs the status of the ingestion or table creation process and raises HTTP exceptions
    when necessary. It adapts its response based on whether the process is a backfill or a daily ingestion.

    - **Args**:
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

    **Raises**:
        HTTPException: Raised for specific ingestion statuses:
            - HTTP 204 if no data was available or no new updates were found (`NO_DATA_AVAILABLE` or `NO_NEW_UPDATES`).
            - HTTP 200 if a backfill was attempted but no new rows were added (`INCOMPLETE_INSERTION`).
            - HTTP 500 if table creation failed or an unexpected error occurred (`TABLE_CREATION_FAILED`).
            - HTTP 500 for any other unexpected exceptions.
    **Description**:
        - If no data or no new updates are available, an HTTP 204 status is raised to indicate no content.
        - If data insertion fails for a backfill, a warning with HTTP 200 is returned.
        - Handles scenarios where the table exists or was created successfully, logging information messages.

    """
    try:
        if result == IngestionStatus.NO_DATA_AVAILABLE:
            logger.info(f"{process_name}: No data available for ingestion.")
            return JSONResponse(status_code=204, content={"status": result.value})

        if result == IngestionStatus.DATA_INSERTED:
            logger.info(f"{process_name} completed successfully.")
            return JSONResponse(
                status_code=200, content={"status": f"{process_name}: {result.value}."}
            )

        if result == IngestionStatus.INCOMPLETE_INSERTION:
            logger.info(f"{process_name}:  {result.value}.")
            status_code = 200 if is_backfill else 206
            return JSONResponse(
                status_code=status_code, content={"status": result.value}
            )

        if result == IngestionStatus.TABLE_EXISTS:
            logger.info(f"{process_name}: Table already exists.")
            return JSONResponse(status_code=200, content={"status": result.value})

        if result == IngestionStatus.TABLE_CREATED:
            logger.info(f"{process_name}: New table created successfully.")
            return JSONResponse(status_code=201, content={"status": result.value})

        if result == IngestionStatus.TABLE_CREATION_FAILED:
            logger.error(f"{process_name}: {result.value}")
            raise HTTPException(status_code=500, detail=result.value)

    except HTTPException as http_exc:
        logger.error(
            f"HTTPException occurred in {process_name}: {http_exc.detail}",
            exc_info=True,
        )
        raise http_exc

    except Exception as e:
        logger.error(f"Unexpected error in {process_name}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An unexpected error occurred during ingestion."
        )
