from enum import Enum

class IngestionStatus(Enum):
    """
    Enum representing the possible outcomes of data ingestion or table creation operations.
    
    Attributes:
        NO_DATA_AVAILABLE (str): Indicates that there was no relevant data available for insertion.
        DATA_INSERTED (str): Confirms that data was successfully inserted into the target table.
        INCOMPLETE_INSERTION (str): The insertion was attempted, but no new rows were added.
        NO_NEW_UPDATES (str): Specifies that no new updates were found; all ads were already up-to-date.
        TABLE_EXISTS (str): The table already exists, so no creation was needed.
        TABLE_CREATED (str): The table was successfully created in BigQuery.
        TABLE_CREATION_FAILED (str): An error occurred during the table creation process.
    """
    NO_DATA_AVAILABLE = "No data available for ingestion."
    DATA_INSERTED = "Data inserted successfully."
    INCOMPLETE_INSERTION = "Insertion attempted, but no new rows were added."
    NO_NEW_UPDATES = "No new updates found; all ads are already up-to-date."
    TABLE_EXISTS = "Table already exists."
    TABLE_CREATED = "Table created successfully."
    TABLE_CREATION_FAILED = "Table creation failed."