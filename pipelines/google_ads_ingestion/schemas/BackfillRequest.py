from typing import List, Union
from pydantic import BaseModel, Field


class BackfillRequest(BaseModel):
    """
    Model representing the request body for backfill ingestion.

    Attributes:
        start_date (str): The start date for the backfill in 'YYYY-MM-DD' format.
        end_date (str): The end date for the backfill in 'YYYY-MM-DD' format.
        advertiser_ids (List[str]): A list of advertiser IDs for which data should be backfilled.
    """

    start_date: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date in 'YYYY-MM-DD' format",
        example="2023-03-01"
    )
    end_date: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date in 'YYYY-MM-DD' format",
        example="2024-03-01"

    )
    advertiser_ids: Union[str, List[str]] = Field(
        ...,
        description="Either a single advertiser ID or a list of advertiser IDs for backfill",
        example="AR18376502735441756161 or ['AR18376502735441756161', 'AR08931047766595993601']"
    )