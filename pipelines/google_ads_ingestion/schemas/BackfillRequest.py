from typing import List
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
    )
    end_date: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date in 'YYYY-MM-DD' format",
    )
    advertiser_ids: List[str] = Field(
        ..., min_items=1, description="List of advertiser_id_secondary for backfill"
    )
