from typing import List
from utils.updateEnum import UpdateMode

from pydantic import BaseModel, Field


class BackfillRequest(BaseModel):
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


class UpdateRequest(BaseModel):
    update_mode: UpdateMode
    advertiser_ids: List[str] = None
    creative_ids: List[str] = None
