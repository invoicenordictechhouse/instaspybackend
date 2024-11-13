from typing import List, Union
from pydantic import BaseModel, Field

class ThreeMonthIngestionRequest(BaseModel):
    """
    Model for the 3-month ingestion request, requiring only the advertiser ID.
    """
    advertiser_ids: Union[str, List[str]] = Field(
        ...,
        description="Either a single advertiser ID or a list of advertiser IDs for backfill",
        example="AR18376502735441756161 or ['AR18376502735441756161', 'AR08931047766595993601']",
    )