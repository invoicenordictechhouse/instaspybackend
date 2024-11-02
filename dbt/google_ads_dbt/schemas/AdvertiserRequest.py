from pydantic import BaseModel, Field
from typing import Union, List

class AdvertiserRequest(BaseModel):
    """
    Request model for specifying advertiser IDs to target for data processing.

    Attributes:
        advertiser_ids (Union[str, List[str]]): Accepts a single advertiser ID as a string or a list of multiple advertiser IDs. 
            - Example (single ID): 'AR18376502735441756161'
            - Example (multiple IDs): ['AR18376502735441756161', 'AR08931047766595993601']

    This model is used to allow flexibility in specifying either a single or multiple advertiser IDs in API requests.
    """
    advertiser_ids: Union[str, List[str]] = Field(
        ..., 
        description="Single advertiser ID as a string or multiple IDs as a list.",
        example="AR18376502735441756161 or ['AR18376502735441756161', 'AR08931047766595993601']"
    )
