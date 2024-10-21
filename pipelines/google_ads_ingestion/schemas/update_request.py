from typing import List
from enums.updateEnum import UpdateMode
from pydantic import BaseModel

class UpdateRequest(BaseModel):
    """
    Model representing the request body for updating Google Ads data.

    Attributes:
        update_mode (UpdateMode): The mode for updating data (ALL, ACTIVE, or SPECIFIC).
        advertiser_ids (List[str], optional): A list of advertiser IDs for updating specific ads (required for SPECIFIC mode).
        creative_ids (List[str], optional): A list of creative IDs for updating specific ads (required for SPECIFIC mode).
    """
    update_mode: UpdateMode
    advertiser_ids: List[str] = None
    creative_ids: List[str] = None
