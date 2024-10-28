from typing import List
from enums.InsertionEnum import InsertionMode
from pydantic import BaseModel


class InsertionRequest(BaseModel):
    """
    Model representing the request body for inserting updated Google Ads data.

    Attributes:
        insertion_mode (InsertionMode): The mode for inserting data (ALL or SPECIFIC).
        advertiser_ids (List[str], optional): A list of advertiser IDs for inserting specific ads (required for SPECIFIC mode).
        creative_ids (List[str], optional): A list of creative IDs for inserting specific ads (required for SPECIFIC mode).
    """

    insertion_mode: InsertionMode
    advertiser_ids: List[str] = None
    creative_ids: List[str] = None
