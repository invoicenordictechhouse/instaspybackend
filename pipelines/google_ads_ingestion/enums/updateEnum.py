from enum import Enum

class UpdateMode(str, Enum):
    """
    Enum representing the different modes for updating Google Ads data.
    
    Attributes:
        ALL: Update all ads in the dataset.
        ACTIVE: Update only active ads.
        SPECIFIC: Update specific ads based on advertiser or creative IDs.
    """
    ALL = "all"
    ACTIVE = "active"
    SPECIFIC = "specific"
