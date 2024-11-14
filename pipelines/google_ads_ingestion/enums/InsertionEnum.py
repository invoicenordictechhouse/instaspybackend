from enum import Enum


class InsertionMode(str, Enum):
    """
    Enum representing modes for inserting updated Google Ads data.

    Attributes:
        ALL: Insert all ads with updates in the dataset.
        SPECIFIC: Insert only specific ads, identified by advertiser or creative IDs.
    """

    ALL = "all"
    SPECIFIC = "specific"
