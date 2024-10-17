from pydantic import BaseModel, Field
from typing import Optional


class AdRecord(BaseModel):
    advertiser_id: Optional[str] = None
    creative_id: Optional[str] = None
    creative_page_url: Optional[str] = None
    ad_format_type: Optional[str] = None
    advertiser_disclosed_name: Optional[str] = None
    advertiser_location: Optional[str] = None
    topic: Optional[str] = None
    is_funded_by_google_ad_grants: Optional[bool] = None
    region_code: Optional[str] = None
    first_shown: Optional[str] = None
    last_shown: Optional[str] = None
    times_shown_start_date: Optional[str] = None
    times_shown_end_date: Optional[str] = None
    youtube_times_shown_lower_bound: Optional[int] = None
    youtube_times_shown_upper_bound: Optional[int] = None
    search_times_shown_lower_bound: Optional[int] = None
    search_times_shown_upper_bound: Optional[int] = None
    shopping_times_shown_lower_bound: Optional[int] = None
    shopping_times_shown_upper_bound: Optional[int] = None
    maps_times_shown_lower_bound: Optional[int] = None
    maps_times_shown_upper_bound: Optional[int] = None
    play_times_shown_lower_bound: Optional[int] = None
    play_times_shown_upper_bound: Optional[int] = None
    youtube_video_url: Optional[str] = None
    youtube_watch_url: Optional[str] = None
