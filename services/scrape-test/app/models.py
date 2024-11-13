# models.py

from pydantic import BaseModel, Field
from typing import Optional


class JobCreateResponse(BaseModel):
    """
    Response model for job creation.

    Attributes:
        job_id (str): Unique identifier for the job.
    """

    job_id: str = Field(
        ...,
        description="Unique identifier for the job",
        example="123e4567-e89b-12d3-a456-426614174000",
    )


class JobStatusResponse(BaseModel):
    """
    Response model for job status.

    Attributes:
        job_id (str): Unique identifier for the job.
        status (str): Current status of the job.
    """

    job_id: str = Field(
        ...,
        description="Unique identifier for the job",
        example="123e4567-e89b-12d3-a456-426614174000",
    )
    status: str = Field(..., description="Current status of the job", example="Running")


class BigQueryRow(BaseModel):
    """
    Model representing a row to be inserted into BigQuery.

    Attributes:
        advertiser_id (str): Unique identifier for the advertiser.
        creative_id (str): Unique identifier for the creative.
        creative_page_url (str): URL of the creative page.
        youtube_video_url (Optional[str]): URL of the YouTube video, if found.
        youtube_watch_url (Optional[str]): Standard YouTube watch URL, if converted.
    """

    advertiser_id: str = Field(
        ..., description="Unique identifier for the advertiser", example="adv123456"
    )
    creative_id: str = Field(
        ..., description="Unique identifier for the creative", example="crt654321"
    )
    creative_page_url: str = Field(
        ...,
        description="URL of the creative page",
        example="https://example.com/creative_page",
    )
    youtube_video_url: Optional[str] = Field(
        None,
        description="URL of the YouTube video",
        example="https://www.youtube.com/embed/dQw4w9WgXcQ",
    )
    youtube_watch_url: Optional[str] = Field(
        None,
        description="Standard YouTube watch URL",
        example="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    )
