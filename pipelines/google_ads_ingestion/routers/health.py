from fastapi import APIRouter

router = APIRouter()


@router.get(
    "/",
    summary="Health Check",
    description="Root endpoint for API health and information.",
)
async def root():
    """
    Health check endpoint providing API status and a summary of key endpoints.
    Each endpoint includes its purpose, method type, and required parameters.
    """
    return {
        "status": "Healthy",
        "message": "Google Ads Data Ingestion API is running",
        "endpoints": [
            {
                "path": "/ingestion/daily",
                "summary": "Daily Ingestion",
                "description": "Initiates Google Ads data ingestion for the previous day.",
                "method": "POST",
            },
            {
                "path": "/ingestion/backfill",
                "summary": "Backfill Ingestion",
                "description": "Triggers backfill ingestion for a date range.",
                "method": "POST",
                "parameters": {
                    "start_date": "YYYY-MM-DD",
                    "end_date": "YYYY-MM-DD",
                    "advertiser_ids": ["AD12345", "AD67890"],
                },
            },
            {
                "path": "/ads/insert-updated",
                "summary": "Insert Updated Ads",
                "description": "Initiates insertion of updated ads based on mode.",
                "method": "POST",
                "parameters": {
                    "update_mode": ["ALL", "SPECIFIC"],
                    "advertiser_ids": ["AD12345", "AD67890"],
                    "creative_ids": ["CR012345", "CR678910"],
                },
            },
        ],
    }
