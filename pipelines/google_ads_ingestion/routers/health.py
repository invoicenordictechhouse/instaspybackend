from fastapi import APIRouter

router = APIRouter()

@router.get("/", summary="Health Check", description="Root endpoint for health check.")
async def root():
    """
    Root endpoint that provides an overview of the API and performs a basic health check.
    It lists the available endpoints along with their purpose and expected parameters.
    """
    return {
        "message": "Google Ads Data Ingestion API is running",
        "endpoints": {
            "/daily": {
                "summary": "Daily Ingestion",
                "description": "Triggers the ingestion of Google Ads data for the previous day.",
                "method": "POST",
                "parameters": None,
                "response": {"status": "Daily ingestion initiated"},
            },
            "/backfill": {
                "summary": "Backfill Ingestion",
                "description": "Triggers backfill ingestion of Google Ads data for a specific date range.",
                "method": "POST",
                "parameters": {
                    "start_date": {
                        "description": "The start date for the backfill in 'YYYY-MM-DD' format.",
                        "required": True,
                        "example": "2023-01-01",
                    },
                    "end_date": {
                        "description": "The end date for the backfill in 'YYYY-MM-DD' format.",
                        "required": True,
                        "example": "2023-01-31",
                    },
                    "advertiser_ids": {
                        "description": "A list of advertiser IDs to backfill data for.",
                        "required": True,
                        "example": ["AD12345", "AD67890"],
                    },
                },
                "response": {
                    "status": "Backfill initiated",
                    "start_date": "YYYY-MM-DD",
                    "end_date": "YYYY-MM-DD",
                    "advertiser_ids": ["AD12345", "AD67890"],
                },
            },
            "/update": {
                "summary": "Update Ads Data",
                "description": "Triggers the update of Google Ads data based on the selected mode.",
                "method": "POST",
                "parameters": {
                    "update_mode": {
                        "description": "Defines the update mode for the operation.",
                        "type": "enum",
                        "values": {
                            "ALL": "Update all ads",
                            "ACTIVE": "Update only active ads",
                            "SPECIFIC": "Update specific ads based on advertiser or creative IDs",
                        },
                        "required": True,
                        "example": "SPECIFIC",
                    },
                    "advertiser_ids": {
                        "description": "A list of advertiser IDs for updating specific ads. Required if 'update_mode' is 'SPECIFIC'.",
                        "required": False,
                        "example": ["AD12345", "AD67890"],
                    },
                    "creative_ids": {
                        "description": "A list of creative IDs for updating specific ads. Required if 'update_mode' is 'SPECIFIC'.",
                        "required": False,
                        "example": ["CR012345", "CR678910"],
                    },
                },
                "response": {"status": "Update initiated", "update_mode": "SPECIFIC"},
            },
        },
    }

