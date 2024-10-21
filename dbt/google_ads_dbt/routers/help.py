from fastapi import APIRouter

router = APIRouter()

@router.get("/", summary="Health Check", description="API Health Check")
async def root():
    """
    Root endpoint for health check.
    """
    return {
        "message": "Google Ads DBT API is running",
        "available_endpoints": ["/clean_all", "/clean_specific"],
    }
