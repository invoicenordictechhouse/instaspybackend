from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", summary="Welcome to the API", response_class=HTMLResponse, include_in_schema=False)
async def root():
    """
    Root endpoint providing a welcome message with a clickable link to the API documentation.
    """
    return """
    <html>
        <body>
            <h2>Welcome to the Google ads ingestion</h2>
            <p><a href="/docs">Click here to view the API documentation</a></p>
        </body>
    </html>
    """
