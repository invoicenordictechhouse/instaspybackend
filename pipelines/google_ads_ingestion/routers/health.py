from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", summary="Welcome to the API", response_class=HTMLResponse, include_in_schema=False)
async def root():
    """
    Root endpoint providing a centered welcome message with a clickable link to the API documentation.
    """
    return """
    <html>
        <head>
            <style>
                body {
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    font-family: Arial, sans-serif;
                    background-color: #f4f4f9;
                    margin: 0;
                }
                .container {
                    text-align: center;
                    padding: 20px;
                    background-color: white;
                    border-radius: 10px;
                    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                }
                h2 {
                    color: #333;
                }
                a {
                    text-decoration: none;
                    color: #007bff;
                    font-weight: bold;
                }
                a:hover {
                    color: #0056b3;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h2>Welcome to the Google Ads Ingestion</h2>
                <p><a href="/docs">Click here to view the API documentation</a></p>
            </div>
        </body>
    </html>
    """
