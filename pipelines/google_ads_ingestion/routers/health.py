from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get(
    "/",
    summary="Welcome to the API",
    response_class=HTMLResponse,
    include_in_schema=False,
)
async def root(request: Request):
    """
    Root endpoint providing a centered welcome message with a clickable link to the API documentation.
    """
    return templates.TemplateResponse("welcome.html", {"request": request})
