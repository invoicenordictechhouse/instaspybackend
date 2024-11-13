# app.py

from fastapi import FastAPI
from contextlib import asynccontextmanager
from routes import router
from logging_config import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for application startup and shutdown events.

    Args:
        app (FastAPI): The FastAPI application instance.
    """
    # Startup code
    logger.info("Application startup")
    yield
    # Shutdown code
    logger.info("Application shutdown")


app = FastAPI(title="YouTube Scraper API", lifespan=lifespan)

app.include_router(router)
