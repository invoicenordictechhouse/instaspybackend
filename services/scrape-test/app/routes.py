# routes.py

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Path
from models import JobCreateResponse, JobStatusResponse
from scraper import process_job
from logging_config import logger
import global_vars
import uuid

router = APIRouter()


@router.post(
    "/start_job",
    response_model=JobCreateResponse,
    summary="Start a new scraping job",
    tags=["Jobs"],
)
async def start_job(
    advertiser_id: str = Query(
        ..., description="Advertiser ID to process", example="AR00642912486307135489"
    ),
    background_tasks: BackgroundTasks = None,
):
    """
    Start a new scraping job for a given advertiser ID.

    Args:
        advertiser_id (str): The ID of the advertiser to process.
        background_tasks (BackgroundTasks): FastAPI background tasks.

    Returns:
        JobCreateResponse: Contains the job ID of the started job.
    """
    job_id = str(uuid.uuid4())
    global_vars.job_statuses[job_id] = "Pending"
    global_vars.active_jobs.add(job_id)

    # Cancel the shutdown timer if it's running
    if (
        global_vars.shutdown_timer_task is not None
        and not global_vars.shutdown_timer_task.done()
    ):
        global_vars.shutdown_timer_task.cancel()
        logger.info("Shutdown timer canceled due to new job submission.")

    background_tasks.add_task(process_job, job_id, advertiser_id)
    logger.info(f"Job {job_id} started for advertiser_id {advertiser_id}")
    return JobCreateResponse(job_id=job_id)


@router.get(
    "/job_status/{job_id}",
    response_model=JobStatusResponse,
    summary="Get the status of a job",
    tags=["Jobs"],
)
async def job_status(
    job_id: str = Path(
        ...,
        description="Job ID to check status for",
        example="123e4567-e89b-12d3-a456-426614174000",
    )
):
    """
    Get the status of a job by its ID.

    Args:
        job_id (str): The ID of the job to check.

    Returns:
        JobStatusResponse: Contains the job ID and its current status.
    """
    status = global_vars.job_statuses.get(job_id)
    if status is None:
        logger.error(f"Job {job_id} not found")
        raise HTTPException(status_code=404, detail="Job not found")
    logger.info(f"Job {job_id} status requested: {status}")
    return JobStatusResponse(job_id=job_id, status=status)
