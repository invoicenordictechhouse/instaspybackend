import httpx  # For async HTTP requests
import logging

logger = logging.getLogger(__name__)

WORKFLOW_TRIGGER_URL = "https://your-cloud-workflow-url-here"


async def trigger_workflow(company_url: str):
    """
    Triggers the Google Cloud workflow via an HTTP POST request.

    Args:
        company_url (str): The company name to be passed to the workflow.

    Returns:
        None
    """
    try:
        logger.info(f"Triggering workflow for company: {company_url}")
        async with httpx.AsyncClient() as client:
            response = await client.post(
                WORKFLOW_TRIGGER_URL, json={"company_url": company_url}
            )
        if response.status_code == 200:
            logger.info(f"Successfully triggered workflow for {company_url}")
        else:
            logger.error(
                f"Failed to trigger workflow. Status code: {response.status_code}, Response: {response.text}"
            )
    except Exception as e:
        logger.error(f"Error triggering workflow: {e}")
