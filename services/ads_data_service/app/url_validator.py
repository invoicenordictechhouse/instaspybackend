import requests
import re
import logging
from urllib.parse import urlparse
from typing import Tuple, Union
from fastapi import HTTPException, status

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# List of blocked protocols
BLOCKED_PROTOCOLS = ["ftp", "file", "data", "mailto", "javascript", "ws", "wss"]


def is_protocol_blocked(url: str) -> bool:
    """
    Check if the URL protocol is blocked.

    Args:
        url (str): The URL to check.

    Returns:
        bool: True if the protocol is blocked, False otherwise.
    """
    parsed_url = urlparse(url)
    blocked = parsed_url.scheme in BLOCKED_PROTOCOLS
    if blocked:
        logger.warning(f"Blocked protocol detected: {parsed_url.scheme}")
    return blocked


def is_valid_url(url: str) -> bool:
    """
    Validate if the URL follows the correct format using regex.

    Args:
        url (str): The URL to validate.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    regex = re.compile(
        r"^(https?://)?"  # Optional protocol
        r"(?!-)(?!.*\.\.)"  # No starting dash or consecutive dots
        r"[a-z0-9-]+"  # Domain name
        r"(?<!-)(\.[a-z0-9-]+)*"  # Subdomains if any
        r"\.[a-z]{2,}"  # Top-level domain with at least two letters
        r"(:[0-9]{1,5})?"  # Optional port
        r"(/.*)?$"  # Optional path
    )
    valid = regex.match(url) is not None
    if not valid:
        logger.warning(f"Invalid URL format: {url}")
    return valid


def normalize_url(url: str) -> str:
    """
    Normalize the URL by adding http:// if missing and validating it.

    Args:
        url (str): The URL to normalize.

    Returns:
        str: Normalized URL.

    Raises:
        HTTPException: If the URL is invalid or contains a blocked protocol.
    """
    url = url.lower().strip()
    logger.info(f"Normalizing URL: {url}")

    if is_protocol_blocked(url):
        logger.error("Blocked protocol in URL")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid URL - The protocol is not allowed.",
        )

    # Add http:// if missing
    if not re.match(r"^https?://", url):
        url = "http://" + url

    # Validate the domain
    if not is_valid_url(url):
        logger.error("Invalid domain name in URL")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid URL - The domain name is not valid.",
        )

    return url


def get_domain(url: str) -> str:
    """
    Extract the domain from the URL, removing 'www.' if present.

    Args:
        url (str): The URL from which to extract the domain.

    Returns:
        str: The domain name without 'www.'.
    """
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.replace("www.", "")
    logger.info(f"Extracted domain: {domain}")
    return domain


def url_exists(url: str) -> Tuple[bool, str]:
    """
    Check if the URL exists by sending a request.

    Args:
        url (str): The URL to check.

    Returns:
        Tuple[bool, str]: Tuple with boolean status (True if exists) and domain or error message.

    Raises:
        HTTPException: If the URL is unreachable or causes a network error.
    """
    normalized_url = normalize_url(url)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    try:
        response = requests.get(
            normalized_url, headers=headers, allow_redirects=True, timeout=(5, 5)
        )
        if response.status_code == 200:
            domain = get_domain(response.url)
            logger.info(f"URL {normalized_url} is reachable.")
            return True, domain
        else:
            logger.warning(
                f"URL {normalized_url} returned status code {response.status_code}"
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="URL does not exist."
            )
    except requests.RequestException as e:
        logger.error(f"Request failed for URL {url}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error: Unable to reach the URL.",
        )


def check_url(url: str) -> str:
    """
    Check the URL and return the domain or an error message.

    Args:
        url (str): The URL to check.

    Returns:
        str: Domain name if URL exists or raises HTTPException if an error occurs.

    Raises:
        HTTPException: If URL is invalid or does not exist.
    """
    exists, result = url_exists(url)
    if exists:
        logger.info(f"Domain for URL '{url}': {result}")
        return result
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=result)
