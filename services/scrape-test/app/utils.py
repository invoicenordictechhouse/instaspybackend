# utils.py

import re
from typing import Optional, Dict, Any


def convert_embed_to_watch_url(embed_url: str) -> Optional[str]:
    """
    Convert a YouTube embed URL to a standard watch URL.

    Args:
        embed_url (str): The YouTube embed URL.

    Returns:
        Optional[str]: The standard YouTube watch URL, or None if conversion fails.

    Example:
        >>> convert_embed_to_watch_url("https://www.youtube.com/embed/dQw4w9WgXcQ")
        'https://www.youtube.com/watch?v=dQw4w9WgXcQ'
    """
    match = re.search(r"/embed/([^?&]+)", embed_url)
    if match:
        video_id = match.group(1)
        watch_url = f"https://www.youtube.com/watch?v={video_id}"
        return watch_url
    else:
        return None


def normalize_row_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize the keys of a dictionary to lowercase.

    Args:
        row (Dict[str, Any]): The dictionary with keys to normalize.

    Returns:
        Dict[str, Any]: The dictionary with lowercase keys.

    Example:
        >>> normalize_row_keys({'Advertiser_ID': 'adv123', 'Creative_ID': 'crt456'})
        {'advertiser_id': 'adv123', 'creative_id': 'crt456'}
    """
    return {key.lower(): value for key, value in row.items()}
