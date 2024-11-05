# global_vars.py

import asyncio
from typing import Dict, Set, Optional

job_statuses: Dict[str, str] = {}
"""
Dictionary to store the status of jobs.
Keys are job IDs, values are status strings.
"""

active_jobs: Set[str] = set()
"""
Set of active job IDs.
"""

shutdown_event: asyncio.Event = asyncio.Event()
"""
Event to signal application shutdown.
"""

shutdown_timer_task: Optional[asyncio.Task] = None
"""
Task for the shutdown timer.
"""
