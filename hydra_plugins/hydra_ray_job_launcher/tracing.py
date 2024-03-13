"""Functions for tracing outputs of Ray jobs.

The tailing of logs is adapted from Ray's CLI tooling.
Ray is distributed under the Apache license: https://github.com/ray-project/ray/blob/master/LICENSE
"""

import asyncio
import logging

from ray.job_submission import JobStatus, JobSubmissionClient

logger = logging.getLogger(__name__)


def _log_job_status(client: JobSubmissionClient, job_id: str):
    """Log status of Ray job"""
    info = client.get_job_info(job_id)
    if info.status == JobStatus.SUCCEEDED:
        logger.info(f"Job '{job_id}' succeeded")
    elif info.status == JobStatus.STOPPED:
        logger.warning(f"Job '{job_id}' was stopped")
    elif info.status == JobStatus.FAILED:
        logger.error(f"Job '{job_id}' failed")
        if info.message is not None:
            logger.error(f'Status message: {info.message}')
    else:
        logger.info(f"Status for job '{job_id}': {info.status}")
        if info.message is not None:
            logger.info(f'Status message: {info.message}')


async def _async_tail_logs(client: JobSubmissionClient, job_id: str):
    """Tail Ray job logs asynchronously."""
    async for lines in client.tail_job_logs(job_id):
        print(lines, end='')

    _log_job_status(client, job_id)


def tail_job_logs(client, job_id):
    """Tail Ray job logs in an asyncio event loop."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_async_tail_logs(client, job_id))
