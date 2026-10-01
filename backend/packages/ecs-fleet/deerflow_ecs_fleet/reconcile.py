"""Reconcile orphan staged submissions independently of user task polling."""

import asyncio
import logging

from sqlalchemy import and_, case, func, or_, select

from .persistence.models import JobRow

logger = logging.getLogger(__name__)


class JobReconciler:
    def __init__(self, jobs, attempts):
        self.jobs = jobs
        self.attempts = attempts
        self._task = None
        self._stop = asyncio.Event()

    def start(self):
        if self._task is not None:
            raise RuntimeError("Fleet reconciler already started")
        self._task = asyncio.create_task(self.run(), name="fleet-staged-reconciler")

    async def run(self):
        while not self._stop.is_set():
            try:
                await self.attempts.expire_pending()
                async with self.jobs.sf() as session:
                    ids = (
                        (
                            await session.execute(
                                select(JobRow.id)
                                .where(or_(JobRow.state == "staged", and_(JobRow.state == "queued", JobRow.queue_deadline <= func.clock_timestamp())))
                                .order_by(case((JobRow.state == "staged", JobRow.staged_deadline), else_=JobRow.queue_deadline), JobRow.id)
                                .limit(100)
                            )
                        )
                        .scalars()
                        .all()
                    )
                for job_id in ids:
                    await self.jobs.reconcile(job_id)
            except Exception:
                logger.exception("Fleet staged reconciliation failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=1)
            except TimeoutError:
                pass

    async def stop(self):
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None
