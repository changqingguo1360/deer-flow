"""Committed delivery hints; execution and replay authority remain in the host."""

import asyncio
import logging

from sqlalchemy import func, text, update

from .persistence.models import EventOutboxRow

logger = logging.getLogger(__name__)

_COMMITTED_HINT = """
local prior = redis.call('GET', KEYS[2])
if prior and (#prior > #ARGV[1] or (#prior == #ARGV[1] and prior >= ARGV[1])) then return 0 end
redis.call('XADD', KEYS[1], 'MAXLEN', '=', ARGV[4], ARGV[1] .. '-0',
           'run_id', ARGV[2], 'attempt_id', ARGV[3], 'seq', ARGV[1])
redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[5])
redis.call('EXPIRE', KEYS[1], ARGV[5])
return 1
"""


class CommittedEventPublisher:
    def __init__(self, *, session_factory, candidate_pointers, load_committed, redis_client, key_prefix, recover_seals, poll_interval=0.25):
        self.sf = session_factory
        self.candidate_pointers = candidate_pointers
        self.load_committed = load_committed
        self.redis = redis_client
        self.key_prefix = key_prefix.rstrip(":")
        self.recover_seals = recover_seals
        self.poll_interval = max(0.25, poll_interval)
        self._task = None
        self._unavailable = False

    async def start(self):
        if self._task is not None:
            raise RuntimeError("Committed publisher already started")
        self._task = asyncio.create_task(self._run(), name="fleet-committed-event-publisher")

    async def stop(self):
        if self._task is None:
            return
        task = self._task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            if task.done():
                self._task = None

    async def _run(self):
        backoff = self.poll_interval
        while True:
            try:
                await self.recover_seals()
                delivered = await self.publish_once()
                if self._unavailable:
                    logger.info("Fleet committed transport available")
                    self._unavailable = False
                backoff = self.poll_interval
            except asyncio.CancelledError:
                raise
            except Exception:
                if not self._unavailable:
                    logger.warning("Fleet committed transport unavailable; durable replay remains available", exc_info=True)
                    self._unavailable = True
                delivered = False
                backoff = min(5.0, max(self.poll_interval, backoff * 2))
            await asyncio.sleep(self.poll_interval if delivered else backoff)

    async def publish_once(self):
        if self.redis is None:
            return False
        # A bounded delivery transaction holds only its private advisory lock.
        # Never acquire execution/lease locks across Redis I/O.
        async with asyncio.timeout(3):
            async with self.sf.begin() as session:
                candidates = await self.candidate_pointers(session, limit=64)
                for pointer in candidates:
                    await session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"), {"key": "fleet:delivery:" + pointer.run_id})
                    heads = await self.candidate_pointers(session, limit=1, run_id=pointer.run_id)
                    if not heads:
                        continue
                    current = heads[0]
                    event = await self.load_committed(current)
                    if event is None:
                        continue  # Deleted/stale pointers are never ACKed as Redis success.
                    key = f"{self.key_prefix}:{current.run_id}:{current.attempt_id}"
                    await self.redis.eval(_COMMITTED_HINT, 2, key, key + ":highwater", str(current.seq), current.run_id, current.attempt_id, 256, 86400)
                    await session.execute(update(EventOutboxRow).where(EventOutboxRow.event_id == current.event_id, EventOutboxRow.published_at.is_(None)).values(published_at=func.clock_timestamp()))
                    return True
                return False
