"""Private pointers inserted on the original event transaction."""

from sqlalchemy import select

from .models import EventOutboxRow, StreamSealRow


class EventOutbox:
    async def insert(self, session, *, identity, event_id, seq):
        seal = await session.scalar(select(StreamSealRow).where(StreamSealRow.run_id == identity.run_id, StreamSealRow.attempt_id == identity.attempt_id))
        if seal is not None:
            raise RuntimeError("Remote stream has already been sealed")
        session.add(
            EventOutboxRow(
                run_id=identity.run_id,
                attempt_id=identity.attempt_id,
                generation=identity.generation,
                user_id=identity.user_id,
                thread_id=identity.thread_id,
                launch_spec_digest=identity.launch_spec_digest,
                event_id=event_id,
                seq=seq,
            )
        )
        await session.flush()
