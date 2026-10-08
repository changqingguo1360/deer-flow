"""Protocol-neutral long-task adapter for staged Fleet jobs."""

import hashlib

from deerflow.mcp.tasks import TaskSnapshot, TaskStatus, TaskSubmission

from .protocol import JobSpec


class FleetTaskDriver:
    def __init__(self, jobs):
        self.jobs = jobs

    async def submit(self, request):
        if request.server_name != "fleet" or request.local_task_id is None:
            raise ValueError("Fleet requires server-issued tracking identity")
        invocation = request.driver_data.get("invocation_id")
        if not isinstance(invocation, str) or not invocation:
            raise ValueError("Fleet requires persisted invocation_id")
        # Provider tool_call_id is deliberately excluded: it is not globally
        # unique and may be reused by a later model turn.
        key = hashlib.sha256(("\0".join([request.user_id, request.run_id or "", invocation])).encode()).hexdigest()
        schedule_id = request.driver_data.get("scheduled_task_id")
        slot = request.driver_data.get("job_slot")
        group = None
        if schedule_id is not None or slot is not None:
            if not isinstance(schedule_id, str) or not schedule_id or len(schedule_id) > 128:
                raise ValueError("Scheduled job requires trusted schedule identity")
            approved = self.jobs.config.scheduled_job_slots.get(slot) if isinstance(slot, str) else None
            if approved is None or request.arguments.get("profile") != approved:
                raise ValueError("Scheduled job requires an approved slot and matching profile")
            group = hashlib.sha256((schedule_id + "\0" + slot).encode()).hexdigest()
        job = await self.jobs.submit(
            user_id=request.user_id,
            thread_id=request.thread_id,
            source_run_id=request.run_id,
            tracking_task_id=request.local_task_id,
            idempotency_key=key,
            spec=JobSpec.model_validate(request.arguments),
            dedupe_group=group,
        )
        return TaskSubmission(remote_task_id=job["id"], snapshot=self.snapshot(job), driver_data={"fleet_tracking_id": job["tracking_task_id"]}, tracking_task_id=job["tracking_task_id"], reuse_existing=job.get("reused_existing", False))

    @staticmethod
    def snapshot(job):
        state = job["state"]
        if state in {"staged", "queued", "claimed"}:
            return TaskSnapshot(status=TaskStatus.SUBMITTED, poll_after_seconds=1)
        if state == "running":
            return TaskSnapshot(status=TaskStatus.WORKING, poll_after_seconds=1)
        if state == "succeeded":
            return TaskSnapshot(status=TaskStatus.COMPLETED, result={"manifest_id": job["accepted_manifest_id"]})
        if state == "failed":
            return TaskSnapshot(status=TaskStatus.FAILED, error=job["error"])
        if state == "cancelled":
            return TaskSnapshot(status=TaskStatus.CANCELLED)
        return TaskSnapshot(status=TaskStatus.INPUT_REQUIRED, input_required={"reason": "execution_unknown", "message": "Physical execution status requires operator reconciliation"})

    async def get_status(self, task):
        await self.jobs.get(task.remote_task_id, user_id=task.user_id, thread_id=task.thread_id)
        await self.jobs.reconcile(task.remote_task_id)
        return self.snapshot(await self.jobs.get(task.remote_task_id, user_id=task.user_id, thread_id=task.thread_id))

    async def cancel(self, task):
        return self.snapshot(await self.jobs.cancel(task.remote_task_id, user_id=task.user_id, thread_id=task.thread_id))
