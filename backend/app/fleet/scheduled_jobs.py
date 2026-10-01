"""Server-owned schedule identity, separate from untrusted request metadata."""


def apply_scheduled_job_context(config, *, schedule_id, context_mode="reuse_thread"):
    for section in ("context", "configurable"):
        value = config.get(section)
        if isinstance(value, dict):
            value.pop("scheduled_task_id", None)
            value.pop("scheduled_context_mode", None)
    if schedule_id is not None:
        if not isinstance(schedule_id, str) or not schedule_id or len(schedule_id) > 128:
            raise ValueError("Invalid trusted schedule identity")
        if context_mode not in {"reuse_thread", "fresh_thread_per_run"}:
            raise ValueError("Invalid trusted schedule context mode")
        config.setdefault("context", {}).update(scheduled_task_id=schedule_id, scheduled_context_mode=context_mode)
