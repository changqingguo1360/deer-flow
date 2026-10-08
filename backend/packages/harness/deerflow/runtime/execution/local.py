"""The existing Gateway-local execution remains the default."""

from .contracts import ExecutionPlan, RunExecutionParameters


class LocalExecutionBackend:
    def plan(self, parameters: RunExecutionParameters) -> ExecutionPlan:
        return ExecutionPlan(public_kwargs=parameters.public_kwargs)
