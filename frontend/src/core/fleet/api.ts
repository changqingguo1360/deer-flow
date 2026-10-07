import { throwGatewayApiError } from "@/core/api/errors";
import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

import type { FleetGoalAction, FleetGoalOperation, FleetTask } from "./types";

export function fleetRunUrl(threadId: string, runId: string) {
  return `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}/runs/${encodeURIComponent(runId)}`;
}

export async function fetchFleetTasks(threadId: string): Promise<FleetTask[]> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}/agent-tasks?limit=20`,
  );
  if (!response.ok)
    await throwGatewayApiError(response, "Failed to load remote Agent tasks");
  return response.json();
}

export class FleetGoalConflictError extends Error {
  constructor() {
    super("Remote Agent goal changed");
    this.name = "FleetGoalConflictError";
  }
}

export async function operateFleetGoal(
  threadId: string,
  action: FleetGoalAction,
  operation: FleetGoalOperation,
) {
  const response = await fetch(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}/agent-tasks/${encodeURIComponent(operation.taskId)}/${action}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        expected_generation: operation.expectedGeneration,
        idempotency_key: operation.idempotencyKey,
      }),
    },
  );
  if (response.status === 409) throw new FleetGoalConflictError();
  if (!response.ok)
    await throwGatewayApiError(response, "Remote Agent goal operation failed");
  return response.json() as Promise<{ run_id?: string }>;
}
