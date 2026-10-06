import { getAPIClient } from "@/core/api/api-client";
import { throwGatewayApiError } from "@/core/api/errors";
import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

import type { FleetTask } from "./types";

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

export async function cancelFleetRun(threadId: string, runId: string) {
  await getAPIClient().runs.cancel(threadId, runId, false, "interrupt");
}
