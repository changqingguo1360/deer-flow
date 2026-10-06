import type { FleetTask } from "./types";

const terminalTasks = new Set([
  "succeeded",
  "failed",
  "cancelled",
  "timed_out",
]);
const terminalRuns = new Set(["success", "error", "interrupted", "timeout"]);

export function isUnsettledFleetTask(task: FleetTask): boolean {
  return !terminalTasks.has(task.state) || task.resources_held;
}

export function fleetTaskPresentation(task: FleetTask) {
  const needsConfirmation =
    task.recovery_required ||
    task.state === "unknown" ||
    task.state === "recovery_required";
  const terminalHeld =
    task.resources_held &&
    (terminalTasks.has(task.state) || terminalRuns.has(task.run_status));
  const stopping =
    task.cancel_requested &&
    (task.stop_state === "unconfirmed" || task.resources_held);
  return {
    label: needsConfirmation
      ? "needsConfirmation"
      : terminalHeld
        ? "stopUnconfirmed"
        : stopping
          ? "stopping"
          : task.state,
    unsettled: isUnsettledFleetTask(task),
    canCancel:
      !task.cancel_requested &&
      !needsConfirmation &&
      (task.run_status === "pending" || task.run_status === "running"),
  } as const;
}
