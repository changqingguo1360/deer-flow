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
  const awaited = (task.jobs ?? []).filter(
    (job) =>
      job.link_mode === "awaited" &&
      job.generation === task.generation &&
      job.parent_run_id === task.current_run_id,
  );
  const settledResults =
    !task.jobs_truncated &&
    awaited.length > 0 &&
    awaited.every(
      (job) =>
        (job.state === "succeeded" && Boolean(job.accepted_manifest_id)) ||
        job.state === "failed" ||
        job.state === "cancelled",
    );
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
      ["queued", "running", "waiting_jobs"].includes(task.state),
    canResume:
      task.state === "waiting_jobs" &&
      !task.cancel_requested &&
      !needsConfirmation &&
      task.stop_state === "confirmed" &&
      !task.resources_held &&
      settledResults,
  } as const;
}
