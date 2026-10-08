import { type Translations } from "@/core/i18n/locales/types";

import { isActiveBackgroundTask, type BackgroundTask } from "./types";

export type BackgroundTaskPresentation = {
  kind: BackgroundTask["status"] | "uncertain";
  label: string;
  active: boolean;
  cancelling: boolean;
  requiresReconciliation: boolean;
  trackingDegraded: boolean;
};

export function backgroundTaskPresentation(
  task: BackgroundTask,
  labels: Pick<Translations["backgroundTasks"], "cancelling" | "status">,
  optimisticCancel = false,
): BackgroundTaskPresentation {
  const active = isActiveBackgroundTask(task);
  const cancelling = active && (task.cancel_requested || optimisticCancel);
  const requiresReconciliation =
    task.status === "input_required" && task.execution_uncertain === true;
  const kind = requiresReconciliation ? "uncertain" : task.status;
  const statusLabel =
    kind === "input_required"
      ? labels.status.inputRequired
      : labels.status[kind];

  return {
    kind,
    label: cancelling ? labels.cancelling : statusLabel,
    active,
    cancelling,
    requiresReconciliation,
    trackingDegraded: task.tracking_degraded,
  };
}
