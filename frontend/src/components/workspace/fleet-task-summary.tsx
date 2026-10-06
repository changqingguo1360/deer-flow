"use client";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { fleetRunUrl } from "@/core/fleet/api";
import { fleetTaskPresentation } from "@/core/fleet/presentation";
import type { FleetTask } from "@/core/fleet/types";
import { useI18n } from "@/core/i18n/hooks";

export function FleetTaskSummary({
  task,
  threadId,
  onCancel,
  isCancelling = false,
}: {
  task: FleetTask;
  threadId: string;
  onCancel: (runId: string) => void;
  isCancelling?: boolean;
}) {
  const { t } = useI18n();
  const presentation = fleetTaskPresentation(task);
  return (
    <article
      className="border-border bg-card rounded-xl border p-3 shadow-xs"
      data-testid={`fleet-task-${task.task_id}`}
    >
      <div className="flex items-start justify-between gap-3">
        <p className="text-sm font-medium">
          {t.fleetTasks.title} · {task.profile}
        </p>
        <Badge variant="outline">
          {t.fleetTasks.status[presentation.label]}
        </Badge>
      </div>
      <p className="text-muted-foreground mt-2 text-xs">
        {t.fleetTasks.location[task.location]}
      </p>
      <a
        className="text-muted-foreground mt-2 block truncate text-xs underline"
        href={fleetRunUrl(threadId, task.current_run_id)}
        target="_blank"
        rel="noreferrer"
      >
        {t.fleetTasks.run}: {task.current_run_id}
      </a>
      {task.stop_state === "unconfirmed" && (
        <p className="mt-2 text-xs text-amber-700 dark:text-amber-300">
          {t.fleetTasks.status.stopUnconfirmed}
        </p>
      )}
      {presentation.canCancel && (
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="mt-3"
          disabled={isCancelling}
          onClick={() => onCancel(task.current_run_id)}
        >
          {isCancelling
            ? t.backgroundTasks.cancelling
            : t.backgroundTasks.cancel}
        </Button>
      )}
    </article>
  );
}
