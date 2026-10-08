"use client";

import { useRef } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { fleetRunUrl } from "@/core/fleet/api";
import { fleetTaskPresentation } from "@/core/fleet/presentation";
import type {
  FleetGoalAction,
  FleetGoalOperation,
  FleetTask,
} from "@/core/fleet/types";
import { useI18n } from "@/core/i18n/hooks";

export function FleetTaskSummary({
  task,
  threadId,
  onOperation,
  isPending = false,
  pendingAction,
}: {
  task: FleetTask;
  threadId: string;
  onOperation: (action: FleetGoalAction, operation: FleetGoalOperation) => void;
  isPending?: boolean;
  pendingAction?: FleetGoalAction;
}) {
  const { t } = useI18n();
  const presentation = fleetTaskPresentation(task);
  const identities = useRef<
    Partial<Record<FleetGoalAction, { scope: string; key: string }>>
  >({});
  const operate = (action: FleetGoalAction) => {
    if (isPending) return;
    const scope = `${threadId}:${task.task_id}:${task.generation}`;
    if (identities.current[action]?.scope !== scope) {
      identities.current[action] = { scope, key: crypto.randomUUID() };
    }
    onOperation(action, {
      taskId: task.task_id,
      expectedGeneration: task.generation,
      idempotencyKey: identities.current[action].key,
    });
  };
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
      <p className="text-muted-foreground mt-2 text-xs">
        {t.fleetTasks.generation}: {task.generation}
      </p>
      <a
        className="text-muted-foreground mt-2 block truncate text-xs underline"
        href={fleetRunUrl(threadId, task.current_run_id)}
        target="_blank"
        rel="noreferrer"
      >
        {t.fleetTasks.run}: {task.current_run_id}
      </a>
      {(task.jobs ?? []).slice(0, 20).map((job) => (
        <div key={job.job_id} className="mt-2 text-xs break-words">
          <p>
            {t.fleetTasks.job}: {job.job_id} · {job.state} · {job.link_mode}
          </p>
          <p>
            {t.fleetTasks.job} · {t.fleetTasks.generation}: {job.generation}
            {job.generation !== task.generation
              ? ` · ${t.fleetTasks.history}`
              : ""}
          </p>
          <a
            href={fleetRunUrl(threadId, job.parent_run_id)}
            target="_blank"
            rel="noreferrer"
            className="underline"
          >
            {t.fleetTasks.parentRun}: {job.parent_run_id}
          </a>
          {job.accepted_manifest_id && (
            <p>
              {t.fleetTasks.acceptedResult}: {job.accepted_manifest_id}
            </p>
          )}
        </div>
      ))}
      {(task.runs ?? [])
        .slice(0, 20)
        .filter((run) => run.run_id !== task.current_run_id)
        .map((run) => (
          <div key={run.run_id} className="mt-2 text-xs break-words">
            <a
              href={fleetRunUrl(threadId, run.run_id)}
              target="_blank"
              rel="noreferrer"
              className="underline"
            >
              {t.fleetTasks.run}: {run.run_id}
            </a>
            <p>
              {t.fleetTasks.generation}: {run.generation} · {run.run_status}
              {run.generation !== task.generation
                ? ` · ${t.fleetTasks.history}`
                : ""}
            </p>
          </div>
        ))}
      {(task.jobs_truncated === true || task.runs_truncated === true) && (
        <p className="text-muted-foreground mt-2 text-xs">
          {t.fleetTasks.relatedTruncated}
        </p>
      )}
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
          disabled={isPending}
          onClick={() => operate("cancel")}
        >
          {isPending && pendingAction === "cancel"
            ? t.backgroundTasks.cancelling
            : t.fleetTasks.cancelGoal}
        </Button>
      )}
      {presentation.canResume && (
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="mt-3 ml-2"
          disabled={isPending}
          onClick={() => operate("resume")}
        >
          {isPending && pendingAction === "resume"
            ? t.fleetTasks.resuming
            : t.fleetTasks.resumeGoal}
        </Button>
      )}
    </article>
  );
}
