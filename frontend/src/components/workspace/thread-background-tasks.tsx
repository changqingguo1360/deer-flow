"use client";

import {
  ChevronDownIcon,
  ChevronUpIcon,
  CircleCheckIcon,
  CircleStopIcon,
  Clock3Icon,
  ListChecksIcon,
  LoaderCircleIcon,
  MessageCircleQuestionIcon,
  TriangleAlertIcon,
} from "lucide-react";
import { useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { FleetTaskSummary } from "@/components/workspace/fleet-task-summary";
import {
  backgroundTaskPresentation,
  isActiveBackgroundTask,
  type BackgroundTask,
  type BackgroundTaskDetail,
  type BackgroundTaskPresentation,
  useBackgroundTask,
  useBackgroundTasks,
  useCancelBackgroundTask,
} from "@/core/background-tasks";
import { useMcpTasksEnabled } from "@/core/features";
import { useFleetTasks, useCancelFleetRun } from "@/core/fleet/hooks";
import { isUnsettledFleetTask } from "@/core/fleet/presentation";
import { useI18n } from "@/core/i18n/hooks";
import { formatTimeAgo } from "@/core/utils/datetime";
import { cn } from "@/lib/utils";

export function ThreadBackgroundTasks({ threadId }: { threadId: string }) {
  const { t } = useI18n();
  const { enabled: mcpTasksEnabled } = useMcpTasksEnabled();
  const tasksQuery = useBackgroundTasks(threadId, {
    enabled: mcpTasksEnabled,
  });
  const cancelTask = useCancelBackgroundTask(threadId);
  const fleetQuery = useFleetTasks(threadId);
  const cancelFleet = useCancelFleetRun(threadId);
  const fleetTasks = fleetQuery.data ?? [];
  const fleetActive = fleetTasks.filter(isUnsettledFleetTask).length;
  const tasks = mcpTasksEnabled ? (tasksQuery.data ?? []) : [];
  const activeTasks = tasks.filter(isActiveBackgroundTask);
  const recentTasks = tasks.filter((task) => !isActiveBackgroundTask(task));

  if (!mcpTasksEnabled && fleetTasks.length === 0 && !fleetQuery.isError) {
    return null;
  }

  return (
    <Sheet>
      <SheetTrigger asChild>
        <Button
          type="button"
          variant="outline"
          size="sm"
          aria-label={t.backgroundTasks.label}
          data-testid="background-tasks-trigger"
          className="relative"
        >
          <ListChecksIcon />
          <span className="hidden lg:inline">{t.backgroundTasks.label}</span>
          {activeTasks.length + fleetActive > 0 && (
            <span className="bg-primary text-primary-foreground grid size-4 place-items-center rounded-full text-[10px] font-semibold">
              {activeTasks.length + fleetActive > 9
                ? "9+"
                : activeTasks.length + fleetActive}
            </span>
          )}
        </Button>
      </SheetTrigger>
      <SheetContent className="w-[min(92vw,420px)] gap-0 p-0 sm:max-w-[420px]">
        <SheetHeader className="border-border border-b px-5 py-4">
          <SheetTitle className="flex items-center gap-2">
            <ListChecksIcon className="size-4" />
            {t.backgroundTasks.title}
          </SheetTitle>
          <SheetDescription>{t.backgroundTasks.description}</SheetDescription>
        </SheetHeader>

        <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4">
          {fleetQuery.isError && (
            <div
              role="alert"
              className="border-destructive/30 mb-4 rounded-xl border p-4 text-sm"
            >
              <p>{t.fleetTasks.loadFailed}</p>
              <Button
                type="button"
                size="sm"
                variant="outline"
                onClick={() => void fleetQuery.refetch()}
              >
                {t.backgroundTasks.retry}
              </Button>
            </div>
          )}
          {fleetTasks.length > 0 && (
            <section className="mb-5 space-y-2" aria-label={t.fleetTasks.title}>
              {fleetTasks.map((task) => (
                <FleetTaskSummary
                  key={task.task_id}
                  task={task}
                  threadId={threadId}
                  onCancel={(runId) => cancelFleet.mutate(runId)}
                  isCancelling={
                    cancelFleet.isPending &&
                    cancelFleet.variables === task.current_run_id
                  }
                />
              ))}
            </section>
          )}
          {mcpTasksEnabled &&
            (tasksQuery.isLoading ? (
              <div
                role="status"
                className="text-muted-foreground flex items-center justify-center gap-2 py-12 text-sm"
              >
                <LoaderCircleIcon className="size-4 animate-spin" />
                {t.common.loading}
              </div>
            ) : tasksQuery.isError ? (
              <div className="border-destructive/30 bg-destructive/5 rounded-xl border p-4 text-sm">
                <p className="text-destructive font-medium">
                  {t.backgroundTasks.loadFailed}
                </p>
                <p className="text-muted-foreground mt-1 text-xs">
                  {tasksQuery.error.message}
                </p>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  className="mt-3"
                  onClick={() => void tasksQuery.refetch()}
                >
                  {t.backgroundTasks.retry}
                </Button>
              </div>
            ) : tasks.length === 0 &&
              fleetTasks.length > 0 ? null : tasks.length === 0 ? (
              <div className="text-muted-foreground flex flex-col items-center px-6 py-14 text-center">
                <ListChecksIcon className="mb-3 size-8 opacity-40" />
                <p className="text-foreground text-sm font-medium">
                  {t.backgroundTasks.empty}
                </p>
                <p className="mt-1 text-xs">{t.backgroundTasks.emptyHint}</p>
              </div>
            ) : (
              <div className="space-y-5">
                {activeTasks.length > 0 && (
                  <TaskSection
                    threadId={threadId}
                    title={t.backgroundTasks.active}
                    tasks={activeTasks}
                    cancellingTaskId={
                      cancelTask.isPending ? cancelTask.variables : undefined
                    }
                    onCancel={(taskId) => cancelTask.mutate(taskId)}
                  />
                )}
                {recentTasks.length > 0 && (
                  <TaskSection
                    threadId={threadId}
                    title={t.backgroundTasks.recent}
                    tasks={recentTasks}
                  />
                )}
              </div>
            ))}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function TaskSection({
  threadId,
  title,
  tasks,
  cancellingTaskId,
  onCancel,
}: {
  threadId: string;
  title: string;
  tasks: BackgroundTask[];
  cancellingTaskId?: string;
  onCancel?: (taskId: string) => void;
}) {
  return (
    <section>
      <h3 className="text-muted-foreground mb-2 px-1 text-xs font-medium tracking-wide uppercase">
        {title}
      </h3>
      <div className="space-y-2">
        {tasks.map((task) => (
          <BackgroundTaskCard
            key={task.task_id}
            threadId={threadId}
            task={task}
            isCancelling={cancellingTaskId === task.task_id}
            onCancel={onCancel}
          />
        ))}
      </div>
    </section>
  );
}

function BackgroundTaskCard({
  threadId,
  task,
  isCancelling,
  onCancel,
}: {
  threadId: string;
  task: BackgroundTask;
  isCancelling: boolean;
  onCancel?: (taskId: string) => void;
}) {
  const { t } = useI18n();
  const [detailsOpen, setDetailsOpen] = useState(false);
  const detailsQuery = useBackgroundTask(threadId, task.task_id, {
    enabled: detailsOpen,
  });
  const presentation = backgroundTaskPresentation(
    task,
    t.backgroundTasks,
    isCancelling,
  );
  const { active, cancelling } = presentation;
  const status = taskStatusPresentation(presentation.kind);
  const canShowDetails =
    task.cancel_requested ||
    (task.status !== "submitted" &&
      (task.status !== "working" || task.tracking_degraded));

  return (
    <article
      className="border-border bg-card rounded-xl border p-3 shadow-xs"
      data-testid={`background-task-${task.task_id}`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="truncate text-sm font-medium" title={task.task_name}>
            {task.task_name}
          </p>
          <div className="text-muted-foreground mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px]">
            <span className="flex items-center gap-1">
              <Clock3Icon className="size-3" />
              {t.backgroundTasks.created(formatTimeAgo(task.created_at))}
            </span>
            <span>
              {t.backgroundTasks.updated(formatTimeAgo(task.updated_at))}
            </span>
          </div>
        </div>
        <Badge variant="outline" className={cn("shrink-0", status.className)}>
          <status.Icon
            className={cn("size-3", status.spinning && "animate-spin")}
          />
          {presentation.label}
        </Badge>
      </div>

      {task.tracking_degraded && (
        <p className="mt-2 flex items-center gap-1.5 text-xs text-amber-700 dark:text-amber-300">
          <TriangleAlertIcon className="size-3.5 shrink-0" />
          {t.backgroundTasks.trackingDegraded}
        </p>
      )}
      {task.error && (
        <p className="bg-destructive/5 text-destructive mt-2 rounded-md px-2 py-1.5 text-xs break-words">
          {task.error}
        </p>
      )}
      {(canShowDetails || (active && onCancel)) && (
        <div className="mt-3 flex items-center justify-between gap-2">
          {canShowDetails ? (
            <Button
              type="button"
              size="sm"
              variant="ghost"
              aria-expanded={detailsOpen}
              onClick={() => setDetailsOpen((open) => !open)}
            >
              {detailsOpen
                ? t.backgroundTasks.hideDetails
                : t.backgroundTasks.viewDetails}
              {detailsOpen ? (
                <ChevronUpIcon className="size-3.5" />
              ) : (
                <ChevronDownIcon className="size-3.5" />
              )}
            </Button>
          ) : (
            <span />
          )}
          {active && onCancel && (
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={cancelling}
              onClick={() => onCancel(task.task_id)}
            >
              {isCancelling && (
                <LoaderCircleIcon className="size-3.5 animate-spin" />
              )}
              {cancelling
                ? t.backgroundTasks.cancelling
                : t.backgroundTasks.cancel}
            </Button>
          )}
        </div>
      )}
      {detailsOpen && (
        <BackgroundTaskDetails
          task={detailsQuery.data}
          isLoading={detailsQuery.isLoading}
          error={detailsQuery.error}
          onRetry={() => void detailsQuery.refetch()}
        />
      )}
    </article>
  );
}

function BackgroundTaskDetails({
  task,
  isLoading,
  error,
  onRetry,
}: {
  task: BackgroundTaskDetail | undefined;
  isLoading: boolean;
  error: Error | null;
  onRetry: () => void;
}) {
  const { t } = useI18n();

  if (isLoading) {
    return (
      <div
        role="status"
        className="text-muted-foreground mt-3 flex items-center gap-2 border-t pt-3 text-xs"
      >
        <LoaderCircleIcon className="size-3.5 animate-spin" />
        {t.common.loading}
      </div>
    );
  }

  if (error) {
    return (
      <div className="border-destructive/30 mt-3 border-t pt-3 text-xs">
        <p className="text-destructive">{t.backgroundTasks.detailsFailed}</p>
        <p className="text-muted-foreground mt-1 break-words">
          {error.message}
        </p>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="mt-2"
          onClick={onRetry}
        >
          {t.backgroundTasks.retry}
        </Button>
      </div>
    );
  }

  if (!task) return null;
  const { requiresReconciliation } = backgroundTaskPresentation(
    task,
    t.backgroundTasks,
  );

  return (
    <div className="border-border mt-3 space-y-3 border-t pt-3">
      {task.last_cancel_error && (
        <div className="flex gap-2 rounded-md bg-amber-500/10 px-2 py-2 text-xs text-amber-700 dark:text-amber-300">
          <TriangleAlertIcon className="mt-0.5 size-3.5 shrink-0" />
          <div className="min-w-0">
            <p className="font-medium">
              {t.backgroundTasks.cancellationRetrying(
                task.cancel_attempt_count,
              )}
            </p>
            <p className="mt-1 break-words">{task.last_cancel_error}</p>
          </div>
        </div>
      )}
      {task.notification_error && (
        <div className="flex gap-2 rounded-md bg-amber-500/10 px-2 py-2 text-xs text-amber-700 dark:text-amber-300">
          <TriangleAlertIcon className="mt-0.5 size-3.5 shrink-0" />
          <div className="min-w-0">
            <p className="font-medium">
              {task.notification_status === "dead_letter"
                ? t.backgroundTasks.notificationStopped
                : t.backgroundTasks.notificationRetrying(
                    task.notification_attempt_count,
                  )}
            </p>
            <p className="mt-1 break-words">{task.notification_error}</p>
          </div>
        </div>
      )}
      <TaskDetailField
        label={t.backgroundTasks.result}
        value={task.result_preview ?? task.result}
      />
      <TaskDetailField
        label={t.backgroundTasks.resultArtifact}
        value={task.result_artifact}
      />
      <TaskDetailField
        label={t.backgroundTasks.lastPollError}
        value={task.last_poll_error}
      />
      {task.input_required != null &&
        (requiresReconciliation ? (
          <p className="text-xs text-amber-700 dark:text-amber-300">
            {t.backgroundTasks.reconciliationRequired}
          </p>
        ) : (
          <div>
            <TaskDetailField
              label={t.backgroundTasks.inputRequired}
              value={task.input_required}
            />
            <p className="text-muted-foreground mt-1 text-[11px]">
              {t.backgroundTasks.inputUnavailable}
            </p>
          </div>
        ))}
    </div>
  );
}

function TaskDetailField({ label, value }: { label: string; value: unknown }) {
  const formatted = formatTaskDetailValue(value);
  if (formatted === null) return null;

  return (
    <div>
      <p className="text-muted-foreground text-[11px] font-medium tracking-wide uppercase">
        {label}
      </p>
      <pre className="bg-muted/60 mt-1 max-h-48 overflow-auto rounded-md px-2 py-1.5 font-sans text-xs break-words whitespace-pre-wrap">
        {formatted}
      </pre>
    </div>
  );
}

function formatTaskDetailValue(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  try {
    return JSON.stringify(value, null, 2) ?? null;
  } catch {
    return null;
  }
}

function taskStatusPresentation(status: BackgroundTaskPresentation["kind"]) {
  switch (status) {
    case "submitted":
      return {
        Icon: Clock3Icon,
        className: "text-blue-700 dark:text-blue-300",
        spinning: false,
      };
    case "working":
      return {
        Icon: LoaderCircleIcon,
        className: "text-blue-700 dark:text-blue-300",
        spinning: true,
      };
    case "input_required":
      return {
        Icon: MessageCircleQuestionIcon,
        className: "text-amber-700 dark:text-amber-300",
        spinning: false,
      };
    case "uncertain":
      return {
        Icon: TriangleAlertIcon,
        className: "text-amber-700 dark:text-amber-300",
        spinning: false,
      };
    case "completed":
      return {
        Icon: CircleCheckIcon,
        className: "text-emerald-700 dark:text-emerald-300",
        spinning: false,
      };
    case "failed":
      return {
        Icon: TriangleAlertIcon,
        className: "text-destructive",
        spinning: false,
      };
    case "cancelled":
      return {
        Icon: CircleStopIcon,
        className: "text-muted-foreground",
        spinning: false,
      };
  }
}
