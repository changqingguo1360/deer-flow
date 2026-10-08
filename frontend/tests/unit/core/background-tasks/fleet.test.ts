import { describe, expect, it } from "@rstest/core";

import { backgroundTaskPresentation } from "@/core/background-tasks/fleet";
import { type BackgroundTask } from "@/core/background-tasks/types";
import { zhCN } from "@/core/i18n/locales/zh-CN";

const task: BackgroundTask = {
  task_id: "tracking-1",
  task_name: "Daily report",
  status: "input_required",
  created_at: "2026-10-02T00:00:00Z",
  updated_at: "2026-10-02T00:01:00Z",
  error: null,
  tracking_degraded: false,
  cancel_requested: false,
  execution_uncertain: true,
};

function present(value: BackgroundTask, optimisticCancel = false) {
  return backgroundTaskPresentation(
    value,
    zhCN.backgroundTasks,
    optimisticCancel,
  );
}

describe("Fleet task presentation", () => {
  it("shows uncertain execution as needing confirmation and keeps it active", () => {
    expect(present(task)).toMatchObject({
      kind: "uncertain",
      label: "需要确认",
      active: true,
      cancelling: false,
      requiresReconciliation: true,
    });
  });

  it("preserves ordinary MCP input requests", () => {
    expect(present({ ...task, execution_uncertain: false })).toMatchObject({
      kind: "input_required",
      label: "需要输入",
      requiresReconciliation: false,
    });
  });

  it("keeps older API payloads compatible without inferring uncertainty", () => {
    const legacyTask = { ...task };
    delete legacyTask.execution_uncertain;
    expect(present(legacyTask).label).toBe("需要输入");
  });

  it.each([false, true])(
    "keeps uncertain cancellation pending with degraded=%s",
    (degraded) => {
      expect(
        present({
          ...task,
          cancel_requested: true,
          tracking_degraded: degraded,
        }),
      ).toMatchObject({
        kind: "uncertain",
        label: "正在取消…",
        active: true,
        cancelling: true,
        requiresReconciliation: true,
        trackingDegraded: degraded,
      });
    },
  );

  it("shows optimistic cancellation without claiming stopped execution", () => {
    expect(
      present({ ...task, status: "working", execution_uncertain: false }, true),
    ).toMatchObject({
      kind: "working",
      label: "正在取消…",
      active: true,
      cancelling: true,
    });
  });

  it.each(["completed", "failed", "cancelled"] as const)(
    "keeps terminal %s honest despite stale request flags",
    (status) => {
      expect(
        present({ ...task, status, cancel_requested: true }, true),
      ).toMatchObject({
        kind: status,
        label: zhCN.backgroundTasks.status[status],
        active: false,
        cancelling: false,
        requiresReconciliation: false,
      });
    },
  );

  it("does not turn a working task into uncertain input from a stray flag", () => {
    expect(present({ ...task, status: "working" })).toMatchObject({
      kind: "working",
      label: "进行中",
      requiresReconciliation: false,
    });
  });
});
