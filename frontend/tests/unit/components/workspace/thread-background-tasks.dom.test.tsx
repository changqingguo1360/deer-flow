import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

import { ThreadBackgroundTasks } from "@/components/workspace/thread-background-tasks";
import { type BackgroundTaskDetail } from "@/core/background-tasks/types";
import { zhCN } from "@/core/i18n/locales/zh-CN";

const state = rs.hoisted(() => ({
  tasks: [] as BackgroundTaskDetail[],
  detail: undefined as BackgroundTaskDetail | undefined,
  cancel: rs.fn(),
}));

rs.mock("@/core/fleet/hooks", () => ({
  useFleetTasks: () => ({ data: [], isError: false }),
  useCancelFleetRun: () => ({ mutate: rs.fn(), isPending: false }),
}));

rs.mock("@/core/features", () => ({
  useMcpTasksEnabled: () => ({ enabled: true }),
}));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: zhCN }),
}));
rs.mock("@/core/background-tasks/hooks", () => ({
  useBackgroundTasks: () => ({
    data: state.tasks,
    isLoading: false,
    isError: false,
  }),
  useBackgroundTask: () => ({
    data: state.detail,
    isLoading: false,
    error: null,
  }),
  useCancelBackgroundTask: () => ({
    isPending: false,
    variables: undefined,
    mutate: state.cancel,
  }),
}));

const UNCERTAIN: BackgroundTaskDetail = {
  task_id: "tracking-1",
  task_name: "日报计算",
  status: "input_required",
  created_at: "2026-10-02T00:00:00Z",
  updated_at: "2026-10-02T00:01:00Z",
  error: null,
  tracking_degraded: true,
  cancel_requested: false,
  execution_uncertain: true,
  last_polled_at: null,
  last_poll_error: null,
  last_cancel_error: null,
  cancel_attempt_count: 0,
  notification_status: "pending",
  notification_error: null,
  notification_attempt_count: 0,
  result: null,
  result_preview: null,
  result_truncated: false,
  result_artifact: null,
  input_required: {
    reason: "execution_unknown",
    message: "Physical execution status requires operator reconciliation",
  },
};

function openTasks(task: BackgroundTaskDetail) {
  state.tasks = [task];
  state.detail = task;
  const view = render(<ThreadBackgroundTasks threadId="thread-1" />);
  fireEvent.click(screen.getByRole("button", { name: "后台任务" }));
  return view;
}

afterEach(() => {
  cleanup();
  state.tasks = [];
  state.detail = undefined;
  state.cancel.mockReset();
});

describe("Fleet background task cards", () => {
  it("shows confirmation and degraded tracking without claiming an input answer can resolve it", async () => {
    openTasks(UNCERTAIN);
    expect(await screen.findByText("需要确认")).toBeDefined();
    expect(
      screen.getByText(zhCN.backgroundTasks.trackingDegraded),
    ).toBeDefined();
    fireEvent.click(screen.getByRole("button", { name: "查看详情" }));
    expect(
      await screen.findByText("执行结果仍无法确认，请联系管理员核对。"),
    ).toBeDefined();
    expect(
      screen.queryByText(zhCN.backgroundTasks.inputUnavailable),
    ).toBeNull();
    expect(screen.queryByText("已取消")).toBeNull();
  });

  it("requests cancellation with the public task ID and stays pending until confirmation", async () => {
    const view = openTasks(UNCERTAIN);
    fireEvent.click(await screen.findByRole("button", { name: "取消任务" }));
    expect(state.cancel).toHaveBeenCalledWith("tracking-1");
    state.tasks = [{ ...UNCERTAIN, cancel_requested: true }];
    state.detail = state.tasks[0];
    view.rerender(<ThreadBackgroundTasks threadId="thread-1" />);
    expect(screen.getByRole("button", { name: "正在取消…" })).toHaveProperty(
      "disabled",
      true,
    );
    expect(
      screen.getByText(zhCN.backgroundTasks.trackingDegraded),
    ).toBeDefined();
    expect(screen.queryByText("已取消")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "查看详情" }));
    expect(
      await screen.findByText("执行结果仍无法确认，请联系管理员核对。"),
    ).toBeDefined();
  });

  it("preserves ordinary MCP input requests", async () => {
    openTasks({
      ...UNCERTAIN,
      execution_uncertain: false,
      input_required: { prompt: "是否批准预算？" },
    });
    expect(await screen.findByText("需要输入")).toBeDefined();
    fireEvent.click(screen.getByRole("button", { name: "查看详情" }));
    expect(
      await screen.findByText(zhCN.backgroundTasks.inputUnavailable),
    ).toBeDefined();
    expect(screen.getByText(/是否批准预算/)).toBeDefined();
    expect(screen.queryByText("需要确认")).toBeNull();
  });

  it("shows server-confirmed cancellation as terminal despite old uncertainty and intent flags", async () => {
    const view = openTasks({ ...UNCERTAIN, cancel_requested: true });
    expect(
      await screen.findByRole("button", { name: "正在取消…" }),
    ).toBeDefined();
    state.tasks = [
      { ...UNCERTAIN, status: "cancelled", cancel_requested: true },
    ];
    state.detail = state.tasks[0];
    view.rerender(<ThreadBackgroundTasks threadId="thread-1" />);
    expect(screen.getByText("已取消")).toBeDefined();
    expect(screen.queryByRole("button", { name: "正在取消…" })).toBeNull();
    expect(screen.queryByRole("button", { name: "取消任务" })).toBeNull();
  });
});
