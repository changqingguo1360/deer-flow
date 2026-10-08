import { readFileSync } from "node:fs";

import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { toast } from "sonner";

import { ThreadBackgroundTasks } from "@/components/workspace/thread-background-tasks";
import type { FleetTask } from "@/core/fleet/types";
import { zhCN } from "@/core/i18n/locales/zh-CN";

interface CapturedResponse {
  status: number;
  body: FleetTask[];
}
interface Flow {
  proof: {
    owner_id: string;
    thread_id: string;
    task_id: string;
    original_run_id: string;
    job_ids: string[];
    manifest_ids: Record<string, string>;
    original_generation: number;
  };
  waiting: CapturedResponse;
  accepted_waiting: CapturedResponse;
  resumed: CapturedResponse;
  cancelled: CapturedResponse;
  resume: { status: number; body: { run_id: string } };
  cancel: { status: number; body: { state: string } };
}

const state = rs.hoisted(() => ({ owner: "", mcpEnabled: false }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: state.owner } }),
}));
rs.mock("@/core/features", () => ({
  useMcpTasksEnabled: () => ({ enabled: state.mcpEnabled }),
}));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: zhCN }) }));
rs.mock("sonner", () => ({ toast: { error: rs.fn() } }));
rs.mock("@/core/background-tasks/hooks", () => ({
  useBackgroundTasks: () => ({ data: [], isLoading: false, isError: false }),
  useBackgroundTask: () => ({ data: undefined }),
  useCancelBackgroundTask: () => ({ mutate: rs.fn(), isPending: false }),
}));

const originalFetch = globalThis.fetch;
let queryClient: QueryClient | undefined;
afterEach(() => {
  cleanup();
  queryClient?.clear();
  globalThis.fetch = originalFetch;
  rs.restoreAllMocks();
});

describe("BC09 actual owned HTTP in the original panel", () => {
  it("waiting goal summary and task operations", async () => {
    // Baseline and GREEN each consume that backend run's unmodified HTTP.
    // SQL proof supplies expectations; it never fills missing response fields.
    const fixturePath = process.env.BC09_HTTP_FIXTURE;
    if (!fixturePath)
      throw new Error(
        "BC09_HTTP_FIXTURE must name the captured native API flow",
      );
    const flow = JSON.parse(readFileSync(fixturePath, "utf8")) as Flow;
    state.owner = flow.proof.owner_id;
    state.mcpEnabled = false;
    let phase: "accepted_waiting" | "resumed" | "cancelled" =
      "accepted_waiting";
    const operations: {
      action: string;
      body: { expected_generation: number; idempotency_key: string };
      credentials: RequestCredentials | undefined;
    }[] = [];
    const path = `/api/threads/${encodeURIComponent(flow.proof.thread_id)}/agent-tasks`;
    const gets: string[] = [];
    globalThis.fetch = rs.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
          "http://localhost",
        );
        if ((init?.method ?? "GET") === "GET") {
          expect(url.pathname).toBe(path);
          expect(url.searchParams.get("limit")).toBe("20");
          gets.push(phase);
          return new Response(JSON.stringify(flow[phase].body), {
            status: flow[phase].status,
          });
        }
        expect(init?.method).toBe("POST");
        const action = url.pathname.split("/").at(-1)!;
        expect(url.pathname).toBe(
          `${path}/${encodeURIComponent(flow.proof.task_id)}/${action}`,
        );
        expect(["cancel", "resume"]).toContain(action);
        if (typeof init?.body !== "string") {
          throw new Error("Expected the original JSON string request body");
        }
        operations.push({
          action,
          body: JSON.parse(init.body) as {
            expected_generation: number;
            idempotency_key: string;
          },
          credentials: init?.credentials,
        });
        const captured = action === "resume" ? flow.resume : flow.cancel;
        phase = action === "resume" ? "resumed" : "cancelled";
        return new Response(JSON.stringify(captured.body), {
          status: captured.status,
        });
      },
    ) as typeof globalThis.fetch;
    queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    });
    render(
      <QueryClientProvider client={queryClient}>
        <ThreadBackgroundTasks threadId={flow.proof.thread_id} />
      </QueryClientProvider>,
    );
    fireEvent.click(await screen.findByTestId("background-tasks-trigger"));
    const card = await screen.findByTestId(`fleet-task-${flow.proof.task_id}`);
    expect(await within(card).findByText("等待计算")).toBeTruthy();
    expect(within(card).queryByText("已完成")).toBeNull();
    expect(
      within(card).getAllByText(flow.proof.original_run_id, { exact: false })
        .length,
    ).toBeGreaterThan(0);
    expect(
      within(card).getByText(`代次: ${flow.proof.original_generation}`),
    ).toBeTruthy();
    for (const id of flow.proof.job_ids)
      expect(within(card).getByText(id, { exact: false })).toBeTruthy();
    for (const id of Object.values(flow.proof.manifest_ids))
      expect(within(card).getByText(id, { exact: false })).toBeTruthy();
    expect(within(card).getByRole("button", { name: "取消目标" })).toBeTruthy();
    expect(card.textContent).not.toContain("untrusted child result");
    fireEvent.click(within(card).getByRole("button", { name: "恢复目标" }));
    await waitFor(() => expect(operations.length).toBe(1));
    await waitFor(() =>
      expect(
        within(card).getAllByText(flow.resume.body.run_id, { exact: false })
          .length,
      ).toBeGreaterThan(0),
    );
    const resume = operations[0]!;
    expect(resume.action).toBe("resume");
    expect(resume.body.expected_generation).toBe(
      flow.proof.original_generation,
    );
    expect(resume.body.idempotency_key).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
    expect(Object.keys(resume.body).sort()).toEqual([
      "expected_generation",
      "idempotency_key",
    ]);
    expect(resume.credentials).toBe("include");
    expect(
      within(card)
        .getByRole("link", { name: new RegExp(flow.resume.body.run_id) })
        .getAttribute("href"),
    ).toContain(`/runs/${encodeURIComponent(flow.resume.body.run_id)}`);
    fireEvent.click(within(card).getByRole("button", { name: "取消目标" }));
    await waitFor(() => expect(operations.length).toBe(2));
    const cancel = operations[1]!;
    expect(cancel.action).toBe("cancel");
    expect(cancel.body.expected_generation).toBe(
      flow.resumed.body[0]!.generation,
    );
    expect(cancel.body.idempotency_key).not.toBe(resume.body.idempotency_key);
    expect(cancel.credentials).toBe("include");
    await waitFor(() => expect(gets).toContain("cancelled"));
    expect(await within(card).findByText("已取消")).toBeTruthy();
    expect(
      within(card).getByText(`代次: ${flow.cancelled.body[0]!.generation}`),
    ).toBeTruthy();
    expect(within(card).queryByRole("button", { name: "取消目标" })).toBeNull();
  });
});

it("pending conflict and confirmation states", async () => {
  const fixturePath = process.env.BC09_HTTP_FIXTURE;
  if (!fixturePath) throw new Error("BC09_HTTP_FIXTURE must name actual HTTP");
  const flow = JSON.parse(readFileSync(fixturePath, "utf8")) as Flow;
  const initial = flow.accepted_waiting.body[0]!;
  state.owner = flow.proof.owner_id;
  state.mcpEnabled = false;
  let observation = initial;
  let gets = 0;
  const bodies: { expected_generation: number; idempotency_key: string }[] = [];
  let finish: ((response: Response) => void) | undefined;
  const path = `/api/threads/${encodeURIComponent(flow.proof.thread_id)}/agent-tasks`;
  globalThis.fetch = rs.fn(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
        "http://localhost",
      );
      if ((init?.method ?? "GET") === "GET") {
        expect(url.pathname).toBe(path);
        gets += 1;
        return new Response(JSON.stringify([observation]), { status: 200 });
      }
      expect(url.pathname).toBe(
        `${path}/${encodeURIComponent(initial.task_id)}/cancel`,
      );
      expect(init?.method).toBe("POST");
      expect(init?.credentials).toBe("include");
      if (typeof init?.body !== "string")
        throw new Error("Expected JSON string body");
      bodies.push(
        JSON.parse(init.body) as {
          expected_generation: number;
          idempotency_key: string;
        },
      );
      return await new Promise<Response>((resolve) => {
        finish = resolve;
      });
    },
  );
  queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  render(
    <QueryClientProvider client={queryClient}>
      <ThreadBackgroundTasks threadId={flow.proof.thread_id} />
    </QueryClientProvider>,
  );
  fireEvent.click(await screen.findByTestId("background-tasks-trigger"));
  const card = await screen.findByTestId(`fleet-task-${flow.proof.task_id}`);
  await within(card).findByText(zhCN.fleetTasks.status.waiting_jobs);
  const cancel = within(card).getByRole("button", {
    name: zhCN.fleetTasks.cancelGoal,
  });
  const resume = within(card).getByRole("button", {
    name: zhCN.fleetTasks.resumeGoal,
  });
  fireEvent.click(cancel);
  fireEvent.click(cancel);
  await waitFor(() => expect(bodies).toHaveLength(1));
  expect(cancel).toHaveProperty("disabled", true);
  expect(resume).toHaveProperty("disabled", true);
  expect(within(card).queryByText(zhCN.fleetTasks.status.cancelled)).toBeNull();
  expect(
    within(card).getByText(zhCN.fleetTasks.status.waiting_jobs),
  ).toBeTruthy();
  const beforeConflictGets = gets;
  observation = {
    ...initial,
    generation: initial.generation + 1,
    current_run_id: flow.resumed.body[0]!.current_run_id,
  };
  finish!(
    new Response(JSON.stringify({ detail: "stale generation" }), {
      status: 409,
    }),
  );
  await waitFor(() => expect(gets).toBeGreaterThan(beforeConflictGets));
  await waitFor(() => expect(cancel).toHaveProperty("disabled", false));
  expect(bodies).toHaveLength(1);
  expect(toast.error).toHaveBeenCalledWith(zhCN.fleetTasks.operationConflict);
  fireEvent.click(cancel);
  await waitFor(() => expect(bodies).toHaveLength(2));
  expect(bodies[1]!.expected_generation).toBe(observation.generation);
  expect(bodies[1]!.idempotency_key).not.toBe(bodies[0]!.idempotency_key);
  finish!(
    new Response(JSON.stringify({ detail: "Controlled transient failure" }), {
      status: 500,
    }),
  );
  await waitFor(() => expect(cancel).toHaveProperty("disabled", false));
  fireEvent.click(cancel);
  await waitFor(() => expect(bodies).toHaveLength(3));
  expect(bodies[2]).toEqual(bodies[1]);
  finish!(
    new Response(JSON.stringify({ detail: "Permission denied" }), {
      status: 403,
    }),
  );
  await waitFor(() => expect(cancel).toHaveProperty("disabled", false));
  expect(bodies).toHaveLength(3);
  expect(within(card).queryByText(zhCN.fleetTasks.status.cancelled)).toBeNull();
  const refetch = async (task: FleetTask) => {
    observation = task;
    await act(async () => {
      await queryClient!.invalidateQueries({
        queryKey: ["fleet-agent-tasks", state.owner, flow.proof.thread_id],
      });
    });
  };
  await refetch({
    ...initial,
    jobs: initial.jobs!.map((job, i) =>
      i === 0 ? job : { ...job, state: "running", accepted_manifest_id: null },
    ),
  });
  await within(card).findByText(/running · awaited/);
  expect(
    within(card).queryByRole("button", { name: zhCN.fleetTasks.resumeGoal }),
  ).toBeNull();
  await refetch({
    ...initial,
    jobs: initial.jobs!.map((job) => ({
      ...job,
      state: "failed",
      accepted_manifest_id: null,
    })),
  });
  expect(
    await within(card).findByRole("button", {
      name: zhCN.fleetTasks.resumeGoal,
    }),
  ).toBeTruthy();
  await refetch({ ...initial, jobs_truncated: true });
  await within(card).findByText(zhCN.fleetTasks.relatedTruncated);
  expect(
    within(card).queryByRole("button", { name: zhCN.fleetTasks.resumeGoal }),
  ).toBeNull();
  await refetch({ ...initial, state: "input_required" });
  expect(
    await within(card).findByText(zhCN.fleetTasks.status.input_required),
  ).toBeTruthy();
  expect(
    within(card).queryByRole("button", { name: zhCN.fleetTasks.resumeGoal }),
  ).toBeNull();
  await refetch({
    ...initial,
    recovery_required: true,
    stop_state: "unconfirmed",
    resources_held: true,
  });
  expect(
    await within(card).findByText(zhCN.fleetTasks.status.needsConfirmation),
  ).toBeTruthy();
  expect(
    within(card).queryByRole("button", { name: zhCN.fleetTasks.resumeGoal }),
  ).toBeNull();
  expect(
    within(card).queryByRole("button", { name: zhCN.fleetTasks.cancelGoal }),
  ).toBeNull();
  await refetch({
    ...initial,
    state: "running",
    run_status: "running",
    cancel_requested: true,
    stop_state: "unconfirmed",
    resources_held: true,
  });
  expect(
    await within(card).findByText(zhCN.fleetTasks.status.stopping),
  ).toBeTruthy();
  expect(within(card).queryByText(zhCN.fleetTasks.status.cancelled)).toBeNull();
  expect(
    within(card).queryByRole("button", { name: zhCN.fleetTasks.resumeGoal }),
  ).toBeNull();
  expect(bodies).toHaveLength(3);
});
