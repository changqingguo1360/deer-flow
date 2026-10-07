import { writeFile } from "node:fs/promises";
import path from "node:path";

import { expect, test } from "@playwright/test";

import type { FleetTask } from "../../src/core/fleet/types";

function required(name: string) {
  const value = process.env[name];
  if (!value) throw new Error(`Installed BC10 prerequisite missing: ${name}`);
  return value;
}

test("installed original Fleet continuation uses the same owned goal", async ({
  page,
  request,
}) => {
  test.setTimeout(240_000);
  const threadId = required("BC10_THREAD_ID");
  const taskId = required("BC10_TASK_ID");
  const evidence = required("BC10_EVIDENCE_DIR");
  const summaries: FleetTask[] = [];
  const replies: Array<{ url: string; status: number }> = [];
  page.on("response", async (response) => {
    if (response.url().includes(`/api/threads/${threadId}/agent-tasks`)) {
      replies.push({ url: response.url(), status: response.status() });
      if (response.ok()) {
        const tasks = (await response.json()) as FleetTask[];
        const owned = tasks.find((task) => task.task_id === taskId);
        if (owned) summaries.push(owned);
      }
    }
  });
  await page.goto("/login");
  await page.locator('input[type="email"]').fill(required("BC10_EMAIL"));
  await page.locator('input[type="password"]').fill(required("BC10_PASSWORD"));
  await page.getByRole("button", { name: "Sign In", exact: true }).click();
  await expect(page).toHaveURL(/\/workspace/, { timeout: 60_000 });
  await page.goto(`/workspace/chats/${threadId}`);
  await page.getByTestId("background-tasks-trigger").click();
  const card = page.getByTestId(`fleet-task-${taskId}`);
  await expect(card).toContainText("Waiting for computation");
  await expect
    .poll(() =>
      summaries.some(
        (task) =>
          task.state === "waiting_jobs" &&
          task.stop_state === "confirmed" &&
          task.resources_held === false,
      ),
    )
    .toBe(true);
  const waiting = summaries.find(
    (task) =>
      task.state === "waiting_jobs" &&
      task.stop_state === "confirmed" &&
      task.resources_held === false,
  )!;
  expect(waiting.jobs).toHaveLength(1);
  const child = waiting.jobs![0]!;
  await expect(card).toContainText(child.job_id);
  await expect(card).toContainText(waiting.current_run_id);
  expect(waiting.stop_state).toBe("confirmed");
  expect(waiting.resources_held).toBe(false);
  await expect(card.getByText("Stop unconfirmed", { exact: true })).toHaveCount(
    0,
  );
  await page.screenshot({
    path: path.join(evidence, "browser-waiting.png"),
    fullPage: true,
  });
  // This controls the scripted child command only. All product responses above
  // and below come from the original authenticated normal Gateway.
  const released = await request.post(
    `${required("BC10_PROVIDER_URL")}/browser/release`,
    {
      data: { token: required("BC10_BROWSER_TOKEN") },
    },
  );
  expect(released.status()).toBe(200);
  await expect(card).toContainText("Completed", { timeout: 150_000 });
  await expect
    .poll(
      () =>
        summaries.some(
          (task) =>
            task.state === "succeeded" &&
            task.stop_state === "confirmed" &&
            task.resources_held === false,
        ),
      {
        timeout: 150_000,
      },
    )
    .toBe(true);
  const completed = summaries.find(
    (task) =>
      task.state === "succeeded" &&
      task.stop_state === "confirmed" &&
      task.resources_held === false,
  )!;
  const result = completed.jobs!.find((job) => job.job_id === child.job_id)!;
  expect(result.accepted_manifest_id).toBeTruthy();
  expect(completed.current_run_id).not.toBe(waiting.current_run_id);
  expect(completed.runs!.map((run) => run.run_id)).toEqual(
    expect.arrayContaining([waiting.current_run_id, completed.current_run_id]),
  );
  expect(completed.stop_state).toBe("confirmed");
  expect(completed.resources_held).toBe(false);
  await expect(card).toContainText(result.accepted_manifest_id!);
  await expect(card).toContainText(waiting.current_run_id);
  await expect(card.getByText("Stop unconfirmed", { exact: true })).toHaveCount(
    0,
  );
  const manifest = await page.request.get(
    `/api/threads/${threadId}/fleet/manifests/${result.accepted_manifest_id}`,
  );
  expect(manifest.status()).toBe(200);
  const file = await page.request.get(
    `/api/threads/${threadId}/fleet/manifests/${result.accepted_manifest_id}/files/child.txt`,
  );
  expect(file.status()).toBe(200);
  expect(await file.text()).toBe("bc10-original-child result\n");
  await page.screenshot({
    path: path.join(evidence, "browser-completed.png"),
    fullPage: true,
  });
  await writeFile(
    path.join(evidence, "browser-observations.json"),
    JSON.stringify(
      {
        thread_id: threadId,
        task_id: taskId,
        job_id: child.job_id,
        accepted_manifest_id: result.accepted_manifest_id,
        waiting,
        completed,
        actual_summary_responses: replies,
        historical_run_ids: completed
          .runs!.filter((run) => run.run_id !== completed.current_run_id)
          .map((run) => run.run_id),
        original_manifest: await manifest.json(),
        original_result_text: await file.text(),
        authentication:
          "actual normal local-login session through Next proxy; no API interception",
      },
      null,
      2,
    ),
    { mode: 0o600 },
  );
});
