import { describe, expect, it } from "@rstest/core";

import { fleetTaskPresentation } from "@/core/fleet/presentation";
import type { FleetTask } from "@/core/fleet/types";

import observed from "./observed-summary.json";

// Recorded actual owned HTTP JSON; edits below are explicit UI boundary cases.
const running = observed.running as FleetTask;

describe("remote Agent summary precedence", () => {
  it("keeps physical stop and task completion separate from run outcome", () => {
    expect(fleetTaskPresentation(observed.queued as FleetTask).label).toBe(
      "queued",
    );
    expect(fleetTaskPresentation(running).canCancel).toBe(true);
    expect(
      fleetTaskPresentation(observed.cancel_intent as FleetTask).label,
    ).toBe("stopping");
    expect(fleetTaskPresentation(observed.after_stop as FleetTask).label).toBe(
      "needsConfirmation",
    );
    expect(
      fleetTaskPresentation({
        ...running,
        state: "succeeded",
        run_status: "success",
      }).label,
    ).toBe("stopUnconfirmed");
    expect(
      fleetTaskPresentation({
        ...running,
        state: "waiting_jobs",
        run_status: "success",
        resources_held: false,
        stop_state: "confirmed",
      }).label,
    ).toBe("waiting_jobs");
  });
});
