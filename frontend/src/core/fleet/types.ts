export type FleetTaskState =
  | "queued"
  | "running"
  | "waiting_jobs"
  | "paused"
  | "input_required"
  | "unknown"
  | "finishing"
  | "recovery_required"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "timed_out";

export interface FleetTask {
  task_id: string;
  state: FleetTaskState;
  current_run_id: string;
  generation: number;
  run_status: string;
  profile: string;
  location: "queued" | "remote";
  cancel_requested: boolean;
  recovery_required: boolean;
  stop_state: "not_started" | "unconfirmed" | "confirmed";
  resources_held: boolean;
}
