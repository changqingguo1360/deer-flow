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
  jobs?: FleetRelatedJob[];
  runs?: FleetRelatedRun[];
  jobs_truncated?: boolean;
  runs_truncated?: boolean;
}

export interface FleetRelatedJob {
  job_id: string;
  generation: number;
  parent_run_id: string;
  link_mode: "awaited" | "detached";
  state: string;
  accepted_manifest_id: string | null;
}

export interface FleetRelatedRun {
  run_id: string;
  generation: number;
  run_status: string;
}

export interface FleetGoalOperation {
  taskId: string;
  expectedGeneration: number;
  idempotencyKey: string;
}

export type FleetGoalAction = "cancel" | "resume";
