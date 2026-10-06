# C09 cancellation and recovery contracts

C09 has isolated local acceptance; public remote Agent activation remains closed
until C10–C12 and C→B→C complete. The implementation plan and actual evidence are recorded in
[implementation progress](superpowers/plans/2026-10-01-ecs-fleet-implementation-progress.md).
The accepted main routes use the installed original Runner, PostgreSQL, authenticated
HTTP, managed Docker containers and NAS. A scripted model supplies deterministic
responses; it does not replace the graph, tools or execution lifecycle.

## Cancellation and physical stop

Public run cancellation retains the original `interrupt` and `rollback` actions.
The first committed request fixes the action and timestamp. Duplicate requests do
not reset its deadline. Cancellation leaves the current generation, run and attempt
intact so the original owner can perform bounded private cleanup. It does not release
capacity or permit a second writer.

The original AgentRunner owns and joins its executor and cancellation observer.
Private cleanup is bounded by the first cancellation timestamp plus 120 seconds,
clipped to the original task, attempt and execution deadlines. This authority does
not permit ordinary checkpoint, memory, event, tool-launch or metadata writes.
A failed or unsettled cleanup remains recovery work.

An authenticated physical STOP acknowledgement is separate from core terminal
status, graph END and accepted files. Only the original STOP transaction releases
capacity. `wait=true` returns204 only after durable physical-stop confirmation;
an unconfirmed stop remains pending. Transport stop reasons cannot change an already
accepted immutable terminal outcome.

## Pauses, rollback and resume

A graph pause retains its actual checkpoint tasks and interrupt identities. A human
input pause becomes `input_required`; another graph pause becomes `paused`.
Both retain the original task. After the exact checkpoint/workspace pair is accepted
and the original execution is stopped with released capacity, the existing public
keyed-resume path admits a new run and increments generation atomically. It preserves
the task deadline and budget; cached completed tool results are not replayed.
Missing stop proof, changed generation/profile, or a missing exact recovery point
rejects admission. Competing requests have one winner.

Rollback restores the original pre-run checkpoint according to the existing Local
contract. It does not promise filesystem rollback. Remote rollback pairs that actual
restored checkpoint with the actual quiescent workspace; a mismatch fails closed.

## Partition and restart boundaries

The accepted same-session transport main shows the original shell continuing after
a real TCP partition while an independent Gateway rejects a second same-thread run.
After local physical STOP, missing acknowledgement keeps capacity charged. Restoring
the connection permits exact private journal STOP replay. Without an accepted recovery
point the task remains `recovery_required`, even after capacity is released.
The original publication-deadline exception is retained as an execution observation,
not converted into successful execution or a fabricated pending result.

Node process restart changes the Node session and fences the old writer. Bootstrap
first physically stops residual containers. For an old started Agent journal it then
uses `POST /api/fleet/node/attempts/{attempt_id}/reconcile-stopped`, authenticated by
the current Node credential/session plus the original attempt token and original
session from its private start grant. Frozen execution identity and the exact process
reference must match. This operation does not replace the recorded attempt session
or grant start, renew, publication or result-writing authority. Ordinary STOP remains
strict and rejects the new session for the old attempt. B bootstrap stays unchanged.

The original stock worker process main proves this separate restart path. With no
exact accepted source, STOP acknowledgement releases capacity but leaves the task
`recovery_required` and heartbeat unknown; the worker exits with recovery required.
For an exact accepted terminal pair, the original immutable outcome and stream seal
remain authoritative. An already acknowledged STOP can return an immutable read-only
receipt after a new generation has started, but only with its own durable stopped and
released reservation. It cannot mutate the new run or release its reservation.
The two existing native cases verify that boundary and dedicated identity refusals.
The final installed image `sha256:960f86678b81020b3f850bcf0ab0ae1674a6f9ab62ee92730fb53ae13b5bc444` passes the restart main and installed-byte checks; the two native boundary checks and nine necessary neighbors also pass. Finite repairs close all ten original regression failures without repeating unchanged main evidence; see [C09 acceptance](ecs-fleet-c09-acceptance.md) for the exact scope.

Started unknown work is never automatically retried. STOP proof alone is not
side-effect review or authority to replay from an arbitrary checkpoint.
