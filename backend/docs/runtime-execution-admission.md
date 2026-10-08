### Trusted execution backend admission

execution/contracts.py owns backend selection and SQL admission participation;
harness must not import app or the optional Fleet package. App injects the backend
through an internal keyword; client metadata/config does not select an executor.
LocalExecutionBackend remains default and passes no new participant keyword to
ordinary stores. Memory stores explicitly reject SQL participants.

RunRepository's RunAdmissionUnitOfWork holds one session/transaction: participant
prepare locks its goal before core thread/run work, then insert persists extension
records after the run flush. Any exception or CancelledError rolls back both;
RunManager registers only committed admissions. Retry validation uses immutable
original identity/inputs and cannot leave freshly prepared goal rows. Remote pending
records have no Gateway owner or lease and create no local asyncio task.
C03 ownership adds SQL local eligibility for absent/local server-owned
backend labels, plus a trusted host predicate applied to scans and mutations. Remote
hydration preserves its label; store_only requires valid nonlocal admission output.
See the C05/C06a write boundaries below. Gateway remote activation remains closed; the Fleet development guide owns app adapters and input codecs.
