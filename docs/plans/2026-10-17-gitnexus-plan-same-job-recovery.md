# GitNexus Engineering Plan

> Task: Add a mechanically enforced same-job, no-POST recovery path for a failed USAspending bulk-export attempt with durably persisted job identity.
> Evidence verified at commit eb19b1b36e1379fda60579374d28bd9717d17403; GitNexus index 2 commits behind, refresh skipped because the only local shim is untracked and resolves the analyzer through unpinned `npx gitnexus@latest`.
> Evidence provenance schema 2; global dirty digest 0a9c85780067d9afcd0764f307b60891e3cee927ee11eaeb5ec7826d10fd82cd; cited-path manifest 16 sorted entries; exact generated plan path excluded.

## 1. Objective

[verified] Add an explicitly authorized recovery route for a failed `http_404` attempt whose export POST was already accepted and whose `status_url`, `file_url`, and `remote_file_name` were committed. The route must preserve the failed row, create exactly one linked recovery row, poll and download only the persisted job, and have no call path to `UsaSpendingClient.submit_bulk_export`.

[verified] The change is limited to application code, one forward migration, and tests. It must not alter the initial-status-404 grace policy, ordinary no-job failed-attempt replacement behavior, root/sibling traversal, checkpoint atomicity, or existing storage and execution bounds.

Non-goals:

- Do not recover or modify the retained May rows as part of implementation.
- Do not add automatic retries, resubmit an accepted export, contact USAspending during tests, or generalize this into a generic attempt repair framework.
- Do not reopen or rewrite historical failed attempts.
- Do not allow a second recovery attempt after a linked recovery exists, regardless of whether that recovery is active, completed, or failed.

## 2. Current Behaviour

- [verified] `UsaSpendingClient.submit_bulk_export` performs the export POST and returns the three job-identity fields after host validation (`services/api/app/usaspending/client.py:162-195`).
- [verified] `TransactionIngestionWorkflow.run_period` owns every pre- and post-submission state. It claims `counted -> submitting`, calls `submit_bulk_export`, commits job identity in `submitting -> submitted`, and only then polls (`services/api/app/usaspending/transaction_workflow.py:248-328`).
- [verified] A resumed `submitted` attempt polls with `initial_status_404_grace=False`; a non-transient `http_404` becomes terminal `failed` while the three job fields remain stored (`transaction_workflow.py:314-353`; regression tests at `services/api/tests/test_usaspending_transaction_workflow.py:1007-1070`).
- [verified] `_locate_or_create_attempt` ignores terminal failed rows, takes the period advisory transaction lock, and creates a new `created` attempt when no active/completed row exists (`transaction_workflow.py:531-596`). That new row can reach the POST branch.
- [verified] `BackfillCoordinator._process_period` authorizes a failed row only through an exact `retry_failed_attempt_id`, then passes its tree parent to the normal workflow; this is explicitly a replacement/new-submission path (`services/api/app/usaspending/transaction_backfill.py:629-782`).
- [verified] `_period_attempt` prefers an active row, then completed, then failed history (`transaction_backfill.py:890-911`). Tree ownership admits failed peers with the same parent and bounds (`transaction_backfill.py:979-1074`).
- [verified] The CLI convention is an action plus exact attempt identifier: `--retry-failed-attempt-id`; it is parsed to `UUID` and passed through `main -> BackfillCoordinator.run` (`services/api/app/usaspending/transaction_backfill_cli.py:97-216,319-382`).
- [verified] Loading and checkpoint completion already share the caller-owned transaction: the attempt row is locked, the archive is loaded, a checkpoint and owned rows are reconciled, and attempt completion happens before commit (`transaction_workflow.py:785-858`; `services/api/app/usaspending/transaction_ingestion.py:221-272,299-428`).

## 3. Relevant Architecture

- [verified] `UsaSpendingTransactionIngestionAttempt` is the durable state machine. A partial unique period index permits one nonterminal attempt, and `parent_attempt_id` preserves subdivision-tree membership while allowing failed history (`services/api/app/db/models.py:204-361`).
- [verified] `TransactionIngestionWorkflow` owns attempt transactions and all network/load transitions. `BackfillCoordinator` owns traversal, explicit failed-attempt authorization, storage reservations, counters, and progress events. The CLI owns operator confirmation and manifest lifecycle.
- [verified] `StorageGuard` reservations are keyed by attempt UUID. `BackfillResult.submitted_leaves` changes only through the submission observer inside `_run_leaf`; therefore a recovery path must not call `_run_leaf` with submission counting enabled (`transaction_backfill.py:784-845`).
- [verified] `sanitize_progress_record` is an allowlist boundary. New lineage identifiers must be added deliberately; job URLs and filenames must remain excluded from manifests (`transaction_backfill.py:1479-1544`).
- [verified] The global ingestion lease serializes normal CLI runs, but workflow-level advisory locks and database uniqueness are still required because tests and non-CLI callers can invoke the workflow concurrently.
- [inferred] The smallest design that satisfies history, tree membership, and no-POST reachability is a second attempt row with a distinct `recovery_of_attempt_id`; `parent_attempt_id` cannot carry both subdivision ancestry and recovery lineage.

## 4. GitNexus Findings

Freshness and provenance:

- [graph] `list_repos(repo=govtracts)` and `gitnexus://repo/govtracts/context` report the index two commits behind HEAD and `incomplete_reasons: [incremental-in-progress]`. Findings below are therefore stale-index evidence and are source-verified before use.
- [verified] `.gitnexus/run.cjs` exists but is untracked/ignored, SHA-256 `c53de9bb30f405804b439b9623d3c5bb11b54fd92bee21e9b52e35b03b159aa7`, and resolves this environment to `npx gitnexus@latest`. No global `gitnexus` executable is installed (`gitnexus --version` probe exited 127). The permitted `node .gitnexus/run.cjs analyze --index-only --pdg` refresh was not run because analyzer provenance was not pinned/current enough for strict mode.
- [graph] The retained index reports analyzer CLI 1.6.12 and a current runner-identity schema, but that describes the existing artifact, not the unpinned refresh that would run now.

Primary symbols and impact (`impact(direction=upstream,maxDepth=3,includeTests=true)`):

- [graph] `BackfillCoordinator.run` — HIGH, exact, 19 direct dependents, all in `test_usaspending_transaction_backfill.py`. Quote: `impactedCount: 19`, `byDepthCounts: {1: 19}`.
- [graph] `BackfillCoordinator._process_period` — LOW, exact, 20 total / 1 direct, with the `run` process affected. Its low rank does not waive the HIGH risk of its caller.
- [graph] `TransactionIngestionWorkflow.run_period` — HIGH, exact, 55 total / 4 direct across Tests and Usaspending; affected processes include coordinator `run`, `_run_leaf`, workflow `run`, and `_process_period`.
- [graph] `TransactionIngestionWorkflow._locate_or_create_attempt` — HIGH at depth 3, exact, 16 total / 3 direct. Directs are `run_period`, the `CountedBarrierWorkflow` override, and that override's call-through.
- [graph] `UsaSpendingTransactionIngestionAttempt` — HIGH, lower-bound, 23 total / 15 direct imports; the boundary is SQLAlchemy `Base` dispatch across 11 implementations. Targeted `rg` confirmed uses in Alembic, health/market routes, ingestion/workflow/backfill, and the five transaction/model test modules, so UNKNOWN use is not treated as absent.
- [graph] `sanitize_progress_record` — CRITICAL, exact, 43 total / 3 direct. Its direct consumers are `_record`, CLI `progress`, and `safe_result_manifest`; seven reporting processes are downstream.
- [graph] CLI `main` — MEDIUM, exact, 15 total / 14 direct; `build_parser` and `parse_args` are LOW at depth 3. `parse_args` is lower-bound because two receiver-typing sites were dropped; targeted source search confirms the production call is `main` and tests drive the CLI entry.
- [graph] `TransactionIngestionWorkflow` and `UsaSpendingClient` classes are MEDIUM (8 and 13 impacted symbols respectively), so adding recovery/validation methods requires their existing construction and fake-client tests to remain compatible.

Direct-dependent accounting:

- [graph] The 19 direct callers of coordinator `run` are the backfill tests for arbitrary-leaf selection, failed authorization, blocking states, completed verification, restart storage guards, count boundaries/subdivision, count drift in both directions, failed-child replacement, overlap guards (attempt/transaction/checkpoint/additional overlap), nested/single-day overflow, pre-submission capacity, multi-FY flow, temporary-path identity, and synthetic end-to-end flow. All remain in the regression command.
- [graph] The four direct `run_period` callers are coordinator `_process_period`, coordinator `_run_leaf`, workflow `run`, and the workflow-test `run_child_period` helper.
- [graph] The attempt model's 15 direct importers are Alembic env, health/market routes, three USAspending production modules plus ingestion, and the model/market/migration/ingestion/workflow/backfill tests. Schema parity and full API tests cover this surface.
- [graph] The three direct manifest-sanitizer consumers are explicitly tested for safe progress, stopped manifests, and final manifests.

Trace findings:

- [graph] `trace(_process_period -> poll_bulk_export,maxDepth=6)` finds `_process_period -> run_period -> poll_bulk_export` in two hops.
- [graph] `trace(run_period -> submit_bulk_export,maxDepth=4)` returned `no_path` even though source contains the direct call at `transaction_workflow.py:282-286`; this is a stale/dynamic-receiver graph gap and is not used as proof of safety.
- [inferred] The implementation must establish no-POST reachability by source structure and tests, not by the stale trace result: the recovery entry must never call `run_period`, `_locate_or_create_attempt`, or a helper that contains the submission branch.

## 5. Statement-Level PDG Findings

- [graph] `pdg_query(target=transaction_workflow.py,mode=controls)` anchors `run_period` at indexed line 160 and shows failure branches controlling `_mark_failed`, archive validation, `archive_hashed -> loading`, and `_load_atomically`. The current-source equivalents are `transaction_workflow.py:314-525`.
- [graph] `pdg_query(target=transaction_workflow.py,mode=flows,variable=attempt)` reports reaching definitions from post-submission transitions into `_mark_failed`, `_record_resumable_failure`, `_load_atomically`, `_read_attempt`, and the final result. Planning consequence: extract the existing `submitted/export_finished/archive_hashed/loading` loop as one parity-preserving helper instead of reimplementing those branches.
- [graph] PDG flows for `submission_claimed`, `submitted_in_this_invocation`, `status`, and coordinator `retry_failed_attempt_id` returned zero rows. `pdg_query` for the coordinator file also returned no control rows. These are recorded coverage gaps caused by the stale/incomplete index; source lines `transaction_workflow.py:197-328` and `transaction_backfill.py:629-782` are authoritative for submission ownership and failed authorization.
- [graph] `pdg_query(target=transaction_ingestion.py,mode=controls)` shows the existing-checkpoint predicate controls checkpoint insertion at current lines 359-379. `mode=flows,variable=checkpoint_id` shows the identifier flowing through staged-row ownership, checkpoint insert/update, reconciliation, and the returned result.
- [verified] `_load_atomically` locks the attempt row and calls `load_with_connection` inside the same `engine.begin()` transaction before setting the attempt to completed (`transaction_workflow.py:785-858`). This ordering is retained unchanged for recovery.
- [verified] Recovery-claim ordering must be: period advisory transaction lock -> original failed row `FOR UPDATE` -> validate source row and all absence predicates -> inspect any linked recovery -> insert the linked `submitted` row with copied identity -> commit. No network, archive I/O, or source-row update may occur inside this transaction.
- [inferred] Interruption before claim commit leaves no recovery row; retrying the exact authorization can claim once. Interruption after commit finds the unique linked row and resumes only its post-submission state. Loading remains idempotent through the existing attempt lock, checkpoint lock, archive identity, and atomic completion path.

## 6. Proposed Changes

### Chosen design

[inferred] Choose design B (a linked recovery attempt beginning at `submitted`) together with design D (a distinct exact-UUID CLI authorization). D is the operator interface for B, not a competing persistence design.

- Historical evidence: the original `failed/http_404` row is locked for validation but never updated.
- Lineage: add nullable `recovery_of_attempt_id` while retaining the original `parent_attempt_id` on the recovery row so subdivision ancestry remains intact.
- Duplicate prevention: a unique index permits only one row for each non-null `recovery_of_attempt_id`; the existing active-period and parent-period indexes provide additional period/tree exclusion.
- No POST: claim and resume are separate workflow methods; the resume method enters only the extracted post-submission helper and cannot call `run_period` or `_locate_or_create_attempt`.
- Existing retries: `--retry-failed-attempt-id` continues to mean “create a replacement that may submit a new export” only for failed attempts with all three job fields absent. Complete or partial accepted-job metadata blocks this path and directs the operator to same-job recovery or manual repair.

Rejected alternatives:

- A, reopen/mutate the failed row: rejected because it erases the terminal failure boundary, makes audit timing ambiguous, and conflicts with the current terminal-field contract.
- C, add a recovery state or a separate recovery table: rejected because a new state on the failed row still rewrites history, while a separate table duplicates attempt state, traversal, storage, and checkpoint relationships without improving no-POST enforcement.
- D alone as an overloaded extension of `--retry-failed-attempt-id`: rejected because the same option would have two opposite meanings (new POST versus no POST) based on database contents. A distinct flag makes the authorization and manifest self-describing.

### Model and migration

`services/api/app/db/models.py` — `UsaSpendingTransactionIngestionAttempt`:

- Add nullable self-FK `recovery_of_attempt_id` with `ON DELETE RESTRICT`.
- Add `recovery_of_attempt_id IS NULL OR recovery_of_attempt_id != id`.
- Add a unique partial index on `recovery_of_attempt_id` where it is not null. This intentionally permits zero or one recovery ever per source attempt, a stronger guarantee than merely one active recovery.
- Do not add a new status: a claimed recovery begins in existing `submitted`, then follows `export_finished -> archive_hashed -> loading -> completed|failed`.

`services/api/alembic/versions/20261017_0007_usaspending_same_job_recovery.py` — new revision after `20261004_0006`:

- Add the nullable column, self-FK, non-self check, and unique partial index with names <=63 bytes.
- Existing rows remain null; no data backfill.
- Downgrade must first fail if any non-null recovery lineage exists, then remove index/check/FK/column in reverse order.

### Job identity validation

`services/api/app/usaspending/client.py` — new pure/public validation method on `UsaSpendingClient`:

- Validate the persisted `BulkExportJob` without network activity.
- Reuse `_validate_remote_url` for exact HTTPS hosts, port, and credential rejection.
- Require a nonempty basename-only `.zip` filename within the modeled 512-character width; reject NUL, `/`, `\\`, dot segments, percent-decoded separators, and path traversal forms.
- Require the percent-decoded basename of `file_url.path` to equal `file_name`. If a status URL includes a `file_name` query parameter, require exactly one value equal to `file_name`; do not require that optional query parameter to exist.
- Return the validated immutable job or raise a stable `UsaSpendingError` code. Do not log or emit the URLs.

### Workflow and transaction contract

`services/api/app/usaspending/transaction_workflow.py` — `TransactionIngestionWorkflow`:

- Add an immutable `RecoveryClaim` result carrying original ID, recovery ID, row snapshot, and `created` boolean.
- Add `claim_failed_job_recovery(failed_attempt_id, fiscal_year, period_start, period_end)`:
  - validate argument types and period;
  - begin one PostgreSQL transaction and take the existing `_attempt_lock_key` advisory transaction lock;
  - select the exact source row `FOR UPDATE` and require source `usaspending`, matching bounds/FY, status `failed`, `last_error_code == 'http_404'`, valid expected rows `0..DEFAULT_MAX_ROWS`, terminal timestamp, and no `recovery_of_attempt_id`;
  - require either all three job fields or none; none means not eligible, partial means corrupt; validate the complete job locally before any insert;
  - require `export_rows`, `export_columns`, archive hash/bytes, loaded rows, signed total, and checkpoint ID all null;
  - conservatively require zero USAspending checkpoints for the exact period and zero transaction rows whose `action_date` is inside the period;
  - select existing rows linked by `recovery_of_attempt_id`; return the single row after verifying exact copied identity and allowed recovery states, or fail on any inconsistency;
  - otherwise insert exactly one `submitted` recovery with copied source/FY/bounds/expected count/job identity/parent, `recovery_of_attempt_id=failed_attempt_id`, zero failures, fresh timestamps, and all export/archive/load/checkpoint fields null;
  - translate uniqueness races to a stable concurrency error or reread and return the one valid linked row. Never update the original.
- Extract the current post-submission branches into `_resume_persisted_attempt(...)`. It accepts an already-selected attempt and only recognizes `submitted`, `export_finished`, `archive_hashed`, `loading`, and terminal `completed|failed`; it contains no count, claim, or submission logic.
- Add `resume_failed_job_recovery(recovery_attempt_id, expected_failed_attempt_id, ..., capacity_guard)` that locks/reads the exact linked row, proves lineage and period, and calls only `_resume_persisted_attempt` with `initial_status_404_grace=False`.
- Keep `run_period` as the ordinary entry. After `_locate_or_create_attempt`, reject any row with non-null `recovery_of_attempt_id` so a normal traversal cannot resume recovery without exact authorization. Delegate ordinary post-submission states to the same extracted helper to avoid behavioral drift.
- Before persisting `submitted -> export_finished` for a recovery, require the poll result's status URL, file URL, and filename to equal the claimed persisted identity byte-for-byte. A changed identity is a deterministic failed recovery, not a redirect to another job.
- Preserve all existing deterministic/transient failure handling, archive hashing, load atomicity, and checkpoint completion. A recovery 404 has no initial grace, marks only the recovery row `failed/http_404`, and leaves the original unchanged.

### Coordinator and traversal contract

`services/api/app/usaspending/transaction_backfill.py` — `BackfillCoordinator.run`, `_validate_invocation`, `_process_period`, new `_run_same_job_recovery`, and `sanitize_progress_record`:

- Thread `recover_failed_job_attempt_id: UUID | None` separately from `retry_failed_attempt_id`; reject both together.
- Require same-job recovery to have exactly one explicitly selected period and `max_periods == 1`. `max_leaf_submissions` remains a new-submission bound: recovery neither consumes it nor is blocked by it.
- In `_process_period`, take the dedicated recovery branch before normal failed replacement. Claim/reuse the exact linked attempt, run overlap validation against the recovery row, reserve storage under its UUID using `export_rows` or `DEFAULT_MAX_ROWS`, and invoke only `resume_failed_job_recovery`.
- Never call `_run_leaf` or pass a submission observer in the recovery branch. `_submitted` must remain zero. `_processed` remains one for the selected leaf.
- For the ordinary failed branch, classify job metadata before authorizing replacement: all absent retains current exact-ID replacement behavior; partial or complete job identity rejects new submission. Also reject ordinary retry of any row with non-null recovery lineage.
- Preserve `_period_attempt`, `_verified_children`, root ordering, and sibling selection. The CLI's exact-leaf requirement ensures recovery does not traverse or process the pristine sibling; normal root traversal encountering a recovery row fails for missing recovery authorization rather than silently resuming it.
- Emit `same_job_recovery_claimed` for a new row or `same_job_recovery_resumed` for an existing row, with `attempt_id` as recovery ID and new safe UUID field `recovery_of_attempt_id` as the original ID. Continue `leaf_completed` after verification.
- Add `recovery_of_attempt_id` to `sanitize_progress_record.optional_uuids`; do not allow filename or URL fields into progress/manifests.

### CLI contract

`services/api/app/usaspending/transaction_backfill_cli.py` — `_Runtime.run`, `build_parser`, `parse_args`, and `main`:

- Add `--recover-failed-job-attempt-id UUID`, following the checked `--retry-failed-attempt-id` naming convention while stating distinct semantics.
- Make it mutually exclusive with `--retry-failed-attempt-id` at parser validation; parse both as exact UUIDs with safe generic errors.
- Recovery is live-only and requires `--period-start`, `--period-end`, exactly one root, `--max-periods 1`, the existing exact `--confirm RUN-<start>-THROUGH-<end>` token, manifest/backup/storage prerequisites, and the global lease. Reject root-month/FY broad selection.
- Pass the UUID through `_Runtime.run` and `main` without changing dry-run behavior for ordinary invocations.
- Map stable recovery authorization/integrity errors to `EXIT_OPERATOR_RESOLUTION`; malformed CLI values remain `EXIT_INVALID_ARGUMENTS`, and operational failures remain `EXIT_OPERATION_FAILED`.
- The completed/stopped manifest proves zero new submissions with `submitted_leaves: 0` and an explicit same-job recovery event containing both UUIDs. It never includes remote job values.

## 7. Implementation Sequence

1. Add migration `20261017_0007` and model parity for `recovery_of_attempt_id`, self-FK/check, and unique lineage index. Add downgrade-use guard and schema tests before application code depends on the column.
2. Add local persisted-job identity validation to `UsaSpendingClient` with table-driven pure tests. Do not alter retry counts, POST payloads, or poll grace.
3. Add `RecoveryClaim` and the atomic `claim_failed_job_recovery` transaction. Test all eligibility predicates, original-row immutability, one-row uniqueness, and concurrent claims before adding network progress.
4. Extract the existing post-submission state machine into `_resume_persisted_attempt`, keep normal `run_period` behavior byte-for-byte equivalent at its boundaries, and add the exact-lineage recovery resume entry that never calls `run_period`.
5. Add coordinator authorization/classification: dedicated exact-leaf recovery branch, ordinary retry rejection for complete/partial job identity, truthful counters/bounds, storage handling, and explicit progress events.
6. Add the CLI flag and mutual-exclusion/exact-selection/confirmation validation; thread it through protocol and `main`; update exit-code and stopped-manifest coverage.
7. Add end-to-end integration and concurrency scenarios, then run focused, disposable-PostgreSQL, and full regression commands. Re-run GitNexus `impact` on every edited existing symbol and `detect_changes(scope=all)`; a partial/truncated result is not acceptable. Do not regenerate any unrelated artifact.

## 8. Test Strategy

`services/api/tests/test_usaspending_bulk_client.py`:

- Complete safe persisted identity -> local validator accepts without issuing a request.
- Unsafe status/file hosts, credentials/ports, path traversal, slash/backslash/NUL filename, encoded separators, non-zip name, file URL basename mismatch, or conflicting repeated status `file_name` query -> stable failure before network.
- Existing POST response and all initial-status-404 grace tests remain unchanged.

`services/api/tests/test_usaspending_transaction_attempt_models.py`:

- Model exposes the new nullable FK/check/index.
- Original failed child plus one linked submitted recovery succeeds and preserves both lineage axes.
- A second recovery for the same source fails, including after the first is failed or completed.
- Self-recovery and deleting the source while a recovery exists fail.

`services/api/tests/test_usaspending_transaction_attempt_migration.py` and `services/api/tests/test_migrations.py`:

- `0007` matches the model, chains from `0006`, compiles bounded PostgreSQL identifiers, preserves populated legacy rows with null lineage, enforces uniqueness/FK/check, and refuses downgrade after lineage is used.
- Alembic has exactly one head, `20261017_0007`; explicit disposable-database upgrade reaches it.

`services/api/tests/test_usaspending_transaction_workflow.py`:

- Failed `http_404` child + exact UUID + complete safe identity + no archive/checkpoint/rows -> claim inserts one linked `submitted` row, copies all identity fields exactly, and leaves the original byte-for-byte unchanged.
- Missing/partial/unsafe/inconsistent identity; wrong status/error/FY/period/UUID; any export/archive/load/checkpoint field; existing exact-period checkpoint; or existing period rows -> no claim and no client call.
- Existing linked recovery -> return/resume it without inserting another; completed linked recovery returns completed; failed linked recovery fails closed and cannot create another.
- Two concurrent claims -> one recovery row, one returned identity, zero POST calls.
- Recovery `running`/`ready` polls only the copied status URL; finished identity must exactly match; ready completion downloads, validates, loads, checkpoints, and completes once.
- Recovery `http_404`, remote rejection, unsafe changed identity, validation failure, archive mismatch, or load failure -> recovery is failed/resumable according to existing rules, original remains unchanged, and no POST occurs.
- Interruption after claim commit -> next exact recovery invocation resumes the same recovery ID and job with zero POST; interruption/load failure after checkpoint work retains existing atomic rollback behavior.
- An exploding `submit_bulk_export` fake plus a source-structure assertion over the dedicated recovery entry/helper proves the recovery call graph contains no call to `run_period`, `_locate_or_create_attempt`, or `submit_bulk_export`.
- Normal `run_period` rejects an active recovery row without exact recovery authorization.
- Existing ordinary failed history without any job identity still creates a new retry; existing submission ownership, resumed-state, 404-grace, and `USASPENDING_MAX_RETRIES=0` tests remain unchanged.

`services/api/tests/test_usaspending_transaction_backfill.py`:

- Exact failed child recovery completes the left leaf, leaves root and pristine sibling unchanged, reports `submitted_leaves == 0`, and records claim/completion events.
- `max_periods=1` bounds recovery; `max_leaf_submissions` is neither consumed nor used to block it; storage reservations use recovery ID and are completed once.
- Missing/wrong authorization, both authorization flags, broad root selection, complete/partial job metadata through ordinary retry, existing linked recovery conflict, and overlap state fail closed.
- Existing `test_authorized_failed_attempt_creates_and_completes_a_new_attempt` and failed-subdivided-child replacement remain valid for failures with all job fields absent.

`services/api/tests/test_usaspending_transaction_backfill_cli.py`:

- New flag parses one UUID, rejects duplicates/invalid UUID/dry-run/broad selection/wrong bounds/missing `--max-periods 1`, and is mutually exclusive with retry.
- Valid live dispatch passes both authorization parameters distinctly.
- Final and stopped manifests allow only the new UUID lineage field, emit explicit same-job recovery event, retain `submitted_leaves: 0`, and contain no URL/filename.
- Existing confirmation, finite-bound, reservation, cancellation, and sanitization tests remain green.

Verification commands already supported by the repository (not run during planning):

```bash
cd services/api && .venv/bin/pytest -q \
  tests/test_usaspending_bulk_client.py \
  tests/test_usaspending_transaction_attempt_models.py \
  tests/test_usaspending_transaction_attempt_migration.py \
  tests/test_usaspending_transaction_workflow.py \
  tests/test_usaspending_transaction_backfill.py \
  tests/test_usaspending_transaction_backfill_cli.py \
  tests/test_migrations.py
```

```bash
cd services/api && \
TEST_DATABASE_URL="${GOVTRACTS_SAME_JOB_RECOVERY_TEST_DATABASE_URL:?explicit postgresql+psycopg disposable _test URL required}" \
.venv/bin/pytest -q -m integration \
  tests/test_usaspending_transaction_attempt_migration.py \
  tests/test_usaspending_transaction_workflow.py \
  tests/test_usaspending_transaction_backfill.py \
  tests/test_migrations.py
```

```bash
cd services/api && .venv/bin/pytest -q
```

## 9. Risk and Impact Analysis

- HIGH — coordinator `run`: 19 direct tests call it. Keep the new parameter optional, preserve defaults, and exercise every existing backfill test named in §4.
- HIGH — workflow `run_period`: 55 upstream impacts. Extract post-submission behavior without changing statuses, error codes, observer timing, grace, storage callbacks, or result fields; parity tests for every restart state are mandatory.
- HIGH/lower-bound — attempt model: schema changes touch Alembic, routes, ingestion, workflow/backfill, and tests. Migration/model parity plus full test suite are required; source search resolved the graph boundary sufficiently for planning, not for skipping regression.
- HIGH at depth 3 — `_locate_or_create_attempt`: do not change its creation semantics. Put the recovery-row authorization guard in `run_period` and use a separate claim method.
- CRITICAL — manifest sanitization: add only one UUID field, retain the closed allowlist, and test direct consumers `_record`, CLI `progress`, and `safe_result_manifest`.
- MEDIUM — client/workflow classes and CLI main: fake clients/runtime protocols will need compatible methods/signatures; keep all additions optional except on the dedicated path.
- Concurrency: application locks alone are insufficient. Use both the period advisory transaction lock and a database unique lineage index; catch `IntegrityError` only to reread and verify the single winner.
- Transaction integrity: the claim transaction may not perform network/I/O. The load transaction remains unchanged and owns checkpoint, rows, and attempt completion atomically.
- Migration: a nullable column is online-friendly, but unique-index creation can still lock/write catalog state. Existing rows are null, so no data rewrite is planned. The downgrade guard prevents lineage loss.
- Compatibility: ordinary failed attempts with no persisted job remain replacement-submittable. Complete or partial accepted-job metadata intentionally changes from “replacement may POST” to an operator-resolution error unless the dedicated recovery flag is used.
- Performance: eligibility adds bounded point/range counts for one exact period under explicit operator invocation. Use existing indexed source/period/action-date columns; do not scan outside the selected period.
- Observability: manifests prove lineage and zero submissions without disclosing remote URLs or filenames. Stable error codes must distinguish ineligible, corrupt, already-recovered, unauthorized, and concurrency cases.

## 10. Files Expected to Change

| File | Symbols | Reason |
| ---- | ------- | ------ |
| `services/api/app/db/models.py` | `UsaSpendingTransactionIngestionAttempt` | Add immutable recovery lineage and uniqueness. |
| `services/api/alembic/versions/20261017_0007_usaspending_same_job_recovery.py` | `upgrade`, `downgrade` | Apply/reverse the lineage schema safely. |
| `services/api/app/usaspending/client.py` | `UsaSpendingClient` new persisted-job validator | Validate recovery identity locally before network. |
| `services/api/app/usaspending/transaction_workflow.py` | `TransactionIngestionWorkflow.run_period`; new claim/resume/helper symbols | Atomic claim plus structurally no-POST post-submission execution. |
| `services/api/app/usaspending/transaction_backfill.py` | `BackfillCoordinator.run`, `_validate_invocation`, `_process_period`; new recovery runner; `sanitize_progress_record` | Exact authorization, traversal/counter semantics, and audit events. |
| `services/api/app/usaspending/transaction_backfill_cli.py` | `_Runtime.run`, `build_parser`, `parse_args`, `main` | Add the distinct exact-UUID operator contract. |
| `services/api/tests/test_usaspending_bulk_client.py` | new identity-validation tests | Safe/unsafe persisted metadata. |
| `services/api/tests/test_usaspending_transaction_attempt_models.py` | model/constraint tests | FK/check/unique lineage contract. |
| `services/api/tests/test_usaspending_transaction_attempt_migration.py` | `0007` parity/live tests | Upgrade, enforcement, preservation, downgrade guard. |
| `services/api/tests/test_usaspending_transaction_workflow.py` | claim/recovery/concurrency/restart tests | No-POST state machine and atomic behavior. |
| `services/api/tests/test_usaspending_transaction_backfill.py` | coordinator recovery tests | Bounds, counters, tree/sibling preservation, ordinary retry regression. |
| `services/api/tests/test_usaspending_transaction_backfill_cli.py` | parser/dispatch/manifest tests | Operator authorization and reporting. |
| `services/api/tests/test_migrations.py` | head assertion | Advance the single Alembic head to `0007`. |

## 11. Reusable Implementation Context

```yaml
implementation_context:
  task_summary: 'Implement an exact-UUID, same-job recovery path that creates one linked submitted attempt and cannot reach the USAspending export POST.'
  acceptance_criteria:
    - 'Original failed/http_404 attempt is never mutated.'
    - 'Exactly one recovery row can link to an eligible failed attempt.'
    - 'Recovery polls/downloads the exact persisted job and has no reachable submit_bulk_export call.'
    - 'Partial/unsafe metadata or any prior archive/checkpoint/period rows fails closed.'
    - 'Recovery requires exact CLI UUID, exact leaf period, existing confirmation, and max_periods=1.'
    - 'submitted_leaves remains zero and manifests expose safe lineage events only.'
    - 'Ordinary no-job failed retries preserve current replacement behavior.'

  evidence_provenance:
    schema_version: 2
    head_commit: 'eb19b1b36e1379fda60579374d28bd9717d17403'
    generated_plan_path: 'docs/plans/2026-10-17-gitnexus-plan-same-job-recovery.md'
    global_dirty_digest:
      algorithm: 'sha256'
      canonicalization: 'gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records'
      value: '0a9c85780067d9afcd0764f307b60891e3cee927ee11eaeb5ec7826d10fd82cd'
    cited_path_manifest:
      - path: '.gitnexus/run.cjs'
        object_kind: {head: 'absent', index: 'absent', worktree: 'absent', untracked: 'regular'}
        state: 'untracked'
        rename_from: null
        rename_to: null
        head_digest: 'absent'
        index_digest: 'absent'
        worktree_digest: 'absent'
        untracked_digest: 'sha256:c53de9bb30f405804b439b9623d3c5bb11b54fd92bee21e9b52e35b03b159aa7'
      - path: 'services/api/alembic/versions/20261017_0007_usaspending_same_job_recovery.py'
        object_kind: {head: 'absent', index: 'absent', worktree: 'absent', untracked: 'absent'}
        state: 'absent'
        rename_from: null
        rename_to: null
        head_digest: 'absent'
        index_digest: 'absent'
        worktree_digest: 'absent'
        untracked_digest: 'absent'
      - path: 'services/api/app/db/models.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:c6f513729c28fc861a3252ca64cfcaaa642f7287a2cfed139f1a04a84e1a7e9c'
        index_digest: 'sha256:c6f513729c28fc861a3252ca64cfcaaa642f7287a2cfed139f1a04a84e1a7e9c'
        worktree_digest: 'sha256:c6f513729c28fc861a3252ca64cfcaaa642f7287a2cfed139f1a04a84e1a7e9c'
        untracked_digest: 'absent'
      - path: 'services/api/app/usaspending/client.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:cf9e65671e46b32e041ff877bcf94d2103f1f43aaa4f5c1f675bd4c695d5d7b0'
        index_digest: 'sha256:cf9e65671e46b32e041ff877bcf94d2103f1f43aaa4f5c1f675bd4c695d5d7b0'
        worktree_digest: 'sha256:cf9e65671e46b32e041ff877bcf94d2103f1f43aaa4f5c1f675bd4c695d5d7b0'
        untracked_digest: 'absent'
      - path: 'services/api/app/usaspending/transaction_backfill.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:b879110377cab2a3320d234479b3ddfec569f3922d6ca79bf80b125298886d56'
        index_digest: 'sha256:b879110377cab2a3320d234479b3ddfec569f3922d6ca79bf80b125298886d56'
        worktree_digest: 'sha256:b879110377cab2a3320d234479b3ddfec569f3922d6ca79bf80b125298886d56'
        untracked_digest: 'absent'
      - path: 'services/api/app/usaspending/transaction_backfill_cli.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:364b20f0c3758a916915b02e1acf7756a08cdb0c184f8d77d78e6361aac08d83'
        index_digest: 'sha256:364b20f0c3758a916915b02e1acf7756a08cdb0c184f8d77d78e6361aac08d83'
        worktree_digest: 'sha256:364b20f0c3758a916915b02e1acf7756a08cdb0c184f8d77d78e6361aac08d83'
        untracked_digest: 'absent'
      - path: 'services/api/app/usaspending/transaction_ingestion.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:f8b1685ecf071091f23f58e48fbca4c528c65ab87efe9b5cd7efc5f1923afd2f'
        index_digest: 'sha256:f8b1685ecf071091f23f58e48fbca4c528c65ab87efe9b5cd7efc5f1923afd2f'
        worktree_digest: 'sha256:f8b1685ecf071091f23f58e48fbca4c528c65ab87efe9b5cd7efc5f1923afd2f'
        untracked_digest: 'absent'
      - path: 'services/api/app/usaspending/transaction_workflow.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:e5797e901b49136825663040de45d79c05f674a7a00cfc256e78e2fd394c0309'
        index_digest: 'sha256:e5797e901b49136825663040de45d79c05f674a7a00cfc256e78e2fd394c0309'
        worktree_digest: 'sha256:e5797e901b49136825663040de45d79c05f674a7a00cfc256e78e2fd394c0309'
        untracked_digest: 'absent'
      - path: 'services/api/pyproject.toml'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:a9355439dcea53320d8cba1c386b1f8986d6168ba6d1fc23d83699df837fe921'
        index_digest: 'sha256:a9355439dcea53320d8cba1c386b1f8986d6168ba6d1fc23d83699df837fe921'
        worktree_digest: 'sha256:a9355439dcea53320d8cba1c386b1f8986d6168ba6d1fc23d83699df837fe921'
        untracked_digest: 'absent'
      - path: 'services/api/tests/test_migrations.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:c822709dbd02342bd45965bfe44838682c1136ab5762d4c4f14ecf7fcaae765a'
        index_digest: 'sha256:c822709dbd02342bd45965bfe44838682c1136ab5762d4c4f14ecf7fcaae765a'
        worktree_digest: 'sha256:c822709dbd02342bd45965bfe44838682c1136ab5762d4c4f14ecf7fcaae765a'
        untracked_digest: 'absent'
      - path: 'services/api/tests/test_usaspending_bulk_client.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:0cfe2f22bccba03d3043083beec74c8fd472526ef488f5f2047537111dbfd87a'
        index_digest: 'sha256:0cfe2f22bccba03d3043083beec74c8fd472526ef488f5f2047537111dbfd87a'
        worktree_digest: 'sha256:0cfe2f22bccba03d3043083beec74c8fd472526ef488f5f2047537111dbfd87a'
        untracked_digest: 'absent'
      - path: 'services/api/tests/test_usaspending_transaction_attempt_migration.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:e9859aea449cc9aa5ce5fd2cc6f294d40ac9d27ed4a0b16d91647be71bef8213'
        index_digest: 'sha256:e9859aea449cc9aa5ce5fd2cc6f294d40ac9d27ed4a0b16d91647be71bef8213'
        worktree_digest: 'sha256:e9859aea449cc9aa5ce5fd2cc6f294d40ac9d27ed4a0b16d91647be71bef8213'
        untracked_digest: 'absent'
      - path: 'services/api/tests/test_usaspending_transaction_attempt_models.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:0668c9cfa1ade9c37737aa44ea2ad2ed972a02c07b93eb1ea72a9317a64b7d25'
        index_digest: 'sha256:0668c9cfa1ade9c37737aa44ea2ad2ed972a02c07b93eb1ea72a9317a64b7d25'
        worktree_digest: 'sha256:0668c9cfa1ade9c37737aa44ea2ad2ed972a02c07b93eb1ea72a9317a64b7d25'
        untracked_digest: 'absent'
      - path: 'services/api/tests/test_usaspending_transaction_backfill.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:58ee3c70111c34709c7f497debab0133018cbe8bc6ef68153ca866a4d454d9c6'
        index_digest: 'sha256:58ee3c70111c34709c7f497debab0133018cbe8bc6ef68153ca866a4d454d9c6'
        worktree_digest: 'sha256:58ee3c70111c34709c7f497debab0133018cbe8bc6ef68153ca866a4d454d9c6'
        untracked_digest: 'absent'
      - path: 'services/api/tests/test_usaspending_transaction_backfill_cli.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:198d53e10372e4542639c422cb1784e1767c25063c94384937b87701b0eafedf'
        index_digest: 'sha256:198d53e10372e4542639c422cb1784e1767c25063c94384937b87701b0eafedf'
        worktree_digest: 'sha256:198d53e10372e4542639c422cb1784e1767c25063c94384937b87701b0eafedf'
        untracked_digest: 'absent'
      - path: 'services/api/tests/test_usaspending_transaction_workflow.py'
        object_kind: {head: 'regular', index: 'regular', worktree: 'regular', untracked: 'absent'}
        state: 'clean'
        rename_from: null
        rename_to: null
        head_digest: 'sha256:206426143c4bdd5c3aed0e2545f884caa5f98b551140860df5e544d841dee6d0'
        index_digest: 'sha256:206426143c4bdd5c3aed0e2545f884caa5f98b551140860df5e544d841dee6d0'
        worktree_digest: 'sha256:206426143c4bdd5c3aed0e2545f884caa5f98b551140860df5e544d841dee6d0'
        untracked_digest: 'absent'

  primary_symbols:
    - symbol: 'UsaSpendingTransactionIngestionAttempt'
      file: 'services/api/app/db/models.py'
      lines: '204-361'
      role: 'Durable attempt state, tree ancestry, and new recovery lineage.'
    - symbol: 'TransactionIngestionWorkflow.run_period / new claim and resume entries'
      file: 'services/api/app/usaspending/transaction_workflow.py'
      lines: '160-529'
      role: 'Separate submission-capable execution from persisted-job-only execution.'
    - symbol: 'BackfillCoordinator.run / _process_period'
      file: 'services/api/app/usaspending/transaction_backfill.py'
      lines: '556-782'
      role: 'Exact authorization, traversal, bounds, storage, and counters.'
    - symbol: 'UsaSpendingClient persisted-job validator'
      file: 'services/api/app/usaspending/client.py'
      lines: '162-343,585-600'
      role: 'Offline validation of URL/filename identity before recovery network use.'
    - symbol: 'build_parser / parse_args / main'
      file: 'services/api/app/usaspending/transaction_backfill_cli.py'
      lines: '97-216,319-431'
      role: 'Distinct exact-UUID operator authorization and dispatch.'

  related_symbols:
    - symbol: 'TransactionIngestionWorkflow._locate_or_create_attempt'
      relationship: 'CALLS from run_period'
      relevance: 'Normal failed history creates a new submission-capable row; recovery must bypass it.'
    - symbol: 'TransactionIngestionWorkflow._job_from_attempt'
      relationship: 'post-submission metadata reconstruction'
      relevance: 'Reconstructs the persisted job consumed by polling/download.'
    - symbol: 'TransactionIngestionWorkflow._load_atomically'
      relationship: 'post-submission terminal path'
      relevance: 'Locks attempt and keeps load/checkpoint/attempt completion atomic.'
    - symbol: 'TransactionIngestionLoader._load_snapshot'
      relationship: 'CALLED by load_with_connection'
      relevance: 'Checkpoint and transaction ownership boundary remains unchanged.'
    - symbol: 'sanitize_progress_record'
      relationship: 'CALLS from _record, CLI progress, safe_result_manifest'
      relevance: 'Closed manifest schema must admit lineage UUID but no remote metadata.'

  execution_path:
    - 'CLI validates exact leaf, confirmation, max_periods=1, and mutually exclusive recovery UUID.'
    - 'Coordinator enters the dedicated recovery branch and never the ordinary retry/submission branch.'
    - 'Workflow claim transaction locks the period and failed row, validates absence/identity, and inserts or reuses one linked submitted attempt.'
    - 'Coordinator records safe lineage, reserves storage, and calls only the persisted-attempt resume entry.'
    - 'Resume polls the copied status URL with no initial grace, requires unchanged job identity, downloads, hashes, and enters existing atomic load.'
    - 'Loader atomically owns rows/checkpoint; workflow completes only the recovery attempt; coordinator verifies and reports zero submissions.'

  pdg_constraints:
    - description: 'Post-submission failures and transitions are control-dependent within run_period; extract them as one parity-preserving helper.'
      affected_statements: ['services/api/app/usaspending/transaction_workflow.py:314', 'services/api/app/usaspending/transaction_workflow.py:374', 'services/api/app/usaspending/transaction_workflow.py:490']
      implementation_consequence: 'Recovery may call only this helper; helper must contain no count, locate/create, or submit branch.'
    - description: 'Checkpoint ID flows through staged row ownership, checkpoint insert/update, reconciliation, and result in one transaction.'
      affected_statements: ['services/api/app/usaspending/transaction_ingestion.py:312', 'services/api/app/usaspending/transaction_ingestion.py:359', 'services/api/app/usaspending/transaction_ingestion.py:381', 'services/api/app/usaspending/transaction_ingestion.py:422']
      implementation_consequence: 'Do not split or bypass the existing load_with_connection/_complete_attempt transaction.'
    - description: 'Coordinator PDG controls/data were unavailable in the stale incomplete index.'
      affected_statements: ['services/api/app/usaspending/transaction_backfill.py:629', 'services/api/app/usaspending/transaction_backfill.py:666', 'services/api/app/usaspending/transaction_backfill.py:776']
      implementation_consequence: 'Use source-defined ordering and re-run PDG after a provenance-pinned analyzer becomes available.'

  architectural_patterns:
    - pattern: 'PostgreSQL advisory transaction lock plus unique index for concurrent attempt claims'
      example_location: 'services/api/app/usaspending/transaction_workflow.py:_locate_or_create_attempt'
      usage_guidance: 'Reuse the same period lock key, lock the source row, and let uniqueness arbitrate races.'
    - pattern: 'Caller-owned atomic load and attempt completion'
      example_location: 'services/api/app/usaspending/transaction_workflow.py:_load_atomically'
      usage_guidance: 'Keep loader, checkpoint, rows, and recovery-attempt completion in one transaction.'
    - pattern: 'Explicit safe manifest allowlist'
      example_location: 'services/api/app/usaspending/transaction_backfill.py:sanitize_progress_record'
      usage_guidance: 'Add only recovery_of_attempt_id as UUID; never emit job URLs or filename.'

  files_to_modify:
    - file: 'services/api/app/db/models.py'
      symbols: ['UsaSpendingTransactionIngestionAttempt']
      intended_change: 'Add recovery lineage FK/check/unique index.'
    - file: 'services/api/alembic/versions/20261017_0007_usaspending_same_job_recovery.py'
      symbols: ['upgrade', 'downgrade']
      intended_change: 'Migrate nullable lineage and guarded downgrade.'
    - file: 'services/api/app/usaspending/client.py'
      symbols: ['UsaSpendingClient.validate_persisted_bulk_export_job']
      intended_change: 'Offline safe/internal identity validation.'
    - file: 'services/api/app/usaspending/transaction_workflow.py'
      symbols: ['TransactionIngestionWorkflow.run_period', 'claim_failed_job_recovery', 'resume_failed_job_recovery', '_resume_persisted_attempt']
      intended_change: 'Atomic linked claim and structurally isolated no-POST resume.'
    - file: 'services/api/app/usaspending/transaction_backfill.py'
      symbols: ['BackfillCoordinator.run', '_validate_invocation', '_process_period', '_run_same_job_recovery', 'sanitize_progress_record']
      intended_change: 'Dedicated authorization/traversal/counter/reporting path.'
    - file: 'services/api/app/usaspending/transaction_backfill_cli.py'
      symbols: ['_Runtime.run', 'build_parser', 'parse_args', 'main']
      intended_change: 'Add --recover-failed-job-attempt-id contract.'
    - file: 'services/api/tests/test_usaspending_bulk_client.py'
      symbols: ['new persisted identity validation tests']
      intended_change: 'Safe and unsafe local metadata coverage.'
    - file: 'services/api/tests/test_usaspending_transaction_attempt_models.py'
      symbols: ['model lineage tests']
      intended_change: 'Schema contract and uniqueness.'
    - file: 'services/api/tests/test_usaspending_transaction_attempt_migration.py'
      symbols: ['0007 parity/live tests']
      intended_change: 'Migration preservation/enforcement/downgrade coverage.'
    - file: 'services/api/tests/test_usaspending_transaction_workflow.py'
      symbols: ['claim/resume/concurrency tests']
      intended_change: 'Core no-POST and fail-closed matrix.'
    - file: 'services/api/tests/test_usaspending_transaction_backfill.py'
      symbols: ['coordinator same-job recovery tests']
      intended_change: 'Bounds, tree, counters, and retry regression.'
    - file: 'services/api/tests/test_usaspending_transaction_backfill_cli.py'
      symbols: ['CLI recovery tests']
      intended_change: 'Authorization, dispatch, and manifest contract.'
    - file: 'services/api/tests/test_migrations.py'
      symbols: ['test_*_is_the_only_head_revision']
      intended_change: 'Expect 20261017_0007 as the sole head.'

  tests:
    - file: 'services/api/tests/test_usaspending_bulk_client.py'
      scenarios: ['safe identity -> accepted offline', 'unsafe URL/filename or inconsistent tuple -> rejected before request']
    - file: 'services/api/tests/test_usaspending_transaction_attempt_models.py'
      scenarios: ['one linked recovery -> accepted', 'second/self/orphan/delete conflict -> rejected']
    - file: 'services/api/tests/test_usaspending_transaction_attempt_migration.py'
      scenarios: ['0006 -> 0007 preserves rows', 'constraints/index enforce lineage', 'used downgrade -> guarded stop']
    - file: 'services/api/tests/test_usaspending_transaction_workflow.py'
      scenarios: ['eligible failure -> one submitted recovery and original unchanged', 'running/ready/404/validation/load branches -> same job, zero POST', 'two claims -> one recovery', 'restart after commit -> same recovery']
    - file: 'services/api/tests/test_usaspending_transaction_backfill.py'
      scenarios: ['exact child recovery -> root/sibling unchanged and submitted_leaves=0', 'ordinary no-job retry -> existing replacement behavior', 'partial/complete metadata ordinary retry -> blocked']
    - file: 'services/api/tests/test_usaspending_transaction_backfill_cli.py'
      scenarios: ['exact flag contract -> dispatch', 'mutual exclusion/broad selection/bounds -> invalid', 'manifest -> safe lineage plus zero submissions']
    - file: 'services/api/tests/test_migrations.py'
      scenarios: ['single head -> 20261017_0007', 'disposable PostgreSQL upgrade -> head']

  verification_commands:
    - 'cd services/api && .venv/bin/pytest -q tests/test_usaspending_bulk_client.py tests/test_usaspending_transaction_attempt_models.py tests/test_usaspending_transaction_attempt_migration.py tests/test_usaspending_transaction_workflow.py tests/test_usaspending_transaction_backfill.py tests/test_usaspending_transaction_backfill_cli.py tests/test_migrations.py'
    - 'cd services/api && TEST_DATABASE_URL="${GOVTRACTS_SAME_JOB_RECOVERY_TEST_DATABASE_URL:?explicit postgresql+psycopg disposable _test URL required}" .venv/bin/pytest -q -m integration tests/test_usaspending_transaction_attempt_migration.py tests/test_usaspending_transaction_workflow.py tests/test_usaspending_transaction_backfill.py tests/test_migrations.py'
    - 'cd services/api && .venv/bin/pytest -q'
    - 'node .gitnexus/run.cjs impact "<each-edited-existing-symbol>" --direction upstream --depth 3 --repo .'
    - 'node .gitnexus/run.cjs detect-changes --scope all --repo .'

  risks:
    - 'HIGH: run_period refactor has 55 upstream impacts; parity tests for every current state are mandatory.'
    - 'HIGH/lower-bound: model changes cross SQLAlchemy dispatch; migration parity and full suite are mandatory.'
    - 'CRITICAL: manifest sanitizer feeds seven reporting processes; retain a closed allowlist.'
    - 'Stale index has no coordinator PDG; rerun graph checks only with a pinned/current analyzer.'
    - 'Unique recovery lineage is intentionally one-ever, so a failed recovery requires a new reviewed design or manual intervention.'

  assumptions:
    - 'The live accepted job file URL path decodes to the persisted filename; verify with read-only data before executing recovery, and fail closed if it does not.'
    - 'Exact-period zero transaction rows is the conservative ownership proof because transactions link to checkpoints, not directly to attempts; verify schema remains unchanged before implementation.'
    - 'Revision 20261017_0007 is still the next Alembic revision at implementation time; re-check ScriptDirectory heads before creating it.'

  open_questions:
    - 'Should a future separately reviewed tool allow a second recovery after the one linked recovery fails? This plan deliberately says no.'
    - 'Should the persisted-job validator require a narrower status endpoint path after inspecting real accepted-job URLs? Host/credential/port validation plus optional query consistency is the planned minimum.'

  avoid:
    - 'Do not repeat full repository discovery unless HEAD or the provenance digest changed.'
    - 'Do not replace established patterns without evidence.'
    - 'Do not call run_period, _locate_or_create_attempt, or submit_bulk_export from the recovery entry/helper.'
    - 'Do not mutate the original failed attempt.'
    - 'Do not overload --retry-failed-attempt-id with same-job semantics.'
    - 'Do not emit remote filename or URLs in manifests/logs.'
    - 'Do not contact USAspending or use retained live state in tests.'
```

## 12. Assumptions and Open Questions

Assumptions to re-verify before implementation:

- [assumed] The real persisted file URL's decoded basename equals `All_PrimeTransactions_2026-10-18_H03M36S44778790.zip`. If not, the implementation must fail closed; do not weaken validation without a new evidence-based decision.
- [verified] Transactions have checkpoint ownership but no direct attempt ID. The claim therefore uses zero exact-period transactions plus zero exact-period checkpoints as the conservative “no owned rows” predicate.
- [assumed] `20261017_0007` remains the next Alembic revision. Re-read the current head before adding the migration.
- [assumed] A single recovery attempt is sufficient. This plan intentionally blocks a second recovery even after the recovery fails; any later retry needs separate operator classification.

Open questions that do not block this implementation:

- After real URL evidence is safely inspected, should the local validator constrain the status URL path more narrowly than current exact-host HTTPS checks?
- Should a later project add a read-only operator command that reports recovery eligibility without claiming it? That is outside this minimal change.

## 13. Definition of Done

- Model and `0007` migration provide immutable recovery lineage, one-ever uniqueness, preservation, and guarded downgrade.
- The exact CLI flag is distinct from retry, UUID/period/confirmation/bounds are enforced, and both flags cannot coexist.
- Claiming validates the exact failed `http_404` row, all job identity, all empty archive/load/checkpoint/period state, and leaves the original unchanged.
- The dedicated recovery call graph reaches polling/download/load but not `submit_bulk_export`, normal `run_period`, or `_locate_or_create_attempt`; tests make any POST an immediate failure.
- Concurrent/restarted recovery returns the same unique linked row; no second recovery or active replacement can be created.
- Running/ready/finished/404/remote failure/validation/load/interruption paths are fail-closed and auditable.
- Root and pristine sibling remain unchanged; ordinary failures with all job fields absent keep current exact-authorized replacement behavior.
- `processed_periods == 1`, `submitted_leaves == 0`, storage accounting is accurate, and manifests contain explicit safe lineage events without remote metadata.
- Focused, disposable-PostgreSQL integration, and full repository tests pass.
- Fresh/pinned GitNexus impact and non-partial/non-truncated `detect_changes(scope=all)` are reviewed before commit; HIGH/CRITICAL results are reported, not waived.
