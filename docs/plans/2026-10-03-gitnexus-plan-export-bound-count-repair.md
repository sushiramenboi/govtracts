# GitNexus Engineering Plan

> Task: Repair USAspending transaction-count reconciliation so the pre-submission count remains audit evidence while the completed export's row count governs parsing, loading, checkpoints, and completion.
> Evidence verified at commit `b8c82ec48bfbdfcaf9d9aa06f8fa7c924cb1d359`; GitNexus index refreshed this session with `node .gitnexus/run.cjs analyze --index-only --pdg .` using GitNexus 1.6.12, pinned to the same commit with no incomplete reasons.
> Evidence provenance schema 2; global dirty digest `0a9c85780067d9afcd0764f307b60891e3cee927ee11eaeb5ec7826d10fd82cd`; cited-path manifest 18 sorted entries; exact generated plan path excluded.

## 1. Objective

Repair the non-atomic count/export mismatch without weakening any archive, CSV, transaction, fiscal-period, Decimal, uniqueness, or atomic-load invariant. Preserve the count endpoint value as pre-submission capacity/audit evidence; require, validate, and persist completed-export `total_rows`; use that export-bound count as the sole exact expectation for parser, staging, target, checkpoint, and completed-attempt reconciliation; expose pre-count, export count, loaded count, and drift safely to operators. [verified]

The terminal failed attempt `4ffa631e-60bd-4b7b-a935-20f4e14d3895` is historical evidence only. The repair must neither reopen nor mutate it; a later pilot may create a new attempt/export only under separate authorization. [verified: user-supplied production evidence]

## 2. Current Behaviour

- `UsaSpendingClient.get_expected_transaction_count()` validates the count endpoint as a nonnegative integer and `TransactionIngestionWorkflow.run()` persists it in attempt `expected_rows` during `created -> counted`. [verified: `services/api/app/usaspending/client.py:125-155`, `services/api/app/usaspending/transaction_workflow.py:129-206`]
- `poll_bulk_export()` validates status/file metadata but discards terminal `total_rows` and `total_columns`; `BulkExportStatus` has no fields for them. [verified: `services/api/app/usaspending/client.py:85-93,192-261`]
- The `submitted -> export_finished` transition persists URLs/name only. Restarts from `export_finished`, `archive_hashed`, or `loading` therefore have no export-bound row count. [verified: `services/api/app/usaspending/transaction_workflow.py:250-443`]
- `_load_atomically()` passes attempt `expected_rows`—the earlier count snapshot—to `load_with_connection()`. The loader correctly passes its exact expectation to the parser, reconciles stage/target totals, and writes it to checkpoint `expected_rows`. [verified: `services/api/app/usaspending/transaction_workflow.py:694-733`, `services/api/app/usaspending/transaction_ingestion.py:107-339,468-511`]
- `TransactionExportParser` correctly rejects excess rows immediately and final under-counts with `transaction_count_mismatch`; this is why the confirmed 490,381-row export failed against the pre-count 490,382 before the loader was reached. [verified: `services/api/app/usaspending/transaction_export.py:90-268`; production outcome supplied by user]
- Attempt `terminal_fields` currently requires completed `expected_rows = loaded_rows`, conflating pre-count with export truth. [verified: `services/api/app/db/models.py:204-312`, `services/api/alembic/versions/20260927_0004_usaspending_transaction_attempts.py:20-150`]

## 3. Relevant Architecture

- The synchronous legacy award-search path (`iter_award_pages`) and its tests are separate from the new bulk workflow; it must not change. [verified: `services/api/app/usaspending/client.py:107-123`, `services/api/tests/test_usaspending.py:33-76`]
- The workflow owns restart/concurrency state. It uses short conditional state transactions; network polling/downloading occur outside DB transactions; final loader, checkpoint, and attempt completion share one caller-owned transaction. [verified: `services/api/app/usaspending/transaction_workflow.py:129-443,535-585,694-762`]
- The parser owns the 500,000-row ceiling and all archive/CSV/identifier/date/type/Decimal validation. The loader owns immutable archive parsing, advisory locking, staging, exact count/distinct/total reconciliation, anti-join replacement, and checkpoint completion. Neither needs a new algorithm for this repair. [verified: `services/api/app/usaspending/transaction_export.py:35-45,90-550`, `services/api/app/usaspending/transaction_ingestion.py:107-339,468-548`]
- The attempt is audit/history; the checkpoint describes the committed snapshot. Attempt pre-count may drift, but checkpoint `expected_rows` must describe the exact export/loaded snapshot. [inferred from verified model and loader responsibilities]

## 4. GitNexus Findings

- `context({name:"BulkExportStatus"})` found workflow, CLI, legacy-ingestion file imports, and four focused test imports; source verification found only client/workflow/test constructors require updates. `impact(..., maxDepth:3)` is MEDIUM/exact with 7 direct importers. [graph] Key output: `"risk":"MEDIUM","direct":7`.
- `impact({target:"poll_bulk_export", direction:"upstream", maxDepth:3})` is MEDIUM/exact with 14 direct callers and the workflow `run` process; its d=1 set is workflow plus terminal/error/redirect/deadline client tests. [graph] Key output: `"impactedCount":33,"direct":14,"processes_affected":1`.
- `impact({target:"UsaSpendingTransactionIngestionAttempt", direction:"upstream", maxDepth:3})` is MEDIUM/lower-bound with 13 d=1 file imports because ORM `Base` dispatch obscures property consumers. Targeted source search resolved actual attempt-field use to the new workflow; health/market/legacy modules do not read these fields. [graph + verified] Key output: `"epistemic":"lower-bound","direct":13`.
- `impact({target:"TransactionIngestionWorkflow", ...})` and `impact({target:"TransactionWorkflowResult", ...})` are LOW/exact; direct consumers are the operator CLI and focused workflow/CLI tests, plus the workflow concurrency test subclass. [graph]
- `impact({target:"TransactionExportParser", ...})` is MEDIUM/exact with five d=1 importers; source verification shows the parser contract is already correct and should remain unchanged. [graph + verified]
- `impact({target:"load_with_connection", direction:"upstream", maxDepth:3})` is HIGH/exact because it participates in loader `load`, workflow `_load_atomically`, and 29 regression paths. Its four d=1 callers are the wrapper, workflow, and loader-test helpers; the plan changes the workflow-supplied value, not the loader algorithm/signature. [graph + verified] Key output: `"risk":"HIGH","direct":4,"processes_affected":3`.
- `impact({target:"transaction_cli.main", ..., maxDepth:1})` is MEDIUM/exact with nine d=1 callers, all self/test paths covering confirmation, redaction, cleanup, and single-period behavior. [graph]

## 5. Statement-Level PDG Findings

The initially fresh index lacked PDG, so the allowed one-time `--index-only --pdg` augmentation completed successfully. `pdg_query` then returned zero Python CDG/REACHING_DEF edges for `poll_bulk_export`, workflow `run`, and loader `_load_snapshot`, whether anchored by method UID or file. No statement edges are fabricated; source verification is authoritative for these control/data observations. [graph limitation]

- Terminal metadata is accepted only inside `status == "finished"`; validation must complete before `BulkExportStatus` construction. [verified: `client.py:214-249`]
- The restart-safe persistence point is the conditional `submitted -> export_finished` transaction immediately after polling; later states reread the attempt and never need in-memory terminal metadata. [verified: `transaction_workflow.py:250-443,535-585`]
- Final loading locks/revalidates the attempt, calls the loader with one exact count, then completes the attempt in the same transaction. The new value must be read from persisted `export_rows` at both validation and loader call sites. [verified: `transaction_workflow.py:694-795`]
- Parser totals are unavailable until complete success, and loader stage/target reconciliation independently checks exact counts/distinct IDs/signed totals. Count drift must not bypass or duplicate these gates. [verified: `transaction_export.py:151-268`, `transaction_ingestion.py:210-339,468-511`]

## 6. Proposed Changes

### A. Strict terminal bulk metadata

- `services/api/app/usaspending/client.py` — extend `BulkExportStatus` with required `total_rows: int` and optional `total_columns: int | None`. In `poll_bulk_export()`, reject missing, boolean, non-integer, negative, or `> DEFAULT_MAX_ROWS` row metadata before returning. Import the parser's existing `DEFAULT_MAX_ROWS` as the single 500,000 safety source; this is one-way because the parser does not import the client. [verified]
- Treat `total_columns` as safely usable only when present: reject boolean/non-integer values and any integer other than `len(BULK_TRANSACTION_FIELDS)` (16); allow absence because repository evidence does not prove it is always returned, while the parser remains the authoritative exact-header validator. Do not ingest or infer from `total_size`; its unit is unproven. [inferred]
- Use stable safe errors such as `invalid_bulk_export_total_rows` and `invalid_bulk_export_total_columns`; add them to deterministic workflow failure classification so malformed finished metadata fails terminally before download. Never include response content. [inferred]

### B. Attempt schema and reversible migration

- `services/api/app/db/models.py` — keep `expected_rows` as the pre-submission count; add nullable `export_rows BIGINT` and `export_columns SMALLINT` to `UsaSpendingTransactionIngestionAttempt`. [inferred]
- Preserve `count_metadata` so counted/submitting/submitted and later nonfailed states still require the pre-count. Update count checks so `export_rows` is either null or `0..500000`; `export_columns` is either null or 16. Add an `export_metadata` state check requiring `export_rows` for `export_finished`, `archive_hashed`, `loading`, and `completed`, but not for `created`, `counted`, `submitting`, `submitted`, `submission_unknown`, or any historical `failed` row. [inferred]
- Change `terminal_fields`: completed still requires checkpoint, both counts, signed total, and completion timestamp, but equality is `loaded_rows = export_rows`; `expected_rows` may differ. Keep terminal status history, partial unique index, checkpoint FK `RESTRICT`, archive/hash, URL, and error constraints unchanged. [inferred]
- Add `services/api/alembic/versions/20261003_0005_usaspending_export_bound_counts.py` with `down_revision = "20260927_0004"`. Upgrade order: add nullable columns; backfill only pre-existing `completed` rows with `export_rows = loaded_rows` (leave `export_columns` null because no historical status metadata was persisted); explicitly fail if a nonterminal post-export row cannot be reconstructed; drop/recreate only affected named checks; leave failed rows untouched. [inferred]
- Downgrade must preflight before schema changes. Fail clearly if an active post-export attempt would resume unsafely, a completed row has pre-count drift that violates the old `expected_rows = loaded_rows` rule, or a failed row contains nonredundant export metadata that would be lost. Otherwise restore the exact `_0004` checks, then drop the new columns. Never rewrite attempt history to make downgrade pass. [inferred]

### C. Restartable workflow and atomic completion

- `services/api/app/usaspending/transaction_workflow.py` — during `submitted -> export_finished`, persist `completed.total_rows` and `completed.total_columns` with the existing status/file metadata in the same short conditional transaction. If that transition fails, remain `submitted`; a restart may repoll the same saved job but must never resubmit. [verified + inferred]
- `_completed_status_from_attempt()` must reconstruct the complete validated status from persisted export metadata for download/restart. `_validate_loading_attempt()` must require valid `export_rows`, and `_load_atomically()` must pass `attempt["export_rows"]` as loader `expected_count`. [inferred]
- Before completing the attempt, explicitly require loader `loaded_rows == export_rows`; then let the existing single transaction commit transaction rows, checkpoint, and completed attempt together. The loader writes checkpoint `expected_rows` from this export-bound value. [inferred]
- Replace the ambiguous workflow result field with `pre_submission_rows`, add `export_rows`, retain `loaded_rows`, and expose derived `count_drift = export_rows - pre_submission_rows` only when both exist. `_result()` maps DB `expected_rows` to `pre_submission_rows`; historical failed attempts naturally return `export_rows=None`, `loaded_rows=None`, and `count_drift=None`. [inferred]
- Count drift alone is neither corruption nor failure. Only malformed/over-limit terminal metadata or mismatch among export rows, CSV/parser totals, staging, target, checkpoint, and loaded rows fails. [inferred]

### D. Safe operator output

- `services/api/app/usaspending/transaction_cli.py` — keep confirmation, one-period arguments, exit codes, error redaction, cleanup, and entry point unchanged. Extend `_safe_result()` to validate and emit only `pre_submission_rows`, `export_rows`, `loaded_rows`, and `count_drift` alongside the existing safe identifiers/status/total/error/operator flag. Validate count types without accepting booleans and verify drift equals the two counts. [verified + inferred]
- Do not emit URLs, filenames, paths, SQL, response bodies, or arbitrary exceptions. [verified]

### E. Unchanged strict boundaries

- Do not modify `TransactionExportParser` or `TransactionIngestionLoader` unless implementation proves a compile/type-only adaptation is unavoidable. Their existing expected-count, 500,000-row, duplicate-ID, field/date/FY/type/Decimal, archive identity, staging/target/checkpoint, correction/deletion, and rollback behavior is the desired contract. [verified]
- Do not modify legacy award search, API routes, frontend, presets, aggregates, production data, or the existing failed attempt. [verified scope]

## 7. Implementation Sequence

1. **Schema contract:** update the attempt ORM model; add `_0005`; update attempt model/migration/head tests. Keep model and migration check SQL byte-semantically aligned, names <=63 characters, and add guarded downgrade preflights. Stop if `_0004` is no longer the sole starting head. [risk: PostgreSQL check replacement/backfill]
2. **Client contract:** add terminal row/column fields and strict validation; update every `BulkExportStatus` constructor/fake and deterministic error classification tests. Run client plus legacy award-search tests. [risk: malformed response classification]
3. **Workflow wiring:** persist export metadata at `submitted -> export_finished`; reconstruct it on restart; pass only export rows to loader; enforce loaded/export equality; update explicit result fields/drift. Preserve conditional transitions, short transactions, no-resubmit behavior, temporary cleanup, and final atomic commit. [risk: HIGH internal loader/workflow breadth]
4. **CLI surface:** add the four count/drift fields to the fixed safe output, with validation/redaction tests; leave confirmation/resource/exit behavior untouched. [risk: operator compatibility]
5. **Focused offline verification:** run updated client/model/migration/workflow/CLI suites plus unchanged parser, loader, and legacy regressions. Do not contact USAspending or use any database URL. [risk: none external]
6. **Disposable PostgreSQL verification:** against an explicitly created PostgreSQL 16 database ending `_test`, run `_0004 -> _0005 -> _0004 -> _0005`, constraint/backfill/downgrade-guard tests, workflow concurrency/restart/atomicity tests, and full Alembic head upgrade. Remove the disposable resources afterward. [risk: PostgreSQL-only behavior]
7. **Review/rollout gate:** run GitNexus `detect_changes --scope all`, review HIGH/CRITICAL flows, commit only after review, then deploy migration and code while no transaction CLI/runner is active. Verify the historical failed attempt is unchanged before authorizing any future pilot. [risk: schema/code coordination]

## 8. Test Strategy

### Client/status tests — `test_usaspending_bulk_client.py`

- Finished payload `total_rows=490381`, `total_columns=16` -> validated `BulkExportStatus` retains both.
- Missing, `-1`, `True`, string, or `500001` `total_rows` -> deterministic fail closed before download.
- `total_columns` absent -> `None`; 16 -> accepted; boolean/string/non-16 -> rejected. `total_size` variation must not affect count validation or be treated as bytes.
- Existing deadline, redirect allowlist, network translation, failure-state, and archive retrieval tests remain green.

### Model/migration tests — attempt model/migration suites and `test_migrations.py`

- Pre-export states accept null export metadata; each post-export/loading/completed state rejects null/negative/over-limit export rows.
- Historical `failed` row with the production shape (pre-count/archive metadata but null export rows) upgrades and remains byte-logically unchanged.
- Completed `pre=490382`, `export=490381`, `loaded=490381` succeeds; `loaded != export` fails regardless of pre-count.
- Multiple terminal histories and one-active-attempt partial uniqueness remain unchanged.
- Offline PostgreSQL DDL compilation verifies normalized check SQL, FK/index parity, identifier lengths, sole head `_0005`, and upgrade/downgrade order.
- Disposable PostgreSQL executes upgrade/backfill, active-post-export upgrade rejection, clean downgrade, drift/nonredundant-history downgrade rejection, and re-upgrade.

### Workflow tests — `test_usaspending_transaction_workflow.py`

- Exact production-number wiring: count endpoint 490382 + terminal export 490381 -> persisted pre/export values, loader spy receives 490381, completed result/checkpoint report 490381, drift is -1.
- Export 490381 + parser/loader reports 490380 or 490382 -> terminal validation failure, no transaction/checkpoint commit, prior snapshot preserved.
- Persisted export rows survive restart from `export_finished`, `archive_hashed`, and `loading`; restart from `submitted` repolls the same job and persists the terminal count before advancing.
- A transition failure after poll leaves `submitted`; concurrency still allows exactly one submit claim and never resubmits `submitting`/`submission_unknown`.
- Final loader/checkpoint/attempt transaction commits or rolls back together; archive identity and URL revalidation remain unchanged.
- Existing failed/completed attempts return without network/loader work; failed history still permits a separately authorized new attempt, never mutation of the old row.

Use exact production-number fakes to prove workflow wiring without constructing a 490k-row CI fixture, and retain real small synthetic ZIP/parser/loader tests to exercise both under- and over-count comparisons through production code. Together these cover the exact metadata scenario and the actual comparison implementation. [inferred testing design]

### Parser/loader regressions — unchanged production code

- Run `test_usaspending_transaction_export.py` for exact count, immediate excess-row, 500,000 ceiling, 16 headers, duplicate IDs, IDs/UEIs/agencies, dates/FY, A–D, Decimal/range, archive and streaming limits.
- Run `test_usaspending_transaction_ingestion.py` for checkpoint expected rows, staging/target count/distinct/total reconciliation, corrections/deletions, rollback, caller-owned transaction, archive identity, and PostgreSQL advisory/staging behavior.

### CLI/legacy regressions

- Completed output contains only safe fields and all four count/drift values; failed pre-repair-shaped result safely shows null export/loaded/drift.
- Invalid/fake count shapes are rejected without echoing values. Confirmation, help, invalid arguments, cleanup, cancellation, single-period invocation, exit codes, and redaction stay unchanged.
- `test_usaspending.py` proves legacy award pagination/request/error behavior remains unchanged.

### Verification commands

```bash
cd services/api && pytest tests/test_usaspending_bulk_client.py tests/test_usaspending_transaction_attempt_models.py tests/test_usaspending_transaction_attempt_migration.py tests/test_usaspending_transaction_workflow.py tests/test_usaspending_transaction_cli.py tests/test_usaspending_transaction_export.py tests/test_usaspending_transaction_ingestion.py tests/test_usaspending.py tests/test_migrations.py
```

For real PostgreSQL behavior, create an ephemeral PostgreSQL 16 database with a name ending `_test`, export only its URL as `TEST_DATABASE_URL` for the focused process, run the same migration/workflow/loader suites, require zero PostgreSQL skips, then remove only those disposable resources. Never source `services/api/.env` or use application `DATABASE_URL`.

## 9. Risk and Impact Analysis

- **HIGH internal breadth:** `load_with_connection` reaches three atomic loader/workflow processes and many tests. The loader stays unchanged; risk is controlled by passing a different persisted count and rerunning its complete atomic/PostgreSQL regressions. [graph + verified]
- **Schema compatibility:** changing completed equality without a guarded migration can invalidate history or make downgrade destructive. Backfill only provable completed `loaded_rows`; fail on irreconcilable active/audit states. [inferred]
- **Restart correctness:** if export rows are persisted after `export_finished`, a crash window recreates the bug. Persist them in the same transition transaction. [verified + inferred]
- **Deployment ordering:** new code cannot run before `_0005`, and old workflow code must not run after stricter `_0005` constraints. Deploy while no CLI/runner is active; migrate then activate code before any ingestion. Existing routes do not consume attempt fields. [inferred from verified consumers]
- **Column semantics:** `total_columns` is observational rather than authoritative when absent; wrong present values fail, and the parser always enforces the actual 16 headers. `total_size` remains unused. [inferred]
- **Operator contract:** explicit result key names are a small intentional CLI contract change; graph shows no production consumer beyond CLI/tests. Keep safe allowlisting and document drift sign as `export - pre_submission`. [graph + inferred]
- **Direct-dependent accounting:** client status direct importers are workflow/CLI/legacy files and focused tests; source search shows constructors only in client/workflow/tests. Attempt-model d=1 imports include Alembic/shared model consumers, but targeted search finds field reads only in workflow. Workflow/result direct consumers are CLI/tests. Parser direct consumers are loader/workflow/tests. Loader direct callers are its wrapper, workflow, and test helpers. CLI direct callers are self/tests. [graph + verified]

## 10. Files Expected to Change

| File | Symbols | Reason |
| ---- | ------- | ------ |
| `services/api/app/usaspending/client.py` | `BulkExportStatus`, `poll_bulk_export`, deterministic metadata errors | Capture and validate export-bound rows/columns. |
| `services/api/app/db/models.py` | `UsaSpendingTransactionIngestionAttempt` | Add export metadata and corrected state/completion checks. |
| `services/api/alembic/versions/20261003_0005_usaspending_export_bound_counts.py` | `upgrade`, `downgrade` | Reversible incremental schema/backfill/guard migration after `_0004`. |
| `services/api/app/usaspending/transaction_workflow.py` | `TransactionWorkflowResult`, `run`, `_completed_status_from_attempt`, `_validate_loading_attempt`, `_load_atomically`, `_complete_attempt`, `_result` | Persist/resume/use export rows and expose drift. |
| `services/api/app/usaspending/transaction_cli.py` | `_safe_result` | Emit validated pre/export/loaded/drift fields only. |
| `services/api/tests/test_usaspending_bulk_client.py` | status helpers and polling tests | Strict terminal metadata matrix. |
| `services/api/tests/test_usaspending_transaction_attempt_models.py` | model contract/state tests | Updated columns/check invariants and historical failed compatibility. |
| `services/api/tests/test_usaspending_transaction_attempt_migration.py` | migration loaders/parity/PG tests | Exercise `_0004 + _0005`, backfill, constraints, downgrade guards. |
| `services/api/tests/test_usaspending_transaction_workflow.py` | stubs, fixtures, drift/restart/atomic tests | Prove exact export-bound wiring and no duplicate submission. |
| `services/api/tests/test_usaspending_transaction_cli.py` | result fixture/output/redaction tests | Safe count/drift output contract. |
| `services/api/tests/test_migrations.py` | head/full-upgrade assertions | Make `_0005` the sole expected head and verify full chain. |

`transaction_export.py`, `transaction_ingestion.py`, their focused tests, and `test_usaspending.py` are verification-only boundaries unless compilation forces a minimal annotation/import adjustment.

## 11. Reusable Implementation Context

```yaml
implementation_context:
  task_summary: "Separate pre-submission and export-bound USAspending counts; validate/persist terminal rows; load and checkpoint against export rows; expose drift safely."
  acceptance_criteria:
    - "Preserve pre-submission count as attempt audit evidence."
    - "Require terminal total_rows as int-not-bool in 0..500000; validate optional total_columns as exactly 16."
    - "Persist export rows before export_finished and use them for parser, loader, checkpoint, and completed equality."
    - "Historical failed attempts remain terminal and valid with null export rows."
    - "Count drift is observable, not corruption; all existing strict data/archive/atomic checks remain."
    - "No retry or mutation of attempt 4ffa631e-60bd-4b7b-a935-20f4e14d3895."
  evidence_provenance:
    schema_version: 2
    head_commit: "b8c82ec48bfbdfcaf9d9aa06f8fa7c924cb1d359"
    generated_plan_path: "docs/plans/2026-10-03-gitnexus-plan-export-bound-count-repair.md"
    global_dirty_digest:
      algorithm: "sha256"
      canonicalization: "gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records"
      value: "0a9c85780067d9afcd0764f307b60891e3cee927ee11eaeb5ec7826d10fd82cd"
    cited_path_manifest:
      - {path: "services/api/alembic/versions/20260927_0004_usaspending_transaction_attempts.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:4844334b03b0269c0e6df1b74dfe306a95ea9ccb36efa6d19e2ae21cc535f82a", index_digest: "sha256:4844334b03b0269c0e6df1b74dfe306a95ea9ccb36efa6d19e2ae21cc535f82a", worktree_digest: "sha256:4844334b03b0269c0e6df1b74dfe306a95ea9ccb36efa6d19e2ae21cc535f82a", untracked_digest: "absent"}
      - {path: "services/api/alembic/versions/20261003_0005_usaspending_export_bound_counts.py", object_kind: {head: "absent", index: "absent", worktree: "absent", untracked: "absent"}, state: "absent", rename_from: null, rename_to: null, head_digest: "absent", index_digest: "absent", worktree_digest: "absent", untracked_digest: "absent"}
      - {path: "services/api/app/db/models.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:f7f5d4196da1c61beb4a7985a4a08eac562bde3aaef19291198a9c0d9dd0eb9e", index_digest: "sha256:f7f5d4196da1c61beb4a7985a4a08eac562bde3aaef19291198a9c0d9dd0eb9e", worktree_digest: "sha256:f7f5d4196da1c61beb4a7985a4a08eac562bde3aaef19291198a9c0d9dd0eb9e", untracked_digest: "absent"}
      - {path: "services/api/app/usaspending/client.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:227b97c0d49cb3ade52a6d706b3448c64e0713228bf9893f3b939c8a4e72dd83", index_digest: "sha256:227b97c0d49cb3ade52a6d706b3448c64e0713228bf9893f3b939c8a4e72dd83", worktree_digest: "sha256:227b97c0d49cb3ade52a6d706b3448c64e0713228bf9893f3b939c8a4e72dd83", untracked_digest: "absent"}
      - {path: "services/api/app/usaspending/transaction_cli.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:9f1a178187c16ec906af285d1f9b9c45c4ecfa4dc70a4a8ce3b6430d96674c5e", index_digest: "sha256:9f1a178187c16ec906af285d1f9b9c45c4ecfa4dc70a4a8ce3b6430d96674c5e", worktree_digest: "sha256:9f1a178187c16ec906af285d1f9b9c45c4ecfa4dc70a4a8ce3b6430d96674c5e", untracked_digest: "absent"}
      - {path: "services/api/app/usaspending/transaction_export.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:01759ee0e89df6c0d18a1388ed2206479014a1dc77e4b23fbab16f7dc05144a6", index_digest: "sha256:01759ee0e89df6c0d18a1388ed2206479014a1dc77e4b23fbab16f7dc05144a6", worktree_digest: "sha256:01759ee0e89df6c0d18a1388ed2206479014a1dc77e4b23fbab16f7dc05144a6", untracked_digest: "absent"}
      - {path: "services/api/app/usaspending/transaction_ingestion.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:533133454015ec982586f21c35781dd9db38a9374a5d9390ddc97e7f6a28b857", index_digest: "sha256:533133454015ec982586f21c35781dd9db38a9374a5d9390ddc97e7f6a28b857", worktree_digest: "sha256:533133454015ec982586f21c35781dd9db38a9374a5d9390ddc97e7f6a28b857", untracked_digest: "absent"}
      - {path: "services/api/app/usaspending/transaction_workflow.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:9c9d155d372c148fc9a351fecc6143950ff2445a0dec508d57564f837f5528a5", index_digest: "sha256:9c9d155d372c148fc9a351fecc6143950ff2445a0dec508d57564f837f5528a5", worktree_digest: "sha256:9c9d155d372c148fc9a351fecc6143950ff2445a0dec508d57564f837f5528a5", untracked_digest: "absent"}
      - {path: "services/api/pyproject.toml", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:a9355439dcea53320d8cba1c386b1f8986d6168ba6d1fc23d83699df837fe921", index_digest: "sha256:a9355439dcea53320d8cba1c386b1f8986d6168ba6d1fc23d83699df837fe921", worktree_digest: "sha256:a9355439dcea53320d8cba1c386b1f8986d6168ba6d1fc23d83699df837fe921", untracked_digest: "absent"}
      - {path: "services/api/tests/test_migrations.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:7d318f7ef75084e841c1a86b4b1c753333b7a1c81df0e97c62a882dede854d6f", index_digest: "sha256:7d318f7ef75084e841c1a86b4b1c753333b7a1c81df0e97c62a882dede854d6f", worktree_digest: "sha256:7d318f7ef75084e841c1a86b4b1c753333b7a1c81df0e97c62a882dede854d6f", untracked_digest: "absent"}
      - {path: "services/api/tests/test_usaspending.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:5e1a6895282b9e36890797d1c4ef2a0958ab8ab551351bb6d1703b6c0a0b36a5", index_digest: "sha256:5e1a6895282b9e36890797d1c4ef2a0958ab8ab551351bb6d1703b6c0a0b36a5", worktree_digest: "sha256:5e1a6895282b9e36890797d1c4ef2a0958ab8ab551351bb6d1703b6c0a0b36a5", untracked_digest: "absent"}
      - {path: "services/api/tests/test_usaspending_bulk_client.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:e05c2f45a4ad05c0ce88de7a6aa5db023fbd3584b9431fdb85b5e54165482b7f", index_digest: "sha256:e05c2f45a4ad05c0ce88de7a6aa5db023fbd3584b9431fdb85b5e54165482b7f", worktree_digest: "sha256:e05c2f45a4ad05c0ce88de7a6aa5db023fbd3584b9431fdb85b5e54165482b7f", untracked_digest: "absent"}
      - {path: "services/api/tests/test_usaspending_transaction_attempt_migration.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:f21cd051f3d029d3ba6f0541e047d01d47da928bb064f82e16200e2eccde1280", index_digest: "sha256:f21cd051f3d029d3ba6f0541e047d01d47da928bb064f82e16200e2eccde1280", worktree_digest: "sha256:f21cd051f3d029d3ba6f0541e047d01d47da928bb064f82e16200e2eccde1280", untracked_digest: "absent"}
      - {path: "services/api/tests/test_usaspending_transaction_attempt_models.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:f1ebbf9bfcfa566e5f2e4d2e1fd6b169142737e819b52c5f3d1174323d5b4bbd", index_digest: "sha256:f1ebbf9bfcfa566e5f2e4d2e1fd6b169142737e819b52c5f3d1174323d5b4bbd", worktree_digest: "sha256:f1ebbf9bfcfa566e5f2e4d2e1fd6b169142737e819b52c5f3d1174323d5b4bbd", untracked_digest: "absent"}
      - {path: "services/api/tests/test_usaspending_transaction_cli.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:806f5cf38e8de550f08cd8ce7d01f49b7133363ef639d7761d4866cd719fc684", index_digest: "sha256:806f5cf38e8de550f08cd8ce7d01f49b7133363ef639d7761d4866cd719fc684", worktree_digest: "sha256:806f5cf38e8de550f08cd8ce7d01f49b7133363ef639d7761d4866cd719fc684", untracked_digest: "absent"}
      - {path: "services/api/tests/test_usaspending_transaction_export.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:6dc4970921ce88cee1a66a0cd5841a13d3ba5c52afe14d8e383f18b80d3eff41", index_digest: "sha256:6dc4970921ce88cee1a66a0cd5841a13d3ba5c52afe14d8e383f18b80d3eff41", worktree_digest: "sha256:6dc4970921ce88cee1a66a0cd5841a13d3ba5c52afe14d8e383f18b80d3eff41", untracked_digest: "absent"}
      - {path: "services/api/tests/test_usaspending_transaction_ingestion.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:ac601c4b471c1ee2c8673b6e64f1da3cd3492ee5bffcf7d69bd6f75dff5cc6e1", index_digest: "sha256:ac601c4b471c1ee2c8673b6e64f1da3cd3492ee5bffcf7d69bd6f75dff5cc6e1", worktree_digest: "sha256:ac601c4b471c1ee2c8673b6e64f1da3cd3492ee5bffcf7d69bd6f75dff5cc6e1", untracked_digest: "absent"}
      - {path: "services/api/tests/test_usaspending_transaction_workflow.py", object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}, state: "clean", rename_from: null, rename_to: null, head_digest: "sha256:e9772efded366acbd72a04e205e34ce2c21dac04fe76ec5fb0a8ba71450cad3a", index_digest: "sha256:e9772efded366acbd72a04e205e34ce2c21dac04fe76ec5fb0a8ba71450cad3a", worktree_digest: "sha256:e9772efded366acbd72a04e205e34ce2c21dac04fe76ec5fb0a8ba71450cad3a", untracked_digest: "absent"}
  primary_symbols:
    - {symbol: "BulkExportStatus / UsaSpendingClient.poll_bulk_export", file: "services/api/app/usaspending/client.py", lines: "85-93,192-261", role: "Validate terminal export-bound metadata."}
    - {symbol: "UsaSpendingTransactionIngestionAttempt", file: "services/api/app/db/models.py", lines: "204-312", role: "Persist pre-count, export metadata, restart state, and terminal invariants."}
    - {symbol: "TransactionIngestionWorkflow.run", file: "services/api/app/usaspending/transaction_workflow.py", lines: "129-443", role: "Persist/resume terminal metadata and orchestrate exact load."}
    - {symbol: "TransactionIngestionLoader", file: "services/api/app/usaspending/transaction_ingestion.py", lines: "82-548", role: "Unchanged exact parser/stage/target/checkpoint contract."}
    - {symbol: "TransactionWorkflowResult", file: "services/api/app/usaspending/transaction_workflow.py", lines: "83-93", role: "Safe count/drift result contract."}
  related_symbols:
    - {symbol: "TransactionExportParser", relationship: "CALLED_BY loader", relevance: "500000 cap and exact CSV count authority; unchanged."}
    - {symbol: "transaction_cli._safe_result", relationship: "CONSUMES TransactionWorkflowResult", relevance: "Operator-safe count/drift output."}
    - {symbol: "UsaSpendingIngestionCheckpoint.expected_rows", relationship: "WRITTEN_BY loader", relevance: "Must receive export-bound count."}
    - {symbol: "20260927_0004 upgrade/downgrade", relationship: "PREDECESSOR", relevance: "Defines exact old constraints for reversible _0005."}
  execution_path:
    - "Get and persist pre-submission count as attempt audit evidence."
    - "Claim submission once; submit outside a DB transaction; persist job metadata."
    - "Poll saved status URL; validate terminal total_rows/optional total_columns."
    - "Atomically persist export metadata with submitted -> export_finished."
    - "Download/redownload and verify archive bytes/SHA using persisted job data."
    - "Lock loading attempt; pass persisted export_rows to parser/loader."
    - "Reconcile parser/stage/target/checkpoint, then complete loader/checkpoint/attempt in one transaction."
    - "Return pre, export, loaded, and derived drift; CLI emits only safe fields."
  pdg_constraints:
    - description: "PDG layer was added, but Python methods returned no CDG/REACHING_DEF edges; source ordering is authoritative."
      affected_statements: ["client.py:214-249", "transaction_workflow.py:250-443", "transaction_workflow.py:694-795"]
      implementation_consequence: "Do not claim inferred PDG guards; preserve source-verified ordering and rerun impact/detect_changes."
  architectural_patterns:
    - {pattern: "Short conditional state transitions around network-free DB work", example_location: "services/api/app/usaspending/transaction_workflow.py:535-585", usage_guidance: "Persist export metadata in the submitted -> export_finished transition; never hold a transaction while polling/downloading."}
    - {pattern: "Caller-owned atomic loader transaction", example_location: "services/api/app/usaspending/transaction_workflow.py:694-762", usage_guidance: "Keep loader/checkpoint/attempt completion atomic."}
    - {pattern: "Guarded disposable PostgreSQL tests", example_location: "services/api/tests/test_usaspending_transaction_attempt_migration.py:204-216", usage_guidance: "Require explicit postgresql+psycopg URL ending _test; never source .env."}
  files_to_modify:
    - {file: "services/api/app/usaspending/client.py", symbols: ["BulkExportStatus", "poll_bulk_export"], intended_change: "Strict terminal row/column metadata."}
    - {file: "services/api/app/db/models.py", symbols: ["UsaSpendingTransactionIngestionAttempt"], intended_change: "Export metadata columns and corrected checks."}
    - {file: "services/api/alembic/versions/20261003_0005_usaspending_export_bound_counts.py", symbols: ["upgrade", "downgrade"], intended_change: "Incremental reversible migration after _0004."}
    - {file: "services/api/app/usaspending/transaction_workflow.py", symbols: ["TransactionWorkflowResult", "run", "_completed_status_from_attempt", "_validate_loading_attempt", "_load_atomically", "_complete_attempt", "_result"], intended_change: "Persist/resume/use export count and expose drift."}
    - {file: "services/api/app/usaspending/transaction_cli.py", symbols: ["_safe_result"], intended_change: "Safe pre/export/loaded/drift output."}
    - {file: "services/api/tests/test_usaspending_bulk_client.py", symbols: ["polling tests", "completed_status"], intended_change: "Terminal metadata validation matrix."}
    - {file: "services/api/tests/test_usaspending_transaction_attempt_models.py", symbols: ["model contract/state tests"], intended_change: "New state/count invariants."}
    - {file: "services/api/tests/test_usaspending_transaction_attempt_migration.py", symbols: ["parity/DDL/live tests"], intended_change: "Sequential _0004/_0005 coverage and downgrade guards."}
    - {file: "services/api/tests/test_usaspending_transaction_workflow.py", symbols: ["StubClient", "attempt_values", "workflow integration tests"], intended_change: "Drift/restart/concurrency/atomic scenarios."}
    - {file: "services/api/tests/test_usaspending_transaction_cli.py", symbols: ["result", "safe-output tests"], intended_change: "Count/drift output and redaction."}
    - {file: "services/api/tests/test_migrations.py", symbols: ["head/full-upgrade tests"], intended_change: "Expected sole head _0005."}
  tests:
    - file: "services/api/tests/test_usaspending_bulk_client.py"
      scenarios: ["490381/16 accepted", "missing/negative/bool/string/>500000 total_rows rejected", "optional columns absent or exactly 16", "total_size ignored"]
    - file: "services/api/tests/test_usaspending_transaction_attempt_models.py"
      scenarios: ["state-dependent export metadata", "completed loaded=export with pre-count drift", "historical failed null export"]
    - file: "services/api/tests/test_usaspending_transaction_attempt_migration.py"
      scenarios: ["parity and PostgreSQL DDL", "upgrade/backfill", "failed-row compatibility", "clean and guarded downgrade"]
    - file: "services/api/tests/test_usaspending_transaction_workflow.py"
      scenarios: ["490382 -> 490381 -> 490381 succeeds", "CSV under/over export fails", "restart every post-export state", "no duplicate submit", "atomic rollback"]
    - file: "services/api/tests/test_usaspending_transaction_cli.py"
      scenarios: ["safe count/drift output", "null historical values", "redaction/offline/cleanup regressions"]
    - file: "services/api/tests/test_usaspending_transaction_export.py"
      scenarios: ["unchanged exact-count/500000/field/archive/Decimal regressions"]
    - file: "services/api/tests/test_usaspending_transaction_ingestion.py"
      scenarios: ["unchanged checkpoint/reconciliation/atomic PostgreSQL regressions"]
    - file: "services/api/tests/test_usaspending.py"
      scenarios: ["legacy award-search request/pagination/error behavior unchanged"]
  verification_commands:
    - "cd services/api && pytest tests/test_usaspending_bulk_client.py tests/test_usaspending_transaction_attempt_models.py tests/test_usaspending_transaction_attempt_migration.py tests/test_usaspending_transaction_workflow.py tests/test_usaspending_transaction_cli.py tests/test_usaspending_transaction_export.py tests/test_usaspending_transaction_ingestion.py tests/test_usaspending.py tests/test_migrations.py"
    - "cd services/api && TEST_DATABASE_URL=\"${GOVTRACTS_EXPORT_COUNT_TEST_DATABASE_URL:?explicit postgresql+psycopg disposable _test URL required}\" pytest tests/test_usaspending_transaction_attempt_migration.py tests/test_usaspending_transaction_workflow.py tests/test_usaspending_transaction_ingestion.py tests/test_migrations.py"
    - "node .gitnexus/run.cjs detect-changes --scope all --repo ."
  risks:
    - "HIGH GitNexus breadth at loader/workflow boundary; keep loader unchanged and rerun all atomic/PostgreSQL regressions."
    - "Migration constraint replacement/backfill/downgrade must preserve historical failed attempts and reject irreconcilable active state."
    - "Persist export rows in the export_finished transition or restart recreates the bug."
    - "Coordinate migration/code activation while no transaction runner is active."
  assumptions:
    - "Reverify Alembic head is still 20260927_0004 before implementation; stop/replan if not."
    - "Treat total_columns as optional-but-strict because repository evidence does not prove it is always present; parser remains authoritative for 16 CSV headers."
    - "Verify no active post-export attempt during migration; migration itself must fail closed rather than rely on this assumption."
  open_questions: []
  avoid:
    - "Do not access production DB, .env, USAspending, or run the CLI while implementing/testing."
    - "Do not reopen, mutate, retry, or reuse attempt 4ffa631e-60bd-4b7b-a935-20f4e14d3895."
    - "Do not infer count from file size or treat total_size as bytes."
    - "Do not relax parser, loader, archive, fiscal, identifier, Decimal, duplicate, or atomicity checks."
    - "Do not modify legacy award ingestion/search, routes, frontend, presets, aggregates, or begin another pilot."
```

## 12. Assumptions and Open Questions

- [assumed] USAspending terminal `total_columns` may be absent even though the observed completed job supplied 16; implement optional-but-strict semantics and let the parser enforce actual headers. Revisit only with authoritative API documentation/evidence.
- [assumed] No active post-export production attempt exists at rollout. The migration must query and fail closed if this assumption is false; it must not fabricate export counts.
- [verified] The supplied production evidence establishes the failed attempt, counts, archive identity, zero committed rows/checkpoints, and `_0004` production revision. This plan does not independently access production.
- [deferred] Applying the migration, deploying code, inspecting production, and authorizing/running a new pilot are separate reviewed operations.

## 13. Definition of Done

- One `_0005` migration is the sole head after `_0004`; model and upgraded PostgreSQL schema match, identifier lengths are valid, clean downgrade works, and incompatible downgrade fails before schema mutation.
- Attempt rows preserve pre-count and export count independently; post-export states require valid export rows; completed loaded rows equal export rows; historical failed rows with null export rows remain valid and unchanged.
- Client rejects every invalid/missing/over-limit terminal row shape, validates optional column count, and never uses `total_size` for correctness.
- Workflow persists export metadata before post-export restart states, never resubmits uncertain jobs, loads/checkpoints against export rows, and commits loader/checkpoint/attempt atomically.
- Exact drift scenario 490382 -> 490381 -> 490381 succeeds and reports -1; either CSV under- or over-count relative to 490381 fails with no committed target/checkpoint mutation.
- CLI safely emits pre-submission, export-bound, loaded, and drift values without sensitive metadata; existing confirmation/exit/cleanup behavior remains.
- Focused offline tests pass; disposable PostgreSQL 16 tests execute with zero integration skips; legacy award-search, parser, and loader regressions pass.
- GitNexus `detect_changes --scope all` is complete/nontruncated and all HIGH/CRITICAL flows are reviewed before commit.
- The existing failed attempt is not reopened, changed, or retried. A future pilot requires a new attempt/export, fresh <=500000 pre-count, migrated/reviewed code, clean reconciliation baselines, and separate explicit live authorization.
