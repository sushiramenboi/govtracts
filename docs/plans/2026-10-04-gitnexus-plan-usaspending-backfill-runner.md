# GitNexus Engineering Plan

> Task: Implement an operator-controlled, serial, resumable USAspending A-D prime-contract transaction backfill for FY2020-FY2026 (2019-10-01 through 2026-09-30), with deterministic count-driven subdivision and fail-closed integrity gates.
> Evidence verified at commit e2fa8436ef847076403cf004295efb7ce4892809; GitNexus index refreshed this session with `--index-only --pdg` (5914 nodes, 27054 edges, 62 clusters, 72 flows). The MCP registry and repository context report the exact HEAD; one repeated concept query incorrectly retained a one-commit-behind envelope, so no freshness-sensitive graph claim is used without source confirmation.
> Evidence provenance schema 2; global dirty digest `0a9c85780067d9afcd0764f307b60891e3cee927ee11eaeb5ec7826d10fd82cd`; cited-path manifest 26 sorted entries; exact generated plan path excluded.

## 1. Objective

Build a new, explicitly confirmed backfill runner that covers exactly FY2020 through FY2026, uses whole calendar months as roots, recursively splits only oversized roots/leaves, processes at most one export at a time, and resumes from durable attempt/checkpoint state. It must reuse the committed USAspending client, parser, loader, workflow, models, and fiscal utilities rather than fork their ingestion rules. [verified: `services/api/app/usaspending/client.py:128-193`, `services/api/app/usaspending/transaction_export.py:90-268`, `services/api/app/usaspending/transaction_ingestion.py:82-339`, `services/api/app/usaspending/transaction_workflow.py:111-449`]

The live runner must preflight the known October 2024 completion (`2024-10-01..2024-10-31`, FY2025, attempt `c6b44991-c217-4e36-bdf8-905690d6bd2d`, checkpoint `ce639524-be6d-4cba-8d82-07657c5c09a9`, 490381 rows, signed total `48714624177.57`) and skip it only after exact relational integrity verification. Those values are user-supplied production expectations and were not queried during planning. [assumed]

Out of scope: preset classification, period aggregates, API routes, frontend, schedulers/cron, legacy award ingestion, automatic repair of corruption, automatic resolution of `submission_unknown`, and any live execution during implementation. [verified: requested scope]

## 2. Current Behaviour

- `fiscal_year_bounds()` returns inclusive October 1 through September 30 bounds, while `fiscal_month_bounds()` validates a calendar month belongs to the supplied federal fiscal year and uses `monthrange`, including leap-year February. [verified: `services/api/app/usaspending/fiscal_years.py:7-45`]
- The one-month CLI requires fiscal/calendar coordinates plus `--execute-live`; it validates arguments before constructing settings, database, client, loader, or workflow, then emits a sanitized JSON result and stable exit category. [verified: `services/api/app/usaspending/transaction_cli.py:70-167`, `services/api/app/usaspending/transaction_cli.py:170-268`]
- `TransactionIngestionWorkflow.run()` derives one exact calendar month, locates/creates an attempt, obtains the advisory pre-submission count, claims submission with a compare-and-swap transition, submits once, resumes persisted post-submission states, persists export-bound rows/columns before download/load, and completes atomically. `failed`, `completed`, and `submission_unknown` return without activity. [verified: `services/api/app/usaspending/transaction_workflow.py:134-449`]
- `_locate_or_create_attempt()` takes a period-keyed PostgreSQL advisory transaction lock, row-locks an active attempt, reuses a completed attempt, and creates a new attempt only when neither exists. The partial unique index treats every status except `completed` and `failed` as blocking. [verified: `services/api/app/usaspending/transaction_workflow.py:451-540`, `services/api/app/db/models.py:271-279`]
- The count endpoint validates an integer nonnegative transaction count but does not cap it. The finished export status validates `total_rows` in `0..500000` and exactly 16 columns when present. [verified: `services/api/app/usaspending/client.py:128-158`, `services/api/app/usaspending/client.py:228-266`]
- The parser already accepts any inclusive date range, enforces at most 500000 rows, bounds ZIP/CSV resources, validates A-D awards and action dates, rejects duplicate stable IDs, and calculates exact signed totals. [verified: `services/api/app/usaspending/transaction_export.py:38-49`, `services/api/app/usaspending/transaction_export.py:90-145`, `services/api/app/usaspending/transaction_export.py:173-268`, `services/api/app/usaspending/transaction_export.py:270-384`]
- The loader is the present blocker for subdivisions: `_validate_period()` requires the exact whole month. Its load path otherwise provides the desired transaction-scoped period advisory lock, private immutable archive copy, stage reconciliation, cross-period stable-ID ownership checks before and after upsert, delete-of-disappeared rows limited to the period, target reconciliation, and checkpoint completion. [verified: `services/api/app/usaspending/transaction_ingestion.py:136-183`, `services/api/app/usaspending/transaction_ingestion.py:210-339`, `services/api/app/usaspending/transaction_ingestion.py:341-358`, `services/api/app/usaspending/transaction_ingestion.py:468-533`]
- `_load_atomically()` row-locks the attempt, validates persisted export/archive/job metadata, passes persisted `export_rows` to the loader, requires `loaded_rows == export_rows`, and commits transaction rows, checkpoint, and attempt completion in one database transaction. [verified: `services/api/app/usaspending/transaction_workflow.py:701-819`]
- Attempts and checkpoints are keyed by exact inclusive period. Transactions carry one checkpoint FK and a globally unique stable transaction ID. The existing constraints require a completed attempt to have a checkpoint, counts, signed total, and `export_rows == loaded_rows`; historical failed attempts may keep null export metadata. [verified: `services/api/app/db/models.py:174-201`, `services/api/app/db/models.py:204-344`]
- The documented October feasibility sample modeled 269622536 bytes (257.13 MiB) for 490377 rows, but explicitly excluded page fill, WAL, TOAST, staging, and future columns and modeled only two indexes. [verified: `docs/spikes/usaspending-transaction-export.md:125-140`]

## 3. Relevant Architecture

1. **Pure planning boundary.** Generate 84 chronological calendar-month roots from `fiscal_year_bounds(2020)` through `fiscal_year_bounds(2026)`. A dry-run must stop here: it may write a sanitized manifest, but it must not load settings, create a database engine, or construct/call the client. [inferred]
2. **Live preflight boundary.** After explicit confirmation, acquire one global session-scoped PostgreSQL advisory lease shared by the existing month CLI and the new runner. Hold it for the entire backfill invocation so concurrent operators cannot submit different periods simultaneously. Existing exact-period attempt and loader locks remain defense in depth. [inferred]
3. **Attempt tree boundary.** A root/child attempt begins `created`; the existing client count moves it to `counted`. A count at or below 500000 is a leaf and may continue through the existing workflow. A larger multi-day period is atomically changed to a durable `subdivided` blocking state while exactly two child attempts are inserted. A larger one-day period stops unrecoverably without submission. [inferred]
4. **Deterministic split rule.** For inclusive `[start,end]`, compute `days=(end-start).days+1`, `left_days=(days+1)//2`, left `[start,start+left_days-1]`, right `[left_end+1,end]`. The children are ordered, nonempty, adjacent, contained, collectively equal to the parent, and converge to days. This produces familiar half-month/quarter/week-scale leaves without special calendar cases. [inferred]
5. **Existing workflow boundary.** Preserve `run(fiscal_year, calendar_year, calendar_month)` as the existing whole-month wrapper. Add a validated arbitrary-subperiod entry inside the same workflow and a stop-after-count control for the runner. The counted branch itself must refuse to submit `expected_rows > DEFAULT_MAX_ROWS`, so even an accidental direct invocation cannot submit an oversized parent. [inferred]
6. **Existing loader boundary.** Keep all current parser, stage, ownership, reconciliation, deletion, and atomicity behavior. Relax only the period shape from “whole month” to “nonempty range contained in one calendar month and one federal FY”; the existing CLI remains whole-month-only. [inferred]
7. **Integrity boundary.** Before skipping a completed leaf/root, one read-only transaction must row-lock or repeatably read the attempt/checkpoint and compare source, FY, exact bounds, IDs, statuses, archive identity linkage, `export_rows`, checkpoint expected/loaded rows, target row count, distinct stable IDs, signed total, checkpoint ownership, no out-of-range rows owned by the checkpoint, and no in-range rows owned by another checkpoint. Any mismatch stops the run; it never reloads automatically. [inferred]
8. **Audit boundary.** Database attempts/checkpoints remain authoritative. Progress and final manifests contain only dates, FYs, UUIDs, statuses, counts, exact decimal totals, safe error codes, limit decisions, storage estimates, and backup gates—never URLs, credentials, response bodies, filenames from upstream, exception text, or row values. [inferred]

No separate backfill-run table is proposed. Existing attempt/checkpoint state is sufficient after adding durable parentage and a non-submittable `subdivided` state; run-level CLI bounds and manifests are operational records, not ingestion truth. [inferred]

## 4. GitNexus Findings

- `query(search_query="USAspending transaction ingestion workflow CLI fiscal month attempts checkpoints loader resumable")` identified `TransactionIngestionWorkflow.run`, `fiscal_month_bounds`, `_run_workflow`, attempts/checkpoints, and loader tests as the central path. The repeated result displayed a stale one-commit envelope even though `list_repos` reported index commit `e2fa8436...`; source was used to verify every cited edge. [graph]
- `context(name="run", file_path="services/api/app/usaspending/transaction_workflow.py")` found exact incoming calls from the workflow test helper and invalid-period test, outgoing calls to fiscal validation, attempt creation, polling, download, atomic load, and failure transitions, plus three indexed `run` processes. [graph]
- `impact(target_uid=TransactionIngestionWorkflow.run, direction="upstream", maxDepth=2)` reported LOW risk, 23 impacted test symbols, and two direct dependents: `run_period` and `test_invalid_fiscal_period_fails_before_database_or_network`. Both must remain compatible. [graph]
- `impact(target_uid=_locate_or_create_attempt, direction="upstream", maxDepth=2)` reported LOW risk, five impacted symbols, and three direct dependents: `TransactionIngestionWorkflow.run`, `CountedBarrierWorkflow`, and its override call. Subdivision changes must preserve the counted-claim concurrency seam used by tests. [graph]
- `impact(target_uid=TransactionIngestionLoader.load, direction="upstream", maxDepth=2)` reported MEDIUM risk, 18 test symbols, and five direct dependents: `load_archive`, the two cross-period ownership tests, the PostgreSQL cross-period test, and the overwidth-checkpoint test. Preserve the self-managed whole-month wrapper and its failure timing. [graph]
- `impact(target_uid=fiscal_month_bounds, direction="upstream", maxDepth=2)` reported **HIGH** risk, 12 impacted symbols, four affected process families, and six direct dependents: CLI `parse_args`, loader `_validate_period`, workflow `run`, and three fiscal tests. Do not broaden or repurpose `fiscal_month_bounds`; add separate subperiod validation/generation around it. [graph]
- `impact(target_uid=_validate_stage_ownership, direction="upstream", maxDepth=2)` reported **HIGH** risk because its sole direct caller `_load_snapshot` participates in loader, workflow, parser, reconciliation, and connection-aware flows. Keep both pre- and post-upsert checks intact. [graph]
- `impact(target_uid=transaction_cli.main, direction="upstream", maxDepth=2)` reported MEDIUM risk with ten direct dependents: the module entry call, test `invoke`, help, invalid count shape, invalid fiscal mapping, missing confirmation, output redaction, runtime cleanup, unknown-value redaction, and valid-month dispatch tests. New backfill behavior belongs in a separate CLI; the existing CLI changes only to share the global lease. [graph]
- `impact(target_uid=UsaSpendingTransactionIngestionAttempt, direction="upstream", maxDepth=2)` and the corresponding checkpoint impact each reported MEDIUM risk with 13 depth-1 import dependents but lower-bound ORM dispatch. Targeted `rg` confirmed explicit semantic consumers in workflow/loader and their model/migration/ingestion tests; `alembic/env.py` and `test_models.py` register all models, while health, market, legacy ingestion, and their tests import other model names and are graph name/module over-attribution. Run their smoke tests but do not change those modules. [graph][verified]
- `impact(target_uid=get_expected_transaction_count, direction="upstream", maxDepth=2)` reported LOW risk with two direct client-test dependents. Reuse it unchanged for every root/child advisory count. [graph]
- `impact(target_uid=_live_runtime, direction="upstream", maxDepth=2)` returned `risk: UNKNOWN` and zero callers. Targeted source search resolved the dynamic/default-argument boundary: `main(..., runtime_factory=_live_runtime)` is the production reference, while CLI tests inject factories. Treat runtime lease changes as covered by `main` and runtime-cleanup tests, not as unused code. [graph][verified: `services/api/app/usaspending/transaction_cli.py:143-167`, `services/api/app/usaspending/transaction_cli.py:246-258`]
- `trace(from=transaction_cli.main, to=TransactionIngestionWorkflow.run)` returned no path at `_Workflow` dynamic dispatch. Source confirms `_run_workflow()` invokes `workflow.run(...)`; the missing trace is an interface boundary, not absence of a call. [graph][verified: `services/api/app/usaspending/transaction_cli.py:57-67`, `services/api/app/usaspending/transaction_cli.py:158-167`]

No CRITICAL impact was reported. The two HIGH results are internal but load-bearing and are not waived: fiscal mapping reaches CLI/workflow/loader, and ownership validation reaches every atomic load path. [graph]

## 5. Statement-Level PDG Findings

The refreshed index contains a PDG layer, but focused `pdg_query` calls for `run`, `_locate_or_create_attempt`, and `_load_atomically` either required ambiguous-name disambiguation or returned zero control/data edges when anchored to the exact source file/symbol. Empty PDG output is treated as unavailable evidence, not a clean result. The following source-verified statement slices are therefore the controlling constraints. [graph][verified]

- **Workflow state order:** count is persisted before the `counted -> submitting` claim (`transaction_workflow.py:175-224`); only the worker that made that CAS may submit (`:226-258`); export rows/columns are persisted before download (`:289-307`); archive identity is persisted/validated before loading (`:309-408`); and load plus completion occurs only from `loading` (`:410-445`). The stop-after-count and oversized guard must sit after count persistence and before the submission claim. [verified]
- **Creation/concurrency:** `_locate_or_create_attempt()` obtains a deterministic advisory lock, row-locks active state, reuses completed state, and only then inserts (`transaction_workflow.py:451-514`). The `subdivided` status must remain inside the partial unique index's blocking predicate and be returned as a no-activity state, otherwise a later call could create and submit a replacement parent. [verified][inferred]
- **Atomic load:** `_load_atomically()` row-locks and revalidates the attempt, supplies persisted `export_rows`, checks the loader result, and completes on the same connection (`transaction_workflow.py:701-774`). Subperiod support must not move completion, checkpointing, or transaction replacement outside this transaction. [verified]
- **Ownership/reconciliation:** `_load_snapshot()` locks the exact period, stages/parses, reconciles counts/totals, checks ownership before and after upsert, deletes only disappeared rows whose action dates are inside the leaf, and reconciles checkpoint ownership before completion (`transaction_ingestion.py:210-339`, `transaction_ingestion.py:468-533`, `transaction_ingestion.py:568-656`). Stable IDs that move across leaves are a hard conflict and operator stop, not an automatic transfer. [verified][inferred]
- **Cancellation:** temporary archives are owned by `ExitStack`/private temporary contexts, and submission is preceded by durable `submitting`. A cancellation during the thread-backed submit can leave `submitting`; restart must convert it to `submission_unknown`, never assume no remote job was created. [verified: `services/api/app/usaspending/transaction_workflow.py:160-165`, `services/api/app/usaspending/transaction_workflow.py:226-255`, `services/api/app/usaspending/transaction_workflow.py:673-699`]

## 6. Proposed Changes

### `services/api/app/usaspending/fiscal_years.py`

- Keep `fiscal_year_for_date`, `fiscal_year_bounds`, and `fiscal_month_bounds` behavior unchanged. [verified]
- Add pure helpers for: generating the 84 chronological FY2020-FY2026 month roots; validating that an arbitrary inclusive period is nonempty, remains inside one calendar month, and belongs wholly to one fiscal year; balanced binary splitting; and asserting ordered coverage has no gaps/overlaps. Reject booleans/non-dates, reversed ranges, cross-month/cross-FY children, FY2027, and dates after 2026-09-30. [inferred]

### `services/api/app/db/models.py` — `UsaSpendingTransactionIngestionAttempt`

- Add nullable `parent_attempt_id` with a named self-FK using `ON DELETE RESTRICT`, an index for parent traversal, a uniqueness constraint on `(parent_attempt_id, period_start, period_end)`, and a self-parent prohibition. Existing rows remain null-parent roots. [inferred]
- Add `subdivided` to allowed states. Treat it as completed-at-present for audit timestamps, require `expected_rows > 500000`, forbid job/export/archive/load/checkpoint metadata, and leave it inside `status NOT IN ('completed','failed')` so the existing partial unique index permanently blocks a new active attempt for that parent period. Do not classify it as retryable failure. [inferred]
- Keep all `_0005` export-bound count and historical-failure guarantees unchanged. [verified: `services/api/app/db/models.py:204-322`]

### `services/api/alembic/versions/20261004_0006_usaspending_backfill_subdivisions.py` (new)

- Make `_0005` the sole parent and mirror the model's column, FK, unique/index, allowed-status, terminal timestamp, self-parent, and subdivision-metadata constraints with bounded names. No data rewrite is needed; all existing attempts backfill to `parent_attempt_id=NULL`. [inferred]
- Replace constraints/indexes in an order that never temporarily permits malformed state. The active-period predicate deliberately remains `status NOT IN ('completed','failed')`. [inferred]
- Downgrade must run guards before schema mutation and refuse if any row is `subdivided` or has `parent_attempt_id IS NOT NULL`; only an unused migration may safely round-trip to `_0005`. Verify `_0006` is the sole Alembic head. [inferred]

### `services/api/app/usaspending/transaction_workflow.py` — `TransactionIngestionWorkflow.run`, `_locate_or_create_attempt`, `_period_attempt_statement`, `_transition`, `_load_atomically`

- Retain `run(...)` as the exact-month API and delegate to a new subperiod-capable workflow entry after `fiscal_month_bounds`. Existing CLI/test callers therefore keep their signature and validation. [inferred]
- Add a stop-after-count mode used only by the backfill service. After persisting `expected_rows`, return the counted result without claiming submission. Independently, make every counted-state advance refuse submission when `expected_rows > DEFAULT_MAX_ROWS`; invalid count types remain fail-closed. [inferred]
- Accept validated leaf bounds without re-deriving a whole month; propagate those bounds unchanged through attempt lookup, count, submission, parser/loader, and result. [inferred]
- Return `subdivided` as a no-network/no-loader blocking result. Keep `failed` rows immutable, `submission_unknown` operator-blocked, and all post-submission resume behavior unchanged. [inferred]
- Preserve compare-and-swap transitions, persisted export metadata validation, archive identity checks, and `export_rows`-based atomic completion. [verified]

### `services/api/app/usaspending/transaction_ingestion.py` — `TransactionIngestionLoader._validate_period`, `load`, `load_with_connection`, `_load_snapshot`, `_validate_stage_ownership`

- Broaden only `_validate_period` to accept an exact inclusive subperiod contained within one calendar month and one FY. Keep type/order validation and reject cross-month/cross-FY ranges. `load()` remains a self-managed transaction wrapper; no parser/SQL safety limit is raised. [inferred]
- Preserve both ownership checks, the global stable-ID conflict behavior, exact row/distinct/decimal reconciliation, period-bounded deletion, checkpoint uniqueness, transaction-scoped advisory lock, and rollback semantics. [verified]

### `services/api/app/usaspending/transaction_backfill.py` (new)

- Implement the pure root planner/split proof, live coordinator, shared session advisory lease, attempt-tree traversal, completed-leaf verifier, storage estimator/gates, stop conditions, and sanitized progress/final manifest data. Reuse `UsaSpendingClient.get_expected_transaction_count`, `TransactionIngestionWorkflow`, `TransactionIngestionLoader`, attempts, checkpoints, and transaction tables. Do not parse archives, issue HTTP, or reproduce loader SQL here. [inferred]
- For each selected root in chronological order:
  1. validate live preconditions and unexpected overlapping state;
  2. if exact completed, verify and skip;
  3. if subdivided, verify exactly two deterministic children cover it and recurse;
  4. if failed, stop unless this invocation carries an exact explicit retry authorization for that failed attempt UUID; never update the failed row;
  5. if `submission_unknown`, stop unconditionally;
  6. otherwise advance/resume to counted only;
  7. if count exceeds 500000, atomically mark parent subdivided and create both child attempts, or stop if it is one day;
  8. for a leaf, recheck storage and limits, then let the existing workflow resume/submit/load;
  9. verify completion before moving to the next leaf. [inferred]
- Subdivision transaction: acquire/hold the global lease, `SELECT ... FOR UPDATE` the counted parent, validate exact status/count/metadata, insert both deterministic children with parent IDs, then CAS the parent to `subdivided` with `completed_at`; any conflict rolls back all three mutations. [inferred]
- Completed verification includes the exact October pilot anchor before any new submission. A completed record that is missing/corrupt, or an unexpected checkpoint/transaction range without its valid completed attempt, stops with a safe code. Never replace or repair it automatically. [inferred]
- Use a runner UUID only in manifests. No durable run table is necessary. Resume reconstructs the tree from attempts plus the deterministic splitter. [inferred]
- Hold a global session advisory lease for the whole live invocation. The existing single-month live runtime must acquire the same lease; second operators fail fast without contacting USAspending. [inferred]
- Storage model: report the documented 257.13 MiB/490377-row baseline (~550 modeled bytes/row) and the naive 84-October scenario (~21.1 GiB) only as a lower-bound illustration. Before every leaf submission, recalculate from actual post-pilot relation/index bytes and observed completed leaves, apply a conservative configurable factor, require local temp free space for archive/uncompressed/staging headroom, and require an operator-supplied verified database-volume capacity/free-space input because PostgreSQL alone cannot reliably report filesystem free bytes. Stop before submission if any value is missing or below the reserve/budget. [verified][inferred]
- Failure threshold is one in this phase. A `--max-failures` surface may exist only if validation fixes it to `1`; continuing after an unresolved failure would violate the stronger stop-on-first-failure requirement. [inferred]

### `services/api/app/usaspending/transaction_cli.py` — `_live_runtime`, `main`

- Acquire the same global transaction-ingestion lease around the existing single-month workflow runtime so legacy/manual month ingestion cannot overlap the backfill. Preserve argument parsing, safe output, exit codes, and one-month behavior. [inferred]

### `services/api/app/usaspending/transaction_backfill_cli.py` (new)

- Default to pure dry-run. Require an explicit `--execute-live` plus a range-specific confirmation token for live mode. Reject conflicting/repeated/secret-bearing arguments without echoing values, following `_SafeArgumentParser`. [inferred]
- Support full range planning plus bounded selection by one exact root/leaf, one FY, maximum leaf submissions/periods, and maximum estimated new bytes. Live mode must require at least one finite bound; “full FY2020-FY2026” is achieved through reviewed resumptions, not one unbounded invocation. [inferred]
- Dry-run emits all exact selected calendar-month roots, including the October root as `requires_live_integrity_check`; it marks count-dependent leaves unresolved and includes the deterministic split rule. It cannot truthfully name final leaves without counts and therefore must not pretend to. [inferred]
- Progress goes to sanitized JSON lines; the final manifest is written with mode 0600 using exclusive no-overwrite creation plus flush/fsync and is also emitted to stdout. On interruption, retain complete progress records and emit/return a safe interrupted code where possible. [inferred]

### Documentation

- Add `docs/runbooks/usaspending-transaction-backfill.md` with dry-run/live commands, confirmation/bounds, storage inputs, status/exit meanings, explicit retry procedure, `submission_unknown` prohibition, backup cadence, cancellation/restart behavior, and the guarded post-review acceptance procedure. Update `readme.md` only to link the runbook and new CLI; do not change legacy award-ingestion instructions. [inferred]
- Backup cadence: require a fresh restore-tested logical backup immediately before migration/live acceptance; run one FY per operational batch; stop for another restore-tested backup after each completed FY and before continuing whenever the storage threshold, schema/config change, corruption investigation, or manual retry boundary is reached. A manifest must record the operator-provided backup identifier, never credentials or paths containing secrets. [inferred]

## 7. Implementation Sequence

1. **Pure calendar/subdivision primitives.** Add root generation, subperiod validation, balanced splitting, and coverage proof; add leap-year/boundary/gap/overlap tests. Do not alter `fiscal_month_bounds`. Review gate: all 84 roots cover exactly 2019-10-01..2026-09-30 and exclude FY2027.
2. **Durable attempt-tree schema.** Update the model and add `_0006`; add SQLite/model checks and disposable PostgreSQL upgrade/downgrade/guard tests. Review gate: existing `_0005` completed/failed fixtures are byte/field-equivalent after upgrade; downgrade fails before mutation once subdivision evidence exists.
3. **Subperiod-safe workflow.** Keep `run` compatible, add validated leaf execution/stop-after-count, make oversized counted state non-submittable, and make `subdivided` a no-activity result. Review gate: no submit call is possible from `expected_rows > 500000`, `subdivided`, `submission_unknown`, `failed`, or corrupt metadata.
4. **Loader period widening.** Allow only same-month/same-FY subperiods while preserving parser limits, two ownership checks, exact reconciliation, checkpoint semantics, and atomic rollback. Review gate: existing whole-month loader suite plus new subperiod/cross-boundary tests pass on PostgreSQL 16.
5. **Coordinator and integrity verifier.** Implement chronological traversal, atomic split creation, pilot anchor verification, completed skip verification, failed/manual-retry rules, restart logic, global lease, storage gates, and sanitized manifest structures. Review gate: synthetic multi-period runs demonstrate one submission at a time and deterministic recovery after every persisted state.
6. **Operator CLI and shared lease.** Add safe dry-run/live parsing and output; wrap the existing CLI live runtime in the same lease. Review gate: help/dry-run never initialize database/settings/network; concurrent processes produce exactly one lease holder and zero second-worker requests.
7. **Operational documentation.** Add the runbook and README link, including backup/recovery cadence and guarded acceptance. Do not add scheduler, API, frontend, preset, or aggregate work.
8. **Focused verification.** Run offline tests first, then the disposable PostgreSQL 16 suite and `_0005 -> _0006 -> _0005 -> _0006` round trip. Capture counts/skips and confirm the test database name ends `_test`. Never point `TEST_DATABASE_URL` at Govtracts.
9. **GitNexus/review gate.** Re-index with PDG, run impact before every edited existing symbol, resolve all UNKNOWN results with targeted source search, run `detect_changes(scope="all")`, require `partial=false` and `truncated=false`, and explicitly review every HIGH/CRITICAL affected flow. Do not commit on an unresolved graph boundary.
10. **Separate post-implementation live acceptance (not part of implementation).** After independent review, verified backup, migration authorization, exact pilot preflight, and capacity checks, run dry-run then at most one or two leaf submissions under explicit live confirmation. Stop, reconcile manifests/database, make another recovery backup, and obtain separate authorization before FY backfill.

## 8. Test Strategy

### Unit/offline

- `services/api/tests/test_fiscal_years.py`: FY2020-FY2026 yields 84 roots; first/last exact; FY2027 excluded; February 2020/2024 leap days; balanced 28/29/30/31-day splits; one-day base case; recursive coverage; deliberate gap/overlap/reversal/cross-month/cross-FY rejection.
- `services/api/tests/test_usaspending_transaction_backfill.py` (new): pure dry-run has zero runtime/client calls; chronological filters and bounds; count 0/500000 remain leaves; 500001 recursively splits; parent never submitted; one-day overflow stops; split insert/update atomicity; restart from created/counted/subdivided/submitted/export_finished/archive_hashed/loading/completed; exact October pilot skip; corrupt attempt/checkpoint/count/distinct/total/ownership stops; failed stops unless exact retry token; retry creates a new row and leaves failed unchanged; `submission_unknown` always stops; corrections moving stable IDs across leaves stop; max-period/storage/one-FY bounds; temp/database capacity failures occur before submit; cancellation cleanup; sanitized manifests.
- `services/api/tests/test_usaspending_transaction_backfill_cli.py` (new): default/help/dry-run avoid settings/DB/network; explicit live token/bounds required; repeated/unknown values are not echoed; manifest exclusive/no-overwrite/0600 behavior; safe exit codes; progress/final records omit URLs, exceptions, response bodies, filenames, credentials, and row data.
- `services/api/tests/test_usaspending_transaction_workflow.py`: preserve existing month helper/invalid-period tests; add stop-after-count; oversized counted guard; arbitrary valid child periods; invalid cross-month/FY periods; subdivided no-activity; every resume state; count drift; archive identity; atomic completion; cancellation during submission becomes operator-resolution on restart.
- `services/api/tests/test_usaspending_transaction_ingestion.py`: retain all five direct `load` dependents; add valid subperiod replacement/deletion, sibling checkpoint ownership, gaps/overlaps supplied to loader, cross-period stable-ID corrections, rollback, and PostgreSQL advisory serialization across sibling leaves.
- `services/api/tests/test_usaspending_transaction_attempt_models.py`: parent FK/unique/index/constraints; subdivided metadata contract; subdivided blocks replacement parent; historical failed null export metadata; completed export/loaded equality.
- `services/api/tests/test_usaspending_transaction_attempt_migration.py`: `_0006` DDL parity, sole head, existing completed/failed preservation, unused round trip, downgrade guards, child FK/uniqueness, and blocking partial-index behavior on PostgreSQL.
- `services/api/tests/test_usaspending_bulk_client.py` and `services/api/tests/test_usaspending_transaction_export.py`: keep count/status untrusted-input, 500000 cap, archive safety, duplicate, date, A-D, decimal, and resource-limit regressions green; do not raise parser limits.
- `services/api/tests/test_usaspending_transaction_cli.py`: existing month CLI behavior plus global-lease busy/cleanup paths; all ten graph-identified `main` direct tests remain green.

### Disposable PostgreSQL 16 integration

- Real migrations on a uniquely named `_test` database: `_0005 -> _0006 -> _0005 -> _0006`, with guard rows proving downgrade refuses used subdivision state before dropping anything.
- Two processes contend for the global session lease; exactly one enters runtime, the loser fails fast, and no network stub is called. Kill the holder and prove connection close releases the lease.
- Two transactions race parent subdivision; exactly one parent transition and one deterministic child pair commit.
- Load sibling subperiods, restart, skip completed leaves after full integrity queries, inject wrong checkpoint ownership/count/total, and prove stop/no mutation.
- Inject parser/SQL failures after staging/upsert/completion and prove transaction rows, checkpoint, and attempt state roll back atomically.

### Synthetic end to end

Use mocked counts, jobs, polling, archives, and downloads with a disposable PostgreSQL 16 database. Cover multiple FY roots, an oversized 31-day root splitting more than once, the known completed-pilot fixture, interruption after each durable state, retry authorization, exact final coverage, sequential call ordering, manifest totals, and temporary archive removal. No real USAspending hostname or real database may be reachable.

### Verification commands

From `services/api` using the existing `.venv`:

```bash
.venv/bin/pytest -q \
  tests/test_fiscal_years.py \
  tests/test_usaspending_bulk_client.py \
  tests/test_usaspending_transaction_export.py \
  tests/test_usaspending_transaction_attempt_models.py \
  tests/test_usaspending_transaction_cli.py \
  tests/test_usaspending_transaction_workflow.py \
  tests/test_usaspending_transaction_backfill.py \
  tests/test_usaspending_transaction_backfill_cli.py
```

Against a disposable PostgreSQL 16 database whose name ends `_test`:

```bash
TEST_DATABASE_URL="$TEST_DATABASE_URL" .venv/bin/pytest -q -m integration \
  tests/test_usaspending_transaction_attempt_migration.py \
  tests/test_usaspending_transaction_ingestion.py \
  tests/test_usaspending_transaction_workflow.py \
  tests/test_usaspending_transaction_backfill.py
```

Migration round trip on that disposable database only:

```bash
DATABASE_URL="$TEST_DATABASE_URL" .venv/bin/alembic upgrade 20261004_0006
DATABASE_URL="$TEST_DATABASE_URL" .venv/bin/alembic downgrade 20261003_0005
DATABASE_URL="$TEST_DATABASE_URL" .venv/bin/alembic upgrade 20261004_0006
DATABASE_URL="$TEST_DATABASE_URL" .venv/bin/alembic heads
```

Then run the complete backend suite and GitNexus gates. Do not use the real `DATABASE_URL`:

```bash
.venv/bin/pytest -q
```

```text
GitNexus impact(direction="upstream", maxDepth=2) before each existing-symbol edit
GitNexus detect_changes(scope="all") before commit; require partial=false and truncated=false
```

## 9. Risk and Impact Analysis

- **HIGH — fiscal boundary:** `fiscal_month_bounds` directly feeds CLI parsing, loader validation, workflow execution, and three fiscal tests. Preserve it unchanged; new subperiod helpers must prove same-month/same-FY containment, leap days, and exact 84-root coverage. [graph][verified]
- **HIGH — ownership boundary:** `_validate_stage_ownership` directly feeds `_load_snapshot`, which reaches loader/workflow parsing, reconciliation, and atomic completion processes. Never remove either invocation. A stable ID corrected across leaves remains an operator-visible hard stop. [graph][verified]
- **MEDIUM — loader:** direct dependents are `load_archive`, three cross-period race/conflict tests (including PostgreSQL), and overwidth checkpoint validation. Keep validation timing and self-managed wrapper behavior; add, do not replace, subperiod scenarios. [graph]
- **MEDIUM — CLI:** every direct `main` caller/test listed in §4 must retain safe parsing/output/runtime cleanup. The new command is separate to avoid broadening the legacy one-month surface. [graph]
- **MEDIUM/lower-bound — ORM models:** graph depth-1 files are `alembic/env.py`, health/market routes, legacy ingestion, transaction loader/workflow, and seven model/market/transaction tests. Source search shows health/market/legacy files do not reference the attempt/checkpoint classes directly, but schema registration means full backend/model/route smoke tests are still required. [graph][verified]
- **LOW — workflow run/attempt lookup/count client:** preserve the test helper, invalid-period test, counted barrier override, and two count-client tests identified at depth 1. [graph]
- **UNKNOWN resolved — `_live_runtime`:** the graph missed the default-argument reference. Source search confirms production `main` and injected test factories; lease cleanup must be tested on success, exception, and cancellation. [graph][verified]
- **Schema risk:** a `subdivided` row must both be audit-terminal and continue to block a replacement active attempt. Constraint/index semantics are intentionally asymmetric. Migration downgrade must refuse used state, and deployment must migrate schema before code that writes parent links/status. [inferred]
- **Concurrency risk:** exact-period locks do not serialize sibling leaves or an unrelated legacy month CLI. The shared global session lease is mandatory for production entry points; period locks and CAS remain defense in depth. Session loss releases the lease, while durable attempt status drives restart. [inferred]
- **Submission ambiguity:** cancellation/network loss after submission starts can create a remote job whose acceptance is unknown. Persist `submitting` first; any restart without the original claim moves to `submission_unknown` and stops. Never infer safety from absence of URLs. [verified][inferred]
- **Count drift:** parent advisory counts decide only subdivision. Leaf parser/load/checkpoint/completion use each persisted export-bound count. Parent count need not equal the sum of child pre-counts/exports because data can change between requests; report drift but do not weaken leaf reconciliation. [verified][inferred]
- **Corrections:** in-period reruns replace a leaf snapshot; cross-period stable-ID movement is rejected to avoid stealing ownership. Corrupt completed state is never auto-reloaded. Both require reviewed operator remediation. [verified][inferred]
- **Storage/performance:** the 21.1 GiB naive modeled total assumes every month resembles October and undercounts current indexes, WAL, TOAST, staging, bloat, and volume reserve. Use it only for initial planning; gate each leaf on live measured relation growth and trusted filesystem/capacity inputs. Index growth and vacuum/WAL headroom are first-class stop conditions. [verified][inferred]
- **Rate/polling:** serial execution bounds concurrency. Keep the client's validated polling intervals, `Retry-After`, timeouts, host allowlists, and resumable transient-state behavior. An HTTP 429/5xx/count failure stops this invocation and is resumed later; the runner must not add a retry loop around ambiguous submission. [verified: `services/api/app/usaspending/client.py:195-280`, `services/api/app/usaspending/client.py:380-562`]
- **Archive/security:** retain HTTPS/host checks, redirect validation, fixed private destination, byte/hash identity, ZIP member restrictions, compression/uncompressed/field/record/row limits, exact headers, A-D/date/fiscal checks, duplicate detection, decimal precision, and cleanup on every exit. [verified]
- **Operational recovery:** one-FY batches plus restore-tested backups create bounded recovery points. A runner failure never triggers rollback of already completed leaves; resumption verifies them and continues only after operator review. [inferred]
- **Compatibility:** no production route, legacy award search, frontend, preset, aggregate, or scheduler imports the new runner. Model registration causes schema visibility only; route behavior must remain unchanged in the full suite. [verified]

## 10. Files Expected to Change

| File | Symbols | Reason |
| ---- | ------- | ------ |
| `services/api/app/usaspending/fiscal_years.py` | existing fiscal helpers; new pure period helpers | Generate/validate roots and deterministic child coverage without changing month semantics. |
| `services/api/app/db/models.py` | `UsaSpendingTransactionIngestionAttempt` | Add parent linkage and constrained `subdivided` state. |
| `services/api/alembic/versions/20261004_0006_usaspending_backfill_subdivisions.py` | new migration | Apply/reverse the minimal durable schema extension with guards. |
| `services/api/app/usaspending/transaction_workflow.py` | `TransactionIngestionWorkflow.run`, `_locate_or_create_attempt`, `_period_attempt_statement`, `_transition`, `_load_atomically` | Add subperiod/count-stop entry while preserving state/atomicity guarantees. |
| `services/api/app/usaspending/transaction_ingestion.py` | `TransactionIngestionLoader._validate_period`, `load`, `load_with_connection`, `_load_snapshot`, `_validate_stage_ownership` | Permit safe same-month leaves without weakening loading. |
| `services/api/app/usaspending/transaction_backfill.py` | new module | Pure planner, durable traversal, integrity, locks, limits, storage, manifests. |
| `services/api/app/usaspending/transaction_backfill_cli.py` | new module | Safe operator dry-run/live command. |
| `services/api/app/usaspending/transaction_cli.py` | `_live_runtime`, `main` | Share the global serial-execution lease. |
| `services/api/tests/test_fiscal_years.py` | fiscal tests | Boundary/leap/subdivision coverage. |
| `services/api/tests/test_usaspending_transaction_attempt_models.py` | attempt model tests | New status/link constraints and preservation. |
| `services/api/tests/test_usaspending_transaction_attempt_migration.py` | migration tests | `_0006` parity, guards, round trip, sole head. |
| `services/api/tests/test_usaspending_transaction_workflow.py` | workflow tests | Count-stop, oversized guard, subperiod/restart/concurrency. |
| `services/api/tests/test_usaspending_transaction_ingestion.py` | loader tests | Subperiod ownership/reconciliation/rollback/PostgreSQL locks. |
| `services/api/tests/test_usaspending_transaction_cli.py` | CLI tests | Shared lease and unchanged safe output. |
| `services/api/tests/test_usaspending_transaction_backfill.py` | new tests | Coordinator unit/integration/synthetic end-to-end coverage. |
| `services/api/tests/test_usaspending_transaction_backfill_cli.py` | new tests | No-I/O dry-run, live guards, sanitization/manifests. |
| `services/api/tests/test_usaspending_bulk_client.py` | existing regression tests if fixture extension is needed | Preserve count/export status validation. |
| `services/api/tests/test_usaspending_transaction_export.py` | existing regression tests if fixture extension is needed | Preserve parser/security limits for child periods. |
| `docs/runbooks/usaspending-transaction-backfill.md` | new runbook | Operator bounds, backups, recovery, acceptance procedure. |
| `readme.md` | tests/operations link only | Discover the new runbook without changing legacy ingestion. |

## 11. Reusable Implementation Context

```yaml
implementation_context:
  task_summary: "Implement a serial, resumable FY2020-FY2026 USAspending transaction backfill runner with whole-month roots, deterministic count-driven child attempts, strict completed-state verification, and safe operator controls."
  acceptance_criteria:
    - "Coverage is exactly 2019-10-01 through 2026-09-30 in chronological, gap-free, nonoverlapping roots/leaves; FY2027 is excluded."
    - "No export is submitted for a counted period over 500000 rows or for a durable subdivided parent."
    - "Only one production transaction-ingestion invocation can hold the shared database lease."
    - "Completed periods, especially the supplied October pilot IDs/count/total, are skipped only after exact integrity verification."
    - "Failed history is immutable; submission_unknown always requires operator resolution."
    - "Leaf parse/load/checkpoint/completion continues to use persisted export_rows atomically."
    - "Dry-run performs no settings, database, filesystem mutation except an explicitly requested safe manifest, or network activity."
    - "No preset, aggregate, API, frontend, scheduler, legacy award, or live backfill behavior is added."
  evidence_provenance: {"schema_version":2,"head_commit":"e2fa8436ef847076403cf004295efb7ce4892809","generated_plan_path":"docs/plans/2026-10-04-gitnexus-plan-usaspending-backfill-runner.md","global_dirty_digest":{"algorithm":"sha256","canonicalization":"gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records","value":"0a9c85780067d9afcd0764f307b60891e3cee927ee11eaeb5ec7826d10fd82cd"},"cited_path_manifest":[{"path":"docs/runbooks/usaspending-transaction-backfill.md","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"docs/spikes/usaspending-transaction-export.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:10c080ca3bdc07d8d33cb7f7c10cbe40f57d7c79b2ea1f453c01795ce89f81f3","index_digest":"sha256:10c080ca3bdc07d8d33cb7f7c10cbe40f57d7c79b2ea1f453c01795ce89f81f3","worktree_digest":"sha256:10c080ca3bdc07d8d33cb7f7c10cbe40f57d7c79b2ea1f453c01795ce89f81f3","untracked_digest":"absent"},{"path":"readme.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:41a4c3d1ab2d65773680e17fa045c90dcbc3c265d154bd2d8b531b090c2d3940","index_digest":"sha256:41a4c3d1ab2d65773680e17fa045c90dcbc3c265d154bd2d8b531b090c2d3940","worktree_digest":"sha256:41a4c3d1ab2d65773680e17fa045c90dcbc3c265d154bd2d8b531b090c2d3940","untracked_digest":"absent"},{"path":"services/api/alembic/versions/20260927_0004_usaspending_transaction_attempts.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:4844334b03b0269c0e6df1b74dfe306a95ea9ccb36efa6d19e2ae21cc535f82a","index_digest":"sha256:4844334b03b0269c0e6df1b74dfe306a95ea9ccb36efa6d19e2ae21cc535f82a","worktree_digest":"sha256:4844334b03b0269c0e6df1b74dfe306a95ea9ccb36efa6d19e2ae21cc535f82a","untracked_digest":"absent"},{"path":"services/api/alembic/versions/20261003_0005_usaspending_export_bound_counts.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:c51e90a0d81ec822b99436d7c19437831e4880bc228e7e8ee07842b98c86566a","index_digest":"sha256:c51e90a0d81ec822b99436d7c19437831e4880bc228e7e8ee07842b98c86566a","worktree_digest":"sha256:c51e90a0d81ec822b99436d7c19437831e4880bc228e7e8ee07842b98c86566a","untracked_digest":"absent"},{"path":"services/api/alembic/versions/20261004_0006_usaspending_backfill_subdivisions.py","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"services/api/app/db/models.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:54c5e5a32a34612551b6353b034462d665599801521c721051ffeacb7b6d81d4","index_digest":"sha256:54c5e5a32a34612551b6353b034462d665599801521c721051ffeacb7b6d81d4","worktree_digest":"sha256:54c5e5a32a34612551b6353b034462d665599801521c721051ffeacb7b6d81d4","untracked_digest":"absent"},{"path":"services/api/app/usaspending/client.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:6efbcfbe3c68e1544552761c1955532fa744bd4c11bad91f7ec28d205e9e991b","index_digest":"sha256:6efbcfbe3c68e1544552761c1955532fa744bd4c11bad91f7ec28d205e9e991b","worktree_digest":"sha256:6efbcfbe3c68e1544552761c1955532fa744bd4c11bad91f7ec28d205e9e991b","untracked_digest":"absent"},{"path":"services/api/app/usaspending/fiscal_years.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:9721cffc58d6983450cbe448b3714d2e5f68a3cbc780d16de4f2db08f38f1a43","index_digest":"sha256:9721cffc58d6983450cbe448b3714d2e5f68a3cbc780d16de4f2db08f38f1a43","worktree_digest":"sha256:9721cffc58d6983450cbe448b3714d2e5f68a3cbc780d16de4f2db08f38f1a43","untracked_digest":"absent"},{"path":"services/api/app/usaspending/transaction_backfill.py","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"services/api/app/usaspending/transaction_backfill_cli.py","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"services/api/app/usaspending/transaction_cli.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:04f53106155e8fe55140d58408c14478a5fa88d6790d3de928016fd5ff02cf39","index_digest":"sha256:04f53106155e8fe55140d58408c14478a5fa88d6790d3de928016fd5ff02cf39","worktree_digest":"sha256:04f53106155e8fe55140d58408c14478a5fa88d6790d3de928016fd5ff02cf39","untracked_digest":"absent"},{"path":"services/api/app/usaspending/transaction_export.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:01759ee0e89df6c0d18a1388ed2206479014a1dc77e4b23fbab16f7dc05144a6","index_digest":"sha256:01759ee0e89df6c0d18a1388ed2206479014a1dc77e4b23fbab16f7dc05144a6","worktree_digest":"sha256:01759ee0e89df6c0d18a1388ed2206479014a1dc77e4b23fbab16f7dc05144a6","untracked_digest":"absent"},{"path":"services/api/app/usaspending/transaction_ingestion.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:533133454015ec982586f21c35781dd9db38a9374a5d9390ddc97e7f6a28b857","index_digest":"sha256:533133454015ec982586f21c35781dd9db38a9374a5d9390ddc97e7f6a28b857","worktree_digest":"sha256:533133454015ec982586f21c35781dd9db38a9374a5d9390ddc97e7f6a28b857","untracked_digest":"absent"},{"path":"services/api/app/usaspending/transaction_workflow.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:e130484bcd6bb12de598302b80a6fb207a5578162e54e336668507cb5cd49b8c","index_digest":"sha256:e130484bcd6bb12de598302b80a6fb207a5578162e54e336668507cb5cd49b8c","worktree_digest":"sha256:e130484bcd6bb12de598302b80a6fb207a5578162e54e336668507cb5cd49b8c","untracked_digest":"absent"},{"path":"services/api/pyproject.toml","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:a9355439dcea53320d8cba1c386b1f8986d6168ba6d1fc23d83699df837fe921","index_digest":"sha256:a9355439dcea53320d8cba1c386b1f8986d6168ba6d1fc23d83699df837fe921","worktree_digest":"sha256:a9355439dcea53320d8cba1c386b1f8986d6168ba6d1fc23d83699df837fe921","untracked_digest":"absent"},{"path":"services/api/tests/test_fiscal_years.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:fa4114e90cd02c7ee0d72e483c654fa1bb4b5466a9bef0565aef1abc32c188eb","index_digest":"sha256:fa4114e90cd02c7ee0d72e483c654fa1bb4b5466a9bef0565aef1abc32c188eb","worktree_digest":"sha256:fa4114e90cd02c7ee0d72e483c654fa1bb4b5466a9bef0565aef1abc32c188eb","untracked_digest":"absent"},{"path":"services/api/tests/test_usaspending_bulk_client.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:25ae3fac03aa28468085d4f81833bffc5f96c25810700ae0a5e5f73fd421be1e","index_digest":"sha256:25ae3fac03aa28468085d4f81833bffc5f96c25810700ae0a5e5f73fd421be1e","worktree_digest":"sha256:25ae3fac03aa28468085d4f81833bffc5f96c25810700ae0a5e5f73fd421be1e","untracked_digest":"absent"},{"path":"services/api/tests/test_usaspending_transaction_attempt_migration.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:2966d811ec76e91841be1f60f62f76d92d45b96e69d8157ee671692e05e636c9","index_digest":"sha256:2966d811ec76e91841be1f60f62f76d92d45b96e69d8157ee671692e05e636c9","worktree_digest":"sha256:2966d811ec76e91841be1f60f62f76d92d45b96e69d8157ee671692e05e636c9","untracked_digest":"absent"},{"path":"services/api/tests/test_usaspending_transaction_attempt_models.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:a4120736d5839f3a3397e4fbcacd5dff86af1c517d4a64e8819281155636a33a","index_digest":"sha256:a4120736d5839f3a3397e4fbcacd5dff86af1c517d4a64e8819281155636a33a","worktree_digest":"sha256:a4120736d5839f3a3397e4fbcacd5dff86af1c517d4a64e8819281155636a33a","untracked_digest":"absent"},{"path":"services/api/tests/test_usaspending_transaction_backfill.py","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"services/api/tests/test_usaspending_transaction_backfill_cli.py","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"services/api/tests/test_usaspending_transaction_cli.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:ca0648f03add4524f48e49ca8203c57ae9e7f08ee4040c52c2e1230c1cf5592b","index_digest":"sha256:ca0648f03add4524f48e49ca8203c57ae9e7f08ee4040c52c2e1230c1cf5592b","worktree_digest":"sha256:ca0648f03add4524f48e49ca8203c57ae9e7f08ee4040c52c2e1230c1cf5592b","untracked_digest":"absent"},{"path":"services/api/tests/test_usaspending_transaction_export.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:6dc4970921ce88cee1a66a0cd5841a13d3ba5c52afe14d8e383f18b80d3eff41","index_digest":"sha256:6dc4970921ce88cee1a66a0cd5841a13d3ba5c52afe14d8e383f18b80d3eff41","worktree_digest":"sha256:6dc4970921ce88cee1a66a0cd5841a13d3ba5c52afe14d8e383f18b80d3eff41","untracked_digest":"absent"},{"path":"services/api/tests/test_usaspending_transaction_ingestion.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:8b637071366169a9aab5d81bac0d105396f831fc983f15a7432c5ec12bf7c66e","index_digest":"sha256:8b637071366169a9aab5d81bac0d105396f831fc983f15a7432c5ec12bf7c66e","worktree_digest":"sha256:8b637071366169a9aab5d81bac0d105396f831fc983f15a7432c5ec12bf7c66e","untracked_digest":"absent"},{"path":"services/api/tests/test_usaspending_transaction_workflow.py","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:77ec566214a6ebb2563efd52a3ea7075429d602503bbbc6ab73a679fa5d6264b","index_digest":"sha256:77ec566214a6ebb2563efd52a3ea7075429d602503bbbc6ab73a679fa5d6264b","worktree_digest":"sha256:77ec566214a6ebb2563efd52a3ea7075429d602503bbbc6ab73a679fa5d6264b","untracked_digest":"absent"}]}
  primary_symbols:
    - symbol: "TransactionIngestionWorkflow.run"
      file: "services/api/app/usaspending/transaction_workflow.py"
      lines: "134-449"
      role: "Existing restartable state machine; preserve whole-month API and add count-stop/subperiod delegation."
    - symbol: "TransactionIngestionWorkflow._locate_or_create_attempt"
      file: "services/api/app/usaspending/transaction_workflow.py"
      lines: "451-514"
      role: "Attempt reuse/creation and exact-period concurrency boundary."
    - symbol: "TransactionIngestionWorkflow._load_atomically"
      file: "services/api/app/usaspending/transaction_workflow.py"
      lines: "701-745"
      role: "Atomic loader/checkpoint/attempt completion boundary using export_rows."
    - symbol: "TransactionIngestionLoader._validate_period"
      file: "services/api/app/usaspending/transaction_ingestion.py"
      lines: "341-358"
      role: "Current whole-month restriction that must be safely widened for child periods."
    - symbol: "UsaSpendingTransactionIngestionAttempt"
      file: "services/api/app/db/models.py"
      lines: "204-322"
      role: "Durable state to extend with parentage and subdivided blocking status."
  related_symbols:
    - symbol: "fiscal_month_bounds"
      relationship: "CALLS"
      relevance: "HIGH-impact exact-month mapping used by CLI/workflow/loader; preserve unchanged."
    - symbol: "UsaSpendingClient.get_expected_transaction_count"
      relationship: "CALLS"
      relevance: "Authoritative advisory count component reused for every tree node."
    - symbol: "TransactionIngestionLoader._validate_stage_ownership"
      relationship: "CALLS"
      relevance: "HIGH-impact cross-period stable-ID safety gate, invoked twice."
    - symbol: "TransactionExportParser"
      relationship: "constructed-by loader"
      relevance: "Already supports arbitrary inclusive ranges and enforces safety/count/duplicate/date/decimal rules."
    - symbol: "transaction_cli._live_runtime"
      relationship: "default callable value in main"
      relevance: "UNKNOWN graph result resolved by source; must share the global lease."
  execution_path:
    - "Pure CLI parse -> 84 root plan/filter -> sanitized dry-run; no live runtime."
    - "Live confirmation -> settings/DB -> global session advisory lease -> exact pilot/state/storage preflight."
    - "Chronological root/tree traversal -> completed verify/skip, subdivided verify/recurse, blocked state stop."
    - "Created node -> existing advisory count -> persist counted -> return before submission."
    - "Oversized multi-day counted node -> atomic parent subdivided + two child attempts; oversized day -> stop."
    - "Leaf -> storage gate -> existing workflow claim/submit/poll/download/hash -> atomic loader/checkpoint/completion."
    - "Post-leaf integrity verify -> fsynced sanitized progress -> next leaf or bounded final manifest."
  pdg_constraints:
    - description: "PDG queries returned no usable Python statement edges after refresh; source control/data order is authoritative and empty graph results are not an all-clear."
      affected_statements:
        - "services/api/app/usaspending/transaction_workflow.py:175"
        - "services/api/app/usaspending/transaction_workflow.py:206"
        - "services/api/app/usaspending/transaction_workflow.py:289"
        - "services/api/app/usaspending/transaction_workflow.py:701"
      implementation_consequence: "Insert stop-after-count/oversize logic before submission claim and retain persisted export/archive/atomic load order."
  architectural_patterns:
    - pattern: "Compare-and-swap attempt transitions plus row/advisory locks"
      example_location: "services/api/app/usaspending/transaction_workflow.py:_transition"
      usage_guidance: "Use the same pattern for counted->subdivided and child insertion in one transaction."
    - pattern: "Caller-owned atomic loader transaction"
      example_location: "services/api/app/usaspending/transaction_workflow.py:_load_atomically"
      usage_guidance: "Never split target rows, checkpoint, and completed attempt across commits."
    - pattern: "Safe argparse and JSON serialization"
      example_location: "services/api/app/usaspending/transaction_cli.py"
      usage_guidance: "Do not echo invalid inputs, exceptions, URLs, or secrets."
  files_to_modify:
    - file: "services/api/app/usaspending/fiscal_years.py"
      symbols: ["fiscal_year_for_date", "fiscal_year_bounds", "fiscal_month_bounds", "new pure period helpers"]
      intended_change: "Add exact range generation/validation/splitting without altering existing helpers."
    - file: "services/api/app/db/models.py"
      symbols: ["UsaSpendingTransactionIngestionAttempt"]
      intended_change: "Parent self-FK and constrained subdivided status."
    - file: "services/api/alembic/versions/20261004_0006_usaspending_backfill_subdivisions.py"
      symbols: ["upgrade", "downgrade"]
      intended_change: "Guarded reversible-until-used migration and sole head."
    - file: "services/api/app/usaspending/transaction_workflow.py"
      symbols: ["TransactionIngestionWorkflow.run", "_locate_or_create_attempt", "_period_attempt_statement", "_transition", "_load_atomically"]
      intended_change: "Count-only stop, hard oversized guard, arbitrary child entry, subdivided no-op."
    - file: "services/api/app/usaspending/transaction_ingestion.py"
      symbols: ["TransactionIngestionLoader._validate_period", "load", "load_with_connection", "_load_snapshot", "_validate_stage_ownership"]
      intended_change: "Same-month/same-FY child periods with unchanged atomic safety."
    - file: "services/api/app/usaspending/transaction_backfill.py"
      symbols: ["new backfill planning/coordinator types"]
      intended_change: "Durable tree traversal, verifier, lease, storage gates, manifests."
    - file: "services/api/app/usaspending/transaction_backfill_cli.py"
      symbols: ["new safe parser/runtime/main"]
      intended_change: "Dry-run by default and bounded explicitly confirmed live execution."
    - file: "services/api/app/usaspending/transaction_cli.py"
      symbols: ["_live_runtime", "main"]
      intended_change: "Acquire shared ingestion lease without changing month semantics/output."
    - file: "services/api/tests/test_usaspending_transaction_backfill.py"
      symbols: ["new tests"]
      intended_change: "Unit, integration, concurrency, and synthetic end-to-end behavior."
    - file: "services/api/tests/test_usaspending_transaction_backfill_cli.py"
      symbols: ["new tests"]
      intended_change: "No-I/O dry-run, live guards, sanitization, manifest safety."
  tests:
    - file: "services/api/tests/test_fiscal_years.py"
      scenarios: ["84 exact roots", "leap years", "balanced recursive splits", "gap/overlap/cross-boundary rejection"]
    - file: "services/api/tests/test_usaspending_transaction_backfill.py"
      scenarios: ["count-driven subdivision", "pilot skip/integrity corruption", "restart/idempotency", "failed/submission_unknown", "storage/cancellation", "PostgreSQL multiprocess lease", "synthetic multi-FY flow"]
    - file: "services/api/tests/test_usaspending_transaction_workflow.py"
      scenarios: ["stop after count", "oversized never submits", "child resume states", "atomic export_rows completion"]
    - file: "services/api/tests/test_usaspending_transaction_ingestion.py"
      scenarios: ["subperiod replacement", "sibling ownership", "cross-period correction stop", "rollback and advisory locks"]
    - file: "services/api/tests/test_usaspending_transaction_attempt_migration.py"
      scenarios: ["model parity", "unused round trip", "used downgrade guards", "sole head"]
    - file: "services/api/tests/test_usaspending_transaction_backfill_cli.py"
      scenarios: ["no DB/network dry-run", "explicit live confirmation", "safe output", "0600 no-overwrite manifest"]
  verification_commands:
    - "cd services/api && .venv/bin/pytest -q tests/test_fiscal_years.py tests/test_usaspending_bulk_client.py tests/test_usaspending_transaction_export.py tests/test_usaspending_transaction_attempt_models.py tests/test_usaspending_transaction_cli.py tests/test_usaspending_transaction_workflow.py tests/test_usaspending_transaction_backfill.py tests/test_usaspending_transaction_backfill_cli.py"
    - "cd services/api && TEST_DATABASE_URL=\"$TEST_DATABASE_URL\" .venv/bin/pytest -q -m integration tests/test_usaspending_transaction_attempt_migration.py tests/test_usaspending_transaction_ingestion.py tests/test_usaspending_transaction_workflow.py tests/test_usaspending_transaction_backfill.py"
    - "On disposable _test only: alembic _0005 -> _0006 -> _0005 -> _0006 and verify sole head"
    - "cd services/api && .venv/bin/pytest -q"
    - "GitNexus impact before edits; detect_changes(scope=all) before commit with partial=false/truncated=false"
  risks:
    - "HIGH fiscal_month_bounds breadth; preserve symbol semantics."
    - "HIGH loader ownership breadth; preserve both checks and atomicity."
    - "Subdivided must be timestamp-terminal but uniqueness-blocking."
    - "Global lease must cover both CLIs and release on process death."
    - "Parent counts choose splits only; leaf export_rows remain reconciliation truth."
    - "Cross-leaf stable-ID corrections and completed corruption require operator review."
    - "October storage model is a lower-bound, not capacity proof."
  assumptions:
    - "Before live acceptance, query the real database read-only to verify schema _0006 and the supplied October attempt/checkpoint/count/total; planning did not verify them."
    - "Before each live invocation, obtain a trusted database-volume capacity/free-space value outside PostgreSQL and pass it to the runner; fail closed if unavailable."
    - "The implementation environment provides PostgreSQL 16 and a disposable database ending _test for integration/migration tests."
  open_questions:
    - "No blocking design question. Operational owners must choose conservative database capacity and reserve values after measuring the post-pilot table/index footprint; defaults must fail closed rather than guess."
  avoid:
    - "Do not repeat full repository discovery; re-anchor provenance and impact only changed symbols."
    - "Do not access the real database or USAspending during implementation/tests."
    - "Do not submit/download exports or invoke either live CLI during implementation."
    - "Do not mutate/reopen terminal failed attempts, auto-resubmit submission_unknown, or auto-repair completed corruption."
    - "Do not raise parser limits or weaken archive, URL, duplicate, date/FY, monetary, ownership, count, transaction, or rollback controls."
    - "Do not add presets, aggregates, API/frontend behavior, scheduler/cron, or change legacy award ingestion."
```

## 12. Assumptions and Open Questions

### Assumptions to re-verify

- [assumed] The real database will still be at the separately authorized migration revision and contain the supplied October attempt/checkpoint/count/total when live acceptance is later authorized. Verify read-only after migration and before any request; stop on any difference.
- [assumed] A PostgreSQL 16 disposable `_test` database is available for integration and migration tests. Never reuse the real Govtracts database.
- [assumed] Operations can provide a trustworthy database-volume capacity/free-space measurement. PostgreSQL relation sizes measure use, not filesystem free space; live mode must fail closed rather than infer it.
- [assumed] USAspending continues to accept inclusive action-date ranges down to one day for A-D bulk exports. Mock this contract offline; a live rejection is a stop, not a reason to change partitioning automatically.

### Resolved design choices

- [inferred] Add no backfill-run table. Extend attempts minimally with parentage and `subdivided`; derive each invocation's manifest from durable attempts/checkpoints.
- [inferred] Use balanced binary inclusive-date splits, not ad hoc 1-15/16-end or calendar-week rules; it is deterministic for every month length and proves coverage recursively.
- [inferred] A pure dry-run reports exact roots and unresolved count-dependent leaves. Claiming exact leaves offline would be misleading because counts are not available without the prohibited network/database work.
- [inferred] Stop on the first unresolved failure. “Failure threshold” is fixed to one in this phase.
- [inferred] Explicit retry authorization creates a new attempt after a failed row; it never edits or reopens the failed row. `submission_unknown` has no equivalent automatic authorization path.
- [inferred] Existing parser limit stays 500000. A one-day count above that limit is an operator-blocking condition requiring a separately reviewed acquisition design.
- [inferred] The newer explicit backfill requirement to subdivide oversized periods supersedes the Phase 0 note against partitioning solely to evade a service limit, while retaining the spike's security and exact-reconciliation conditions. [verified: `docs/spikes/usaspending-transaction-export.md:13-23`, `docs/spikes/usaspending-transaction-export.md:144-159`]

### Nonblocking operational question

- Choose capacity/reserve numbers only after measuring the actual post-pilot heap, all current indexes, database size, volume free bytes, and largest expected archive. Record the choice in the reviewed runbook/manifest; do not encode the 257.13 MiB feasibility model as a capacity guarantee. [inferred]

## 13. Definition of Done

- The planner produces exactly 84 ordered month roots spanning 2019-10-01..2026-09-30, with leap-year tests and no FY2027 dates.
- Recursive balanced splits are deterministic, nonempty, adjacent, gap-free, overlap-free, parent-contained, and terminate at a day.
- A counted period above 500000 can never reach submission; a multi-day parent becomes durable `subdivided` with exactly two atomic children, and a one-day overflow stops.
- `subdivided` parents permanently block replacement attempts; migration/model constraints preserve all `_0005` completed and historical failed rows. `_0006` is the sole head, unused round trip passes, and used downgrade refuses before mutation.
- Whole-month CLI and workflow behavior remain compatible. The loader accepts only safe same-month/same-FY child ranges and retains all archive/parser/ownership/reconciliation/atomic rollback controls.
- One shared PostgreSQL session lease prevents simultaneous backfill and legacy transaction-ingestion operators; multiprocess PostgreSQL tests prove acquisition, fail-fast loser behavior, and crash release.
- Completed skip verification covers attempt/checkpoint identity, exact period/FY/source, row/distinct/count/total, checkpoint ownership, and absence of cross-range ownership. Any corruption stops without mutation.
- The supplied October pilot anchor is verified exactly and skipped; it is never automatically resubmitted/replaced. Failed rows remain unchanged; `submission_unknown` always stops.
- Restart tests cover every durable state, process cancellation, machine-style interruption, archive redownload identity, count drift, and bounded partial-FY resumption without duplicate submission.
- Dry-run performs no network or database initialization/mutation and clearly distinguishes exact roots from unresolved count-dependent leaves.
- Live mode requires explicit range-specific confirmation, a finite execution bound, a restore-tested backup identifier, and passing local/database storage gates before every submission.
- Progress/final manifests are durable, mode 0600, no-overwrite, deterministic/sanitized, and contain no credentials, URLs, response bodies, upstream filenames, exception text, row values, presets, or aggregates.
- Focused offline tests, full backend tests, disposable PostgreSQL 16 tests, migration round trip, and synthetic multi-period tests pass without real USAspending or real Govtracts access.
- GitNexus pre-edit impacts and final `detect_changes(scope="all")` are reviewed; `partial=false`, `truncated=false`, and every HIGH/CRITICAL affected flow is accounted for before commit.
- A separate reviewed live-acceptance runbook limits first execution to one or two leaves after backup/migration/preflight and requires reconciliation plus a new recovery point before any FY2020-FY2026 backfill begins.
