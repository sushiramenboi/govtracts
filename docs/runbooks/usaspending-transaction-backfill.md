# USAspending transaction backfill runbook

This command plans and, only after a separate review, coordinates the FY2020–FY2026 A–D prime-contract transaction backfill. It is serial and resumable. It does not classify presets, compute aggregates, serve API routes, schedule itself, or alter the completed October 2024 pilot automatically.

Do not run live during implementation or ordinary testing. A live invocation requires a separately authorized migration, a verified backup, the exact production preflight, measured storage inputs, and a reviewed finite bound.

## Offline dry-run

From `services/api`, the default command is a pure offline plan:

```sh
.venv/bin/python -m app.usaspending.transaction_backfill_cli
```

It emits exactly 84 calendar-month roots from `2019-10-01` through `2026-09-30`. It does not load settings, initialize a database, construct the USAspending client, or contact a network. Final leaf ranges remain `unresolved_count_dependent` because subdivision depends on advisory counts that are unavailable offline.

Optional offline filters are `--fiscal-year 2025`, `--root-month 2024-10`, or one exact existing leaf with `--period-start YYYY-MM-DD --period-end YYYY-MM-DD`. These selectors are mutually exclusive; both period bounds are required and must stay inside one controlled calendar month and fiscal year. `--manifest PATH` writes the same sanitized JSON with mode `0600`, exclusive no-overwrite creation, and file/directory fsync.

## Required live acceptance gates

Before any live invocation:

1. Obtain independent approval for the exact root or FY selection and the maximum work allowed.
2. Create and restore-test a fresh logical backup. Record only its non-secret reference in the exact form `backup-YYYYMMDDTHHMMSSZ-<checksum>`, where the timestamp is the real UTC creation time and the checksum is 16–64 hexadecimal characters. Never use a credential, path, filename, URL, or free-form label.
3. Apply and verify Alembic revision `20261004_0006`. Confirm it is the sole head.
4. Read-only verify the October 2024 pilot: period `2024-10-01..2024-10-31`, FY2025, attempt `c6b44991-c217-4e36-bdf8-905690d6bd2d`, checkpoint `ce639524-be6d-4cba-8d82-07657c5c09a9`, 490381 rows, and signed total `48714624177.57`.
5. Measure the database volume’s capacity and free bytes outside PostgreSQL immediately before each invocation. Measure free bytes on the exact directory passed as `--temporary-path`. Choose explicit database and local reserve bytes plus a conservative factor. PostgreSQL relation size is usage, not filesystem free space; an earlier manifest never refreshes these inputs.
6. Ensure no other transaction-ingestion operator is running. Both commands share one PostgreSQL session advisory lease and a second operator fails fast.
7. For first acceptance, select one root and allow no more than one or two leaf submissions. Never authorize the full seven-year range in one unbounded invocation.

The live confirmation token is exact and range-specific:

```text
RUN-<first-root-start>-THROUGH-<last-root-end>
```

Example command shape only—replace every bracketed value from the reviewed acceptance record:

```sh
.venv/bin/python -m app.usaspending.transaction_backfill_cli \
  --root-month 2024-11 \
  --execute-live \
  --confirm RUN-2024-11-01-THROUGH-2024-11-30 \
  --max-leaf-submissions 1 \
  --max-periods 8 \
  --max-estimated-new-bytes '[reviewed-byte-limit]' \
  --backup-identifier 'backup-20261004t202143z-9f21643cf5b6c8df' \
  --database-volume-capacity-bytes '[measured-capacity]' \
  --database-volume-free-bytes '[measured-free-bytes]' \
  --database-reserve-bytes '[reviewed-reserve]' \
  --local-reserve-bytes '[reviewed-local-reserve]' \
  --storage-factor '[reviewed-factor-at-least-1]' \
  --temporary-path '[private-local-temp-directory]' \
  --manifest '[new-exclusive-manifest-path]'
```

At least one finite bound is required. `--max-failures`, if supplied, must equal `1`. Processing is sequential; there is no automatic parallel mode.

## State, restart, and stopping rules

- `created` is counted before any submission decision.
- A count at or below 500000 may proceed as a leaf after the preliminary storage gate. The advisory count is not authoritative: persisted export metadata triggers a second capacity and byte-bound check using `export_rows` before download, immutable copying, parsing, or loading.
- A multi-day count above 500000 becomes a permanent `subdivided` parent with exactly two balanced, adjacent child attempts in the same database transaction. The parent can never submit.
- A one-day count above 500000 stops for a separately reviewed acquisition design.
- Every exact attempt is checked for overlapping attempts, checkpoints, and transactions before it is resumed, traversed, retried, or verified. Completed periods are skipped only after exact attempt, checkpoint, row, distinct-ID, signed-total, fiscal-year, date-range, archive-linkage, and ownership verification.
- `failed` stops immediately. An explicitly reviewed retry uses `--retry-failed-attempt-id UUID`; it creates a new attempt and never edits the failed row. A failed subdivided child replacement retains the original parent linkage so traversal resolves the replacement while preserving the failed child as history.
- `submission_unknown` always stops for operator resolution. There is no retry flag for it and no automatic resubmission.
- A bulk export submitted by the current workflow invocation receives a bounded initial-status visibility grace: only an initial `http_404` may retry the same persisted status URL with GET requests. The submission POST is never repeated, resumed jobs receive no grace, and any later 404 remains terminal.
- Cancellation or machine interruption leaves durable state. Restart with a new reviewed invocation; completed leaves are reverified, post-submission states resume without a duplicate submission, and archive redownloads must match persisted identity. A post-submission restart without `export_rows` is guarded at the 500000-row worst case; once export metadata exists, the runner rechecks all capacity and byte bounds using that validated count before any protected file or database work.
- Corruption, count/ownership mismatch, capacity failure, lease contention, or an incomplete leaf stops the invocation without automatic repair.

Exit status `0` means the selected bounded work completed; `10` means a finite bound stopped cleanly; `30` means operator resolution is required; `64` means arguments were rejected; `70` means a safe operational failure. Output contains stable codes only, never exception text or upstream response data.

## Storage and manifests

The historical 257.13 MiB October estimate and roughly 550 modeled bytes per row are lower-bound planning illustrations, not capacity guarantees. Before every new leaf submission the runner combines current relation/index/TOAST bytes, completed-row observations, the conservative factor, explicit WAL headroom, trusted database-volume capacity/free values, outstanding leaf reservations, and local archive/uncompressed headroom. A refresh value is an absolute current free-byte measurement, not a delta: completed consumption already reflected by that measurement is not subtracted again, while outstanding reservations remain charged. Refreshes constrain the trusted budget to the lower remaining value and can never replenish it. Missing, stale, overflowing, or insufficient measurements fail closed before submission.

The configured `--temporary-path` is the filesystem measured by the local guard and is also the parent for both the downloaded workflow archive and the loader's immutable archive copy. Before client construction and submission, the runner opens the directory without following symlinks, records its device/inode identity, and proves private mode-`0600` file creation succeeds. It repeats the identity and creation probe before critical workflow and immutable-copy file creation; replacement, removal, filesystem identity change, or loss of writability fails closed. Probe and interrupted archive-copy files are removed. The guard reserves space for both maximum-size archive copies, the parser's maximum uncompressed allowance, and the operator-selected local reserve. The legacy one-period command retains its existing system-temporary-directory behavior.

Progress JSON lines and the final manifest contain only reviewed dates, fiscal years, UUIDs, statuses, counts, decimal totals, safe codes, bounds, storage estimates, and the canonical backup reference. The reference grammar is `backup-YYYYMMDDTHHMMSSZ-<checksum>` with a real UTC date/time and 16–64 hexadecimal checksum characters; uppercase input is accepted only when the complete value otherwise matches and is persisted lowercase. Arbitrary labels, credentials, encoded secrets, paths, filenames, and URLs are rejected without echoing the rejected input. Manifests also exclude upstream filenames, response bodies, exception text, row values, presets, and aggregates. Final manifests are `0600`, exclusive, and never overwritten.

## Backup and continuation cadence

Use one fiscal year per operational batch. Stop for another restore-tested backup after each completed FY and before continuing after any storage threshold, schema/configuration change, corruption investigation, or manual retry boundary. After the first one or two acceptance leaves, reconcile the database and manifest, create a new recovery point, and obtain separate authorization before any FY2020–FY2026 backfill begins.
