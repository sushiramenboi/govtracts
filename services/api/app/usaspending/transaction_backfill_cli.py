"""Operator CLI for the controlled FY2020-FY2026 transaction backfill."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol, TextIO
from uuid import UUID

from app.core.config import load_settings
from app.db.session import Database
from app.usaspending.client import UsaSpendingClient
from app.usaspending.fiscal_years import (
    FiscalPeriod,
    backfill_month_roots,
    fiscal_year_for_date,
    validate_backfill_period,
)
from app.usaspending.transaction_backfill import (
    BackfillCoordinator,
    BackfillError,
    BackfillLimits,
    BackfillResult,
    ExclusiveManifestReservation,
    PILOT_PERIOD,
    StorageGuard,
    canonicalize_backup_identifier,
    global_ingestion_lease,
    reserve_manifest_exclusive,
    safe_result_manifest,
    sanitize_progress_record,
    write_manifest_exclusive,
)
from app.usaspending.transaction_ingestion import (
    TransactionIngestionError,
    TransactionIngestionLoader,
    probe_private_directory,
)
from app.usaspending.transaction_workflow import (
    TransactionIngestionWorkflow,
    TransactionWorkflowError,
)


EXIT_COMPLETED = 0
EXIT_BOUNDED = 10
EXIT_OPERATOR_RESOLUTION = 30
EXIT_INVALID_ARGUMENTS = 64
EXIT_OPERATION_FAILED = 70
ROOT_MONTH = re.compile(r"(20[0-9]{2})-(0[1-9]|1[0-2])\Z")
RECOVERY_OPERATOR_CODES = frozenset(
    {
        "conflicting_retry_authorizations",
        "failed_job_identity_partial",
        "invalid_failed_attempt_id",
        "invalid_recovery_attempt_id",
        "invalid_recovery_authorization",
        "recovery_attempt_mismatch",
        "recovery_attempt_not_found",
        "recovery_authorization_required",
        "recovery_claim_conflict",
        "recovery_claim_mismatch",
        "recovery_job_absent",
        "recovery_job_corrupt",
        "recovery_job_identity_mismatch",
        "recovery_job_unsafe",
        "recovery_lineage_mismatch",
        "recovery_link_conflict",
        "recovery_link_invalid",
        "recovery_period_conflict",
        "recovery_period_not_empty",
        "recovery_requires_one_period",
        "recovery_source_dirty",
        "recovery_source_ineligible",
        "same_job_recovery_required",
    }
)
RECOVERY_INTEGRITY_CODES = frozenset(
    {
        "completed_attempt_corrupt",
        "completed_attempt_missing",
        "completed_checkpoint_corrupt",
        "completed_period_corrupt",
        "completed_period_mismatch",
        "completed_transactions_corrupt",
        "invalid_persisted_export_rows",
        "unexpected_overlapping_state",
    }
)


class _Runtime(Protocol):
    async def run(
        self,
        roots: Sequence[FiscalPeriod],
        *,
        limits: BackfillLimits,
        backup_identifier: str,
        retry_failed_attempt_id: UUID | None,
        recover_failed_job_attempt_id: UUID | None,
    ) -> BackfillResult: ...


RuntimeFactory = Callable[
    [argparse.Namespace, Callable[[dict[str, object]], None]],
    AbstractContextManager[_Runtime],
]


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        self.print_usage(sys.stderr)
        self.exit(EXIT_INVALID_ARGUMENTS, f"{self.prog}: error: invalid arguments\n")


class _SingleValueAction(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        del option_string
        if getattr(namespace, self.dest, None) is not None:
            parser.error("duplicate argument")
        setattr(namespace, self.dest, values)


def build_parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(
        description=(
            "Plan the exact FY2020-FY2026 month roots offline, or run one "
            "explicitly confirmed and finitely bounded live batch."
        )
    )
    parser.add_argument("--fiscal-year", type=int, action=_SingleValueAction)
    parser.add_argument("--root-month", action=_SingleValueAction)
    parser.add_argument("--period-start", action=_SingleValueAction)
    parser.add_argument("--period-end", action=_SingleValueAction)
    parser.add_argument("--manifest", action=_SingleValueAction)
    parser.add_argument("--execute-live", action="store_true")
    parser.add_argument("--confirm", action=_SingleValueAction)
    parser.add_argument("--max-periods", type=int, action=_SingleValueAction)
    parser.add_argument(
        "--max-leaf-submissions", type=int, action=_SingleValueAction
    )
    parser.add_argument(
        "--max-estimated-new-bytes", type=int, action=_SingleValueAction
    )
    parser.add_argument("--max-failures", type=int, action=_SingleValueAction)
    parser.add_argument("--backup-identifier", action=_SingleValueAction)
    parser.add_argument("--retry-failed-attempt-id", action=_SingleValueAction)
    parser.add_argument("--recover-failed-job-attempt-id", action=_SingleValueAction)
    parser.add_argument(
        "--database-volume-capacity-bytes", type=int, action=_SingleValueAction
    )
    parser.add_argument(
        "--database-volume-free-bytes", type=int, action=_SingleValueAction
    )
    parser.add_argument(
        "--database-reserve-bytes", type=int, action=_SingleValueAction
    )
    parser.add_argument("--local-reserve-bytes", type=int, action=_SingleValueAction)
    parser.add_argument("--storage-factor", action=_SingleValueAction)
    parser.add_argument("--temporary-path", action=_SingleValueAction)
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    roots = list(backfill_month_roots())
    has_period_start = args.period_start is not None
    has_period_end = args.period_end is not None
    if has_period_start != has_period_end:
        parser.error("both period bounds are required")
    if has_period_start:
        if args.fiscal_year is not None or args.root_month is not None:
            parser.error("conflicting period selection")
        try:
            period_start = date.fromisoformat(args.period_start)
            period_end = date.fromisoformat(args.period_end)
            fiscal_year = fiscal_year_for_date(period_start)
            validate_backfill_period(fiscal_year, period_start, period_end)
        except (TypeError, ValueError):
            parser.error("invalid period selection")
        roots = [FiscalPeriod(fiscal_year, period_start, period_end)]
    if args.fiscal_year is not None:
        if args.fiscal_year not in range(2020, 2027):
            parser.error("invalid fiscal year")
        roots = [root for root in roots if root.fiscal_year == args.fiscal_year]
    if args.root_month is not None:
        if args.fiscal_year is not None or not isinstance(args.root_month, str):
            parser.error("conflicting root selection")
        match = ROOT_MONTH.fullmatch(args.root_month)
        if match is None:
            parser.error("invalid root month")
        year, month = map(int, match.groups())
        roots = [
            root
            for root in roots
            if root.start.year == year and root.start.month == month
        ]
        if len(roots) != 1:
            parser.error("root outside controlled range")
    args.roots = tuple(roots)
    args.confirmation_token = confirmation_token(args.roots)
    if args.max_failures is not None and args.max_failures != 1:
        parser.error("max failures is fixed")
    if (
        args.retry_failed_attempt_id is not None
        and args.recover_failed_job_attempt_id is not None
    ):
        parser.error("conflicting retry authorizations")
    try:
        args.recover_failed_job_attempt_uuid = (
            UUID(args.recover_failed_job_attempt_id)
            if args.recover_failed_job_attempt_id is not None
            else None
        )
    except (TypeError, ValueError):
        parser.error("invalid recovery authorization")
    if args.recover_failed_job_attempt_uuid is not None:
        if not args.execute_live:
            parser.error("recovery requires live execution")
        if not has_period_start or not has_period_end or args.root_month is not None:
            parser.error("recovery requires explicit period bounds")
        if len(args.roots) != 1 or args.max_periods != 1:
            parser.error("recovery requires one bounded period")

    if not args.execute_live:
        return args

    required = (
        args.confirm,
        args.manifest,
        args.backup_identifier,
        args.database_volume_capacity_bytes,
        args.database_volume_free_bytes,
        args.database_reserve_bytes,
        args.local_reserve_bytes,
        args.storage_factor,
        args.temporary_path,
    )
    if any(value is None for value in required):
        parser.error("missing live precondition")
    if args.confirm != args.confirmation_token:
        parser.error("invalid live confirmation")
    try:
        args.limits = BackfillLimits(
            max_periods=args.max_periods,
            max_leaf_submissions=args.max_leaf_submissions,
            max_estimated_new_bytes=args.max_estimated_new_bytes,
        )
        args.storage_factor_decimal = Decimal(args.storage_factor)
        args.backup_identifier = canonicalize_backup_identifier(
            args.backup_identifier
        )
    except (BackfillError, InvalidOperation, TypeError, ValueError):
        parser.error("invalid live bounds")
    try:
        args.retry_failed_attempt_uuid = (
            UUID(args.retry_failed_attempt_id)
            if args.retry_failed_attempt_id is not None
            else None
        )
    except (TypeError, ValueError):
        parser.error("invalid retry authorization")
    return args


def confirmation_token(roots: Sequence[FiscalPeriod]) -> str:
    if not roots:
        raise BackfillError("empty_backfill_selection")
    return f"RUN-{roots[0].start.isoformat()}-THROUGH-{roots[-1].end.isoformat()}"


def dry_run_manifest(roots: Sequence[FiscalPeriod]) -> dict[str, object]:
    return {
        "mode": "dry_run",
        "root_count": len(roots),
        "coverage_start": roots[0].start.isoformat(),
        "coverage_end": roots[-1].end.isoformat(),
        "leaves": "unresolved_count_dependent",
        "split_rule": "balanced_binary_inclusive_left_ceiling",
        "roots": [
            {
                "fiscal_year": root.fiscal_year,
                "period_start": root.start.isoformat(),
                "period_end": root.end.isoformat(),
                "requires_live_integrity_check": root == PILOT_PERIOD,
            }
            for root in roots
        ],
    }


@contextmanager
def _live_runtime(
    args: argparse.Namespace,
    progress: Callable[[dict[str, object]], None],
) -> Iterator[BackfillCoordinator]:
    database: Database | None = None
    try:
        settings = load_settings()
        database = Database(settings)
        with global_ingestion_lease(database.engine):
            temporary_path = Path(args.temporary_path)
            try:
                temporary_identity = probe_private_directory(temporary_path)
            except TransactionIngestionError:
                raise BackfillError("invalid_temporary_directory") from None
            client = UsaSpendingClient(settings)
            loader = TransactionIngestionLoader(
                database.engine,
                archive_copy_directory=temporary_path,
                archive_copy_directory_identity=temporary_identity,
            )

            @contextmanager
            def temporary_directory() -> Iterator[Path]:
                try:
                    probe_private_directory(
                        temporary_path,
                        expected_identity=temporary_identity,
                    )
                except TransactionIngestionError:
                    raise BackfillError("invalid_temporary_directory") from None
                with tempfile.TemporaryDirectory(
                    prefix="govtracts-usaspending-workflow-",
                    dir=temporary_path,
                ) as name:
                    try:
                        probe_private_directory(
                            temporary_path,
                            expected_identity=temporary_identity,
                        )
                    except TransactionIngestionError:
                        raise BackfillError("invalid_temporary_directory") from None
                    yield Path(name)

            workflow = TransactionIngestionWorkflow(
                database.engine,
                client,
                loader,
                temporary_directory_factory=temporary_directory,
            )
            guard = StorageGuard(
                database.engine,
                temporary_path,
                args.database_volume_capacity_bytes,
                args.database_volume_free_bytes,
                args.database_reserve_bytes,
                args.local_reserve_bytes,
                args.storage_factor_decimal,
            )
            yield BackfillCoordinator(
                database.engine,
                workflow,
                guard,
                progress=progress,
            )
    finally:
        if database is not None:
            database.dispose()


def _write_json(stream: TextIO, payload: Mapping[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")), file=stream)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_factory: RuntimeFactory = _live_runtime,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    output = stdout if stdout is not None else sys.stdout
    errors = stderr if stderr is not None else sys.stderr
    args = parse_args(argv)

    if not args.execute_live:
        payload = dry_run_manifest(args.roots)
        if args.manifest is not None:
            try:
                write_manifest_exclusive(Path(args.manifest), payload)
            except BackfillError as error:
                _write_json(errors, {"error_code": error.code})
                return EXIT_OPERATION_FAILED
        _write_json(output, payload)
        return EXIT_COMPLETED

    try:
        manifest = reserve_manifest_exclusive(Path(args.manifest))
    except BackfillError as error:
        _write_json(errors, {"error_code": error.code})
        return EXIT_OPERATION_FAILED

    progress_records: list[dict[str, object]] = []

    def progress(record: dict[str, object]) -> None:
        safe_record = sanitize_progress_record(record)
        progress_records.append(safe_record)
        _write_json(output, {"progress": safe_record})

    def write_stopped_manifest(
        reservation: ExclusiveManifestReservation,
        status: str,
        error_code: str,
    ) -> None:
        try:
            backup_identifier = canonicalize_backup_identifier(
                args.backup_identifier
            )
        except BackfillError:
            backup_identifier = None
        payload = {
            "status": status,
            "error_code": error_code,
            "backup_identifier": backup_identifier,
            "records": progress_records,
        }
        reservation.write(payload)

    try:
        try:
            with runtime_factory(args, progress) as coordinator:
                result = asyncio.run(
                    coordinator.run(
                        args.roots,
                        limits=args.limits,
                        backup_identifier=args.backup_identifier,
                        retry_failed_attempt_id=args.retry_failed_attempt_uuid,
                        recover_failed_job_attempt_id=args.recover_failed_job_attempt_uuid,
                    )
                )
            payload = safe_result_manifest(
                result,
                backup_identifier=args.backup_identifier,
            )
            manifest.write(payload)
        except (BackfillError, TransactionWorkflowError) as error:
            code = error.code if isinstance(error, BackfillError) else str(error)
            operator_resolution = (
                code in RECOVERY_OPERATOR_CODES
                or (
                    args.recover_failed_job_attempt_uuid is not None
                    and code in RECOVERY_INTEGRITY_CODES
                )
            )
            safe_code = (
                code
                if isinstance(error, BackfillError) or operator_resolution
                else "backfill_operation_failed"
            )
            try:
                write_stopped_manifest(manifest, "stopped", safe_code)
            except BackfillError as manifest_error:
                _write_json(errors, {"error_code": manifest_error.code})
                return EXIT_OPERATION_FAILED
            _write_json(errors, {"error_code": safe_code})
            if operator_resolution or code in {
                "submission_unknown_requires_operator",
                "failed_attempt_requires_authorization",
                "pilot_anchor_mismatch",
            }:
                return EXIT_OPERATOR_RESOLUTION
            return EXIT_OPERATION_FAILED
        except (KeyboardInterrupt, asyncio.CancelledError):
            try:
                write_stopped_manifest(
                    manifest,
                    "interrupted",
                    "backfill_interrupted",
                )
            except BackfillError as manifest_error:
                _write_json(errors, {"error_code": manifest_error.code})
                return EXIT_OPERATION_FAILED
            _write_json(errors, {"error_code": "backfill_interrupted"})
            return EXIT_OPERATION_FAILED
        except Exception:
            try:
                write_stopped_manifest(
                    manifest,
                    "stopped",
                    "backfill_operation_failed",
                )
            except BackfillError as manifest_error:
                _write_json(errors, {"error_code": manifest_error.code})
                return EXIT_OPERATION_FAILED
            _write_json(errors, {"error_code": "backfill_operation_failed"})
            return EXIT_OPERATION_FAILED
    finally:
        manifest.abort()

    _write_json(output, payload)
    return EXIT_COMPLETED if result.status == "completed" else EXIT_BOUNDED


if __name__ == "__main__":
    raise SystemExit(main())
