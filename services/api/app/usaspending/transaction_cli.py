"""Operator CLI for one restartable USAspending transaction-ingestion month."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, contextmanager
from decimal import Decimal
from typing import Protocol, TextIO
from uuid import UUID

from app.core.config import load_settings
from app.db.session import Database
from app.usaspending.client import UsaSpendingClient
from app.usaspending.fiscal_years import fiscal_month_bounds
from app.usaspending.transaction_ingestion import TransactionIngestionLoader
from app.usaspending.transaction_workflow import (
    TransactionIngestionWorkflow,
    TransactionWorkflowError,
    TransactionWorkflowResult,
)


EXIT_COMPLETED = 0
EXIT_RESUMABLE = 10
EXIT_FAILED = 20
EXIT_OPERATOR_RESOLUTION = 30
EXIT_INVALID_ARGUMENTS = 64
EXIT_OPERATION_FAILED = 70

ALLOWED_STATUSES = frozenset(
    {
        "created",
        "counted",
        "submitting",
        "submitted",
        "export_finished",
        "archive_hashed",
        "loading",
        "completed",
        "failed",
        "submission_unknown",
    }
)
RESUMABLE_STATUSES = ALLOWED_STATUSES - {
    "completed",
    "failed",
    "submission_unknown",
}
SAFE_ERROR_CODE = re.compile(r"[a-z0-9_]{1,128}\Z")


class _Workflow(Protocol):
    async def run(
        self,
        *,
        fiscal_year: int,
        calendar_year: int,
        calendar_month: int,
    ) -> TransactionWorkflowResult: ...


RuntimeFactory = Callable[[], AbstractContextManager[_Workflow]]


class _SafeArgumentParser(argparse.ArgumentParser):
    """Argparse variant that never echoes an invalid argument value."""

    def error(self, message: str) -> None:
        del message
        self.print_usage(sys.stderr)
        self.exit(EXIT_INVALID_ARGUMENTS, f"{self.prog}: error: invalid arguments\n")


class _SingleValueAction(argparse.Action):
    """Accept one value for an option and reject repeated occurrences safely."""

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
            "Run the restartable USAspending transaction workflow for one "
            "explicit federal fiscal month."
        )
    )
    parser.add_argument(
        "--fiscal-year", required=True, type=int, action=_SingleValueAction
    )
    parser.add_argument(
        "--calendar-year", required=True, type=int, action=_SingleValueAction
    )
    parser.add_argument(
        "--calendar-month", required=True, type=int, action=_SingleValueAction
    )
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Confirm that database and USAspending operations may run.",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        fiscal_month_bounds(
            args.fiscal_year,
            calendar_year=args.calendar_year,
            calendar_month=args.calendar_month,
        )
    except (TypeError, ValueError):
        parser.exit(
            EXIT_INVALID_ARGUMENTS,
            f"{parser.prog}: error: invalid fiscal-month selection\n",
        )

    if not args.execute_live:
        parser.exit(
            EXIT_INVALID_ARGUMENTS,
            f"{parser.prog}: error: --execute-live is required\n",
        )
    return args


@contextmanager
def _live_runtime() -> _Workflow:
    """Construct live dependencies only after explicit operator confirmation."""
    database: Database | None = None
    try:
        settings = load_settings()
        database = Database(settings)
        client = UsaSpendingClient(settings)
        loader = TransactionIngestionLoader(database.engine)
        yield TransactionIngestionWorkflow(database.engine, client, loader)
    finally:
        if database is not None:
            database.dispose()


async def _run_workflow(
    args: argparse.Namespace,
    runtime_factory: RuntimeFactory,
) -> TransactionWorkflowResult:
    with runtime_factory() as workflow:
        return await workflow.run(
            fiscal_year=args.fiscal_year,
            calendar_year=args.calendar_year,
            calendar_month=args.calendar_month,
        )


def _safe_error_code(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and SAFE_ERROR_CODE.fullmatch(value):
        return value
    return "workflow_error"


def _safe_result(result: TransactionWorkflowResult) -> dict[str, object]:
    if result.status not in ALLOWED_STATUSES:
        raise TransactionWorkflowError("invalid_workflow_result")
    if not isinstance(result.attempt_id, UUID):
        raise TransactionWorkflowError("invalid_workflow_result")
    if result.checkpoint_id is not None and not isinstance(result.checkpoint_id, UUID):
        raise TransactionWorkflowError("invalid_workflow_result")
    if result.signed_obligation_total is not None and not isinstance(
        result.signed_obligation_total,
        Decimal,
    ):
        raise TransactionWorkflowError("invalid_workflow_result")

    return {
        "attempt_id": str(result.attempt_id),
        "status": result.status,
        "expected_rows": result.expected_rows,
        "loaded_rows": result.loaded_rows,
        "signed_obligation_total": (
            str(result.signed_obligation_total)
            if result.signed_obligation_total is not None
            else None
        ),
        "checkpoint_id": (
            str(result.checkpoint_id) if result.checkpoint_id is not None else None
        ),
        "error_code": _safe_error_code(result.error_code),
        "operator_resolution_required": bool(
            result.operator_resolution_required
        ),
    }


def _exit_code(result: TransactionWorkflowResult) -> int:
    if result.operator_resolution_required or result.status == "submission_unknown":
        return EXIT_OPERATOR_RESOLUTION
    if result.status == "completed":
        return EXIT_COMPLETED
    if result.status == "failed":
        return EXIT_FAILED
    if result.status in RESUMABLE_STATUSES:
        return EXIT_RESUMABLE
    return EXIT_OPERATION_FAILED


def _write_json(stream: TextIO, payload: dict[str, object]) -> None:
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

    try:
        result = asyncio.run(_run_workflow(args, runtime_factory))
        payload = _safe_result(result)
    except TransactionWorkflowError as error:
        _write_json(errors, {"error_code": _safe_error_code(str(error))})
        return EXIT_OPERATION_FAILED
    except Exception:
        _write_json(errors, {"error_code": "operation_failed"})
        return EXIT_OPERATION_FAILED

    _write_json(output, payload)
    return _exit_code(result)


if __name__ == "__main__":
    raise SystemExit(main())
