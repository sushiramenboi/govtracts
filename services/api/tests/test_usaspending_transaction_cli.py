from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from io import StringIO
from typing import Any
from uuid import UUID, uuid4

import pytest

from app.usaspending.transaction_cli import (
    EXIT_COMPLETED,
    EXIT_FAILED,
    EXIT_INVALID_ARGUMENTS,
    EXIT_OPERATION_FAILED,
    EXIT_OPERATOR_RESOLUTION,
    EXIT_RESUMABLE,
    _run_workflow,
    main,
    parse_args,
)
from app.usaspending.transaction_workflow import TransactionWorkflowResult


VALID_ARGS = [
    "--fiscal-year",
    "2025",
    "--calendar-year",
    "2024",
    "--calendar-month",
    "10",
    "--execute-live",
]


def result(
    status: str,
    *,
    error_code: str | None = None,
    operator_resolution_required: bool = False,
) -> TransactionWorkflowResult:
    return TransactionWorkflowResult(
        attempt_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        status=status,
        expected_rows=1,
        loaded_rows=1 if status == "completed" else None,
        signed_obligation_total=(
            Decimal("-12.34") if status == "completed" else None
        ),
        checkpoint_id=(
            UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
            if status == "completed"
            else None
        ),
        error_code=error_code,
        operator_resolution_required=operator_resolution_required,
    )


class FakeWorkflow:
    def __init__(
        self,
        workflow_result: TransactionWorkflowResult | None = None,
        *,
        error: BaseException | None = None,
    ) -> None:
        self.workflow_result = workflow_result
        self.error = error
        self.calls: list[dict[str, int]] = []

    async def run(self, **kwargs: int) -> TransactionWorkflowResult:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        assert self.workflow_result is not None
        return self.workflow_result


class RuntimeTracker:
    def __init__(self, workflow: FakeWorkflow) -> None:
        self.workflow = workflow
        self.entered = 0
        self.closed = 0

    @contextmanager
    def factory(self) -> Iterator[FakeWorkflow]:
        self.entered += 1
        try:
            yield self.workflow
        finally:
            self.closed += 1


def invoke(
    workflow_result: TransactionWorkflowResult,
) -> tuple[int, dict[str, Any], str, RuntimeTracker]:
    tracker = RuntimeTracker(FakeWorkflow(workflow_result))
    stdout = StringIO()
    stderr = StringIO()
    exit_code = main(
        VALID_ARGS,
        runtime_factory=tracker.factory,
        stdout=stdout,
        stderr=stderr,
    )
    return exit_code, json.loads(stdout.getvalue()), stderr.getvalue(), tracker


def test_help_is_safe_without_runtime_initialization(capsys: Any) -> None:
    called = False

    @contextmanager
    def forbidden_runtime() -> Iterator[FakeWorkflow]:
        nonlocal called
        called = True
        raise AssertionError("runtime must not initialize")
        yield  # pragma: no cover

    with pytest.raises(SystemExit) as raised:
        main(["--help"], runtime_factory=forbidden_runtime)

    captured = capsys.readouterr()
    assert raised.value.code == 0
    assert "--fiscal-year" in captured.out
    assert "--calendar-year" in captured.out
    assert "--calendar-month" in captured.out
    assert "--execute-live" in captured.out
    assert called is False


def test_argument_parsing_accepts_exactly_one_confirmed_month() -> None:
    args = parse_args(VALID_ARGS)

    assert vars(args) == {
        "fiscal_year": 2025,
        "calendar_year": 2024,
        "calendar_month": 10,
        "execute_live": True,
    }


def test_missing_confirmation_prevents_runtime_initialization(capsys: Any) -> None:
    called = False

    @contextmanager
    def forbidden_runtime() -> Iterator[FakeWorkflow]:
        nonlocal called
        called = True
        raise AssertionError("runtime must not initialize")
        yield  # pragma: no cover

    with pytest.raises(SystemExit) as raised:
        main(VALID_ARGS[:-1], runtime_factory=forbidden_runtime)

    captured = capsys.readouterr()
    assert raised.value.code == EXIT_INVALID_ARGUMENTS
    assert "--execute-live is required" in captured.err
    assert called is False


def test_valid_fiscal_month_mapping_reaches_exactly_one_workflow_call() -> None:
    workflow = FakeWorkflow(result("completed"))
    tracker = RuntimeTracker(workflow)

    exit_code = main(VALID_ARGS, runtime_factory=tracker.factory)

    assert exit_code == EXIT_COMPLETED
    assert workflow.calls == [
        {
            "fiscal_year": 2025,
            "calendar_year": 2024,
            "calendar_month": 10,
        }
    ]
    assert tracker.entered == tracker.closed == 1


def test_invalid_fiscal_month_mapping_prevents_runtime_initialization(
    capsys: Any,
) -> None:
    tracker = RuntimeTracker(FakeWorkflow(result("completed")))
    invalid = [
        "--fiscal-year",
        "2024",
        "--calendar-year",
        "2024",
        "--calendar-month",
        "10",
        "--execute-live",
    ]

    with pytest.raises(SystemExit) as raised:
        main(invalid, runtime_factory=tracker.factory)

    captured = capsys.readouterr()
    assert raised.value.code == EXIT_INVALID_ARGUMENTS
    assert "invalid fiscal-month selection" in captured.err
    assert tracker.entered == tracker.closed == 0


def test_completed_result_uses_safe_fields_and_success_exit() -> None:
    exit_code, payload, errors, tracker = invoke(result("completed"))

    assert exit_code == EXIT_COMPLETED
    assert payload == {
        "attempt_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "status": "completed",
        "expected_rows": 1,
        "loaded_rows": 1,
        "signed_obligation_total": "-12.34",
        "checkpoint_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "error_code": None,
        "operator_resolution_required": False,
    }
    assert errors == ""
    assert tracker.entered == tracker.closed == 1


@pytest.mark.parametrize("status", ["created", "submitted", "loading"])
def test_resumable_result_uses_nonzero_exit(status: str) -> None:
    exit_code, payload, errors, tracker = invoke(result(status))

    assert exit_code == EXIT_RESUMABLE
    assert payload["status"] == status
    assert errors == ""
    assert tracker.entered == tracker.closed == 1


def test_failed_result_uses_nonzero_exit() -> None:
    exit_code, payload, errors, tracker = invoke(
        result("failed", error_code="transaction_validation_failed")
    )

    assert exit_code == EXIT_FAILED
    assert payload["status"] == "failed"
    assert payload["error_code"] == "transaction_validation_failed"
    assert errors == ""
    assert tracker.entered == tracker.closed == 1


def test_submission_unknown_uses_distinct_operator_resolution_exit() -> None:
    exit_code, payload, errors, tracker = invoke(
        result(
            "submission_unknown",
            error_code="submission_outcome_unknown",
            operator_resolution_required=True,
        )
    )

    assert exit_code == EXIT_OPERATOR_RESOLUTION
    assert payload["operator_resolution_required"] is True
    assert payload["error_code"] == "submission_outcome_unknown"
    assert errors == ""
    assert tracker.entered == tracker.closed == 1


def test_output_redacts_unsafe_result_codes_and_exception_text() -> None:
    unsafe = result("failed", error_code="postgresql://user:secret@db.example/app")
    exit_code, payload, errors, _ = invoke(unsafe)

    assert exit_code == EXIT_FAILED
    assert payload["error_code"] == "workflow_error"
    assert "secret" not in json.dumps(payload)
    assert errors == ""

    tracker = RuntimeTracker(
        FakeWorkflow(error=RuntimeError("password=secret sql=SELECT * FROM attempts"))
    )
    stderr = StringIO()
    failed_exit = main(
        VALID_ARGS,
        runtime_factory=tracker.factory,
        stdout=StringIO(),
        stderr=stderr,
    )

    assert failed_exit == EXIT_OPERATION_FAILED
    assert json.loads(stderr.getvalue()) == {"error_code": "operation_failed"}
    assert "secret" not in stderr.getvalue()
    assert "SELECT" not in stderr.getvalue()
    assert tracker.entered == tracker.closed == 1


def test_runtime_is_closed_after_workflow_failure() -> None:
    tracker = RuntimeTracker(FakeWorkflow(error=RuntimeError("failure")))

    exit_code = main(
        VALID_ARGS,
        runtime_factory=tracker.factory,
        stdout=StringIO(),
        stderr=StringIO(),
    )

    assert exit_code == EXIT_OPERATION_FAILED
    assert tracker.entered == tracker.closed == 1


def test_cancellation_propagates_after_runtime_cleanup() -> None:
    tracker = RuntimeTracker(FakeWorkflow(error=asyncio.CancelledError()))
    args = argparse.Namespace(
        fiscal_year=2025,
        calendar_year=2024,
        calendar_month=10,
        execute_live=True,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(_run_workflow(args, tracker.factory))

    assert tracker.entered == tracker.closed == 1


def test_unconfirmed_command_rejects_unknown_values_without_echoing_them(
    capsys: Any,
) -> None:
    secret = "postgresql://user:password@example.test/database"
    tracker = RuntimeTracker(FakeWorkflow(result("completed")))

    with pytest.raises(SystemExit) as raised:
        main([*VALID_ARGS[:-1], secret], runtime_factory=tracker.factory)

    captured = capsys.readouterr()
    assert raised.value.code == EXIT_INVALID_ARGUMENTS
    assert secret not in captured.err
    assert tracker.entered == tracker.closed == 0


def test_each_invocation_accepts_only_one_period() -> None:
    extra_period = [*VALID_ARGS, "--calendar-month", "11"]

    with pytest.raises(SystemExit) as raised:
        parse_args(extra_period)

    assert raised.value.code == EXIT_INVALID_ARGUMENTS
