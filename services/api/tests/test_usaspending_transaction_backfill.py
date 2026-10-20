from __future__ import annotations

import ast
import csv
import hashlib
import io
import multiprocessing
import os
import tempfile
import zipfile
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, func, select, update
from sqlalchemy.engine import make_url

from app.usaspending.client import BulkExportJob, BulkExportStatus
from app.usaspending.fiscal_years import (
    FiscalPeriod,
    backfill_month_roots,
    fiscal_year_for_date,
    split_period,
)
from app.usaspending.transaction_export import (
    DEFAULT_MAX_ROWS,
    EXPECTED_TRANSACTION_HEADERS,
)
from app.usaspending.transaction_ingestion import TransactionIngestionLoader
from app.usaspending.transaction_backfill import (
    BackfillCoordinator,
    BackfillError,
    BackfillLimits,
    CompletedVerification,
    CompletedPeriodVerifier,
    PILOT_ATTEMPT_ID,
    PILOT_CHECKPOINT_ID,
    PILOT_PERIOD,
    PILOT_ROWS,
    PILOT_SIGNED_TOTAL,
    LOCAL_ARCHIVE_HEADROOM_BYTES,
    MAX_STORAGE_BYTES,
    StorageDecision,
    StorageGuard,
    canonicalize_backup_identifier,
    global_ingestion_lease,
    safe_result_manifest,
    write_manifest_exclusive,
)
from app.usaspending import transaction_backfill
from app.usaspending.transaction_workflow import (
    TransactionIngestionWorkflow,
    TransactionWorkflowResult,
)
from app.db.models import (
    UsaSpendingIngestionCheckpoint,
    UsaSpendingTransaction,
    UsaSpendingTransactionIngestionAttempt,
)


NOW = datetime(2026, 10, 4, tzinfo=timezone.utc)
VALID_BACKUP_IDENTIFIER = "backup-20261004t202143z-9f21643cf5b6c8df"


def _synthetic_slack_token_shape() -> str:
    return "".join(
        (
            "xo",
            "xb",
            "-",
            "123456789",
            "-",
            "abcdefghijklmnopqrstuvwxyz",
        )
    )


def workflow_result(
    *,
    attempt_id: UUID,
    status: str,
    count: int | None,
    export_count: int | None = None,
) -> TransactionWorkflowResult:
    final_count = count if export_count is None else export_count
    return TransactionWorkflowResult(
        attempt_id=attempt_id,
        status=status,
        pre_submission_rows=count,
        export_rows=final_count if status == "completed" else None,
        loaded_rows=final_count if status == "completed" else None,
        count_drift=(
            final_count - count
            if status == "completed" and count is not None and final_count is not None
            else None
        ),
        signed_obligation_total=Decimal("1.00") if status == "completed" else None,
        checkpoint_id=uuid4() if status == "completed" else None,
        error_code=None,
        operator_resolution_required=status == "submission_unknown",
    )


class FakeWorkflow:
    def __init__(
        self,
        counts: dict[tuple[date, date], int],
        *,
        export_counts: dict[tuple[date, date], int] | None = None,
        emit_submission_event: bool = True,
    ) -> None:
        self.counts = counts
        self.export_counts = export_counts or counts
        self.ids: dict[tuple[date, date], UUID] = {}
        self.calls: list[tuple[date, date, bool]] = []
        self.submissions: list[tuple[date, date]] = []
        self.emit_submission_event = emit_submission_event

    async def run_period(
        self,
        *,
        fiscal_year: int,
        period_start: date,
        period_end: date,
        stop_after_count: bool,
        capacity_guard: Any | None = None,
        retry_parent_attempt_id: UUID | None = None,
        submission_observer: Any | None = None,
    ) -> TransactionWorkflowResult:
        del fiscal_year, retry_parent_attempt_id
        key = (period_start, period_end)
        self.calls.append((period_start, period_end, stop_after_count))
        attempt_id = self.ids.setdefault(key, uuid4())
        count = self.counts[key]
        if stop_after_count:
            return workflow_result(attempt_id=attempt_id, status="counted", count=count)
        if self.emit_submission_event:
            self.submissions.append(key)
            if submission_observer is not None:
                submission_observer()
        export_count = self.export_counts[key]
        if capacity_guard is not None:
            capacity_guard(export_count)
        return workflow_result(
            attempt_id=attempt_id,
            status="completed",
            count=count,
            export_count=export_count,
        )


class FakeVerifier:
    def __init__(self) -> None:
        self.verified: list[tuple[UUID, FiscalPeriod | None]] = []

    def verify_pilot(self) -> CompletedVerification:
        return CompletedVerification(
            PILOT_ATTEMPT_ID,
            PILOT_CHECKPOINT_ID,
            PILOT_PERIOD.fiscal_year,
            PILOT_PERIOD.start,
            PILOT_PERIOD.end,
            PILOT_ROWS,
            PILOT_SIGNED_TOTAL,
        )

    def verify(
        self,
        attempt_id: UUID,
        *,
        expected_period: FiscalPeriod | None = None,
    ) -> CompletedVerification:
        self.verified.append((attempt_id, expected_period))
        assert expected_period is not None
        return CompletedVerification(
            attempt_id,
            uuid4(),
            expected_period.fiscal_year,
            expected_period.start,
            expected_period.end,
            1,
            Decimal("1.00"),
        )


class FakeStorageGuard:
    def __init__(self) -> None:
        self.counts: list[int] = []
        self.keys: list[object | None] = []
        self.completed: list[object] = []

    def check(
        self,
        expected_rows: int,
        *,
        reservation_key: object | None = None,
    ) -> StorageDecision:
        self.counts.append(expected_rows)
        self.keys.append(reservation_key)
        return StorageDecision(expected_rows * 1000, 1, 10**12, 10**12)

    def complete(self, reservation_key: object) -> None:
        self.completed.append(reservation_key)


class RecoveryWorkflowDouble:
    def __init__(
        self,
        failed_id: UUID,
        recovery_id: UUID,
        *,
        export_rows: int | None = None,
        created: bool = True,
        result_status: str = "completed",
        capacity_rows: int | None = None,
    ) -> None:
        self.failed_id = failed_id
        self.recovery_id = recovery_id
        self.export_rows = export_rows
        self.created = created
        self.result_status = result_status
        self.capacity_rows = capacity_rows
        self.claim_calls: list[tuple[object, ...]] = []
        self.resume_calls: list[dict[str, Any]] = []

    def claim_failed_job_recovery(
        self, attempt_id: UUID, fiscal_year: int, start: date, end: date
    ) -> Any:
        self.claim_calls.append((attempt_id, fiscal_year, start, end))
        return SimpleNamespace(
            failed_attempt_id=self.failed_id,
            recovery_attempt_id=self.recovery_id,
            created=self.created,
            attempt={
                "id": self.recovery_id,
                "export_rows": self.export_rows,
                "status_url": "https://secret.invalid/status",
                "file_url": "https://secret.invalid/archive.zip",
                "remote_file_name": "secret.zip",
                "last_error_detail": "secret error detail",
            },
        )

    async def resume_failed_job_recovery(self, **kwargs: Any) -> Any:
        self.resume_calls.append(kwargs)
        if self.capacity_rows is not None:
            kwargs["capacity_guard"](self.capacity_rows)
        return workflow_result(
            attempt_id=self.recovery_id,
            status=self.result_status,
            count=self.export_rows if self.export_rows is not None else 1,
        )

    async def run_period(self, **_kwargs: Any) -> Any:
        pytest.fail("ordinary export workflow must not run during recovery")


class MemoryCoordinator(BackfillCoordinator):
    def __init__(
        self,
        workflow: FakeWorkflow,
        verifier: FakeVerifier,
        storage: FakeStorageGuard,
        *,
        attempts: dict[tuple[date, date], dict[str, Any]] | None = None,
    ) -> None:
        engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        super().__init__(
            engine,  # type: ignore[arg-type]
            workflow,  # type: ignore[arg-type]
            storage,  # type: ignore[arg-type]
            verifier=verifier,  # type: ignore[arg-type]
            clock=lambda: NOW,
        )
        self.attempts = attempts or {}

    def _period_attempt(self, period: FiscalPeriod) -> dict[str, Any] | None:
        return self.attempts.get((period.start, period.end))

    def _subdivide(
        self,
        attempt_id: UUID,
        period: FiscalPeriod,
        expected_rows: int,
    ) -> tuple[FiscalPeriod, FiscalPeriod]:
        del attempt_id, expected_rows
        children = tuple(
            FiscalPeriod(period.fiscal_year, start, end)
            for start, end in split_period(period.start, period.end)
        )
        for child in children:
            self.attempts[(child.start, child.end)] = {
                "id": uuid4(),
                "status": "created",
            }
        return children  # type: ignore[return-value]

    def _assert_no_overlapping_state(
        self,
        period: FiscalPeriod,
        *,
        exact_attempt: Mapping[str, Any] | None,
    ) -> None:
        del period, exact_attempt


def run(coroutine: Any) -> Any:
    import asyncio

    return asyncio.run(coroutine)


def test_count_driven_subdivision_is_balanced_and_sequential() -> None:
    root = FiscalPeriod(2025, date(2024, 10, 1), date(2024, 10, 31))
    left, right = split_period(root.start, root.end)
    counts = {(root.start, root.end): 500001, left: 7, right: 8}
    workflow = FakeWorkflow(counts)
    storage = FakeStorageGuard()
    subject = MemoryCoordinator(workflow, FakeVerifier(), storage)

    result = run(
        subject.run(
            [root],
            limits=BackfillLimits(max_leaf_submissions=2),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )

    assert result.status == "completed"
    assert result.subdivided_periods == 1
    assert result.submitted_leaves == 2
    assert workflow.submissions == [left, right]
    assert (root.start, root.end) not in workflow.submissions
    assert storage.counts == [7, 7, 8, 8]


def test_one_day_overflow_stops_without_submission() -> None:
    day = FiscalPeriod(2025, date(2024, 10, 1), date(2024, 10, 1))
    workflow = FakeWorkflow({(day.start, day.end): 500001})
    subject = MemoryCoordinator(
        workflow,
        FakeVerifier(),
        FakeStorageGuard(),
        attempts={
            (day.start, day.end): {"id": uuid4(), "status": "created"}
        },
    )

    with pytest.raises(BackfillError, match="oversized_single_day"):
        run(
            subject.run(
                [day],
                limits=BackfillLimits(max_periods=1),
                backup_identifier=VALID_BACKUP_IDENTIFIER,
                require_pilot=False,
            )
        )

    assert workflow.submissions == []


@pytest.mark.parametrize("count", [0, 500000])
def test_count_boundaries_remain_submittable_leaves(count: int) -> None:
    root = FiscalPeriod(2020, date(2019, 10, 1), date(2019, 10, 31))
    workflow = FakeWorkflow({(root.start, root.end): count})
    subject = MemoryCoordinator(workflow, FakeVerifier(), FakeStorageGuard())

    result = run(
        subject.run(
            [root],
            limits=BackfillLimits(max_leaf_submissions=1),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )

    assert result.status == "completed"
    assert result.submitted_leaves == 1
    assert workflow.submissions == [(root.start, root.end)]


def test_upward_count_drift_rechecks_export_bound_before_completion() -> None:
    root = FiscalPeriod(2020, date(2019, 10, 1), date(2019, 10, 31))
    workflow = FakeWorkflow(
        {(root.start, root.end): 1},
        export_counts={(root.start, root.end): 500_000},
    )
    storage = FakeStorageGuard()
    subject = MemoryCoordinator(workflow, FakeVerifier(), storage)

    result = run(
        subject.run(
            [root],
            limits=BackfillLimits(max_estimated_new_bytes=1_000),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )

    assert result.status == "bounded"
    assert result.submitted_leaves == 1
    assert storage.counts == [1, 500_000]
    assert workflow.submissions == [(root.start, root.end)]


def test_pre_submission_capacity_failure_counts_no_submission() -> None:
    root = FiscalPeriod(2020, date(2019, 10, 1), date(2019, 10, 31))
    workflow = FakeWorkflow({(root.start, root.end): 1})
    subject = MemoryCoordinator(workflow, FakeVerifier(), FakeStorageGuard())

    result = run(
        subject.run(
            [root],
            limits=BackfillLimits(max_estimated_new_bytes=999),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )

    assert result.status == "bounded"
    assert result.submitted_leaves == 0
    assert workflow.submissions == []


def test_downward_count_drift_does_not_replenish_run_budget() -> None:
    root = FiscalPeriod(2020, date(2019, 10, 1), date(2019, 10, 31))
    workflow = FakeWorkflow(
        {(root.start, root.end): 500_000},
        export_counts={(root.start, root.end): 1},
    )
    storage = FakeStorageGuard()
    subject = MemoryCoordinator(workflow, FakeVerifier(), storage)

    result = run(
        subject.run(
            [root],
            limits=BackfillLimits(max_estimated_new_bytes=500_000_000),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )

    assert result.status == "completed"
    assert result.submitted_leaves == 1
    assert storage.counts == [500_000, 1]
    assert result.estimated_new_bytes == 500_000_000


def test_nested_oversized_periods_split_until_sequential_leaves() -> None:
    root = FiscalPeriod(2020, date(2019, 10, 1), date(2019, 10, 31))
    left, right = split_period(root.start, root.end)
    left_left, left_right = split_period(*left)
    counts = {
        (root.start, root.end): 900000,
        left: 600000,
        left_left: 10,
        left_right: 11,
        right: 12,
    }
    workflow = FakeWorkflow(counts)
    subject = MemoryCoordinator(workflow, FakeVerifier(), FakeStorageGuard())

    result = run(
        subject.run(
            [root],
            limits=BackfillLimits(max_leaf_submissions=3),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )

    assert result.status == "completed"
    assert result.subdivided_periods == 2
    assert result.submitted_leaves == 3
    assert workflow.submissions == [left_left, left_right, right]


def test_arbitrary_leaf_must_already_exist_in_durable_tree() -> None:
    leaf = FiscalPeriod(2025, date(2024, 11, 1), date(2024, 11, 15))
    subject = MemoryCoordinator(
        FakeWorkflow({(leaf.start, leaf.end): 1}),
        FakeVerifier(),
        FakeStorageGuard(),
    )

    with pytest.raises(BackfillError, match="selected_leaf_missing"):
        run(
            subject.run(
                [leaf],
                limits=BackfillLimits(max_periods=1),
                backup_identifier=VALID_BACKUP_IDENTIFIER,
                require_pilot=False,
            )
        )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("submission_unknown", "submission_unknown_requires_operator"),
        ("failed", "failed_attempt_requires_authorization"),
    ],
)
def test_blocking_attempt_states_stop_before_workflow(
    status: str,
    expected: str,
) -> None:
    root = FiscalPeriod(2025, date(2024, 11, 1), date(2024, 11, 30))
    attempt = {
        "id": uuid4(),
        "status": status,
    }
    workflow = FakeWorkflow({(root.start, root.end): 1})
    subject = MemoryCoordinator(
        workflow,
        FakeVerifier(),
        FakeStorageGuard(),
        attempts={(root.start, root.end): attempt},
    )

    with pytest.raises(BackfillError, match=expected):
        run(
            subject.run(
                [root],
                limits=BackfillLimits(max_periods=1),
                backup_identifier=VALID_BACKUP_IDENTIFIER,
                require_pilot=False,
            )
        )
    assert workflow.calls == []


def test_authorized_failed_attempt_creates_and_completes_a_new_attempt() -> None:
    root = FiscalPeriod(2025, date(2024, 11, 1), date(2024, 11, 30))
    failed_id = uuid4()
    workflow = FakeWorkflow({(root.start, root.end): 1})
    subject = MemoryCoordinator(
        workflow,
        FakeVerifier(),
        FakeStorageGuard(),
        attempts={
            (root.start, root.end): {"id": failed_id, "status": "failed"}
        },
    )

    result = run(
        subject.run(
            [root],
            limits=BackfillLimits(max_leaf_submissions=1),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            retry_failed_attempt_id=failed_id,
            require_pilot=False,
        )
    )

    assert result.status == "completed"
    assert workflow.submissions == [(root.start, root.end)]
    assert any(
        record["event"] == "failed_retry_authorized"
        and record["attempt_id"] == str(failed_id)
        for record in result.records
    )


def test_same_job_recovery_selects_exact_job_without_submission() -> None:
    period = FiscalPeriod(2025, date(2025, 5, 1), date(2025, 5, 15))
    failed_id = uuid4()
    recovery_id = uuid4()
    calls: list[tuple[object, ...]] = []

    class RecoveryWorkflow:
        def claim_failed_job_recovery(
            self, attempt_id: UUID, fiscal_year: int, start: date, end: date
        ) -> Any:
            calls.append(("claim", attempt_id, fiscal_year, start, end))
            return SimpleNamespace(
                recovery_attempt_id=recovery_id,
                failed_attempt_id=failed_id,
                attempt={"id": recovery_id, "export_rows": 1},
                created=True,
            )

        async def resume_failed_job_recovery(self, **kwargs: Any) -> Any:
            calls.append(("resume", kwargs["recovery_attempt_id"]))
            kwargs["capacity_guard"](1)
            return workflow_result(attempt_id=recovery_id, status="completed", count=1)

        async def run_period(self, **_kwargs: Any) -> Any:
            pytest.fail("ordinary export workflow must not run during recovery")

    subject = MemoryCoordinator(
        RecoveryWorkflow(),  # type: ignore[arg-type]
        FakeVerifier(),
        FakeStorageGuard(),
    )
    result = run(
        subject.run(
            [period],
            limits=BackfillLimits(max_periods=1, max_leaf_submissions=1),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            recover_failed_job_attempt_id=failed_id,
            require_pilot=False,
        )
    )

    assert result.status == "completed"
    assert (result.processed_periods, result.submitted_leaves) == (1, 0)
    assert calls == [
        ("claim", failed_id, period.fiscal_year, period.start, period.end),
        ("resume", recovery_id),
    ]
    assert [record["event"] for record in result.records if record["event"].startswith("same_job")] == [
        "same_job_recovery_claimed"
    ]


@pytest.mark.parametrize(
    ("created", "export_rows", "expected_rows"),
    [(True, None, DEFAULT_MAX_ROWS), (False, 0, 0), (False, 17, 17)],
)
def test_same_job_recovery_uses_only_claimed_job_and_safe_lineage(
    monkeypatch: pytest.MonkeyPatch,
    created: bool,
    export_rows: int | None,
    expected_rows: int,
) -> None:
    period = FiscalPeriod(2025, date(2025, 5, 1), date(2025, 5, 15))
    failed_id, recovery_id = uuid4(), uuid4()
    workflow = RecoveryWorkflowDouble(
        failed_id,
        recovery_id,
        export_rows=export_rows,
        created=created,
        capacity_rows=export_rows if export_rows is not None else 1,
    )
    storage = FakeStorageGuard()
    verifier = FakeVerifier()
    subject = MemoryCoordinator(workflow, verifier, storage)  # type: ignore[arg-type]
    progress: list[dict[str, object]] = []
    subject._progress = progress.append

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("recovery must not select, traverse, subdivide, or run a leaf")

    for name in ("_period_attempt", "_run_leaf", "_subdivide", "_verified_children"):
        monkeypatch.setattr(subject, name, forbidden)

    result = run(
        subject.run(
            [period],
            limits=BackfillLimits(max_periods=1, max_leaf_submissions=1),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            recover_failed_job_attempt_id=failed_id,
            require_pilot=False,
        )
    )
    assert result.status == "completed"
    assert (result.processed_periods, result.submitted_leaves,
            result.skipped_completed, result.subdivided_periods) == (1, 0, 0, 0)
    assert workflow.claim_calls == [
        (failed_id, period.fiscal_year, period.start, period.end)
    ]
    assert len(workflow.resume_calls) == 1
    assert workflow.resume_calls[0]["recovery_attempt_id"] == recovery_id
    assert workflow.resume_calls[0]["expected_failed_attempt_id"] == failed_id
    assert storage.counts == [expected_rows, export_rows if export_rows is not None else 1]
    assert storage.keys == [recovery_id, recovery_id]
    assert storage.completed == [recovery_id]
    assert verifier.verified == [(recovery_id, period)]
    assert progress == list(result.records)
    assert [record["event"] for record in result.records if record["event"].startswith("same_job")] == [
        "same_job_recovery_claimed" if created else "same_job_recovery_resumed"
    ]
    for record in result.records:
        assert not set(record) & {
            "status_url", "file_url", "remote_file_name", "last_error_detail",
            "archive_path", "metadata",
        }
    for record in result.records:
        if record["event"] in {
            "same_job_recovery_claimed", "same_job_recovery_resumed", "leaf_completed"
        }:
            assert record["attempt_id"] == str(recovery_id)
            assert record["recovery_of_attempt_id"] == str(failed_id)


@pytest.mark.parametrize(
    ("roots", "limits", "retry", "recovery", "error"),
    [
        ("one", BackfillLimits(max_periods=1), None, "not-a-uuid", "invalid_recovery_authorization"),
        ("one", BackfillLimits(max_periods=1), "uuid", "uuid", "conflicting_retry_authorizations"),
        ("none", BackfillLimits(max_periods=1), None, "uuid", "recovery_requires_one_period"),
        ("two", BackfillLimits(max_periods=1), None, "uuid", "recovery_requires_one_period"),
        ("one", BackfillLimits(max_periods=2), None, "uuid", "recovery_requires_one_period"),
        ("one", BackfillLimits(max_leaf_submissions=1), None, "uuid", "recovery_requires_one_period"),
    ],
)
def test_same_job_recovery_requires_exact_authorization_and_one_period(
    roots: str,
    limits: BackfillLimits,
    retry: str | None,
    recovery: str,
    error: str,
) -> None:
    first = FiscalPeriod(2025, date(2025, 5, 1), date(2025, 5, 15))
    second = FiscalPeriod(2025, date(2025, 5, 16), date(2025, 5, 31))
    selected = {"one": [first], "none": [], "two": [first, second]}[roots]
    failed_id = uuid4()
    workflow = RecoveryWorkflowDouble(failed_id, uuid4())
    subject = MemoryCoordinator(
        workflow, FakeVerifier(), FakeStorageGuard()  # type: ignore[arg-type]
    )
    with pytest.raises(BackfillError, match=error):
        run(
            subject.run(
                selected,
                limits=limits,
                backup_identifier=VALID_BACKUP_IDENTIFIER,
                retry_failed_attempt_id=failed_id if retry else None,
                recover_failed_job_attempt_id=(
                    failed_id if recovery == "uuid" else recovery  # type: ignore[arg-type]
                ),
                require_pilot=False,
            )
        )
    assert workflow.claim_calls == []
    assert workflow.resume_calls == []


@pytest.mark.parametrize(("mode", "expected"), [("initial", "bounded"), ("callback", "bounded"), ("failed", "period_failed")])
def test_same_job_recovery_bounding_and_failure_do_not_complete_reservation(
    mode: str, expected: str
) -> None:
    period = FiscalPeriod(2025, date(2025, 5, 1), date(2025, 5, 15))
    failed_id, recovery_id = uuid4(), uuid4()
    workflow = RecoveryWorkflowDouble(
        failed_id,
        recovery_id,
        export_rows=2 if mode == "initial" else 0,
        capacity_rows=2 if mode == "callback" else None,
        result_status="failed" if mode == "failed" else "completed",
    )
    storage = FakeStorageGuard()
    subject = MemoryCoordinator(
        workflow, FakeVerifier(), storage  # type: ignore[arg-type]
    )
    limits = BackfillLimits(
        max_periods=1,
        max_estimated_new_bytes=1000 if mode != "failed" else 10000,
    )
    if expected == "period_failed":
        with pytest.raises(BackfillError, match="period_failed"):
            run(subject.run([period], limits=limits,
                            backup_identifier=VALID_BACKUP_IDENTIFIER,
                            recover_failed_job_attempt_id=failed_id,
                            require_pilot=False))
    else:
        result = run(subject.run([period], limits=limits,
                                 backup_identifier=VALID_BACKUP_IDENTIFIER,
                                 recover_failed_job_attempt_id=failed_id,
                                 require_pilot=False))
        assert result.status == "bounded"
        assert (result.processed_periods, result.submitted_leaves) == (1, 0)
    assert storage.completed == []
    assert len(workflow.resume_calls) == (0 if mode == "initial" else 1)


@pytest.mark.parametrize(
    ("attempt", "error"),
    [
        ({"status": "failed", "status_url": "persisted"}, "failed_job_identity_partial"),
        ({"status": "failed", "status_url": "persisted", "file_url": "file", "remote_file_name": "name.zip"}, "same_job_recovery_required"),
        ({"status": "failed", "recovery_of_attempt_id": "linked"}, "same_job_recovery_required"),
        ({"status": "submitted", "recovery_of_attempt_id": "linked"}, "same_job_recovery_required"),
    ],
)
def test_ordinary_retry_rejects_persisted_or_linked_jobs(
    attempt: dict[str, Any], error: str
) -> None:
    period = FiscalPeriod(2025, date(2025, 5, 1), date(2025, 5, 15))
    failed_id = uuid4()
    workflow = FakeWorkflow({(period.start, period.end): 1})
    subject = MemoryCoordinator(
        workflow, FakeVerifier(), FakeStorageGuard(),
        attempts={(period.start, period.end): {"id": failed_id, **attempt}},
    )
    with pytest.raises(BackfillError, match=error):
        run(subject.run([period], limits=BackfillLimits(max_periods=1),
                        backup_identifier=VALID_BACKUP_IDENTIFIER,
                        retry_failed_attempt_id=failed_id,
                        require_pilot=False))
    assert workflow.calls == []


def test_completed_linked_recovery_uses_existing_verification_only() -> None:
    period = FiscalPeriod(2025, date(2025, 5, 1), date(2025, 5, 15))
    recovery_id = uuid4()
    workflow = FakeWorkflow({})
    verifier = FakeVerifier()
    subject = MemoryCoordinator(
        workflow, verifier, FakeStorageGuard(),
        attempts={(period.start, period.end): {
            "id": recovery_id, "status": "completed",
            "recovery_of_attempt_id": uuid4(),
        }},
    )
    result = run(subject.run([period], limits=BackfillLimits(max_periods=1),
                             backup_identifier=VALID_BACKUP_IDENTIFIER,
                             require_pilot=False))
    assert result.skipped_completed == 1
    assert verifier.verified == [(recovery_id, period)]
    assert workflow.calls == []


def test_recovery_path_ast_has_no_export_or_traversal_calls() -> None:
    tree = ast.parse(Path(transaction_backfill.__file__).read_text())
    coordinator = next(node for node in tree.body if isinstance(node, ast.ClassDef)
                       and node.name == "BackfillCoordinator")
    method = next(node for node in coordinator.body if isinstance(node, ast.AsyncFunctionDef)
                  and node.name == "_run_same_job_recovery")
    calls = {
        node.func.attr for node in ast.walk(method)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert calls.isdisjoint({
        "run_period", "_run_leaf", "_process_period", "_subdivide",
        "submit_bulk_export", "get_expected_transaction_count", "_period_attempt",
    })
    workflow_calls = {
        node.func.attr for node in ast.walk(method)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "_workflow"
    }
    assert workflow_calls == {
        "claim_failed_job_recovery", "resume_failed_job_recovery"
    }


def test_completed_period_is_verified_before_skip() -> None:
    root = FiscalPeriod(2025, date(2024, 12, 1), date(2024, 12, 31))
    attempt_id = uuid4()
    verifier = FakeVerifier()
    workflow = FakeWorkflow({(root.start, root.end): 1})
    subject = MemoryCoordinator(
        workflow,
        verifier,
        FakeStorageGuard(),
        attempts={(root.start, root.end): {"id": attempt_id, "status": "completed"}},
    )

    result = run(
        subject.run(
            [root],
            limits=BackfillLimits(max_periods=1),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )

    assert result.skipped_completed == 1
    assert verifier.verified == [(attempt_id, root)]
    assert workflow.calls == []


def test_finite_bounds_and_manifest_are_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(BackfillError, match="finite_execution_bound_required"):
        BackfillLimits()

    path = tmp_path / "manifest.json"
    payload = {"status": "completed", "records": []}
    write_manifest_exclusive(path, payload)
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    assert path.read_text() == '{"records":[],"status":"completed"}\n'
    with pytest.raises(BackfillError, match="manifest_exists"):
        write_manifest_exclusive(path, payload)


def test_safe_result_manifest_contains_only_reviewed_fields() -> None:
    from app.usaspending.transaction_backfill import BackfillResult

    result = BackfillResult(uuid4(), "bounded", 1, 0, 1, 0, 0, ())
    payload = safe_result_manifest(result, backup_identifier=VALID_BACKUP_IDENTIFIER)
    encoded = str(payload).lower()
    assert "http" not in encoded
    assert "password" not in encoded
    assert "exception" not in encoded

    unsafe = BackfillResult(
        uuid4(),
        "completed",
        1,
        0,
        0,
        0,
        0,
        ({"event": "ok", "url": "https://example.invalid"},),
    )
    with pytest.raises(BackfillError, match="unsafe_manifest_record"):
        safe_result_manifest(unsafe, backup_identifier=VALID_BACKUP_IDENTIFIER)


def test_storage_guard_rejects_untrusted_capacity_and_local_measurement() -> None:
    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    with pytest.raises(BackfillError, match="invalid_storage_input"):
        StorageGuard(
            engine,  # type: ignore[arg-type]
            Path("/tmp"),
            100,
            101,
            10,
            10,
            Decimal("1"),
        )

    guard = StorageGuard(
        engine,  # type: ignore[arg-type]
        Path("/tmp"),
        1000,
        900,
        100,
        100,
        Decimal("2"),
        disk_usage=lambda _path: (_ for _ in ()).throw(OSError("unavailable")),
    )
    with pytest.raises(BackfillError, match="local_storage_measurement_unavailable"):
        guard.check(1)


class _StorageConnection:
    def __init__(self, measurements: list[tuple[int, int]]) -> None:
        self._measurements = measurements
        self._index = 0
        self._scalar_index = 0

    def __enter__(self) -> _StorageConnection:
        self._scalar_index = 0
        return self

    def __exit__(self, *_: object) -> None:
        self._index += 1

    def scalar(self, _statement: object) -> int:
        value = self._measurements[min(self._index, len(self._measurements) - 1)]
        result = value[self._scalar_index]
        self._scalar_index += 1
        return result


class _StorageEngine:
    dialect = SimpleNamespace(name="postgresql")

    def __init__(self, measurements: list[tuple[int, int]]) -> None:
        self.connection = _StorageConnection(measurements)

    def connect(self) -> _StorageConnection:
        return self.connection


def test_storage_guard_measures_configured_filesystem_and_exact_local_threshold(
    tmp_path: Path,
) -> None:
    measured: list[Path] = []
    required = LOCAL_ARCHIVE_HEADROOM_BYTES + 123
    guard = StorageGuard(
        _StorageEngine([(0, 0)]),  # type: ignore[arg-type]
        tmp_path,
        10_000_000,
        9_000_000,
        1_000,
        123,
        Decimal("1"),
        disk_usage=lambda path: (
            measured.append(path) or SimpleNamespace(free=required)
        ),
    )

    decision = guard.check(1)

    assert measured == [tmp_path]
    assert decision.local_required_bytes == required
    assert decision.estimated_relation_bytes == 550
    assert decision.estimated_wal_bytes == 550

    failing = StorageGuard(
        _StorageEngine([(0, 0)]),  # type: ignore[arg-type]
        tmp_path,
        10_000_000,
        9_000_000,
        1_000,
        123,
        Decimal("1"),
        disk_usage=lambda _path: SimpleNamespace(free=required - 1),
    )
    with pytest.raises(BackfillError, match="local_storage_reserve_failed"):
        failing.check(1)


def test_storage_guard_consumes_one_run_budget_across_multiple_leaves(
    tmp_path: Path,
) -> None:
    guard = StorageGuard(
        _StorageEngine([(1_000, 0), (1_000, 0), (1_000, 0)]),  # type: ignore[arg-type]
        tmp_path,
        20_000,
        4_000,
        500,
        0,
        Decimal("1"),
        disk_usage=lambda _path: SimpleNamespace(
            free=LOCAL_ARCHIVE_HEADROOM_BYTES
        ),
    )

    first = guard.check(1)
    second = guard.check(1)

    assert first.database_budget_remaining_bytes == 2_400
    assert second.database_budget_remaining_bytes == 1_300
    with pytest.raises(BackfillError, match="database_storage_reserve_failed"):
        guard.check(2)


def test_storage_guard_absolute_refresh_is_monotonic_without_double_counting(
    tmp_path: Path,
) -> None:
    refreshed = iter([8_000, 8_000, 9_000, 7_000, 8_000, 8_000])
    guard = StorageGuard(
        _StorageEngine([(1_000, 0)] * 6),  # type: ignore[arg-type]
        tmp_path,
        10_000,
        8_000,
        500,
        0,
        Decimal("1"),
        disk_usage=lambda _path: SimpleNamespace(
            free=LOCAL_ARCHIVE_HEADROOM_BYTES
        ),
        database_free_bytes_provider=lambda: next(refreshed),
    )

    first = guard.check(1, reservation_key="leaf-1")
    unchanged = guard.check(1, reservation_key="leaf-1")
    apparently_increased = guard.check(1, reservation_key="leaf-1")
    decreased = guard.check(1, reservation_key="leaf-1")
    stale_increase = guard.check(1, reservation_key="leaf-1")

    assert first.database_budget_remaining_bytes == 6_400
    assert unchanged.database_budget_remaining_bytes == 6_400
    assert apparently_increased.database_budget_remaining_bytes == 6_400
    assert decreased.database_budget_remaining_bytes == 5_400
    assert stale_increase.database_budget_remaining_bytes == 5_400
    assert stale_increase.database_free_bytes == 7_000

    guard.complete("leaf-1")
    completed_consumption = guard.check(1, reservation_key="leaf-2")
    assert completed_consumption.database_budget_remaining_bytes == 4_300


@pytest.mark.parametrize("refreshed", [True, -1, 10_001, "8000"])
def test_storage_guard_rejects_invalid_absolute_refresh(
    tmp_path: Path,
    refreshed: object,
) -> None:
    guard = StorageGuard(
        _StorageEngine([(0, 0)]),  # type: ignore[arg-type]
        tmp_path,
        10_000,
        8_000,
        500,
        0,
        Decimal("1"),
        disk_usage=lambda _path: SimpleNamespace(
            free=LOCAL_ARCHIVE_HEADROOM_BYTES
        ),
        database_free_bytes_provider=lambda: refreshed,  # type: ignore[arg-type,return-value]
    )

    with pytest.raises(
        BackfillError, match="database_storage_measurement_unavailable"
    ):
        guard.check(1)


def test_storage_guard_rejects_overflowing_estimate(tmp_path: Path) -> None:
    guard = StorageGuard(
        _StorageEngine([(0, 0)]),  # type: ignore[arg-type]
        tmp_path,
        MAX_STORAGE_BYTES,
        MAX_STORAGE_BYTES - 1,
        0,
        0,
        Decimal(MAX_STORAGE_BYTES),
        disk_usage=lambda _path: SimpleNamespace(
            free=LOCAL_ARCHIVE_HEADROOM_BYTES
        ),
    )
    with pytest.raises(BackfillError, match="storage_estimate_overflow"):
        guard.check(500_000)


@pytest.mark.parametrize("mutation", ["removed", "replaced", "symlink_swapped"])
def test_temporary_path_identity_failure_stops_before_submission(
    tmp_path: Path,
    mutation: str,
) -> None:
    configured = tmp_path / "configured"
    configured.mkdir()
    guard = StorageGuard(
        _StorageEngine([(0, 0)]),  # type: ignore[arg-type]
        configured,
        10_000_000,
        9_000_000,
        1_000,
        0,
        Decimal("1"),
        disk_usage=lambda _path: SimpleNamespace(
            free=LOCAL_ARCHIVE_HEADROOM_BYTES
        ),
    )
    original = tmp_path / "original"
    if mutation == "removed":
        configured.rmdir()
    else:
        configured.rename(original)
        if mutation == "replaced":
            configured.mkdir()
        else:
            configured.symlink_to(original, target_is_directory=True)

    period = FiscalPeriod(2025, date(2024, 12, 1), date(2024, 12, 31))
    workflow = FakeWorkflow({(period.start, period.end): 1})
    subject = MemoryCoordinator(
        workflow,
        FakeVerifier(),
        guard,  # type: ignore[arg-type]
    )

    with pytest.raises(
        BackfillError, match="local_storage_measurement_unavailable"
    ):
        run(
            subject.run(
                [period],
                limits=BackfillLimits(max_leaf_submissions=1),
                backup_identifier=VALID_BACKUP_IDENTIFIER,
                require_pilot=False,
            )
        )
    assert workflow.submissions == []


@pytest.mark.parametrize(
    "value",
    [
        "password-backup",
        "token-123",
        "https://backup.invalid/x",
        "/sensitive/backup",
        "backup.json",
        "backup-1",
        "backup\nidentifier",
        "x" * 129,
        "backup-20260230t202143z-9f21643cf5b6c8df",
        "backup-20261004t246000z-9f21643cf5b6c8df",
        "backup-20261004t202143z-9f21643cf5b6c8d",
        f"backup-20261004t202143z-{'a' * 65}",
        "backup-20261004t202143z-9f21643cf5b6c8dg",
    ],
)
def test_backup_identifier_boundary_rejects_sensitive_values(value: str) -> None:
    with pytest.raises(BackfillError, match="invalid_backup_identifier") as caught:
        canonicalize_backup_identifier(value)
    assert value not in str(caught.value)


@pytest.mark.parametrize(
    "value",
    [
        "AKIAABCDEFGHIJKLMNOP",
        "ASIAABCDEFGHIJKLMNOP",
        f"ghp_{'A' * 36}",
        f"github_pat_{'A' * 22}",
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signaturevalue",
        "cGFzc3dvcmQ",
        "backup-c2VjcmV0",
        "sk-proj-abcdefghijklmnopqrstuvwxyz123456",
        _synthetic_slack_token_shape(),
        "736563726574",
    ],
)
def test_backup_identifier_rejects_credential_shapes_and_encoded_secrets(
    value: str,
) -> None:
    with pytest.raises(BackfillError, match="invalid_backup_identifier") as caught:
        canonicalize_backup_identifier(value)

    assert value not in str(caught.value)


def test_backup_identifier_accepts_only_strict_reference_and_canonicalizes() -> None:
    assert canonicalize_backup_identifier(VALID_BACKUP_IDENTIFIER) == (
        VALID_BACKUP_IDENTIFIER
    )
    assert canonicalize_backup_identifier(VALID_BACKUP_IDENTIFIER.upper()) == (
        VALID_BACKUP_IDENTIFIER
    )
    maximum = f"backup-20261004t202143z-{'A' * 64}"
    assert canonicalize_backup_identifier(maximum) == maximum.lower()


def test_manifest_retries_short_writes_and_eintr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "manifest.json"
    real_write = os.write
    calls = 0

    def interrupted_short_write(descriptor: int, data: bytes) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InterruptedError
        return real_write(descriptor, data[:3])

    monkeypatch.setattr(transaction_backfill.os, "write", interrupted_short_write)
    write_manifest_exclusive(path, {"status": "completed", "records": []})

    assert calls > 2
    assert path.read_text() == '{"records":[],"status":"completed"}\n'


@pytest.mark.parametrize(
    "invalid_result",
    [0, -1, "oversized", True, False, 1.5, "1"],
)
def test_manifest_rejects_impossible_write_counts_without_leaving_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid_result: object,
) -> None:
    path = tmp_path / "manifest.json"
    real_write = os.write

    def impossible_write(_descriptor: int, data: bytes) -> int:
        if invalid_result == "oversized":
            return len(data) + 1
        return invalid_result  # type: ignore[return-value]

    monkeypatch.setattr(transaction_backfill.os, "write", impossible_write)
    with pytest.raises(BackfillError, match="manifest_write_failed") as caught:
        write_manifest_exclusive(path, {"status": "completed", "records": []})
    assert caught.value.code == "manifest_write_failed"
    assert str(caught.value) == "manifest_write_failed"
    assert not path.exists()

    monkeypatch.setattr(transaction_backfill.os, "write", real_write)
    write_manifest_exclusive(path, {"status": "completed", "records": []})
    assert path.read_text() == '{"records":[],"status":"completed"}\n'


def test_manifest_rejects_unserializable_values_without_creating_output(
    tmp_path: Path,
) -> None:
    path = tmp_path / "manifest.json"
    with pytest.raises(BackfillError, match="unsafe_manifest_record"):
        write_manifest_exclusive(path, {"unsafe": object()})
    assert not path.exists()


@pytest.mark.parametrize(
    "failure", ["zero_write", "file_fsync", "file_close", "dir_fsync"]
)
def test_manifest_failure_removes_partial_output_and_allows_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    path = tmp_path / "manifest.json"
    real_write = os.write
    real_fsync = os.fsync
    real_close = os.close
    calls = {"fsync": 0, "close": 0}

    if failure == "zero_write":
        monkeypatch.setattr(transaction_backfill.os, "write", lambda *_: 0)
    elif failure in {"file_fsync", "dir_fsync"}:
        target = 1 if failure == "file_fsync" else 2

        def failing_fsync(descriptor: int) -> None:
            calls["fsync"] += 1
            if calls["fsync"] == target:
                raise OSError("sensitive fsync failure")
            real_fsync(descriptor)

        monkeypatch.setattr(transaction_backfill.os, "fsync", failing_fsync)
    else:
        def failing_close(descriptor: int) -> None:
            calls["close"] += 1
            if calls["close"] == 1:
                real_close(descriptor)
                raise OSError("sensitive close failure")
            real_close(descriptor)

        monkeypatch.setattr(transaction_backfill.os, "close", failing_close)

    with pytest.raises(BackfillError, match="manifest_write_failed") as caught:
        write_manifest_exclusive(path, {"status": "completed", "records": []})
    assert "sensitive" not in str(caught.value)
    assert not path.exists()

    monkeypatch.setattr(transaction_backfill.os, "write", real_write)
    monkeypatch.setattr(transaction_backfill.os, "fsync", real_fsync)
    monkeypatch.setattr(transaction_backfill.os, "close", real_close)
    write_manifest_exclusive(path, {"status": "completed", "records": []})
    assert path.exists()


def _test_database_url() -> str:
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is not set")
    url = make_url(value)
    if url.drivername != "postgresql+psycopg":
        pytest.fail("TEST_DATABASE_URL must use postgresql+psycopg")
    if not (url.database or "").endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must end in _test")
    return value


@pytest.fixture
def backfill_postgres_engine() -> Any:
    admin = create_engine(_test_database_url())
    schema = f"usaspending_backfill_{uuid4().hex}"
    with admin.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    engine = create_engine(
        _test_database_url(),
        connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as connection:
            UsaSpendingIngestionCheckpoint.__table__.create(connection)
            UsaSpendingTransaction.__table__.create(connection)
            UsaSpendingTransactionIngestionAttempt.__table__.create(connection)
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.dispose()


def _synthetic_archive(period: FiscalPeriod) -> bytes:
    action_date = period.start
    row = {
        "contract_transaction_unique_key": f"tx-{action_date.isoformat()}",
        "contract_award_unique_key": f"award-{action_date.isoformat()}",
        "award_id_piid": f"piid-{action_date.isoformat()}",
        "award_type_code": "A",
        "action_date": action_date.isoformat(),
        "federal_action_obligation": "1.25",
        "recipient_name": "Synthetic Recipient",
        "recipient_uei": "ABC123DEF456",
        "awarding_agency_name": "Synthetic Agency",
        "awarding_sub_agency_name": "Synthetic Subagency",
        "transaction_description": "Offline synthetic row",
        "last_modified_date": "2026-10-04T00:00:00Z",
    }
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(EXPECTED_TRANSACTION_HEADERS)
    writer.writerow([row.get(header, "") for header in EXPECTED_TRANSACTION_HEADERS])
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("transactions.csv", stream.getvalue().encode("utf-8"))
    return output.getvalue()


class _SyntheticMultiPeriodClient:
    def __init__(self, periods: list[FiscalPeriod]) -> None:
        self._archives = {
            (period.start, period.end): _synthetic_archive(period)
            for period in periods
        }
        self._jobs: dict[str, tuple[date, date]] = {}
        self.submissions: list[tuple[date, date]] = []
        self.poll_grace_values: list[bool] = []

    def get_expected_transaction_count(self, start: date, end: date) -> int:
        assert (start, end) in self._archives
        return 1

    def submit_bulk_export(self, start: date, end: date) -> BulkExportJob:
        key = (start, end)
        assert key in self._archives
        self.submissions.append(key)
        status_url = (
            "https://api.usaspending.gov/api/v2/download/status/"
            f"{start.isoformat()}-{end.isoformat()}"
        )
        self._jobs[status_url] = key
        return BulkExportJob(
            status_url=status_url,
            file_url=(
                "https://files.usaspending.gov/generated_downloads/"
                f"{start.isoformat()}.zip"
            ),
            file_name="synthetic.zip",
        )

    async def poll_bulk_export(
        self,
        job: BulkExportJob,
        *,
        initial_status_404_grace: bool = False,
    ) -> BulkExportStatus:
        assert job.status_url in self._jobs
        self.poll_grace_values.append(initial_status_404_grace)
        return BulkExportStatus(
            status="finished",
            status_url=job.status_url,
            file_url=job.file_url,
            file_name=job.file_name,
            total_rows=1,
            total_columns=len(EXPECTED_TRANSACTION_HEADERS),
            message=None,
            seconds_elapsed="0.0",
        )

    async def retrieve_bulk_export(
        self,
        completed: BulkExportStatus,
        destination: str | os.PathLike[str],
        **_: object,
    ) -> int:
        archive = self._archives[self._jobs[completed.status_url]]
        Path(destination).write_bytes(archive)
        return len(archive)


class _RestartClient:
    def __init__(self, period: FiscalPeriod) -> None:
        self.archive = _synthetic_archive(period)
        self.poll_calls = 0
        self.poll_grace_values: list[bool] = []
        self.download_calls = 0

    def get_expected_transaction_count(self, _start: date, _end: date) -> int:
        pytest.fail("restart must not recount")

    def submit_bulk_export(self, _start: date, _end: date) -> BulkExportJob:
        pytest.fail("restart must not resubmit")

    async def poll_bulk_export(
        self,
        job: BulkExportJob,
        *,
        initial_status_404_grace: bool = False,
    ) -> BulkExportStatus:
        self.poll_calls += 1
        self.poll_grace_values.append(initial_status_404_grace)
        return BulkExportStatus(
            status="finished",
            status_url=job.status_url,
            file_url=job.file_url,
            file_name=job.file_name,
            total_rows=1,
            total_columns=len(EXPECTED_TRANSACTION_HEADERS),
            message=None,
            seconds_elapsed="0.0",
        )

    async def retrieve_bulk_export(
        self,
        _completed: BulkExportStatus,
        destination: str | os.PathLike[str],
        **_: object,
    ) -> int:
        self.download_calls += 1
        Path(destination).write_bytes(self.archive)
        return len(self.archive)


class _RecoveryNoPostClient(_RestartClient):
    def __init__(self, period: FiscalPeriod) -> None:
        super().__init__(period)
        self.validated_jobs: list[BulkExportJob] = []

    def validate_persisted_bulk_export_job(self, job: BulkExportJob) -> BulkExportJob:
        self.validated_jobs.append(job)
        assert job.file_name == "recovery.zip"
        return job


class _RecordingStorageGuard(StorageGuard):
    def __post_init__(self) -> None:
        self.checked_rows: list[int] = []
        self.checked_keys: list[object | None] = []
        self.completed_keys: list[object] = []
        super().__post_init__()

    def check(
        self,
        expected_rows: int,
        *,
        reservation_key: object | None = None,
    ) -> StorageDecision:
        self.checked_rows.append(expected_rows)
        self.checked_keys.append(reservation_key)
        return super().check(
            expected_rows,
            reservation_key=reservation_key,
        )

    def complete(self, reservation_key: object) -> None:
        self.completed_keys.append(reservation_key)
        super().complete(reservation_key)


@pytest.mark.integration
@pytest.mark.parametrize("invalid_parent", ["outside_period", "not_subdivided"])
def test_recovery_rejects_invalid_ancestry_before_claim_commit(
    backfill_postgres_engine: Any,
    invalid_parent: str,
) -> None:
    engine = backfill_postgres_engine
    root = FiscalPeriod(2025, date(2025, 5, 1), date(2025, 5, 31))
    left_bounds, right_bounds = split_period(root.start, root.end)
    left = FiscalPeriod(root.fiscal_year, *left_bounds)
    right = FiscalPeriod(root.fiscal_year, *right_bounds)
    root_id, outside_id, source_id, sibling_id, checkpoint_id = (
        uuid4() for _ in range(5)
    )
    attempts = UsaSpendingTransactionIngestionAttempt.__table__
    checkpoints = UsaSpendingIngestionCheckpoint.__table__
    transactions = UsaSpendingTransaction.__table__
    root_status = "failed" if invalid_parent == "not_subdivided" else "subdivided"
    source_parent_id = outside_id if invalid_parent == "outside_period" else root_id
    with engine.begin() as connection:
        for values in (
            {
                "id": root_id, "source": "usaspending", "fiscal_year": 2025,
                "period_start": root.start, "period_end": root.end,
                "status": root_status,
                "expected_rows": DEFAULT_MAX_ROWS + 1 if root_status == "subdivided" else None,
                "last_error_code": "synthetic_parent_failure" if root_status == "failed" else None,
                "failure_count": 1 if root_status == "failed" else 0,
                "started_at": NOW, "completed_at": NOW,
                "created_at": NOW, "updated_at": NOW,
            },
            {
                "id": outside_id, "source": "usaspending", "fiscal_year": 2025,
                "period_start": date(2025, 4, 1), "period_end": date(2025, 4, 30),
                "status": "subdivided", "expected_rows": DEFAULT_MAX_ROWS + 1,
                "failure_count": 0, "started_at": NOW, "completed_at": NOW,
                "created_at": NOW, "updated_at": NOW,
            },
        ):
            connection.execute(attempts.insert(), values)
        connection.execute(checkpoints.insert(), {
            "id": checkpoint_id, "source": "usaspending", "fiscal_year": 2025,
            "period_start": right.start, "period_end": right.end,
            "status": "complete", "archive_sha256": "a" * 64,
            "expected_rows": 1, "loaded_rows": 1,
            "started_at": NOW, "completed_at": NOW,
            "created_at": NOW, "updated_at": NOW,
        })
        for values in (
            {
                "id": source_id, "source": "usaspending", "fiscal_year": 2025,
                "period_start": left.start, "period_end": left.end,
                "parent_attempt_id": source_parent_id, "status": "failed",
                "expected_rows": 1,
                "status_url": "https://api.usaspending.gov/api/v2/download/status/rehydrate",
                "file_url": "https://files.usaspending.gov/generated_downloads/recovery.zip",
                "remote_file_name": "recovery.zip", "last_error_code": "http_404",
                "failure_count": 1, "started_at": NOW, "completed_at": NOW,
                "created_at": NOW, "updated_at": NOW,
            },
            {
                "id": sibling_id, "source": "usaspending", "fiscal_year": 2025,
                "period_start": right.start, "period_end": right.end,
                "parent_attempt_id": root_id, "status": "completed",
                "expected_rows": 1, "export_rows": 1, "loaded_rows": 1,
                "export_columns": len(EXPECTED_TRANSACTION_HEADERS),
                "status_url": "https://api.usaspending.gov/api/v2/download/status/sibling",
                "file_url": "https://files.usaspending.gov/generated_downloads/sibling.zip",
                "remote_file_name": "sibling.zip",
                "archive_sha256": "a" * 64, "archive_bytes": 100,
                "signed_obligation_total": Decimal("1.00"),
                "checkpoint_id": checkpoint_id, "failure_count": 0,
                "started_at": NOW, "completed_at": NOW,
                "created_at": NOW, "updated_at": NOW,
            },
        ):
            connection.execute(attempts.insert(), values)
        connection.execute(transactions.insert(), {
            "stable_transaction_id": "unchanged-sibling-row",
            "ingestion_checkpoint_id": checkpoint_id,
            "generated_award_id": "unchanged-award", "display_award_id": "unchanged",
            "award_type_code": "A", "action_date": right.start,
            "fiscal_year": 2025, "federal_action_obligation": Decimal("1.00"),
            "recipient_name": "recipient", "recipient_uei": "ABC123DEF456",
            "awarding_agency_name": "agency", "awarding_subagency_name": "subagency",
            "transaction_description": "unchanged", "source_updated_at": NOW,
            "fetched_at": NOW, "created_at": NOW, "updated_at": NOW,
        })

    def snapshot() -> tuple[dict[Any, dict[str, Any]], ...]:
        with engine.connect() as connection:
            return tuple(
                {row[key]: dict(row) for row in connection.execute(select(table)).mappings()}
                for table, key in (
                    (attempts, "id"), (checkpoints, "id"),
                    (transactions, "stable_transaction_id"),
                )
            )

    before = snapshot()
    client = _RecoveryNoPostClient(left)
    storage = FakeStorageGuard()

    def unexpected_directory() -> Any:
        pytest.fail("rejected recovery must not create a temporary directory")

    workflow = TransactionIngestionWorkflow(
        engine, client, object(),  # type: ignore[arg-type]
        temporary_directory_factory=unexpected_directory,
    )
    coordinator = BackfillCoordinator(
        engine, workflow, storage, verifier=FakeVerifier(), clock=lambda: NOW,
    )
    with pytest.raises(BackfillError, match="^unexpected_overlapping_state$") as rejection:
        run(coordinator.run(
            [left], limits=BackfillLimits(max_periods=1),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            recover_failed_job_attempt_id=source_id, require_pilot=False,
        ))
    after = snapshot()
    linked = [
        row for row in after[0].values()
        if row["recovery_of_attempt_id"] == source_id
    ]
    assert {
        attempt_id: row for attempt_id, row in after[0].items()
        if row["recovery_of_attempt_id"] != source_id
    } == before[0]
    assert after[1:] == before[1:]
    assert client.poll_calls == client.download_calls == 0
    assert storage.counts == storage.completed == []
    assert coordinator._submitted == 0
    linked_count = len(linked)
    assert linked_count == 0, (
        f"{invalid_parent}: coordinator_rejected={isinstance(rejection.value, BackfillError)}; "
        f"expected_linked_count=0; observed_linked_count={linked_count}"
    )


@pytest.mark.integration
def test_coordinator_same_job_recovery_completes_without_post_or_sibling_change(
    backfill_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = backfill_postgres_engine
    root = FiscalPeriod(2025, date(2025, 5, 1), date(2025, 5, 31))
    left_bounds, right_bounds = split_period(root.start, root.end)
    left = FiscalPeriod(root.fiscal_year, *left_bounds)
    right = FiscalPeriod(root.fiscal_year, *right_bounds)
    root_id, failed_id, sibling_id = uuid4(), uuid4(), uuid4()
    status_url = "https://api.usaspending.gov/api/v2/download/status/rehydrate"
    file_url = "https://files.usaspending.gov/generated_downloads/recovery.zip"
    with engine.begin() as connection:
        for values in (
            {
                "id": root_id, "source": "usaspending", "fiscal_year": root.fiscal_year,
                "period_start": root.start, "period_end": root.end,
                "status": "subdivided", "expected_rows": DEFAULT_MAX_ROWS + 1,
                "failure_count": 0, "started_at": NOW, "completed_at": NOW,
                "created_at": NOW, "updated_at": NOW,
            },
            {
                "id": failed_id, "source": "usaspending", "fiscal_year": left.fiscal_year,
                "period_start": left.start, "period_end": left.end,
                "status": "failed", "expected_rows": 1,
                "status_url": status_url, "file_url": file_url,
                "remote_file_name": "recovery.zip", "parent_attempt_id": root_id,
                "last_error_code": "http_404", "last_error_detail": "synthetic failure",
                "failure_count": 1, "started_at": NOW, "completed_at": NOW,
                "created_at": NOW, "updated_at": NOW,
            },
            {
                "id": sibling_id, "source": "usaspending", "fiscal_year": right.fiscal_year,
                "period_start": right.start, "period_end": right.end,
                "status": "created", "parent_attempt_id": root_id,
                "failure_count": 0, "started_at": NOW,
                "created_at": NOW, "updated_at": NOW,
            },
        ):
            connection.execute(
                UsaSpendingTransactionIngestionAttempt.__table__.insert(), values
            )
    with engine.connect() as connection:
        original = {
            row["id"]: dict(row) for row in connection.execute(
                select(UsaSpendingTransactionIngestionAttempt).where(
                    UsaSpendingTransactionIngestionAttempt.id.in_(
                        (root_id, failed_id, sibling_id)
                    )
                )
            ).mappings()
        }

    client = _RecoveryNoPostClient(left)

    @contextmanager
    def workflow_directory() -> Iterator[Path]:
        with tempfile.TemporaryDirectory(dir=tmp_path) as name:
            yield Path(name)

    guard = _RecordingStorageGuard(
        engine, tmp_path, 10**12, 9 * 10**11, 10**9, 0, Decimal("1")
    )
    workflow = TransactionIngestionWorkflow(
        engine,
        client,  # type: ignore[arg-type]
        TransactionIngestionLoader(
            engine,
            clock=lambda: NOW,
            archive_copy_directory=tmp_path,
            archive_copy_directory_identity=guard.local_identity,
        ),
        clock=lambda: NOW,
        temporary_directory_factory=workflow_directory,
    )
    result = run(
        BackfillCoordinator(
            engine, workflow, guard,
            verifier=CompletedPeriodVerifier(engine), clock=lambda: NOW,
        ).run(
            [left], limits=BackfillLimits(max_periods=1, max_leaf_submissions=1),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            recover_failed_job_attempt_id=failed_id,
            require_pilot=False,
        )
    )

    assert result.status == "completed"
    assert (result.processed_periods, result.submitted_leaves,
            result.skipped_completed, result.subdivided_periods) == (1, 0, 0, 0)
    assert client.poll_calls == 1
    assert client.poll_grace_values == [False]
    assert client.download_calls == 1
    assert len(client.validated_jobs) >= 2
    with engine.connect() as connection:
        rows = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().all()
    by_id = {row["id"]: dict(row) for row in rows}
    assert all(by_id[attempt_id] == original[attempt_id] for attempt_id in original)
    recovery_rows = [row for row in rows if row["recovery_of_attempt_id"] == failed_id]
    assert len(recovery_rows) == 1
    recovery = recovery_rows[0]
    assert recovery["status"] == "completed"
    assert recovery["parent_attempt_id"] == root_id
    assert guard.checked_keys == [recovery["id"]] * len(guard.checked_keys)
    assert guard.completed_keys == [recovery["id"]]
    assert [record["event"] for record in result.records if record["event"] in {
        "same_job_recovery_claimed", "same_job_recovery_resumed", "leaf_completed"
    }] == ["same_job_recovery_claimed", "leaf_completed"]
    assert all("status_url" not in record and "file_url" not in record
               and "remote_file_name" not in record for record in result.records)


@pytest.mark.integration
@pytest.mark.parametrize(
    "resume_status",
    ["submitted", "export_finished", "archive_hashed", "loading"],
)
def test_coordinator_restart_uses_real_storage_guard_before_activity(
    backfill_postgres_engine: Any,
    tmp_path: Path,
    resume_status: str,
) -> None:
    engine = backfill_postgres_engine
    period = FiscalPeriod(2025, date(2024, 12, 1), date(2024, 12, 31))
    client = _RestartClient(period)
    attempt_id = uuid4()
    values: dict[str, object] = {
        "id": attempt_id,
        "source": "usaspending",
        "fiscal_year": period.fiscal_year,
        "period_start": period.start,
        "period_end": period.end,
        "status": resume_status,
        "expected_rows": 1,
        "status_url": "https://api.usaspending.gov/api/v2/download/status/restart",
        "file_url": (
            "https://files.usaspending.gov/generated_downloads/restart.zip"
        ),
        "remote_file_name": "restart.zip",
        "failure_count": 0,
        "started_at": NOW,
        "created_at": NOW,
        "updated_at": NOW,
    }
    if resume_status != "submitted":
        values.update(export_rows=1, export_columns=len(EXPECTED_TRANSACTION_HEADERS))
    if resume_status in {"archive_hashed", "loading"}:
        values.update(
            archive_sha256=hashlib.sha256(client.archive).hexdigest(),
            archive_bytes=len(client.archive),
        )
    with engine.begin() as connection:
        connection.execute(
            UsaSpendingTransactionIngestionAttempt.__table__.insert(), values
        )

    @contextmanager
    def workflow_directory() -> Iterator[Path]:
        with tempfile.TemporaryDirectory(dir=tmp_path) as name:
            yield Path(name)

    guard = _RecordingStorageGuard(
        engine,
        tmp_path,
        10**12,
        9 * 10**11,
        10**9,
        0,
        Decimal("1"),
    )
    workflow = TransactionIngestionWorkflow(
        engine,
        client,  # type: ignore[arg-type]
        TransactionIngestionLoader(
            engine,
            clock=lambda: NOW,
            archive_copy_directory=tmp_path,
            archive_copy_directory_identity=guard.local_identity,
        ),
        clock=lambda: NOW,
        temporary_directory_factory=workflow_directory,
    )

    result = run(
        BackfillCoordinator(
            engine,
            workflow,
            guard,
            verifier=CompletedPeriodVerifier(engine),
            clock=lambda: NOW,
        ).run(
            [period],
            limits=BackfillLimits(max_leaf_submissions=1),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )

    assert result.status == "completed"
    assert result.submitted_leaves == 0
    assert guard.checked_rows
    assert guard.checked_rows[-1] == 1
    if resume_status == "submitted":
        assert DEFAULT_MAX_ROWS in guard.checked_rows
    assert client.poll_calls == (1 if resume_status == "submitted" else 0)
    assert client.poll_grace_values == ([False] if resume_status == "submitted" else [])
    assert client.download_calls == 1


@pytest.mark.integration
def test_failed_subdivided_child_retry_preserves_parent_and_traverses_replacement(
    backfill_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = backfill_postgres_engine
    root = FiscalPeriod(2025, date(2024, 11, 1), date(2024, 11, 30))
    left_bounds, right_bounds = split_period(root.start, root.end)
    left = FiscalPeriod(root.fiscal_year, *left_bounds)
    right = FiscalPeriod(root.fiscal_year, *right_bounds)
    parent_id = uuid4()
    failed_id = uuid4()
    right_attempt_id = uuid4()
    right_checkpoint_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            UsaSpendingIngestionCheckpoint.__table__.insert(),
            {
                "id": right_checkpoint_id,
                "source": "usaspending",
                "fiscal_year": right.fiscal_year,
                "period_start": right.start,
                "period_end": right.end,
                "status": "complete",
                "archive_sha256": "e" * 64,
                "expected_rows": 0,
                "loaded_rows": 0,
                "started_at": NOW,
                "completed_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )
        for attempt_values in (
            {
                "id": parent_id,
                "source": "usaspending",
                "fiscal_year": root.fiscal_year,
                "period_start": root.start,
                "period_end": root.end,
                "status": "subdivided",
                "expected_rows": 500001,
                "failure_count": 0,
                "started_at": NOW,
                "completed_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
            {
                "id": failed_id,
                "source": "usaspending",
                "fiscal_year": left.fiscal_year,
                "period_start": left.start,
                "period_end": left.end,
                "status": "failed",
                "parent_attempt_id": parent_id,
                "last_error_code": "test_failure",
                "failure_count": 1,
                "started_at": NOW,
                "completed_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
            {
                "id": right_attempt_id,
                "source": "usaspending",
                "fiscal_year": right.fiscal_year,
                "period_start": right.start,
                "period_end": right.end,
                "status": "completed",
                "parent_attempt_id": parent_id,
                "expected_rows": 0,
                "export_rows": 0,
                "export_columns": len(EXPECTED_TRANSACTION_HEADERS),
                "status_url": "https://api.usaspending.gov/status/right",
                "file_url": "https://files.usaspending.gov/right.zip",
                "remote_file_name": "right.zip",
                "archive_sha256": "e" * 64,
                "archive_bytes": 0,
                "loaded_rows": 0,
                "signed_obligation_total": Decimal("0.00"),
                "checkpoint_id": right_checkpoint_id,
                "failure_count": 0,
                "started_at": NOW,
                "completed_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        ):
            connection.execute(
                UsaSpendingTransactionIngestionAttempt.__table__.insert(),
                attempt_values,
            )

    client = _SyntheticMultiPeriodClient([left])

    @contextmanager
    def workflow_directory() -> Iterator[Path]:
        with tempfile.TemporaryDirectory(dir=tmp_path) as name:
            yield Path(name)

    guard = _RecordingStorageGuard(
        engine,
        tmp_path,
        10**12,
        9 * 10**11,
        10**9,
        0,
        Decimal("1"),
    )
    coordinator = BackfillCoordinator(
        engine,
        TransactionIngestionWorkflow(
            engine,
            client,  # type: ignore[arg-type]
            TransactionIngestionLoader(
                engine,
                clock=lambda: NOW,
                archive_copy_directory=tmp_path,
                archive_copy_directory_identity=guard.local_identity,
            ),
            clock=lambda: NOW,
            temporary_directory_factory=workflow_directory,
        ),
        guard,
        verifier=CompletedPeriodVerifier(engine),
        clock=lambda: NOW,
    )

    first = run(
        coordinator.run(
            [root],
            limits=BackfillLimits(max_leaf_submissions=1),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            retry_failed_attempt_id=failed_id,
            require_pilot=False,
        )
    )

    assert first.status == "completed"
    with engine.connect() as connection:
        child_rows = connection.execute(
            select(
                UsaSpendingTransactionIngestionAttempt.id,
                UsaSpendingTransactionIngestionAttempt.status,
                UsaSpendingTransactionIngestionAttempt.parent_attempt_id,
            ).where(
                UsaSpendingTransactionIngestionAttempt.period_start == left.start,
                UsaSpendingTransactionIngestionAttempt.period_end == left.end,
            )
        ).all()
    assert (failed_id, "failed", parent_id) in child_rows
    replacements = [row for row in child_rows if row.id != failed_id]
    assert len(replacements) == 1
    assert replacements[0].status == "completed"
    assert replacements[0].parent_attempt_id == parent_id

    second_client = _SyntheticMultiPeriodClient([])
    second_guard = _RecordingStorageGuard(
        engine,
        tmp_path,
        10**12,
        9 * 10**11,
        10**9,
        0,
        Decimal("1"),
    )
    second = run(
        BackfillCoordinator(
            engine,
            TransactionIngestionWorkflow(
                engine,
                second_client,  # type: ignore[arg-type]
                TransactionIngestionLoader(engine, clock=lambda: NOW),
                clock=lambda: NOW,
                temporary_directory_factory=workflow_directory,
            ),
            second_guard,
            verifier=CompletedPeriodVerifier(engine),
            clock=lambda: NOW,
        ).run(
            [root],
            limits=BackfillLimits(max_periods=3),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )
    assert second.status == "completed"
    assert second.skipped_completed == 2
    assert second_client.submissions == []


@pytest.mark.integration
def test_synthetic_multi_fiscal_year_coordinator_client_parser_loader_flow(
    backfill_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = backfill_postgres_engine
    roots = [
        root
        for root in backfill_month_roots()
        if date(2019, 10, 1) <= root.start <= date(2020, 10, 1)
    ]
    assert len(roots) == 13
    assert {root.fiscal_year for root in roots} == {2020, 2021}
    client = _SyntheticMultiPeriodClient(roots)

    @contextmanager
    def workflow_directory() -> Iterator[Path]:
        with tempfile.TemporaryDirectory(dir=tmp_path) as name:
            yield Path(name)

    loader = TransactionIngestionLoader(
        engine,
        clock=lambda: NOW,
        archive_copy_directory=tmp_path,
    )
    workflow = TransactionIngestionWorkflow(
        engine,
        client,  # type: ignore[arg-type]
        loader,
        clock=lambda: NOW,
        temporary_directory_factory=workflow_directory,
    )
    result = run(
        BackfillCoordinator(
            engine,
            workflow,
            FakeStorageGuard(),  # type: ignore[arg-type]
            verifier=CompletedPeriodVerifier(engine),
            clock=lambda: NOW,
        ).run(
            roots,
            limits=BackfillLimits(max_leaf_submissions=len(roots)),
            backup_identifier=VALID_BACKUP_IDENTIFIER,
            require_pilot=False,
        )
    )

    assert result.status == "completed"
    assert result.submitted_leaves == len(roots)
    assert client.submissions == [(root.start, root.end) for root in roots]
    assert client.poll_grace_values == [True] * len(roots)
    with engine.connect() as connection:
        assert connection.scalar(
            select(func.count()).select_from(UsaSpendingTransaction)
        ) == len(roots)
        assert set(
            connection.scalars(
                select(UsaSpendingTransaction.fiscal_year).distinct()
            )
        ) == {2020, 2021}
    assert list(tmp_path.iterdir()) == []


def _hold_lease(database_url: str, ready: Any) -> None:
    engine = create_engine(database_url)
    try:
        with global_ingestion_lease(engine):
            ready.send("held")
            while True:
                ready.recv()
    finally:
        engine.dispose()


@pytest.mark.integration
def test_postgresql_global_lease_has_one_holder_and_releases_on_process_death() -> None:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_hold_lease, args=(_test_database_url(), child))
    process.start()
    try:
        assert parent.poll(10)
        assert parent.recv() == "held"
        contender = create_engine(_test_database_url())
        try:
            with pytest.raises(BackfillError, match="ingestion_lease_busy"):
                with global_ingestion_lease(contender):
                    raise AssertionError("contender must not acquire")
        finally:
            contender.dispose()
    finally:
        process.terminate()
        process.join(10)
    assert not process.is_alive()

    replacement = create_engine(_test_database_url())
    try:
        with global_ingestion_lease(replacement):
            pass
    finally:
        replacement.dispose()


@pytest.mark.integration
def test_concurrent_parent_subdivision_commits_one_exact_child_pair(
    backfill_postgres_engine: Any,
) -> None:
    engine = backfill_postgres_engine
    root = FiscalPeriod(2025, date(2024, 10, 1), date(2024, 10, 31))
    parent_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            UsaSpendingTransactionIngestionAttempt.__table__.insert(),
            {
                "id": parent_id,
                "source": "usaspending",
                "fiscal_year": root.fiscal_year,
                "period_start": root.start,
                "period_end": root.end,
                "status": "counted",
                "expected_rows": 500001,
                "failure_count": 0,
                "started_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )

    def invoke(_: int) -> Any:
        return BackfillCoordinator(
            engine,
            FakeWorkflow({}),  # type: ignore[arg-type]
            FakeStorageGuard(),  # type: ignore[arg-type]
            verifier=FakeVerifier(),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )._subdivide(parent_id, root, 500001)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(invoke, range(2)))
    assert results[0] == results[1]
    with engine.connect() as connection:
        rows = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt).order_by(
                UsaSpendingTransactionIngestionAttempt.period_start
            )
        ).mappings().all()
    assert len(rows) == 3
    parent = next(row for row in rows if row["id"] == parent_id)
    assert parent["status"] == "subdivided"
    assert sum(row["parent_attempt_id"] == parent_id for row in rows) == 2


def _seed_completed_period(engine: Any) -> tuple[UUID, UUID, FiscalPeriod]:
    period = FiscalPeriod(2025, date(2024, 11, 1), date(2024, 11, 30))
    attempt_id = uuid4()
    checkpoint_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            UsaSpendingIngestionCheckpoint.__table__.insert(),
            {
                "id": checkpoint_id,
                "source": "usaspending",
                "fiscal_year": period.fiscal_year,
                "period_start": period.start,
                "period_end": period.end,
                "status": "complete",
                "archive_sha256": "a" * 64,
                "expected_rows": 1,
                "loaded_rows": 1,
                "started_at": NOW,
                "completed_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )
        connection.execute(
            UsaSpendingTransaction.__table__.insert(),
            {
                "stable_transaction_id": "tx-1",
                "ingestion_checkpoint_id": checkpoint_id,
                "generated_award_id": "award-1",
                "display_award_id": "piid-1",
                "award_type_code": "A",
                "action_date": date(2024, 11, 15),
                "fiscal_year": 2025,
                "federal_action_obligation": Decimal("12.34"),
                "recipient_name": "recipient",
                "recipient_uei": "ABC123DEF456",
                "awarding_agency_name": "agency",
                "awarding_subagency_name": "subagency",
                "transaction_description": "description",
                "source_updated_at": NOW,
                "fetched_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )
        connection.execute(
            UsaSpendingTransactionIngestionAttempt.__table__.insert(),
            {
                "id": attempt_id,
                "source": "usaspending",
                "fiscal_year": period.fiscal_year,
                "period_start": period.start,
                "period_end": period.end,
                "status": "completed",
                "expected_rows": 1,
                "export_rows": 1,
                "export_columns": 16,
                "status_url": "https://api.usaspending.gov/status/1",
                "file_url": "https://files.usaspending.gov/export.zip",
                "remote_file_name": "export.zip",
                "archive_sha256": "a" * 64,
                "archive_bytes": 123,
                "loaded_rows": 1,
                "signed_obligation_total": Decimal("12.34"),
                "checkpoint_id": checkpoint_id,
                "failure_count": 0,
                "started_at": NOW,
                "completed_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )
    return attempt_id, checkpoint_id, period


@pytest.mark.integration
def test_completed_verifier_accepts_exact_state_and_detects_corruption_without_mutation(
    backfill_postgres_engine: Any,
) -> None:
    engine = backfill_postgres_engine
    attempt_id, checkpoint_id, period = _seed_completed_period(engine)
    verifier = CompletedPeriodVerifier(engine)

    verified = verifier.verify(attempt_id, expected_period=period)
    assert verified.checkpoint_id == checkpoint_id
    assert verified.rows == 1
    assert verified.signed_obligation_total == Decimal("12.34")

    with engine.begin() as connection:
        connection.execute(
            update(UsaSpendingIngestionCheckpoint)
            .where(UsaSpendingIngestionCheckpoint.id == checkpoint_id)
            .values(loaded_rows=2)
        )
    with pytest.raises(BackfillError, match="completed_checkpoint_corrupt"):
        verifier.verify(attempt_id, expected_period=period)
    with engine.connect() as connection:
        assert connection.scalar(
            select(func.count()).select_from(UsaSpendingTransaction)
        ) == 1


@pytest.mark.integration
@pytest.mark.parametrize(
    "corruption",
    [
        "archive_linkage",
        "signed_total",
        "out_of_range_ownership",
        "wrong_fiscal_year",
        "wrong_checkpoint_ownership",
    ],
)
def test_completed_verifier_rejects_each_integrity_dimension(
    backfill_postgres_engine: Any,
    corruption: str,
) -> None:
    engine = backfill_postgres_engine
    attempt_id, checkpoint_id, period = _seed_completed_period(engine)
    with engine.begin() as connection:
        if corruption == "archive_linkage":
            connection.execute(
                update(UsaSpendingIngestionCheckpoint)
                .where(UsaSpendingIngestionCheckpoint.id == checkpoint_id)
                .values(archive_sha256="b" * 64)
            )
        elif corruption == "signed_total":
            connection.execute(
                update(UsaSpendingTransactionIngestionAttempt)
                .where(UsaSpendingTransactionIngestionAttempt.id == attempt_id)
                .values(signed_obligation_total=Decimal("99.99"))
            )
        elif corruption == "out_of_range_ownership":
            connection.execute(
                update(UsaSpendingTransaction)
                .where(UsaSpendingTransaction.stable_transaction_id == "tx-1")
                .values(action_date=date(2025, 1, 1))
            )
        elif corruption == "wrong_fiscal_year":
            connection.execute(
                update(UsaSpendingTransaction)
                .where(UsaSpendingTransaction.stable_transaction_id == "tx-1")
                .values(fiscal_year=2024)
            )
        else:
            other_checkpoint = uuid4()
            connection.execute(
                UsaSpendingIngestionCheckpoint.__table__.insert(),
                {
                    "id": other_checkpoint,
                    "source": "usaspending",
                    "fiscal_year": period.fiscal_year,
                    "period_start": date(2024, 12, 1),
                    "period_end": date(2024, 12, 31),
                    "status": "complete",
                    "archive_sha256": "c" * 64,
                    "expected_rows": 0,
                    "loaded_rows": 0,
                    "started_at": NOW,
                    "completed_at": NOW,
                    "created_at": NOW,
                    "updated_at": NOW,
                },
            )
            connection.execute(
                update(UsaSpendingTransaction)
                .where(UsaSpendingTransaction.stable_transaction_id == "tx-1")
                .values(ingestion_checkpoint_id=other_checkpoint)
            )

    with pytest.raises(
        BackfillError,
        match="completed_(checkpoint|transactions)_corrupt",
    ):
        CompletedPeriodVerifier(engine).verify(attempt_id, expected_period=period)


@pytest.mark.integration
def test_fresh_root_rejects_overlapping_checkpoint_before_workflow(
    backfill_postgres_engine: Any,
) -> None:
    engine = backfill_postgres_engine
    root = FiscalPeriod(2025, date(2024, 12, 1), date(2024, 12, 31))
    with engine.begin() as connection:
        connection.execute(
            UsaSpendingIngestionCheckpoint.__table__.insert(),
            {
                "id": uuid4(),
                "source": "usaspending",
                "fiscal_year": 2025,
                "period_start": date(2024, 12, 1),
                "period_end": date(2024, 12, 15),
                "status": "complete",
                "archive_sha256": "b" * 64,
                "expected_rows": 0,
                "loaded_rows": 0,
                "started_at": NOW,
                "completed_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )
    workflow = FakeWorkflow({(root.start, root.end): 1})
    subject = BackfillCoordinator(
        engine,
        workflow,  # type: ignore[arg-type]
        FakeStorageGuard(),  # type: ignore[arg-type]
        verifier=FakeVerifier(),  # type: ignore[arg-type]
    )

    with pytest.raises(BackfillError, match="unexpected_overlapping_state"):
        run(
            subject.run(
                [root],
                limits=BackfillLimits(max_periods=1),
                backup_identifier=VALID_BACKUP_IDENTIFIER,
                require_pilot=False,
            )
        )
    assert workflow.calls == []


@pytest.mark.integration
@pytest.mark.parametrize("state_kind", ["attempt", "transaction"])
def test_fresh_root_rejects_overlapping_attempt_or_transaction_state(
    backfill_postgres_engine: Any,
    state_kind: str,
) -> None:
    engine = backfill_postgres_engine
    root = FiscalPeriod(2025, date(2024, 12, 1), date(2024, 12, 31))
    with engine.begin() as connection:
        if state_kind == "attempt":
            connection.execute(
                UsaSpendingTransactionIngestionAttempt.__table__.insert(),
                {
                    "id": uuid4(),
                    "source": "usaspending",
                    "fiscal_year": 2025,
                    "period_start": date(2024, 12, 15),
                    "period_end": date(2024, 12, 20),
                    "status": "failed",
                    "last_error_code": "test_failure",
                    "failure_count": 1,
                    "started_at": NOW,
                    "completed_at": NOW,
                    "created_at": NOW,
                    "updated_at": NOW,
                },
            )
        else:
            checkpoint_id = uuid4()
            connection.execute(
                UsaSpendingIngestionCheckpoint.__table__.insert(),
                {
                    "id": checkpoint_id,
                    "source": "usaspending",
                    "fiscal_year": 2025,
                    "period_start": date(2024, 11, 1),
                    "period_end": date(2024, 11, 30),
                    "status": "complete",
                    "archive_sha256": "d" * 64,
                    "expected_rows": 1,
                    "loaded_rows": 1,
                    "started_at": NOW,
                    "completed_at": NOW,
                    "created_at": NOW,
                    "updated_at": NOW,
                },
            )
            connection.execute(
                UsaSpendingTransaction.__table__.insert(),
                {
                    "stable_transaction_id": "orphan-overlap",
                    "ingestion_checkpoint_id": checkpoint_id,
                    "generated_award_id": "award-overlap",
                    "display_award_id": "piid-overlap",
                    "award_type_code": "A",
                    "action_date": date(2024, 12, 15),
                        "fiscal_year": 2025,
                        "federal_action_obligation": Decimal("1.00"),
                        "recipient_name": "recipient",
                        "recipient_uei": "ABC123DEF456",
                        "awarding_agency_name": "agency",
                        "awarding_subagency_name": "subagency",
                        "transaction_description": "overlap",
                        "source_updated_at": NOW,
                    "fetched_at": NOW,
                    "created_at": NOW,
                    "updated_at": NOW,
                },
            )
    workflow = FakeWorkflow({(root.start, root.end): 1})
    subject = BackfillCoordinator(
        engine,
        workflow,  # type: ignore[arg-type]
        FakeStorageGuard(),  # type: ignore[arg-type]
        verifier=FakeVerifier(),  # type: ignore[arg-type]
    )

    with pytest.raises(BackfillError, match="unexpected_overlapping_state"):
        run(
            subject.run(
                [root],
                limits=BackfillLimits(max_periods=1),
                backup_identifier=VALID_BACKUP_IDENTIFIER,
                require_pilot=False,
            )
        )
    assert workflow.calls == []


@pytest.mark.integration
def test_completed_exact_attempt_with_extra_zero_row_checkpoint_is_not_skipped(
    backfill_postgres_engine: Any,
) -> None:
    engine = backfill_postgres_engine
    attempt_id, _checkpoint_id, period = _seed_completed_period(engine)
    with engine.begin() as connection:
        connection.execute(
            UsaSpendingIngestionCheckpoint.__table__.insert(),
            {
                "id": uuid4(),
                "source": "usaspending",
                "fiscal_year": period.fiscal_year,
                "period_start": date(2024, 11, 15),
                "period_end": date(2024, 11, 15),
                "status": "complete",
                "archive_sha256": "f" * 64,
                "expected_rows": 0,
                "loaded_rows": 0,
                "started_at": NOW,
                "completed_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )
    workflow = FakeWorkflow({(period.start, period.end): 1})
    subject = BackfillCoordinator(
        engine,
        workflow,  # type: ignore[arg-type]
        FakeStorageGuard(),  # type: ignore[arg-type]
        verifier=FakeVerifier(),  # type: ignore[arg-type]
    )

    with pytest.raises(BackfillError, match="unexpected_overlapping_state"):
        run(
            subject.run(
                [period],
                limits=BackfillLimits(max_periods=1),
                backup_identifier=VALID_BACKUP_IDENTIFIER,
                require_pilot=False,
            )
        )
    assert attempt_id is not None
    assert workflow.calls == []


@pytest.mark.integration
@pytest.mark.parametrize("conflict_kind", ["attempt", "checkpoint", "transaction"])
def test_incomplete_exact_attempt_rejects_each_additional_overlap(
    backfill_postgres_engine: Any,
    conflict_kind: str,
) -> None:
    engine = backfill_postgres_engine
    period = FiscalPeriod(2025, date(2024, 12, 1), date(2024, 12, 31))
    exact_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            UsaSpendingTransactionIngestionAttempt.__table__.insert(),
            {
                "id": exact_id,
                "source": "usaspending",
                "fiscal_year": period.fiscal_year,
                "period_start": period.start,
                "period_end": period.end,
                "status": "created",
                "failure_count": 0,
                "started_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )
        if conflict_kind == "attempt":
            connection.execute(
                UsaSpendingTransactionIngestionAttempt.__table__.insert(),
                {
                    "id": uuid4(),
                    "source": "usaspending",
                    "fiscal_year": period.fiscal_year,
                    "period_start": date(2024, 12, 15),
                    "period_end": date(2024, 12, 20),
                    "status": "failed",
                    "last_error_code": "test_failure",
                    "failure_count": 1,
                    "started_at": NOW,
                    "completed_at": NOW,
                    "created_at": NOW,
                    "updated_at": NOW,
                },
            )
        else:
            checkpoint_id = uuid4()
            checkpoint_period = (
                period
                if conflict_kind == "checkpoint"
                else FiscalPeriod(2025, date(2024, 11, 1), date(2024, 11, 30))
            )
            connection.execute(
                UsaSpendingIngestionCheckpoint.__table__.insert(),
                {
                    "id": checkpoint_id,
                    "source": "usaspending",
                    "fiscal_year": checkpoint_period.fiscal_year,
                    "period_start": checkpoint_period.start,
                    "period_end": checkpoint_period.end,
                    "status": "complete",
                    "archive_sha256": "1" * 64,
                    "expected_rows": 1 if conflict_kind == "transaction" else 0,
                    "loaded_rows": 1 if conflict_kind == "transaction" else 0,
                    "started_at": NOW,
                    "completed_at": NOW,
                    "created_at": NOW,
                    "updated_at": NOW,
                },
            )
            if conflict_kind == "transaction":
                connection.execute(
                    UsaSpendingTransaction.__table__.insert(),
                    {
                        "stable_transaction_id": "existing-attempt-overlap",
                        "ingestion_checkpoint_id": checkpoint_id,
                        "generated_award_id": "award-overlap",
                        "display_award_id": "piid-overlap",
                        "award_type_code": "A",
                        "action_date": date(2024, 12, 15),
                        "fiscal_year": period.fiscal_year,
                        "federal_action_obligation": Decimal("1.00"),
                        "recipient_name": "recipient",
                        "recipient_uei": "ABC123DEF456",
                        "awarding_agency_name": "agency",
                        "awarding_subagency_name": "subagency",
                        "transaction_description": "overlap",
                        "source_updated_at": NOW,
                        "fetched_at": NOW,
                        "created_at": NOW,
                        "updated_at": NOW,
                    },
                )
    workflow = FakeWorkflow({(period.start, period.end): 1})
    subject = BackfillCoordinator(
        engine,
        workflow,  # type: ignore[arg-type]
        FakeStorageGuard(),  # type: ignore[arg-type]
        verifier=FakeVerifier(),  # type: ignore[arg-type]
    )

    with pytest.raises(BackfillError, match="unexpected_overlapping_state"):
        run(
            subject.run(
                [period],
                limits=BackfillLimits(max_periods=1),
                backup_identifier=VALID_BACKUP_IDENTIFIER,
                require_pilot=False,
            )
        )
    assert workflow.calls == []
