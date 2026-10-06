from __future__ import annotations

import asyncio
import io
import json
import os
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator
from uuid import uuid4

import pytest

from app.usaspending import transaction_backfill, transaction_backfill_cli
from app.usaspending.transaction_backfill import BackfillError, BackfillResult
from app.usaspending.transaction_backfill_cli import (
    EXIT_COMPLETED,
    EXIT_INVALID_ARGUMENTS,
    EXIT_OPERATION_FAILED,
    main,
)


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


def forbidden_runtime(*_: Any, **__: Any) -> Any:
    raise AssertionError("dry-run must not initialize live runtime")


def test_default_dry_run_has_all_84_roots_and_no_live_io() -> None:
    output = io.StringIO()

    exit_code = main([], runtime_factory=forbidden_runtime, stdout=output)

    payload = json.loads(output.getvalue())
    assert exit_code == EXIT_COMPLETED
    assert payload["mode"] == "dry_run"
    assert payload["root_count"] == 84
    assert payload["coverage_start"] == "2019-10-01"
    assert payload["coverage_end"] == "2026-09-30"
    assert payload["leaves"] == "unresolved_count_dependent"
    assert payload["roots"][0]["period_start"] == "2019-10-01"
    assert payload["roots"][-1]["period_end"] == "2026-09-30"
    assert sum(
        bool(root["requires_live_integrity_check"]) for root in payload["roots"]
    ) == 1


def test_dry_run_manifest_is_0600_and_never_overwritten(tmp_path: Path) -> None:
    path = tmp_path / "dry-run.json"
    assert (
        main(
            ["--manifest", str(path)],
            runtime_factory=forbidden_runtime,
            stdout=io.StringIO(),
        )
        == EXIT_COMPLETED
    )
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    before = path.read_bytes()
    assert (
        main(
            ["--manifest", str(path)],
            runtime_factory=forbidden_runtime,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )
        != EXIT_COMPLETED
    )
    assert path.read_bytes() == before


def test_live_requires_exact_confirmation_and_finite_bound(capsys: Any) -> None:
    secret = "wrong-secret-confirmation"
    with pytest.raises(SystemExit) as failure:
        main(
            [
                "--execute-live",
                "--confirm",
                secret,
                "--manifest",
                "result.json",
                "--backup-identifier",
                VALID_BACKUP_IDENTIFIER,
                "--database-volume-capacity-bytes",
                "1000000",
                "--database-volume-free-bytes",
                "900000",
                "--database-reserve-bytes",
                "1000",
                "--local-reserve-bytes",
                "1000",
                "--storage-factor",
                "2",
                "--temporary-path",
                "/tmp",
            ],
            runtime_factory=forbidden_runtime,
        )
    assert failure.value.code == EXIT_INVALID_ARGUMENTS
    assert secret not in capsys.readouterr().err


def test_valid_bounded_live_dispatch_writes_safe_final_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "result.json"
    output = io.StringIO()

    class Runtime:
        async def run(self, *_: Any, **__: Any) -> BackfillResult:
            return BackfillResult(uuid4(), "completed", 1, 1, 0, 0, 123, ())

    @contextmanager
    def runtime_factory(*_: Any, **__: Any) -> Iterator[Runtime]:
        yield Runtime()

    exit_code = main(
        [
            "--root-month",
            "2019-10",
            "--execute-live",
            "--confirm",
            "RUN-2019-10-01-THROUGH-2019-10-31",
            "--max-leaf-submissions",
            "1",
            "--manifest",
            str(manifest),
            "--backup-identifier",
            VALID_BACKUP_IDENTIFIER.upper(),
            "--database-volume-capacity-bytes",
            "1000000",
            "--database-volume-free-bytes",
            "900000",
            "--database-reserve-bytes",
            "1000",
            "--local-reserve-bytes",
            "1000",
            "--storage-factor",
            "2",
            "--temporary-path",
            str(tmp_path),
        ],
        runtime_factory=runtime_factory,
        stdout=output,
    )

    assert exit_code == EXIT_COMPLETED
    payload = json.loads(manifest.read_text())
    assert payload["status"] == "completed"
    assert payload["backup_identifier"] == VALID_BACKUP_IDENTIFIER
    assert oct(manifest.stat().st_mode & 0o777) == "0o600"
    assert "http" not in manifest.read_text().lower()


def test_exact_existing_leaf_selection_is_offline_and_range_specific() -> None:
    output = io.StringIO()
    exit_code = main(
        ["--period-start", "2024-11-01", "--period-end", "2024-11-15"],
        runtime_factory=forbidden_runtime,
        stdout=output,
    )

    payload = json.loads(output.getvalue())
    assert exit_code == EXIT_COMPLETED
    assert payload["root_count"] == 1
    assert payload["coverage_start"] == "2024-11-01"
    assert payload["coverage_end"] == "2024-11-15"


def test_leaf_selection_rejects_missing_or_cross_month_bounds(capsys: Any) -> None:
    for argv in (
        ["--period-start", "2024-11-01"],
        ["--period-start", "2024-11-15", "--period-end", "2024-12-01"],
    ):
        with pytest.raises(SystemExit) as failure:
            main(argv, runtime_factory=forbidden_runtime)
        assert failure.value.code == EXIT_INVALID_ARGUMENTS
    assert "2024-12-01" not in capsys.readouterr().err


def test_live_failure_retains_sanitized_progress_in_exclusive_manifest(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "stopped.json"

    class FailingRuntime:
        async def run(self, *_: Any, **__: Any) -> BackfillResult:
            raise RuntimeError("password=secret")

    @contextmanager
    def runtime_factory(
        _args: Any,
        progress: Any,
    ) -> Iterator[FailingRuntime]:
        progress(
            {
                "event": "counted",
                "fiscal_year": 2020,
                "period_start": "2019-10-01",
                "period_end": "2019-10-31",
                "expected_rows": 1,
            }
        )
        yield FailingRuntime()

    stderr = io.StringIO()
    exit_code = main(
        [
            "--root-month",
            "2019-10",
            "--execute-live",
            "--confirm",
            "RUN-2019-10-01-THROUGH-2019-10-31",
            "--max-periods",
            "1",
            "--manifest",
            str(manifest),
            "--backup-identifier",
            VALID_BACKUP_IDENTIFIER,
            "--database-volume-capacity-bytes",
            "1000000",
            "--database-volume-free-bytes",
            "900000",
            "--database-reserve-bytes",
            "1000",
            "--local-reserve-bytes",
            "1000",
            "--storage-factor",
            "2",
            "--temporary-path",
            str(tmp_path),
        ],
        runtime_factory=runtime_factory,
        stdout=io.StringIO(),
        stderr=stderr,
    )

    payload = json.loads(manifest.read_text())
    assert exit_code == EXIT_OPERATION_FAILED
    assert payload["status"] == "stopped"
    assert payload["error_code"] == "backfill_operation_failed"
    assert payload["records"][0]["expected_rows"] == 1
    assert "secret" not in manifest.read_text()
    assert json.loads(stderr.getvalue()) == {
        "error_code": "backfill_operation_failed"
    }


@pytest.mark.parametrize(
    "unsafe_identifier",
    [
        "password=secret",
        "https://user:pass@example.invalid/backup",
        "/private/backups/id",
        "backup.json",
        "backup-1",
        "backup\ncontrol",
        "x" * 129,
    ],
)
def test_live_rejects_unsafe_backup_identifier_without_persisting_it(
    tmp_path: Path,
    unsafe_identifier: str,
    capsys: Any,
) -> None:
    manifest = tmp_path / "must-not-exist.json"
    with pytest.raises(SystemExit) as failure:
        main(
            [
                "--root-month",
                "2019-10",
                "--execute-live",
                "--confirm",
                "RUN-2019-10-01-THROUGH-2019-10-31",
                "--max-periods",
                "1",
                "--manifest",
                str(manifest),
                "--backup-identifier",
                unsafe_identifier,
                "--database-volume-capacity-bytes",
                "1000000",
                "--database-volume-free-bytes",
                "900000",
                "--database-reserve-bytes",
                "1000",
                "--local-reserve-bytes",
                "1000",
                "--storage-factor",
                "2",
                "--temporary-path",
                str(tmp_path),
            ],
            runtime_factory=forbidden_runtime,
        )
    assert failure.value.code == EXIT_INVALID_ARGUMENTS
    assert not manifest.exists()
    assert unsafe_identifier not in capsys.readouterr().err


def test_stopped_manifest_revalidates_backup_identifier_at_persistence_boundary(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "stopped.json"

    class Runtime:
        async def run(self, *_: Any, **__: Any) -> BackfillResult:
            raise RuntimeError("opaque")

    @contextmanager
    def runtime_factory(args: Any, _progress: Any) -> Iterator[Runtime]:
        args.backup_identifier = "password=secret"
        yield Runtime()

    exit_code = main(
        [
            "--root-month",
            "2019-10",
            "--execute-live",
            "--confirm",
            "RUN-2019-10-01-THROUGH-2019-10-31",
            "--max-periods",
            "1",
            "--manifest",
            str(manifest),
            "--backup-identifier",
            VALID_BACKUP_IDENTIFIER.upper(),
            "--database-volume-capacity-bytes",
            "1000000",
            "--database-volume-free-bytes",
            "900000",
            "--database-reserve-bytes",
            "1000",
            "--local-reserve-bytes",
            "1000",
            "--storage-factor",
            "2",
            "--temporary-path",
            str(tmp_path),
        ],
        runtime_factory=runtime_factory,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    payload = json.loads(manifest.read_text())
    assert exit_code == EXIT_OPERATION_FAILED
    assert payload["backup_identifier"] is None
    assert "secret" not in manifest.read_text()


@pytest.mark.parametrize(
    "unsafe_identifier",
    [
        "AKIAABCDEFGHIJKLMNOP",
        f"ghp_{'A' * 36}",
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signaturevalue",
        "cGFzc3dvcmQ",
        "sk-proj-abcdefghijklmnopqrstuvwxyz123456",
        _synthetic_slack_token_shape(),
        "736563726574",
        "backup-20260230t202143z-9f21643cf5b6c8df",
        "backup-20261004t202143z-9f21643cf5b6c8d",
    ],
)
def test_live_rejects_credential_shaped_identifiers_without_disclosure(
    tmp_path: Path,
    unsafe_identifier: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    manifest = tmp_path / "must-not-exist.json"
    stdout = io.StringIO()
    stderr = io.StringIO()

    with pytest.raises(SystemExit) as failure:
        main(
            [
                "--root-month",
                "2019-10",
                "--execute-live",
                "--confirm",
                "RUN-2019-10-01-THROUGH-2019-10-31",
                "--max-periods",
                "1",
                "--manifest",
                str(manifest),
                "--backup-identifier",
                unsafe_identifier,
                "--database-volume-capacity-bytes",
                "1000000",
                "--database-volume-free-bytes",
                "900000",
                "--database-reserve-bytes",
                "1000",
                "--local-reserve-bytes",
                "1000",
                "--storage-factor",
                "2",
                "--temporary-path",
                str(tmp_path),
            ],
            runtime_factory=forbidden_runtime,
            stdout=stdout,
            stderr=stderr,
        )

    assert failure.value.code == EXIT_INVALID_ARGUMENTS
    assert not manifest.exists()
    observed = stdout.getvalue() + stderr.getvalue() + caplog.text
    assert unsafe_identifier not in observed
    assert unsafe_identifier not in str(failure.value)


@pytest.mark.parametrize("failure_mode", ["preexisting", "replaced", "unwritable"])
def test_live_manifest_reservation_failure_prevents_runtime_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    manifest = tmp_path / "result.json"
    runtime_entered = False
    real_open = os.open
    real_fstat = os.fstat

    @contextmanager
    def runtime_factory(*_: Any, **__: Any) -> Iterator[Any]:
        nonlocal runtime_entered
        runtime_entered = True
        raise AssertionError("runtime must not be entered")
        yield

    if failure_mode == "preexisting":
        manifest.write_text("existing")
    elif failure_mode == "unwritable":
        def unwritable_open(
            path: os.PathLike[str] | str,
            flags: int,
            mode: int = 0o777,
        ) -> int:
            if Path(path) == manifest and flags & os.O_EXCL:
                raise PermissionError("sensitive destination detail")
            return real_open(path, flags, mode)

        monkeypatch.setattr(transaction_backfill.os, "open", unwritable_open)
    else:
        replaced = False

        def replacing_fstat(descriptor: int) -> os.stat_result:
            nonlocal replaced
            current = real_fstat(descriptor)
            if not replaced:
                replaced = True
                manifest.rename(tmp_path / "reserved-original")
                manifest.write_text("replacement")
            return current

        monkeypatch.setattr(transaction_backfill.os, "fstat", replacing_fstat)

    stderr = io.StringIO()
    exit_code = main(
        [
            "--root-month",
            "2019-10",
            "--execute-live",
            "--confirm",
            "RUN-2019-10-01-THROUGH-2019-10-31",
            "--max-periods",
            "1",
            "--manifest",
            str(manifest),
            "--backup-identifier",
            VALID_BACKUP_IDENTIFIER,
            "--database-volume-capacity-bytes",
            "1000000",
            "--database-volume-free-bytes",
            "900000",
            "--database-reserve-bytes",
            "1000",
            "--local-reserve-bytes",
            "1000",
            "--storage-factor",
            "2",
            "--temporary-path",
            str(tmp_path),
        ],
        runtime_factory=runtime_factory,
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert exit_code == EXIT_OPERATION_FAILED
    assert runtime_entered is False
    assert json.loads(stderr.getvalue())["error_code"] in {
        "manifest_exists",
        "manifest_write_failed",
    }
    if failure_mode == "replaced":
        assert manifest.read_text() == "replacement"


def test_stopped_manifest_write_failure_is_reported_and_cleaned_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = tmp_path / "stopped.json"

    class Runtime:
        async def run(self, *_: Any, **__: Any) -> BackfillResult:
            raise RuntimeError("opaque")

    @contextmanager
    def runtime_factory(*_: Any, **__: Any) -> Iterator[Runtime]:
        yield Runtime()

    monkeypatch.setattr(transaction_backfill.os, "write", lambda *_: 0)
    stderr = io.StringIO()
    exit_code = main(
        [
            "--root-month",
            "2019-10",
            "--execute-live",
            "--confirm",
            "RUN-2019-10-01-THROUGH-2019-10-31",
            "--max-periods",
            "1",
            "--manifest",
            str(manifest),
            "--backup-identifier",
            VALID_BACKUP_IDENTIFIER,
            "--database-volume-capacity-bytes",
            "1000000",
            "--database-volume-free-bytes",
            "900000",
            "--database-reserve-bytes",
            "1000",
            "--local-reserve-bytes",
            "1000",
            "--storage-factor",
            "2",
            "--temporary-path",
            str(tmp_path),
        ],
        runtime_factory=runtime_factory,
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert exit_code == EXIT_OPERATION_FAILED
    assert json.loads(stderr.getvalue()) == {"error_code": "manifest_write_failed"}
    assert not manifest.exists()


def test_cancellation_writes_interrupted_manifest_and_closes_runtime(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "interrupted.json"
    closed = False

    class Runtime:
        async def run(self, *_: Any, **__: Any) -> BackfillResult:
            raise asyncio.CancelledError

    @contextmanager
    def runtime_factory(*_: Any, **__: Any) -> Iterator[Runtime]:
        nonlocal closed
        try:
            yield Runtime()
        finally:
            closed = True

    exit_code = main(
        [
            "--root-month",
            "2019-10",
            "--execute-live",
            "--confirm",
            "RUN-2019-10-01-THROUGH-2019-10-31",
            "--max-periods",
            "1",
            "--manifest",
            str(manifest),
            "--backup-identifier",
            VALID_BACKUP_IDENTIFIER,
            "--database-volume-capacity-bytes",
            "1000000",
            "--database-volume-free-bytes",
            "900000",
            "--database-reserve-bytes",
            "1000",
            "--local-reserve-bytes",
            "1000",
            "--storage-factor",
            "2",
            "--temporary-path",
            str(tmp_path),
        ],
        runtime_factory=runtime_factory,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert exit_code == EXIT_OPERATION_FAILED
    assert closed
    assert json.loads(manifest.read_text())["status"] == "interrupted"


def test_live_runtime_threads_one_configured_filesystem_through_all_archives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    captured: dict[str, Any] = {}

    class Database:
        def __init__(self, _settings: object) -> None:
            self.engine = engine

        def dispose(self) -> None:
            captured["disposed"] = True

    class Loader:
        def __init__(
            self,
            _engine: object,
            *,
            archive_copy_directory: Path,
            archive_copy_directory_identity: object,
        ) -> None:
            captured["loader_directory"] = archive_copy_directory
            captured["loader_identity"] = archive_copy_directory_identity

    class Workflow:
        def __init__(
            self,
            _engine: object,
            _client: object,
            _loader: object,
            *,
            temporary_directory_factory: Any,
        ) -> None:
            with temporary_directory_factory() as path:
                captured["workflow_directory_parent"] = path.parent

    class Guard:
        def __init__(self, _engine: object, local_path: Path, *_: object) -> None:
            captured["guard_directory"] = local_path

    @contextmanager
    def lease(_engine: object) -> Iterator[None]:
        yield

    monkeypatch.setattr(transaction_backfill_cli, "load_settings", object)
    monkeypatch.setattr(transaction_backfill_cli, "Database", Database)
    monkeypatch.setattr(transaction_backfill_cli, "global_ingestion_lease", lease)
    monkeypatch.setattr(
        transaction_backfill_cli, "UsaSpendingClient", lambda _settings: object()
    )
    monkeypatch.setattr(transaction_backfill_cli, "TransactionIngestionLoader", Loader)
    monkeypatch.setattr(transaction_backfill_cli, "TransactionIngestionWorkflow", Workflow)
    monkeypatch.setattr(transaction_backfill_cli, "StorageGuard", Guard)
    monkeypatch.setattr(
        transaction_backfill_cli,
        "BackfillCoordinator",
        lambda *_args, **_kwargs: object(),
    )
    args = SimpleNamespace(
        temporary_path=str(tmp_path),
        database_volume_capacity_bytes=10,
        database_volume_free_bytes=9,
        database_reserve_bytes=1,
        local_reserve_bytes=1,
        storage_factor_decimal=Decimal("1"),
    )

    with transaction_backfill_cli._live_runtime(args, lambda _record: None):
        pass

    assert captured == {
        "loader_directory": tmp_path,
        "loader_identity": transaction_backfill_cli.probe_private_directory(tmp_path),
        "workflow_directory_parent": tmp_path,
        "guard_directory": tmp_path,
        "disposed": True,
    }


@pytest.mark.parametrize("path_kind", ["missing", "file", "symlink"])
def test_live_runtime_rejects_invalid_temporary_path_before_client_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    path_kind: str,
) -> None:
    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    client_created = False

    class Database:
        def __init__(self, _settings: object) -> None:
            self.engine = engine

        def dispose(self) -> None:
            pass

    @contextmanager
    def lease(_engine: object) -> Iterator[None]:
        yield

    configured = tmp_path / "configured"
    if path_kind == "file":
        configured.write_text("not a directory")
    elif path_kind == "symlink":
        target = tmp_path / "target"
        target.mkdir()
        configured.symlink_to(target, target_is_directory=True)

    def client(_settings: object) -> object:
        nonlocal client_created
        client_created = True
        return object()

    monkeypatch.setattr(transaction_backfill_cli, "load_settings", object)
    monkeypatch.setattr(transaction_backfill_cli, "Database", Database)
    monkeypatch.setattr(transaction_backfill_cli, "global_ingestion_lease", lease)
    monkeypatch.setattr(transaction_backfill_cli, "UsaSpendingClient", client)
    args = SimpleNamespace(temporary_path=str(configured))

    with pytest.raises(BackfillError, match="invalid_temporary_directory"):
        with transaction_backfill_cli._live_runtime(args, lambda _record: None):
            pytest.fail("invalid temporary path must not yield a live runtime")
    assert client_created is False
