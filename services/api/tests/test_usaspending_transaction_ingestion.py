from __future__ import annotations

import copy
import csv
import hashlib
import io
import os
import stat
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from app.db.models import UsaSpendingIngestionCheckpoint, UsaSpendingTransaction
from app.usaspending import transaction_ingestion as transaction_ingestion_module
from app.usaspending.transaction_export import (
    EXPECTED_TRANSACTION_HEADERS,
    TransactionExportError,
)
from app.usaspending.transaction_ingestion import (
    BulkJobMetadata,
    TransactionIngestionError,
    TransactionIngestionLoader,
    _advisory_lock_key,
    _create_stage_table_statement,
    _delete_disappeared_statement,
    _stage_table,
    _stage_ownership_conflicts_statement,
    _upsert_from_stage_statement,
)


PERIOD_START = date(2024, 10, 1)
PERIOD_END = date(2024, 10, 31)
FISCAL_YEAR = 2025
NOW = datetime(2024, 11, 2, 12, 0, tzinfo=timezone.utc)
BULK_JOB = BulkJobMetadata(
    status_url="https://api.usaspending.gov/api/v2/bulk_download/status/example/",
    file_name="transactions.zip",
)
BASE_ROW = {
    "contract_transaction_unique_key": "transaction-1",
    "contract_award_unique_key": "CONT_AWD_1",
    "award_id_piid": "PIID-1",
    "award_type_code": "A",
    "action_date": "2024-10-15",
    "federal_action_obligation": "100.10",
    "recipient_name": "Example Recipient",
    "recipient_uei": "ABC123DEF456",
    "recipient_parent_name": "Example Parent",
    "recipient_parent_uei": "PARENT123456",
    "awarding_agency_name": "Example Agency",
    "awarding_sub_agency_name": "Example Subagency",
    "naics_code": "541512",
    "product_or_service_code": "D310",
    "transaction_description": "Example transaction",
    "last_modified_date": "2024-11-01T12:34:56Z",
}


def transaction_row(**overrides: str) -> dict[str, str]:
    return {**BASE_ROW, **overrides}


def write_archive(tmp_path: Path, rows: list[dict[str, str]], *, name: str) -> Path:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(EXPECTED_TRANSACTION_HEADERS)
    for row in rows:
        writer.writerow([row.get(header, "") for header in EXPECTED_TRANSACTION_HEADERS])
    archive_path = tmp_path / name
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("transactions.csv", stream.getvalue().encode("utf-8"))
    return archive_path


class FakeResult:
    def __init__(
        self,
        *,
        mapping: dict[str, Any] | None = None,
        row: tuple[Any, ...] | None = None,
        rowcount: int = -1,
    ) -> None:
        self._mapping = mapping
        self._row = row
        self.rowcount = rowcount

    def mappings(self) -> FakeResult:
        return self

    def one_or_none(self) -> dict[str, Any] | None:
        return copy.deepcopy(self._mapping)

    def one(self) -> tuple[Any, ...]:
        assert self._row is not None
        return self._row


@dataclass
class FakeDatabaseState:
    checkpoints: dict[tuple[date, date], dict[str, Any]] = field(default_factory=dict)
    transactions: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def checkpoint(self) -> dict[str, Any] | None:
        return self.checkpoints.get((PERIOD_START, PERIOD_END))


class FakeConnection:
    def __init__(self, state: FakeDatabaseState) -> None:
        self.state = state
        self.stage: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, Any]] = []
        self.active_period = (PERIOD_START, PERIOD_END)
        self.stage_total_override: Decimal | None = None
        self.target_total_override: Decimal | None = None
        self.lock_keys: list[int] = []

    def execute(self, statement: Any, parameters: Any = None) -> FakeResult:
        compiled = statement.compile(dialect=postgresql.dialect())
        sql = str(compiled)
        self.calls.append((sql, copy.deepcopy(parameters)))

        if "pg_advisory_xact_lock" in sql:
            self.lock_keys.append(parameters["lock_key"])
            return FakeResult()
        if sql.lstrip().startswith("SELECT") and "FROM usaspending_ingestion_checkpoints" in sql:
            dates = [value for value in compiled.params.values() if isinstance(value, date)]
            if len(dates) >= 2:
                self.active_period = (min(dates), max(dates))
            return FakeResult(mapping=self.state.checkpoints.get(self.active_period))
        if sql.lstrip().startswith("CREATE TEMPORARY TABLE"):
            self.stage = {}
            return FakeResult()
        if sql.startswith("INSERT INTO usaspending_transaction_stage"):
            assert isinstance(parameters, list)
            for values in parameters:
                self.stage[values["stable_transaction_id"]] = copy.deepcopy(values)
            return FakeResult()
        if sql.startswith("SELECT") and "FROM usaspending_transaction_stage" in sql:
            count, total = self._totals(self.stage.values())
            return FakeResult(
                row=(count, count, self.stage_total_override or total)
            )
        if sql.startswith("SELECT") and "JOIN usaspending_transaction_stage" in sql:
            start, end = self.active_period
            conflicts = sum(
                stable_id in self.state.transactions
                and (
                    not start
                    <= self.state.transactions[stable_id]["action_date"]
                    <= end
                    or self.state.transactions[stable_id]["fiscal_year"]
                    != values["fiscal_year"]
                )
                for stable_id, values in self.stage.items()
            )
            return FakeResult(row=(conflicts,))
        if sql.startswith("INSERT INTO usaspending_ingestion_checkpoints"):
            assert isinstance(parameters, dict)
            period = (parameters["period_start"], parameters["period_end"])
            self.state.checkpoints[period] = copy.deepcopy(parameters)
            return FakeResult()
        if sql.startswith("INSERT INTO usaspending_transactions"):
            assert self.state.checkpoints.get(self.active_period) is not None
            affected_rows = 0
            start, end = self.active_period
            for stable_id, values in self.stage.items():
                prior = self.state.transactions.get(stable_id)
                if prior is not None and (
                    not start <= prior["action_date"] <= end
                    or prior["fiscal_year"] != values["fiscal_year"]
                ):
                    continue
                replacement = copy.deepcopy(values)
                if prior is not None:
                    replacement["created_at"] = prior["created_at"]
                self.state.transactions[stable_id] = replacement
                affected_rows += 1
            return FakeResult(rowcount=affected_rows)
        if sql.startswith("DELETE FROM usaspending_transactions"):
            start, end = self.active_period
            for stable_id, values in list(self.state.transactions.items()):
                if start <= values["action_date"] <= end and stable_id not in self.stage:
                    del self.state.transactions[stable_id]
            return FakeResult()
        if sql.startswith("SELECT") and "FROM usaspending_transactions" in sql:
            start, end = self.active_period
            rows = [
                values
                for values in self.state.transactions.values()
                if start <= values["action_date"] <= end
            ]
            count, total = self._totals(rows)
            checkpoint = self.state.checkpoints.get(self.active_period)
            checkpoint_id = checkpoint["id"] if checkpoint else None
            other = sum(
                row["ingestion_checkpoint_id"] != checkpoint_id for row in rows
            )
            return FakeResult(row=(count, count, self.target_total_override or total, other))
        if sql.startswith("UPDATE usaspending_ingestion_checkpoints"):
            assert isinstance(parameters, dict)
            matching = [
                checkpoint
                for checkpoint in self.state.checkpoints.values()
                if checkpoint["id"] in compiled.params.values()
            ]
            assert len(matching) == 1
            matching[0].update(copy.deepcopy(parameters))
            return FakeResult()
        raise AssertionError(f"unhandled SQL: {sql}")

    @staticmethod
    def _totals(rows: Any) -> tuple[int, Decimal]:
        materialized = list(rows)
        return len(materialized), sum(
            (row["federal_action_obligation"] for row in materialized),
            Decimal("0.00"),
        )


class FakeEngine:
    dialect = SimpleNamespace(name="postgresql")

    def __init__(self) -> None:
        self.state = FakeDatabaseState()
        self.connection = FakeConnection(self.state)
        self.commits = 0
        self.rollbacks = 0

    @contextmanager
    def begin(self) -> Iterator[FakeConnection]:
        snapshot = copy.deepcopy(self.state)
        self.connection.stage = {}
        try:
            yield self.connection
        except BaseException:
            self.state.checkpoints = snapshot.checkpoints
            self.state.transactions = snapshot.transactions
            self.rollbacks += 1
            raise
        else:
            self.commits += 1
        finally:
            self.connection.stage = {}


def loader(engine: FakeEngine, *, batch_size: int = 2) -> TransactionIngestionLoader:
    return TransactionIngestionLoader(  # type: ignore[arg-type]
        engine,
        batch_size=batch_size,
        clock=lambda: NOW,
    )


def load_archive(
    subject: TransactionIngestionLoader,
    archive_path: Path,
    *,
    expected_count: int,
) -> Any:
    return subject.load(
        archive_path=archive_path,
        fiscal_year=FISCAL_YEAR,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        expected_count=expected_count,
        bulk_job=BULK_JOB,
    )


def test_first_load_maps_every_field_and_preserves_exact_signed_amounts(tmp_path: Path) -> None:
    rows = [
        transaction_row(
            contract_transaction_unique_key="positive",
            federal_action_obligation="100.10",
        ),
        transaction_row(
            contract_transaction_unique_key="zero",
            federal_action_obligation="0.00",
            recipient_parent_name="",
            recipient_parent_uei="",
            naics_code="",
            product_or_service_code="",
        ),
        transaction_row(
            contract_transaction_unique_key="negative",
            federal_action_obligation="-25.00",
        ),
    ]
    archive = write_archive(tmp_path, rows, name="first.zip")
    engine = FakeEngine()

    result = load_archive(loader(engine), archive, expected_count=3)

    assert result.loaded_rows == 3
    assert result.signed_obligation_total == Decimal("75.10")
    assert result.no_op is False
    assert engine.commits == 1
    assert engine.rollbacks == 0
    assert engine.state.checkpoint is not None
    assert engine.state.checkpoint["status"] == "complete"
    assert engine.state.checkpoint["expected_rows"] == 3
    assert engine.state.checkpoint["loaded_rows"] == 3
    mapped = engine.state.transactions["positive"]
    assert mapped == {
        "stable_transaction_id": "positive",
        "ingestion_checkpoint_id": result.checkpoint_id,
        "generated_award_id": "CONT_AWD_1",
        "display_award_id": "PIID-1",
        "award_type_code": "A",
        "action_date": date(2024, 10, 15),
        "fiscal_year": 2025,
        "federal_action_obligation": Decimal("100.10"),
        "recipient_name": "Example Recipient",
        "recipient_uei": "ABC123DEF456",
        "recipient_parent_name": "Example Parent",
        "recipient_parent_uei": "PARENT123456",
        "awarding_agency_name": "Example Agency",
        "awarding_subagency_name": "Example Subagency",
        "naics_code": "541512",
        "psc_code": "D310",
        "transaction_description": "Example transaction",
        "source_updated_at": datetime(2024, 11, 1, 12, 34, 56, tzinfo=timezone.utc),
        "fetched_at": NOW,
        "created_at": NOW,
        "updated_at": NOW,
    }
    assert engine.state.transactions["zero"]["federal_action_obligation"] == Decimal("0.00")
    assert engine.state.transactions["negative"]["federal_action_obligation"] == Decimal("-25.00")
    all_sql = "\n".join(sql for sql, _ in engine.connection.calls).lower()
    assert " awards" not in all_sql
    assert "ingestion_runs" not in all_sql


@pytest.mark.parametrize("reported_rowcount", [-1, 0])
def test_non_authoritative_upsert_rowcount_allows_valid_first_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reported_rowcount: int,
) -> None:
    archive = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="valid-id")],
        name=f"rowcount-{reported_rowcount}.zip",
    )
    engine = FakeEngine()
    real_execute = engine.connection.execute

    def execute_with_non_authoritative_rowcount(
        statement: Any,
        parameters: Any = None,
    ) -> FakeResult:
        result = real_execute(statement, parameters)
        sql = str(statement.compile(dialect=postgresql.dialect()))
        if sql.startswith("INSERT INTO usaspending_transactions"):
            result.rowcount = reported_rowcount
        return result

    monkeypatch.setattr(engine.connection, "execute", execute_with_non_authoritative_rowcount)

    result = load_archive(loader(engine), archive, expected_count=1)

    assert result.loaded_rows == 1
    assert set(engine.state.transactions) == {"valid-id"}
    assert engine.commits == 1
    assert engine.rollbacks == 0


@pytest.mark.parametrize(
    ("field", "length", "error"),
    [
        ("contract_transaction_unique_key", 513, "stable_transaction_id_too_long"),
        ("contract_award_unique_key", 513, "generated_award_id_too_long"),
        ("award_id_piid", 256, "display_award_id_too_long"),
        ("recipient_name", 513, "recipient_name_too_long"),
        ("recipient_parent_name", 513, "recipient_parent_name_too_long"),
        ("awarding_agency_name", 256, "awarding_agency_name_too_long"),
        ("awarding_sub_agency_name", 256, "awarding_subagency_name_too_long"),
        ("naics_code", 7, "naics_code_too_long"),
        ("product_or_service_code", 5, "psc_code_too_long"),
    ],
)
def test_overwidth_transaction_values_fail_before_persistent_mutation(
    tmp_path: Path,
    field: str,
    length: int,
    error: str,
) -> None:
    archive = write_archive(
        tmp_path,
        [transaction_row(**{field: "X" * length})],
        name=f"{field}.zip",
    )
    engine = FakeEngine()

    with pytest.raises(TransactionIngestionError, match=error):
        load_archive(loader(engine), archive, expected_count=1)

    assert engine.state.checkpoint is None
    assert engine.state.transactions == {}
    assert engine.rollbacks == 1
    assert not any(
        sql.startswith("INSERT INTO usaspending_transactions")
        for sql, _ in engine.connection.calls
    )


@pytest.mark.parametrize(
    ("bulk_job", "error"),
    [
        (BulkJobMetadata(status_url="x" * 2049, file_name="a.zip"), "status_url"),
        (BulkJobMetadata(status_url="https://api.usaspending.gov/status", file_name="x" * 513), "file_name"),
    ],
)
def test_overwidth_checkpoint_metadata_fails_before_opening_transaction(
    tmp_path: Path,
    bulk_job: BulkJobMetadata,
    error: str,
) -> None:
    archive = write_archive(tmp_path, [transaction_row()], name="metadata.zip")
    engine = FakeEngine()
    subject = loader(engine)

    with pytest.raises(TransactionIngestionError, match=error):
        subject.load(
            archive_path=archive,
            fiscal_year=FISCAL_YEAR,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            expected_count=1,
            bulk_job=bulk_job,
        )

    assert engine.commits == engine.rollbacks == 0


@pytest.mark.parametrize(
    "damage",
    [
        "altered_fields",
        "missing_row",
        "substituted_id",
        "incorrect_total",
        "incorrect_checkpoint",
    ],
)
def test_same_hash_rerun_revalidates_and_repairs_target(
    tmp_path: Path,
    damage: str,
) -> None:
    archive = write_archive(tmp_path, [transaction_row()], name="same.zip")
    engine = FakeEngine()
    subject = loader(engine)
    first = load_archive(subject, archive, expected_count=1)
    expected = copy.deepcopy(engine.state.transactions["transaction-1"])
    if damage == "altered_fields":
        engine.state.transactions["transaction-1"]["recipient_name"] = "Corrupted"
    elif damage == "missing_row":
        del engine.state.transactions["transaction-1"]
    elif damage == "substituted_id":
        substitute = engine.state.transactions.pop("transaction-1")
        substitute["stable_transaction_id"] = "substitute"
        engine.state.transactions["substitute"] = substitute
    elif damage == "incorrect_total":
        engine.state.transactions["transaction-1"]["federal_action_obligation"] = Decimal(
            "999.99"
        )
    else:
        engine.state.transactions["transaction-1"]["ingestion_checkpoint_id"] = uuid4()
    prior_calls = len(engine.connection.calls)

    second = load_archive(subject, archive, expected_count=1)

    assert second.no_op is False
    assert second.checkpoint_id == first.checkpoint_id
    assert engine.state.transactions == {"transaction-1": expected}
    repeated_sql = [sql for sql, _ in engine.connection.calls[prior_calls:]]
    assert any(sql.lstrip().startswith("CREATE TEMPORARY TABLE") for sql in repeated_sql)
    assert any(sql.startswith("INSERT INTO usaspending_transactions") for sql in repeated_sql)


def test_cross_period_stable_id_is_rejected_without_changing_either_snapshot(
    tmp_path: Path,
) -> None:
    september_start = date(2024, 9, 1)
    september_end = date(2024, 9, 30)
    september = write_archive(
        tmp_path,
        [
            transaction_row(
                contract_transaction_unique_key="moving-id",
                action_date="2024-09-15",
            )
        ],
        name="september.zip",
    )
    october = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="october-id")],
        name="october.zip",
    )
    corrected = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="moving-id")],
        name="moving.zip",
    )
    engine = FakeEngine()
    subject = loader(engine)
    subject.load(
        archive_path=september,
        fiscal_year=2024,
        period_start=september_start,
        period_end=september_end,
        expected_count=1,
        bulk_job=BULK_JOB,
    )
    load_archive(subject, october, expected_count=1)
    prior = copy.deepcopy(engine.state)

    with pytest.raises(
        TransactionIngestionError,
        match="cross_period_transaction_ownership_conflict",
    ):
        load_archive(subject, corrected, expected_count=1)

    assert engine.state == prior
    assert engine.state.checkpoints[(september_start, september_end)]["loaded_rows"] == 1
    assert engine.state.checkpoint is not None
    assert engine.state.checkpoint["loaded_rows"] == 1


def test_cross_period_race_after_preflight_is_rejected_and_rolled_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    september_start = date(2024, 9, 1)
    september_end = date(2024, 9, 30)
    september = write_archive(
        tmp_path,
        [
            transaction_row(
                contract_transaction_unique_key="september-owner",
                action_date="2024-09-15",
            )
        ],
        name="race-september.zip",
    )
    october = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="october-owner")],
        name="race-october.zip",
    )
    racing = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="racing-id")],
        name="race-new.zip",
    )
    engine = FakeEngine()
    subject = loader(engine)
    subject.load(
        archive_path=september,
        fiscal_year=2024,
        period_start=september_start,
        period_end=september_end,
        expected_count=1,
        bulk_job=BULK_JOB,
    )
    load_archive(subject, october, expected_count=1)
    prior = copy.deepcopy(engine.state)
    real_execute = engine.connection.execute
    race_injected = False

    def execute_with_cross_period_race(
        statement: Any,
        parameters: Any = None,
    ) -> FakeResult:
        nonlocal race_injected
        sql = str(statement.compile(dialect=postgresql.dialect()))
        if sql.startswith("INSERT INTO usaspending_transactions") and not race_injected:
            conflicting = copy.deepcopy(
                engine.state.transactions["september-owner"]
            )
            conflicting["stable_transaction_id"] = "racing-id"
            engine.state.transactions["racing-id"] = conflicting
            race_injected = True
        result = real_execute(statement, parameters)
        if sql.startswith("INSERT INTO usaspending_transactions"):
            result.rowcount = len(engine.connection.stage)
        return result

    monkeypatch.setattr(engine.connection, "execute", execute_with_cross_period_race)

    with pytest.raises(
        TransactionIngestionError,
        match="cross_period_transaction_ownership_conflict",
    ):
        load_archive(subject, racing, expected_count=1)

    assert race_injected is True
    assert engine.state == prior
    assert engine.state.checkpoints[(september_start, september_end)]["loaded_rows"] == 1
    assert engine.state.checkpoint is not None
    assert engine.state.checkpoint["loaded_rows"] == 1
    assert engine.rollbacks == 1


def test_hash_and_parser_use_one_private_copy_when_original_is_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="original-id")],
        name="original.zip",
    )
    original_bytes = original.read_bytes()
    replacement = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="replacement-id")],
        name="replacement.zip",
    )
    parser_paths: list[Path] = []
    real_parser = transaction_ingestion_module.TransactionExportParser

    def replacing_parser(archive_path: Path, **kwargs: Any) -> Any:
        parser_path = Path(archive_path)
        parser_paths.append(parser_path)
        assert parser_path != original
        assert stat.S_IMODE(parser_path.stat().st_mode) == stat.S_IRUSR
        os.replace(replacement, original)
        return real_parser(parser_path, **kwargs)

    monkeypatch.setattr(
        transaction_ingestion_module,
        "TransactionExportParser",
        replacing_parser,
    )
    engine = FakeEngine()

    result = load_archive(loader(engine), original, expected_count=1)

    assert result.archive_sha256 == hashlib.sha256(original_bytes).hexdigest()
    assert set(engine.state.transactions) == {"original-id"}
    assert parser_paths and all(not path.exists() for path in parser_paths)


@pytest.mark.parametrize("failure", ["parser", "database"])
def test_private_archive_copy_is_removed_after_every_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    archive = tmp_path / f"{failure}.zip"
    if failure == "parser":
        archive.write_bytes(b"not a zip archive")
    else:
        archive = write_archive(tmp_path, [transaction_row()], name="database.zip")
    parser_paths: list[Path] = []
    real_parser = transaction_ingestion_module.TransactionExportParser

    def recording_parser(archive_path: Path, **kwargs: Any) -> Any:
        parser_path = Path(archive_path)
        parser_paths.append(parser_path)
        assert stat.S_IMODE(parser_path.stat().st_mode) == stat.S_IRUSR
        return real_parser(parser_path, **kwargs)

    monkeypatch.setattr(
        transaction_ingestion_module,
        "TransactionExportParser",
        recording_parser,
    )
    engine = FakeEngine()
    subject = loader(engine)
    if failure == "database":
        monkeypatch.setattr(
            subject,
            "_delete_disappeared_transactions",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("database failure")),
        )

    with pytest.raises((TransactionExportError, RuntimeError)):
        load_archive(subject, archive, expected_count=1)

    assert parser_paths and all(not path.exists() for path in parser_paths)
    assert engine.state.transactions == {}


def test_rerun_correction_updates_every_mutable_field_and_preserves_created_at(
    tmp_path: Path,
) -> None:
    first_archive = write_archive(tmp_path, [transaction_row()], name="before.zip")
    corrected = transaction_row(
        contract_award_unique_key="CONT_AWD_2",
        award_id_piid="PIID-2",
        award_type_code="D",
        action_date="2024-10-31",
        federal_action_obligation="-10.25",
        recipient_name="Corrected Recipient",
        recipient_uei="ZZZ999YYY888",
        recipient_parent_name="Corrected Parent",
        recipient_parent_uei="QQQ111WWW222",
        awarding_agency_name="Corrected Agency",
        awarding_sub_agency_name="Corrected Subagency",
        naics_code="541519",
        product_or_service_code="D399",
        transaction_description="Corrected description",
        last_modified_date="2024-12-03T04:05:06-05:00",
    )
    corrected_archive = write_archive(tmp_path, [corrected], name="after.zip")
    engine = FakeEngine()
    subject = loader(engine)
    first = load_archive(subject, first_archive, expected_count=1)
    original_created_at = engine.state.transactions["transaction-1"]["created_at"]

    second = load_archive(subject, corrected_archive, expected_count=1)

    actual = engine.state.transactions["transaction-1"]
    assert second.checkpoint_id == first.checkpoint_id
    assert actual["created_at"] == original_created_at
    assert actual["ingestion_checkpoint_id"] == first.checkpoint_id
    assert actual["generated_award_id"] == "CONT_AWD_2"
    assert actual["display_award_id"] == "PIID-2"
    assert actual["award_type_code"] == "D"
    assert actual["action_date"] == date(2024, 10, 31)
    assert actual["fiscal_year"] == FISCAL_YEAR
    assert actual["federal_action_obligation"] == Decimal("-10.25")
    assert actual["recipient_name"] == "Corrected Recipient"
    assert actual["recipient_uei"] == "ZZZ999YYY888"
    assert actual["recipient_parent_name"] == "Corrected Parent"
    assert actual["recipient_parent_uei"] == "QQQ111WWW222"
    assert actual["awarding_agency_name"] == "Corrected Agency"
    assert actual["awarding_subagency_name"] == "Corrected Subagency"
    assert actual["naics_code"] == "541519"
    assert actual["psc_code"] == "D399"
    assert actual["transaction_description"] == "Corrected description"
    assert actual["source_updated_at"] == datetime(
        2024, 12, 3, 9, 5, 6, tzinfo=timezone.utc
    )
    assert actual["fetched_at"] == NOW
    assert actual["updated_at"] == NOW


def test_rerun_deletes_transactions_missing_from_replacement_export(tmp_path: Path) -> None:
    before = write_archive(
        tmp_path,
        [
            transaction_row(contract_transaction_unique_key="keep"),
            transaction_row(contract_transaction_unique_key="remove"),
        ],
        name="two.zip",
    )
    after = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="keep", federal_action_obligation="1.00")],
        name="one.zip",
    )
    engine = FakeEngine()
    subject = loader(engine)
    load_archive(subject, before, expected_count=2)

    load_archive(subject, after, expected_count=1)

    assert set(engine.state.transactions) == {"keep"}
    assert engine.state.transactions["keep"]["federal_action_obligation"] == Decimal("1.00")


def test_parser_count_failure_preserves_previous_snapshot_and_checkpoint(tmp_path: Path) -> None:
    first = write_archive(tmp_path, [transaction_row()], name="valid.zip")
    changed = write_archive(
        tmp_path,
        [transaction_row(transaction_description="different")],
        name="count-mismatch.zip",
    )
    engine = FakeEngine()
    subject = loader(engine)
    load_archive(subject, first, expected_count=1)
    prior = copy.deepcopy(engine.state)

    with pytest.raises(TransactionExportError, match="transaction_count_mismatch"):
        load_archive(subject, changed, expected_count=2)

    assert engine.state == prior
    assert engine.rollbacks == 1


@pytest.mark.parametrize(
    ("override", "error"),
    [
        ("stage", "staging_signed_total_mismatch"),
        ("target", "committed_period_signed_total_mismatch"),
    ],
)
def test_reconciliation_failure_rolls_back_all_persistent_changes(
    tmp_path: Path,
    override: str,
    error: str,
) -> None:
    first = write_archive(tmp_path, [transaction_row()], name="baseline.zip")
    changed = write_archive(
        tmp_path,
        [transaction_row(federal_action_obligation="200.00")],
        name=f"{override}.zip",
    )
    engine = FakeEngine()
    subject = loader(engine)
    load_archive(subject, first, expected_count=1)
    prior = copy.deepcopy(engine.state)
    if override == "stage":
        engine.connection.stage_total_override = Decimal("999.00")
    else:
        engine.connection.target_total_override = Decimal("999.00")

    with pytest.raises(TransactionIngestionError, match=error):
        load_archive(subject, changed, expected_count=1)

    assert engine.state == prior
    assert engine.rollbacks == 1


def test_failure_after_upsert_rolls_back_and_preserves_completed_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = write_archive(tmp_path, [transaction_row()], name="prior.zip")
    changed = write_archive(
        tmp_path,
        [transaction_row(federal_action_obligation="300.00")],
        name="failure.zip",
    )
    engine = FakeEngine()
    subject = loader(engine)
    load_archive(subject, first, expected_count=1)
    prior = copy.deepcopy(engine.state)

    def fail_after_upsert(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("injected deletion failure")

    monkeypatch.setattr(subject, "_delete_disappeared_transactions", fail_after_upsert)
    with pytest.raises(RuntimeError, match="injected deletion failure"):
        load_archive(subject, changed, expected_count=1)

    assert engine.state == prior
    assert engine.state.checkpoint is not None
    assert engine.state.checkpoint["status"] == "complete"


def test_postgresql_lock_staging_upsert_and_delete_sql_are_bounded_and_atomic() -> None:
    target = UsaSpendingTransaction.__table__
    stage = _stage_table(target)
    dialect = postgresql.dialect()
    create_sql = str(_create_stage_table_statement().compile(dialect=dialect))
    ownership_sql = str(
        _stage_ownership_conflicts_statement(
            target,
            stage,
            fiscal_year=FISCAL_YEAR,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        ).compile(dialect=dialect)
    )
    upsert_sql = str(
        _upsert_from_stage_statement(
            target,
            stage,
            fiscal_year=FISCAL_YEAR,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        ).compile(dialect=dialect)
    )
    delete_sql = str(
        _delete_disappeared_statement(
            target,
            stage,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        ).compile(dialect=dialect)
    )

    assert "CREATE TEMPORARY TABLE usaspending_transaction_stage" in create_sql
    assert "LIKE usaspending_transactions INCLUDING DEFAULTS INCLUDING CONSTRAINTS" in create_sql
    assert "ON COMMIT DROP" in create_sql
    assert "JOIN usaspending_transaction_stage" in ownership_sql
    assert "usaspending_transactions.action_date NOT BETWEEN" in ownership_sql
    assert "usaspending_transactions.fiscal_year !=" in ownership_sql
    assert "ON CONFLICT (stable_transaction_id) DO UPDATE" in upsert_sql
    assert "ingestion_checkpoint_id = excluded.ingestion_checkpoint_id" in upsert_sql
    assert "federal_action_obligation = excluded.federal_action_obligation" in upsert_sql
    assert "created_at = excluded.created_at" not in upsert_sql
    assert "WHERE usaspending_transactions.action_date BETWEEN" in upsert_sql
    assert "usaspending_transactions.fiscal_year =" in upsert_sql
    assert "DELETE FROM usaspending_transactions" in delete_sql
    assert "NOT (EXISTS (SELECT usaspending_transaction_stage.stable_transaction_id" in delete_sql
    assert _advisory_lock_key("usaspending", PERIOD_START, PERIOD_END) == _advisory_lock_key(
        "usaspending", PERIOD_START, PERIOD_END
    )
    assert _advisory_lock_key("usaspending", PERIOD_START, PERIOD_END) != _advisory_lock_key(
        "usaspending", date(2024, 11, 1), date(2024, 11, 30)
    )


@pytest.fixture
def disposable_postgres_engine() -> Iterator[Any]:
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is not set")
    url = make_url(value)
    if not url.database or not url.database.endswith("_test"):
        pytest.skip("TEST_DATABASE_URL must name a disposable database ending in _test")

    admin_engine = create_engine(url)
    schema = f"usaspending_ingestion_{uuid4().hex}"
    with admin_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    scoped_engine = create_engine(
        url,
        connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with scoped_engine.begin() as connection:
            UsaSpendingIngestionCheckpoint.__table__.create(connection)
            UsaSpendingTransaction.__table__.create(connection)
        yield scoped_engine
    finally:
        scoped_engine.dispose()
        with admin_engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin_engine.dispose()


@pytest.mark.integration
def test_disposable_postgres_load_correction_delete_rollback_and_temp_cleanup(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = disposable_postgres_engine
    first_archive = write_archive(
        tmp_path,
        [
            transaction_row(
                contract_transaction_unique_key="keep",
                federal_action_obligation="100.10",
                last_modified_date="2024-11-01T12:34:56-05:00",
            ),
            transaction_row(
                contract_transaction_unique_key="remove",
                federal_action_obligation="-25.00",
            ),
        ],
        name="pg-first.zip",
    )
    second_archive = write_archive(
        tmp_path,
        [
            transaction_row(
                contract_transaction_unique_key="keep",
                federal_action_obligation="0.00",
                transaction_description="corrected",
            )
        ],
        name="pg-second.zip",
    )
    subject = TransactionIngestionLoader(engine, clock=lambda: NOW)
    first = load_archive(subject, first_archive, expected_count=2)
    second = load_archive(subject, second_archive, expected_count=1)

    assert second.checkpoint_id == first.checkpoint_id
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                UsaSpendingTransaction.stable_transaction_id,
                UsaSpendingTransaction.federal_action_obligation,
                UsaSpendingTransaction.transaction_description,
                UsaSpendingTransaction.source_updated_at,
            )
        ).all()
        assert rows == [
            (
                "keep",
                Decimal("0.00"),
                "corrected",
                datetime(2024, 11, 1, 12, 34, 56, tzinfo=timezone.utc),
            )
        ]
        assert connection.scalar(
            text("SELECT to_regclass('pg_temp.usaspending_transaction_stage')")
        ) is None

    with engine.begin() as connection:
        connection.execute(
            UsaSpendingTransaction.__table__.update().values(
                recipient_name="Corrupted",
                federal_action_obligation=Decimal("999.99"),
            )
        )
    repaired = load_archive(subject, second_archive, expected_count=1)
    assert repaired.no_op is False
    with engine.connect() as connection:
        repaired_row = connection.execute(
            select(
                UsaSpendingTransaction.recipient_name,
                UsaSpendingTransaction.federal_action_obligation,
            )
        ).one()
        assert repaired_row == ("Example Recipient", Decimal("0.00"))

    with engine.begin() as connection:
        connection.execute(UsaSpendingTransaction.__table__.delete())
    load_archive(subject, second_archive, expected_count=1)
    with engine.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(UsaSpendingTransaction)) == 1

    prior_checkpoint = None
    with engine.connect() as connection:
        prior_checkpoint = connection.execute(
            select(UsaSpendingIngestionCheckpoint)
        ).mappings().one()

    def fail_after_upsert(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("injected PostgreSQL rollback")

    monkeypatch.setattr(subject, "_delete_disappeared_transactions", fail_after_upsert)
    third_archive = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="keep", federal_action_obligation="9.99")],
        name="pg-rollback.zip",
    )
    with pytest.raises(RuntimeError, match="injected PostgreSQL rollback"):
        load_archive(subject, third_archive, expected_count=1)

    with engine.connect() as connection:
        assert connection.scalar(
            select(UsaSpendingTransaction.federal_action_obligation)
        ) == Decimal("0.00")
        assert connection.execute(
            select(UsaSpendingIngestionCheckpoint)
        ).mappings().one() == prior_checkpoint
        assert connection.scalar(
            text("SELECT to_regclass('pg_temp.usaspending_transaction_stage')")
        ) is None


@pytest.mark.integration
def test_disposable_postgres_rejects_cross_period_stable_id_without_mutation(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    subject = TransactionIngestionLoader(engine, clock=lambda: NOW)
    september_start = date(2024, 9, 1)
    september_end = date(2024, 9, 30)
    september = write_archive(
        tmp_path,
        [
            transaction_row(
                contract_transaction_unique_key="moving-id",
                action_date="2024-09-15",
            )
        ],
        name="pg-september.zip",
    )
    october = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="october-id")],
        name="pg-october.zip",
    )
    moved = write_archive(
        tmp_path,
        [transaction_row(contract_transaction_unique_key="moving-id")],
        name="pg-moved.zip",
    )
    subject.load(
        archive_path=september,
        fiscal_year=2024,
        period_start=september_start,
        period_end=september_end,
        expected_count=1,
        bulk_job=BULK_JOB,
    )
    load_archive(subject, october, expected_count=1)
    with engine.connect() as connection:
        prior_transactions = connection.execute(
            select(UsaSpendingTransaction).order_by(
                UsaSpendingTransaction.stable_transaction_id
            )
        ).mappings().all()
        prior_checkpoints = connection.execute(
            select(UsaSpendingIngestionCheckpoint).order_by(
                UsaSpendingIngestionCheckpoint.period_start
            )
        ).mappings().all()

    with pytest.raises(
        TransactionIngestionError,
        match="cross_period_transaction_ownership_conflict",
    ):
        load_archive(subject, moved, expected_count=1)

    with engine.connect() as connection:
        assert connection.execute(
            select(UsaSpendingTransaction).order_by(
                UsaSpendingTransaction.stable_transaction_id
            )
        ).mappings().all() == prior_transactions
        assert connection.execute(
            select(UsaSpendingIngestionCheckpoint).order_by(
                UsaSpendingIngestionCheckpoint.period_start
            )
        ).mappings().all() == prior_checkpoints


@pytest.mark.integration
def test_disposable_postgres_advisory_lock_serializes_same_period(
    disposable_postgres_engine: Any,
) -> None:
    engine = disposable_postgres_engine
    key = _advisory_lock_key("usaspending", PERIOD_START, PERIOD_END)
    with engine.connect() as first, engine.connect() as second:
        first_transaction = first.begin()
        second_transaction = second.begin()
        try:
            first.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
            second.execute(text("SET LOCAL lock_timeout = '100ms'"))
            with pytest.raises(DBAPIError):
                second.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
        finally:
            second_transaction.rollback()
            first_transaction.rollback()
