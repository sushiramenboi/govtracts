import csv
import io
import stat
import zipfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from app.usaspending import transaction_export as transaction_export_module
from app.usaspending.client import BULK_TRANSACTION_FIELDS
from app.usaspending.transaction_export import (
    EXPECTED_TRANSACTION_HEADERS,
    TransactionExportError,
    TransactionExportParser,
)


PERIOD_START = date(2024, 10, 1)
PERIOD_END = date(2024, 10, 31)
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


def write_archive(
    tmp_path: Path,
    *,
    rows: list[dict[str, str]] | None = None,
    headers: list[str] | tuple[str, ...] = EXPECTED_TRANSACTION_HEADERS,
    member_name: str = "transactions.csv",
    bom: bool = False,
    compression: int = zipfile.ZIP_DEFLATED,
    extras: dict[str, bytes] | None = None,
) -> Path:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(headers)
    for row in rows if rows is not None else [transaction_row()]:
        writer.writerow([row.get(header, "") for header in headers])
    payload = stream.getvalue().encode("utf-8")
    if bom:
        payload = b"\xef\xbb\xbf" + payload

    archive_path = tmp_path / "transactions.zip"
    with zipfile.ZipFile(archive_path, "w", compression=compression) as archive:
        archive.writestr(member_name, payload)
        for name, content in (extras or {}).items():
            archive.writestr(name, content)
    return archive_path


def parser(archive_path: Path, **overrides: object) -> TransactionExportParser:
    options = {
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        **overrides,
    }
    return TransactionExportParser(archive_path, **options)  # type: ignore[arg-type]


def consume(archive_path: Path, **overrides: object):  # type: ignore[no-untyped-def]
    export = parser(archive_path, **overrides)
    with export.open_rows() as reader:
        rows = list(reader)
    return rows, export.totals


def mark_archive_encrypted(archive_path: Path) -> None:
    payload = bytearray(archive_path.read_bytes())
    for signature, flag_offset in ((b"PK\x03\x04", 6), (b"PK\x01\x02", 8)):
        position = payload.find(signature)
        assert position >= 0
        flags = int.from_bytes(
            payload[position + flag_offset : position + flag_offset + 2],
            "little",
        )
        payload[position + flag_offset : position + flag_offset + 2] = (
            flags | 0x1
        ).to_bytes(2, "little")
    archive_path.write_bytes(payload)


def test_parser_contract_matches_committed_bulk_client_fields() -> None:
    assert EXPECTED_TRANSACTION_HEADERS == BULK_TRANSACTION_FIELDS


def test_valid_export_streams_typed_rows_and_exact_signed_totals(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[
            transaction_row(
                contract_transaction_unique_key="positive",
                federal_action_obligation="100.10",
            ),
            transaction_row(
                contract_transaction_unique_key="zero",
                federal_action_obligation="0.00",
            ),
            transaction_row(
                contract_transaction_unique_key="negative",
                federal_action_obligation="-25.05",
            ),
        ],
    )

    rows, totals = consume(archive_path, expected_count=3)

    assert [row.federal_action_obligation for row in rows] == [
        Decimal("100.10"),
        Decimal("0.00"),
        Decimal("-25.05"),
    ]
    assert rows[0].action_date == date(2024, 10, 15)
    assert rows[0].fiscal_year == 2025
    assert rows[0].source_updated_at == datetime(
        2024, 11, 1, 12, 34, 56, tzinfo=timezone.utc
    )
    assert totals.row_count == 3
    assert totals.distinct_transaction_count == 3
    assert totals.signed_obligation_total == Decimal("75.05")
    assert sorted(path.name for path in tmp_path.iterdir()) == ["transactions.zip"]


def test_reordered_headers_are_accepted(tmp_path: Path) -> None:
    headers = list(reversed(EXPECTED_TRANSACTION_HEADERS))
    archive_path = write_archive(tmp_path, headers=headers)

    rows, totals = consume(archive_path)

    assert rows[0].stable_transaction_id == "transaction-1"
    assert rows[0].psc_code == "D310"
    assert totals.row_count == 1


def test_utf8_bom_is_accepted(tmp_path: Path) -> None:
    rows, _ = consume(write_archive(tmp_path, bom=True))

    assert rows[0].stable_transaction_id == "transaction-1"


def test_optional_parent_and_classification_values_may_be_empty(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[
            transaction_row(
                recipient_parent_name="",
                recipient_parent_uei="",
                naics_code="",
                product_or_service_code="",
            )
        ],
    )

    rows, _ = consume(archive_path)

    assert rows[0].recipient_parent_name is None
    assert rows[0].recipient_parent_uei is None
    assert rows[0].naics_code is None
    assert rows[0].psc_code is None


def test_duplicate_transaction_ids_are_rejected(tmp_path: Path) -> None:
    archive_path = write_archive(tmp_path, rows=[transaction_row(), transaction_row()])

    with pytest.raises(TransactionExportError, match="duplicate_transaction_id"):
        consume(archive_path)


@pytest.mark.parametrize(
    "field",
    [
        "contract_transaction_unique_key",
        "contract_award_unique_key",
        "award_id_piid",
        "recipient_uei",
        "awarding_agency_name",
        "awarding_sub_agency_name",
        "recipient_name",
        "transaction_description",
        "last_modified_date",
    ],
)
def test_missing_required_values_are_rejected(tmp_path: Path, field: str) -> None:
    archive_path = write_archive(tmp_path, rows=[transaction_row(**{field: ""})])

    with pytest.raises(TransactionExportError, match=f"missing_required_{field}"):
        consume(archive_path)


@pytest.mark.parametrize("value", ["not-a-number", "NaN", "Infinity", ""])
def test_invalid_obligations_are_rejected(tmp_path: Path, value: str) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(federal_action_obligation=value)],
    )

    with pytest.raises(TransactionExportError, match="invalid_federal_action_obligation"):
        consume(archive_path)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("action_date", "2024-02-30", "invalid_action_date"),
        ("last_modified_date", "not-a-date", "invalid_last_modified_date"),
    ],
)
def test_invalid_dates_are_rejected(
    tmp_path: Path,
    field: str,
    value: str,
    error: str,
) -> None:
    archive_path = write_archive(tmp_path, rows=[transaction_row(**{field: value})])

    with pytest.raises(TransactionExportError, match=error):
        consume(archive_path)


def test_action_dates_outside_selected_period_are_rejected(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(action_date="2024-11-01")],
    )

    with pytest.raises(TransactionExportError, match="action_date_outside_period"):
        consume(archive_path)


@pytest.mark.parametrize("award_type", ["E", "a", "contract"])
def test_non_prime_award_types_are_rejected(tmp_path: Path, award_type: str) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(award_type_code=award_type)],
    )

    with pytest.raises(TransactionExportError, match="invalid_award_type"):
        consume(archive_path)


@pytest.mark.parametrize("field", ["recipient_uei", "recipient_parent_uei"])
def test_invalid_uei_values_are_rejected(tmp_path: Path, field: str) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(**{field: "too-short"})],
    )

    with pytest.raises(TransactionExportError, match=f"invalid_{field}"):
        consume(archive_path)


def test_expected_count_mismatch_is_rejected_after_streaming(tmp_path: Path) -> None:
    export = parser(write_archive(tmp_path), expected_count=2)

    with pytest.raises(TransactionExportError, match="transaction_count_mismatch"):
        with export.open_rows() as reader:
            list(reader)

    with pytest.raises(TransactionExportError, match="not_fully_consumed"):
        export.totals


def test_missing_headers_are_rejected(tmp_path: Path) -> None:
    headers = list(EXPECTED_TRANSACTION_HEADERS[:-1])

    with pytest.raises(TransactionExportError, match="missing_csv_headers"):
        consume(write_archive(tmp_path, headers=headers))


def test_duplicate_headers_are_rejected(tmp_path: Path) -> None:
    headers = [*EXPECTED_TRANSACTION_HEADERS, EXPECTED_TRANSACTION_HEADERS[0]]

    with pytest.raises(TransactionExportError, match="duplicate_csv_headers"):
        consume(write_archive(tmp_path, headers=headers))


def test_unexpected_headers_are_rejected(tmp_path: Path) -> None:
    headers = [*EXPECTED_TRANSACTION_HEADERS, "unexpected_column"]

    with pytest.raises(TransactionExportError, match="unexpected_csv_headers"):
        consume(write_archive(tmp_path, headers=headers))


def test_rows_with_wrong_column_count_are_rejected(tmp_path: Path) -> None:
    archive_path = tmp_path / "transactions.zip"
    csv_payload = ",".join(EXPECTED_TRANSACTION_HEADERS) + "\nonly-one-value\n"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("transactions.csv", csv_payload)

    with pytest.raises(TransactionExportError, match="invalid_csv_column_count"):
        consume(archive_path)


def test_malformed_archive_is_rejected(tmp_path: Path) -> None:
    archive_path = tmp_path / "transactions.zip"
    archive_path.write_bytes(b"not a zip archive")

    with pytest.raises(TransactionExportError, match="invalid_transaction_export_archive"):
        consume(archive_path)


def test_multiple_archive_members_are_rejected(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        extras={"second.csv": b"other"},
    )

    with pytest.raises(TransactionExportError, match="unexpected_archive_members"):
        consume(archive_path)


def test_unexpected_archive_member_is_rejected(tmp_path: Path) -> None:
    archive_path = write_archive(tmp_path, member_name="readme.txt")

    with pytest.raises(TransactionExportError, match="unexpected_archive_member"):
        consume(archive_path)


@pytest.mark.parametrize(
    "member_name",
    [
        "../transactions.csv",
        "folder/transactions.csv",
        "..\\transactions.csv",
        "C:transactions.csv",
    ],
)
def test_path_traversing_or_nested_members_are_rejected(
    tmp_path: Path,
    member_name: str,
) -> None:
    archive_path = write_archive(tmp_path, member_name=member_name)

    with pytest.raises(TransactionExportError, match="unsafe_archive_member"):
        consume(archive_path)


def test_encrypted_archive_member_is_rejected(tmp_path: Path) -> None:
    archive_path = write_archive(tmp_path)
    mark_archive_encrypted(archive_path)

    with pytest.raises(TransactionExportError, match="encrypted_archive_member"):
        consume(archive_path)


def test_symbolic_link_archive_member_is_rejected(tmp_path: Path) -> None:
    archive_path = tmp_path / "transactions.zip"
    member = zipfile.ZipInfo("transactions.csv")
    member.create_system = 3
    member.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(member, b"target")

    with pytest.raises(TransactionExportError, match="unsafe_archive_member"):
        consume(archive_path)


def test_archive_entry_count_limit_is_enforced(tmp_path: Path) -> None:
    archive_path = write_archive(tmp_path, extras={"second.csv": b"other"})

    with pytest.raises(TransactionExportError, match="archive_entry_count_limit"):
        consume(archive_path, max_archive_entries=1)


def test_total_uncompressed_size_limit_is_enforced(tmp_path: Path) -> None:
    archive_path = write_archive(tmp_path)

    with pytest.raises(TransactionExportError, match="uncompressed_size_limit"):
        consume(archive_path, max_uncompressed_bytes=32)


def test_excessive_compression_ratio_is_rejected(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(transaction_description="x" * 20_000)],
    )

    with pytest.raises(TransactionExportError, match="compression_ratio_limit"):
        consume(archive_path, max_compression_ratio=2)


def corrupt_central_directory_crc(archive_path: Path) -> None:
    payload = bytearray(archive_path.read_bytes())
    position = payload.find(b"PK\x01\x02")
    assert position >= 0
    payload[position + 16 : position + 20] = (0).to_bytes(4, "little")
    archive_path.write_bytes(payload)


def track_parser_archives(monkeypatch: pytest.MonkeyPatch) -> list[zipfile.ZipFile]:
    opened: list[zipfile.ZipFile] = []
    real_zip_file = transaction_export_module.zipfile.ZipFile

    def tracked_zip_file(*args: object, **kwargs: object) -> zipfile.ZipFile:
        archive = real_zip_file(*args, **kwargs)  # type: ignore[arg-type]
        opened.append(archive)
        return archive

    monkeypatch.setattr(transaction_export_module.zipfile, "ZipFile", tracked_zip_file)
    return opened


@pytest.mark.parametrize(
    "value",
    [
        "0.001",
        "-0.001",
        "9999999999999999999.99",
        "-9999999999999999999.99",
    ],
)
def test_obligations_outside_numeric_20_2_are_rejected(
    tmp_path: Path,
    value: str,
) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(federal_action_obligation=value)],
    )

    with pytest.raises(
        TransactionExportError,
        match="(excess_federal_action_obligation_scale|federal_action_obligation_out_of_range)",
    ):
        consume(archive_path)


@pytest.mark.parametrize("value", ["1E+1000000", "-1E+1000000"])
def test_extreme_obligation_exponents_are_deterministic(
    tmp_path: Path,
    value: str,
) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(federal_action_obligation=value)],
    )

    with pytest.raises(
        TransactionExportError,
        match="federal_action_obligation_out_of_range",
    ):
        consume(archive_path)


def test_numeric_20_2_minimum_and_maximum_are_preserved_exactly(tmp_path: Path) -> None:
    maximum = "999999999999999999.99"
    minimum = "-999999999999999999.99"
    archive_path = write_archive(
        tmp_path,
        rows=[
            transaction_row(
                contract_transaction_unique_key="maximum",
                federal_action_obligation=maximum,
            ),
            transaction_row(
                contract_transaction_unique_key="minimum",
                federal_action_obligation=minimum,
            ),
        ],
    )

    rows, totals = consume(archive_path)

    assert [row.federal_action_obligation for row in rows] == [
        Decimal(maximum),
        Decimal(minimum),
    ]
    assert totals.signed_obligation_total == Decimal("0.00")


def test_signed_obligation_total_overflow_is_rejected(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[
            transaction_row(
                contract_transaction_unique_key="maximum",
                federal_action_obligation="999999999999999999.99",
            ),
            transaction_row(
                contract_transaction_unique_key="overflow",
                federal_action_obligation="0.01",
            ),
        ],
    )

    with pytest.raises(
        TransactionExportError,
        match="signed_obligation_total_out_of_range",
    ):
        consume(archive_path)


def test_expected_count_cannot_exceed_row_limit(tmp_path: Path) -> None:
    archive_path = write_archive(tmp_path)

    with pytest.raises(TransactionExportError, match="expected_count_exceeds_max_rows"):
        parser(archive_path, expected_count=2, max_rows=1)


def test_row_limit_is_enforced_before_duplicate_state_grows(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[
            transaction_row(contract_transaction_unique_key="first"),
            transaction_row(contract_transaction_unique_key="second"),
        ],
    )

    with pytest.raises(TransactionExportError, match="transaction_row_limit_exceeded"):
        consume(archive_path, max_rows=1)


def test_expected_count_fails_immediately_on_first_excess_row(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[
            transaction_row(contract_transaction_unique_key="first"),
            transaction_row(
                contract_transaction_unique_key="second",
                award_type_code="invalid",
            ),
        ],
    )

    with pytest.raises(
        TransactionExportError,
        match="transaction_count_exceeded_expected",
    ):
        consume(archive_path, expected_count=1)


def test_csv_field_limit_is_parser_owned(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(transaction_description="x" * 100)],
    )
    original_limit = csv.field_size_limit()
    csv.field_size_limit(10_000_000)
    try:
        with pytest.raises(TransactionExportError, match="csv_field_limit_exceeded"):
            consume(archive_path, max_csv_field_chars=50)
    finally:
        csv.field_size_limit(original_limit)


def test_csv_record_limit_is_enforced_before_csv_parsing(tmp_path: Path) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(transaction_description="x" * 1000)],
    )

    with pytest.raises(TransactionExportError, match="csv_record_limit_exceeded"):
        consume(
            archive_path,
            max_csv_field_chars=2000,
            max_csv_record_chars=512,
        )


def test_quoted_multiline_fields_still_use_real_csv_semantics(tmp_path: Path) -> None:
    description = 'quoted, "value"\nnext line'
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(transaction_description=description)],
    )

    rows, _ = consume(archive_path)

    assert rows[0].transaction_description == description


def test_actual_streaming_backstop_counts_decompressed_bytes() -> None:
    raw = io.BytesIO(b"12345")
    bounded = transaction_export_module._BoundedArchiveReader(
        raw,  # type: ignore[arg-type]
        maximum_bytes=4,
    )

    with pytest.raises(TransactionExportError, match="uncompressed_size_limit_exceeded"):
        bounded.read()

    bounded.close()
    assert raw.closed


def test_directory_archive_member_is_rejected(tmp_path: Path) -> None:
    archive_path = tmp_path / "transactions.zip"
    member = zipfile.ZipInfo("transactions.csv/")
    member.create_system = 3
    member.external_attr = (stat.S_IFDIR | 0o755) << 16
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(member, b"")

    with pytest.raises(TransactionExportError, match="unsafe_archive_member"):
        consume(archive_path)


def test_zero_size_csv_member_is_rejected(tmp_path: Path) -> None:
    archive_path = tmp_path / "transactions.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("transactions.csv", b"")

    with pytest.raises(TransactionExportError, match="missing_csv_header"):
        consume(archive_path)


def test_malformed_zip_metadata_is_translated(tmp_path: Path) -> None:
    archive_path = write_archive(tmp_path)
    corrupt_central_directory_crc(archive_path)

    with pytest.raises(
        TransactionExportError,
        match="invalid_transaction_export_archive",
    ):
        consume(archive_path)


def test_resources_close_after_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = write_archive(tmp_path)
    opened = track_parser_archives(monkeypatch)

    consume(archive_path)

    assert len(opened) == 1
    assert opened[0].fp is None


def test_resources_close_after_validation_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(), transaction_row()],
    )
    opened = track_parser_archives(monkeypatch)

    with pytest.raises(TransactionExportError, match="duplicate_transaction_id"):
        consume(archive_path)

    assert len(opened) == 1
    assert opened[0].fp is None


def test_context_exit_closes_early_stream_and_invalidates_retained_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[
            transaction_row(contract_transaction_unique_key="first"),
            transaction_row(contract_transaction_unique_key="second"),
        ],
    )
    opened = track_parser_archives(monkeypatch)
    export = parser(archive_path)
    reader = export.open_rows()

    with pytest.raises(RuntimeError, match="caller failure"):
        with reader:
            next(reader)
            raise RuntimeError("caller failure")

    assert opened[0].fp is None
    with pytest.raises(TransactionExportError, match="reader_not_active"):
        next(reader)
    with pytest.raises(TransactionExportError, match="not_fully_consumed"):
        export.totals
    with pytest.raises(TransactionExportError, match="already_consumed"):
        with export.open_rows():
            pass


def test_reader_rejects_iteration_outside_context(tmp_path: Path) -> None:
    reader = parser(write_archive(tmp_path)).open_rows()

    with pytest.raises(TransactionExportError, match="reader_not_active"):
        next(reader)


def test_extremely_small_obligation_exponent_is_rejected_deterministically(
    tmp_path: Path,
) -> None:
    archive_path = write_archive(
        tmp_path,
        rows=[transaction_row(federal_action_obligation="1E-1000000")],
    )

    with pytest.raises(
        TransactionExportError,
        match="excess_federal_action_obligation_scale",
    ):
        consume(archive_path)
