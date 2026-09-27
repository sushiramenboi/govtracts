"""Offline streaming validation for USAspending transaction-export archives."""

from __future__ import annotations

import csv
import io
import os
import re
import stat
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Context, Decimal, DecimalException, InvalidOperation, localcontext
from pathlib import Path

from app.usaspending.fiscal_years import fiscal_year_for_date


EXPECTED_TRANSACTION_HEADERS = (
    "contract_transaction_unique_key",
    "contract_award_unique_key",
    "award_id_piid",
    "award_type_code",
    "action_date",
    "federal_action_obligation",
    "recipient_name",
    "recipient_uei",
    "recipient_parent_name",
    "recipient_parent_uei",
    "awarding_agency_name",
    "awarding_sub_agency_name",
    "naics_code",
    "product_or_service_code",
    "transaction_description",
    "last_modified_date",
)
ALLOWED_AWARD_TYPE_CODES = frozenset({"A", "B", "C", "D"})
DEFAULT_MAX_ARCHIVE_ENTRIES = 8
DEFAULT_MAX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_COMPRESSION_RATIO = Decimal("100")
# The Phase 0 October 2024 sample contained 490,377 rows. This limit preserves
# a small safety margin while bounding the in-memory duplicate-ID set.
DEFAULT_MAX_ROWS = 500_000
DEFAULT_MAX_CSV_FIELD_CHARS = 1024 * 1024
DEFAULT_MAX_CSV_RECORD_CHARS = 4 * 1024 * 1024
MONEY_QUANTUM = Decimal("0.01")
MAX_NUMERIC_20_2 = Decimal("999999999999999999.99")
MONEY_CONTEXT = Context(prec=40)
SUPPORTED_COMPRESSION_TYPES = frozenset({zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED})
UEI_PATTERN = re.compile(r"^[A-Za-z0-9]{12}$")


class TransactionExportError(RuntimeError):
    """A safe validation failure for a local USAspending transaction export."""


@dataclass(frozen=True)
class TransactionRow:
    """Canonical, load-ready representation of one validated export row."""

    stable_transaction_id: str
    generated_award_id: str
    display_award_id: str
    award_type_code: str
    action_date: date
    fiscal_year: int
    federal_action_obligation: Decimal
    recipient_name: str
    recipient_uei: str
    recipient_parent_name: str | None
    recipient_parent_uei: str | None
    awarding_agency_name: str
    awarding_subagency_name: str
    naics_code: str | None
    psc_code: str | None
    transaction_description: str
    source_updated_at: datetime


@dataclass(frozen=True)
class ValidationTotals:
    """Exact totals available after the stream has been consumed successfully."""

    row_count: int
    distinct_transaction_count: int
    signed_obligation_total: Decimal


class TransactionExportParser:
    """Validate and stream one local USAspending transaction ZIP exactly once."""

    def __init__(
        self,
        archive_path: str | os.PathLike[str],
        *,
        period_start: date,
        period_end: date,
        expected_count: int | None = None,
        max_archive_entries: int = DEFAULT_MAX_ARCHIVE_ENTRIES,
        max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
        max_compression_ratio: Decimal | int = DEFAULT_MAX_COMPRESSION_RATIO,
        max_rows: int = DEFAULT_MAX_ROWS,
        max_csv_field_chars: int = DEFAULT_MAX_CSV_FIELD_CHARS,
        max_csv_record_chars: int = DEFAULT_MAX_CSV_RECORD_CHARS,
    ) -> None:
        if (
            not isinstance(period_start, date)
            or not isinstance(period_end, date)
            or period_end < period_start
        ):
            raise TransactionExportError("invalid_ingestion_period")
        if (
            expected_count is not None
            and (
                isinstance(expected_count, bool)
                or not isinstance(expected_count, int)
                or expected_count < 0
            )
        ):
            raise TransactionExportError("invalid_expected_count")
        self._validate_positive_integer("max_archive_entries", max_archive_entries)
        self._validate_positive_integer("max_uncompressed_bytes", max_uncompressed_bytes)
        self._validate_positive_integer("max_rows", max_rows)
        self._validate_positive_integer("max_csv_field_chars", max_csv_field_chars)
        self._validate_positive_integer("max_csv_record_chars", max_csv_record_chars)
        if expected_count is not None and expected_count > max_rows:
            raise TransactionExportError("expected_count_exceeds_max_rows")
        if isinstance(max_compression_ratio, bool) or not isinstance(
            max_compression_ratio, (Decimal, int)
        ):
            raise TransactionExportError("invalid_max_compression_ratio")
        ratio = Decimal(max_compression_ratio)
        if not ratio.is_finite() or ratio <= 0:
            raise TransactionExportError("invalid_max_compression_ratio")

        self.archive_path = Path(archive_path)
        self.period_start = period_start
        self.period_end = period_end
        self.expected_count = expected_count
        self.max_archive_entries = max_archive_entries
        self.max_uncompressed_bytes = max_uncompressed_bytes
        self.max_compression_ratio = ratio
        self.max_rows = max_rows
        self.max_csv_field_chars = max_csv_field_chars
        self.max_csv_record_chars = max_csv_record_chars
        self._state = "new"
        self._totals: ValidationTotals | None = None

    @property
    def totals(self) -> ValidationTotals:
        if self._state != "succeeded" or self._totals is None:
            raise TransactionExportError("transaction_export_not_fully_consumed")
        return self._totals

    def open_rows(self) -> TransactionExportReader:
        """Return the context-managed, one-shot transaction-row reader."""
        return TransactionExportReader(self)

    def _begin(self) -> None:
        if self._state != "new":
            raise TransactionExportError("transaction_export_already_consumed")
        self._state = "active"

    def _mark_failed(self) -> None:
        self._totals = None
        self._state = "failed"

    def _mark_succeeded(self, totals: ValidationTotals) -> None:
        self._totals = totals
        self._state = "succeeded"

    def _stream_rows(self) -> Iterator[TransactionRow]:
        row_count = 0
        transaction_ids: set[str] = set()
        obligation_total = Decimal("0.00")

        try:
            with zipfile.ZipFile(self.archive_path) as archive:
                member = self._validate_archive(archive)
                with archive.open(member, "r") as raw_member:
                    bounded = _BoundedArchiveReader(
                        raw_member,
                        maximum_bytes=self.max_uncompressed_bytes,
                    )
                    with io.TextIOWrapper(
                        io.BufferedReader(bounded),
                        encoding="utf-8-sig",
                        errors="strict",
                        newline="",
                    ) as text_member:
                        bounded_csv = _BoundedCsvInput(
                            text_member,
                            maximum_field_chars=self.max_csv_field_chars,
                            maximum_record_chars=self.max_csv_record_chars,
                        )
                        reader = csv.reader(bounded_csv, strict=True)
                        headers = self._read_headers(reader)
                        indexes = {header: index for index, header in enumerate(headers)}

                        for csv_row_number, values in enumerate(reader, start=2):
                            next_row_count = row_count + 1
                            if next_row_count > self.max_rows:
                                raise TransactionExportError("transaction_row_limit_exceeded")
                            if (
                                self.expected_count is not None
                                and next_row_count > self.expected_count
                            ):
                                raise TransactionExportError(
                                    "transaction_count_exceeded_expected"
                                )
                            if len(values) != len(EXPECTED_TRANSACTION_HEADERS):
                                raise TransactionExportError(
                                    f"invalid_csv_column_count_at_row_{csv_row_number}"
                                )
                            if any("\x00" in value for value in values):
                                raise TransactionExportError(
                                    f"unsafe_csv_value_at_row_{csv_row_number}"
                                )
                            raw_row = {
                                header: values[index].strip()
                                for header, index in indexes.items()
                            }
                            transaction = self._parse_row(raw_row, csv_row_number)
                            if transaction.stable_transaction_id in transaction_ids:
                                raise TransactionExportError(
                                    f"duplicate_transaction_id_at_row_{csv_row_number}"
                                )
                            transaction_ids.add(transaction.stable_transaction_id)
                            row_count = next_row_count
                            try:
                                with localcontext(MONEY_CONTEXT):
                                    next_total = (
                                        obligation_total
                                        + transaction.federal_action_obligation
                                    )
                            except DecimalException:
                                raise TransactionExportError(
                                    "signed_obligation_total_arithmetic_failure"
                                ) from None
                            if next_total.copy_abs() > MAX_NUMERIC_20_2:
                                raise TransactionExportError(
                                    "signed_obligation_total_out_of_range"
                                )
                            obligation_total = next_total
                            yield transaction
        except TransactionExportError:
            raise
        except (
            csv.Error,
            EOFError,
            NotImplementedError,
            OSError,
            UnicodeError,
            zipfile.BadZipFile,
            zipfile.LargeZipFile,
        ):
            raise TransactionExportError("invalid_transaction_export_archive") from None

        if self.expected_count is not None and row_count != self.expected_count:
            raise TransactionExportError("transaction_count_mismatch")
        self._mark_succeeded(
            ValidationTotals(
                row_count=row_count,
                distinct_transaction_count=len(transaction_ids),
                signed_obligation_total=obligation_total,
            )
        )

    def _validate_archive(self, archive: zipfile.ZipFile) -> zipfile.ZipInfo:
        members = archive.infolist()
        if not members:
            raise TransactionExportError("empty_transaction_export_archive")
        if len(members) > self.max_archive_entries:
            raise TransactionExportError("archive_entry_count_limit_exceeded")
        if len(members) != 1:
            raise TransactionExportError("unexpected_archive_members")

        member = members[0]
        filename = member.filename
        if (
            not filename
            or "\x00" in filename
            or ":" in filename
            or "/" in filename
            or "\\" in filename
            or filename in {".", ".."}
        ):
            raise TransactionExportError("unsafe_archive_member")
        if member.is_dir() or Path(filename).suffix.lower() != ".csv":
            raise TransactionExportError("unexpected_archive_member")
        unix_mode = member.external_attr >> 16
        file_type = stat.S_IFMT(unix_mode)
        if file_type not in {0, stat.S_IFREG}:
            raise TransactionExportError("unsafe_archive_member")
        if member.flag_bits & 0x1:
            raise TransactionExportError("encrypted_archive_member")
        if member.compress_type not in SUPPORTED_COMPRESSION_TYPES:
            raise TransactionExportError("unsupported_archive_compression")
        if member.file_size > self.max_uncompressed_bytes:
            raise TransactionExportError("uncompressed_size_limit_exceeded")
        if member.file_size:
            if member.compress_size <= 0:
                raise TransactionExportError("compression_ratio_limit_exceeded")
            ratio = Decimal(member.file_size) / Decimal(member.compress_size)
            if ratio > self.max_compression_ratio:
                raise TransactionExportError("compression_ratio_limit_exceeded")
        return member

    @staticmethod
    def _read_headers(reader: Iterator[list[str]]) -> list[str]:
        try:
            headers = next(reader)
        except StopIteration:
            raise TransactionExportError("missing_csv_header") from None
        if any("\x00" in header for header in headers):
            raise TransactionExportError("unsafe_csv_header")
        duplicates = sorted(
            header for header in set(headers) if headers.count(header) > 1
        )
        if duplicates:
            raise TransactionExportError("duplicate_csv_headers")
        expected = set(EXPECTED_TRANSACTION_HEADERS)
        actual = set(headers)
        if expected - actual:
            raise TransactionExportError("missing_csv_headers")
        if actual - expected:
            raise TransactionExportError("unexpected_csv_headers")
        if len(headers) != len(EXPECTED_TRANSACTION_HEADERS):
            raise TransactionExportError("invalid_csv_headers")
        return headers

    def _parse_row(self, row: dict[str, str], row_number: int) -> TransactionRow:
        stable_transaction_id = self._required(row, "contract_transaction_unique_key", row_number)
        generated_award_id = self._required(row, "contract_award_unique_key", row_number)
        display_award_id = self._required(row, "award_id_piid", row_number)
        award_type_code = self._required(row, "award_type_code", row_number)
        if award_type_code not in ALLOWED_AWARD_TYPE_CODES:
            raise TransactionExportError(f"invalid_award_type_at_row_{row_number}")

        action_date = self._parse_date(row["action_date"], "action_date", row_number)
        if not self.period_start <= action_date <= self.period_end:
            raise TransactionExportError(f"action_date_outside_period_at_row_{row_number}")
        obligation = self._parse_decimal(row["federal_action_obligation"], row_number)

        recipient_name = self._required(row, "recipient_name", row_number)
        recipient_uei = self._parse_uei(
            self._required(row, "recipient_uei", row_number),
            "recipient_uei",
            row_number,
        )
        parent_uei = self._optional(row["recipient_parent_uei"])
        if parent_uei is not None:
            parent_uei = self._parse_uei(parent_uei, "recipient_parent_uei", row_number)

        awarding_agency = self._required(row, "awarding_agency_name", row_number)
        awarding_subagency = self._required(
            row, "awarding_sub_agency_name", row_number
        )
        description = self._required(row, "transaction_description", row_number)
        source_updated_at = self._parse_datetime(
            self._required(row, "last_modified_date", row_number),
            row_number,
        )

        return TransactionRow(
            stable_transaction_id=stable_transaction_id,
            generated_award_id=generated_award_id,
            display_award_id=display_award_id,
            award_type_code=award_type_code,
            action_date=action_date,
            fiscal_year=fiscal_year_for_date(action_date),
            federal_action_obligation=obligation,
            recipient_name=recipient_name,
            recipient_uei=recipient_uei,
            recipient_parent_name=self._optional(row["recipient_parent_name"]),
            recipient_parent_uei=parent_uei,
            awarding_agency_name=awarding_agency,
            awarding_subagency_name=awarding_subagency,
            naics_code=self._optional(row["naics_code"]),
            psc_code=self._optional(row["product_or_service_code"]),
            transaction_description=description,
            source_updated_at=source_updated_at,
        )

    @staticmethod
    def _required(row: dict[str, str], field: str, row_number: int) -> str:
        value = row[field]
        if not value:
            raise TransactionExportError(
                f"missing_required_{field}_at_row_{row_number}"
            )
        return value

    @staticmethod
    def _optional(value: str) -> str | None:
        return value or None

    @staticmethod
    def _parse_date(value: str, field: str, row_number: int) -> date:
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise TransactionExportError(
                f"invalid_{field}_at_row_{row_number}"
            ) from None

    @staticmethod
    def _parse_decimal(value: str, row_number: int) -> Decimal:
        if not value:
            raise TransactionExportError(
                f"invalid_federal_action_obligation_at_row_{row_number}"
            )
        try:
            obligation = Decimal(value)
        except (InvalidOperation, ValueError):
            raise TransactionExportError(
                f"invalid_federal_action_obligation_at_row_{row_number}"
            ) from None
        if not obligation.is_finite():
            raise TransactionExportError(
                f"invalid_federal_action_obligation_at_row_{row_number}"
            )
        if obligation.copy_abs() > MAX_NUMERIC_20_2:
            raise TransactionExportError(
                f"federal_action_obligation_out_of_range_at_row_{row_number}"
            )
        try:
            with localcontext(MONEY_CONTEXT):
                quantized = obligation.quantize(MONEY_QUANTUM)
        except DecimalException:
            raise TransactionExportError(
                f"invalid_federal_action_obligation_precision_at_row_{row_number}"
            ) from None
        if quantized != obligation:
            raise TransactionExportError(
                f"excess_federal_action_obligation_scale_at_row_{row_number}"
            )
        return quantized

    @staticmethod
    def _parse_datetime(value: str, row_number: int) -> datetime:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            raise TransactionExportError(
                f"invalid_last_modified_date_at_row_{row_number}"
            ) from None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _parse_uei(value: str, field: str, row_number: int) -> str:
        if not UEI_PATTERN.fullmatch(value):
            raise TransactionExportError(f"invalid_{field}_at_row_{row_number}")
        return value

    @staticmethod
    def _validate_positive_integer(name: str, value: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise TransactionExportError(f"invalid_{name}")


class _BoundedArchiveReader(io.RawIOBase):
    """Count actual expanded bytes instead of trusting ZIP metadata alone."""

    def __init__(self, raw: zipfile.ZipExtFile, *, maximum_bytes: int) -> None:
        super().__init__()
        self._raw = raw
        self._maximum_bytes = maximum_bytes
        self._bytes_read = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray) -> int | None:
        read = self._raw.readinto(buffer)
        if read is None:
            return None
        self._bytes_read += read
        if self._bytes_read > self._maximum_bytes:
            raise TransactionExportError("uncompressed_size_limit_exceeded")
        return read

    def close(self) -> None:
        try:
            self._raw.close()
        finally:
            super().close()


class _BoundedCsvInput(Iterator[str]):
    """Bound CSV fields and logical records before csv.reader allocates them."""

    def __init__(
        self,
        text: io.TextIOWrapper,
        *,
        maximum_field_chars: int,
        maximum_record_chars: int,
    ) -> None:
        self._text = text
        self._maximum_field_chars = maximum_field_chars
        self._maximum_record_chars = maximum_record_chars
        self._record_chars = 0
        self._field_chars = 0
        self._in_quotes = False

    def __iter__(self) -> _BoundedCsvInput:
        return self

    def __next__(self) -> str:
        remaining = self._maximum_record_chars - self._record_chars
        line = self._text.readline(remaining + 1)
        if line == "":
            raise StopIteration
        if len(line) > remaining:
            raise TransactionExportError("csv_record_limit_exceeded")

        self._record_chars += len(line)
        index = 0
        while index < len(line):
            character = line[index]
            if self._in_quotes:
                self._field_chars += 1
                if character == '"':
                    if index + 1 < len(line) and line[index + 1] == '"':
                        self._field_chars += 1
                        index += 1
                    else:
                        self._in_quotes = False
            elif character == ",":
                self._field_chars = 0
            elif character == "\n":
                self._record_chars = 0
                self._field_chars = 0
            else:
                if character == '"' and self._field_chars == 0:
                    self._in_quotes = True
                self._field_chars += 1

            if self._field_chars > self._maximum_field_chars:
                raise TransactionExportError("csv_field_limit_exceeded")
            index += 1
        return line


class TransactionExportReader(Iterator[TransactionRow]):
    """Context-managed owner of one parser stream and all underlying resources."""

    def __init__(self, parser: TransactionExportParser) -> None:
        self._parser = parser
        self._iterator: Iterator[TransactionRow] | None = None
        self._active = False

    def __enter__(self) -> TransactionExportReader:
        if self._active:
            raise TransactionExportError("transaction_export_reader_already_active")
        self._parser._begin()
        self._iterator = self._parser._stream_rows()
        self._active = True
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        try:
            if self._iterator is not None:
                self._iterator.close()
        finally:
            self._active = False
            if self._parser._state == "active":
                self._parser._mark_failed()
        return False

    def __iter__(self) -> TransactionExportReader:
        return self

    def __next__(self) -> TransactionRow:
        if not self._active or self._iterator is None:
            raise TransactionExportError("transaction_export_reader_not_active")
        try:
            return next(self._iterator)
        except StopIteration:
            raise
        except BaseException:
            self._parser._mark_failed()
            raise
