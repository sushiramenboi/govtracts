from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import stat
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable
from urllib.parse import urljoin, urlparse

import httpx


API_HOST = "api.usaspending.gov"
FILE_HOST = "files.usaspending.gov"
MAX_REDIRECTS = 5
AWARD_TYPE_CODES = ["A", "B", "C", "D"]
COUNT_ENDPOINT = "/api/v2/download/count/"
TRANSACTION_DOWNLOAD_ENDPOINT = "/api/v2/download/transactions/"
BULK_MONTHLY_ENDPOINT = "/api/v2/bulk_download/list_monthly_files/"
BULK_DOWNLOAD_ENDPOINT = "/api/v2/bulk_download/awards/"
SPENDING_OVER_TIME_ENDPOINT = "/api/v2/search/spending_over_time/"
CATEGORY_ENDPOINTS = {
    "naics": "/api/v2/search/spending_by_category/naics/",
    "psc": "/api/v2/search/spending_by_category/psc/",
    "awarding_agency": "/api/v2/search/spending_by_category/awarding_agency/",
}

# Internal column identifiers accepted by the transaction download endpoint.
# The resulting CSV headers are discovered and reported rather than assumed.
REQUESTED_COLUMNS = [
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
]

FIELD_ALIASES = {
    "stable_transaction_id": (
        "usaspending_unique_transaction_id",
        "transaction_unique_id",
        "contract_transaction_unique_key",
        "detached_award_proc_unique",
    ),
    "generated_award_id": (
        "contract_award_unique_key",
        "generated_unique_award_id",
        "generated_internal_id",
    ),
    "display_award_id": ("piid", "award_id_piid", "award_id"),
    "award_type": ("type", "award_type_code", "contract_award_type"),
    "action_date": ("action_date",),
    "signed_obligation": ("federal_action_obligation",),
    "recipient_name": ("recipient_name", "recipient_name_raw"),
    "recipient_uei": ("recipient_uei",),
    "parent_recipient_name": (
        "recipient_parent_name",
        "parent_recipient_name",
        "parent_recipient_name_raw",
    ),
    "parent_recipient_uei": ("recipient_parent_uei", "parent_uei", "parent_recipient_uei"),
    "awarding_agency": ("awarding_toptier_agency_name", "awarding_agency_name"),
    "awarding_subagency": ("awarding_subtier_agency_name", "awarding_sub_agency_name"),
    "naics": ("naics_code", "naics"),
    "psc": ("product_or_service_code", "psc"),
    "description": ("transaction_description", "award_description", "description"),
    "last_modified": ("last_modified_date", "action_date_fiscal_year"),
}

REQUIRED_SEMANTICS = {
    "stable_transaction_id",
    "generated_award_id",
    "display_award_id",
    "award_type",
    "action_date",
    "signed_obligation",
    "recipient_name",
    "awarding_agency",
    "awarding_subagency",
    "naics",
    "psc",
    "description",
    "last_modified",
}
GO_REQUIRED_NON_NULL_SEMANTICS = (
    "generated_award_id",
    "display_award_id",
    "recipient_uei",
    "awarding_agency",
    "awarding_subagency",
)


class FeasibilityError(RuntimeError):
    """A bounded Phase 0 failure with a safe, reportable message."""


@dataclass(frozen=True)
class MonthWindow:
    start: date
    end: date

    @classmethod
    def from_strings(cls, start: str, end: str, today: date | None = None) -> "MonthWindow":
        try:
            start_date = date.fromisoformat(start)
            end_date = date.fromisoformat(end)
        except ValueError as exc:
            raise FeasibilityError("start and end must be ISO dates") from exc
        if start_date.day != 1:
            raise FeasibilityError("start must be the first day of a calendar month")
        next_month = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1)
        expected_end = next_month - timedelta(days=1)
        if end_date != expected_end:
            raise FeasibilityError("end must be the final day of the same calendar month")
        if end_date >= (today or date.today()):
            raise FeasibilityError("the sample month must be completed")
        return cls(start=start_date, end=end_date)

    @property
    def fiscal_year(self) -> int:
        return self.start.year + 1 if self.start.month >= 10 else self.start.year

    @property
    def filters(self) -> dict[str, Any]:
        return {
            "award_type_codes": AWARD_TYPE_CODES,
            "time_period": [
                {
                    "start_date": self.start.isoformat(),
                    "end_date": self.end.isoformat(),
                    "date_type": "action_date",
                }
            ],
        }


def _normalise_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def resolve_headers(headers: Iterable[str]) -> dict[str, str | None]:
    normalised = {_normalise_header(header): header for header in headers}
    return {
        semantic: next((normalised[alias] for alias in aliases if alias in normalised), None)
        for semantic, aliases in FIELD_ALIASES.items()
    }


def _as_decimal(value: str) -> Decimal | None:
    if not value.strip():
        return None
    try:
        return Decimal(value.replace(",", "").strip())
    except InvalidOperation:
        return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    stripped = value.strip()
    if stripped.isdigit():
        return float(stripped)
    try:
        retry_at = parsedate_to_datetime(stripped)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


def _structural_checks_pass(inspection: dict[str, Any]) -> bool:
    row_checks_pass = all(
        inspection[field] == 0
        for field in (
            "missing_transaction_ids",
            "duplicate_transaction_rows",
            "invalid_action_dates",
            "outside_window_rows",
            "invalid_obligations",
        )
    )
    null_counts = inspection.get("null_counts") or {}
    required_values_present = all(
        null_counts.get(semantic) == 0 for semantic in GO_REQUIRED_NON_NULL_SEMANTICS
    )
    return row_checks_pass and required_values_present


class FeasibilityHarness:
    def __init__(
        self,
        client: httpx.Client,
        window: MonthWindow,
        output_dir: Path,
        poll_initial_seconds: float = 10.0,
        poll_max_seconds: float = 60.0,
        poll_timeout_seconds: float = 3600.0,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.client = client
        self.window = window
        self.output_dir = output_dir
        self.poll_initial_seconds = poll_initial_seconds
        self.poll_max_seconds = poll_max_seconds
        self.poll_timeout_seconds = poll_timeout_seconds
        self.sleeper = sleeper
        self.journal: dict[str, Any] = {}

    def _checkpoint(self, name: str, value: Any) -> None:
        self.journal[name] = value
        self.output_dir.mkdir(parents=True, exist_ok=True)
        destination = self.output_dir / "run-journal.json"
        temporary = self.output_dir / "run-journal.json.tmp"
        temporary.write_text(
            json.dumps(_json_safe(self.journal), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(destination)

    def _post(self, endpoint: str, body: dict[str, Any]) -> dict[str, Any]:
        request = self.client.build_request("POST", endpoint, json=body)
        response = self._send_with_validated_redirects(request, {API_HOST})
        try:
            response.raise_for_status()
            payload = response.json()
        finally:
            response.close()
        if not isinstance(payload, dict):
            raise FeasibilityError(f"{endpoint} returned a non-object response")
        return payload

    @staticmethod
    def _validate_remote_url(url: str, allowed_hosts: set[str]) -> str:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in allowed_hosts:
            raise FeasibilityError(f"refusing unexpected remote URL host: {parsed.hostname or 'missing'}")
        return url

    def _send_with_validated_redirects(
        self,
        request: httpx.Request,
        allowed_hosts: set[str],
        *,
        stream: bool = False,
    ) -> httpx.Response:
        redirect_count = 0
        current_request = request
        while True:
            self._validate_remote_url(str(current_request.url), allowed_hosts)
            response = self.client.send(
                current_request,
                stream=stream,
                follow_redirects=False,
            )
            next_request = response.next_request
            if next_request is None:
                return response
            response.close()
            redirect_count += 1
            if redirect_count > MAX_REDIRECTS:
                raise FeasibilityError(f"too many redirects (maximum {MAX_REDIRECTS})")
            current_request = next_request

    def preflight(self) -> tuple[dict[str, Any], dict[str, Any]]:
        request = {"filters": self.window.filters, "spending_level": "transactions"}
        response = self._post(COUNT_ENDPOINT, request)
        if response.get("spending_level") != "transactions":
            raise FeasibilityError("count response did not confirm transaction spending level")
        for field in ("calculated_count", "maximum_limit", "rows_gt_limit"):
            if field not in response:
                raise FeasibilityError(f"count response is missing {field}")
        return request, response

    def evaluate_bulk_options(self) -> tuple[dict[str, Any], dict[str, Any]]:
        request = {"agency": "all", "fiscal_year": self.window.fiscal_year, "type": "contracts"}
        response = self._post(BULK_MONTHLY_ENDPOINT, request)
        files = response.get("monthly_files")
        if not isinstance(files, list):
            raise FeasibilityError("bulk monthly listing is missing monthly_files")
        return request, response

    def request_download(self, count_response: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        if bool(count_response["rows_gt_limit"]):
            raise FeasibilityError(
                "custom export exceeds the returned service limit; evaluate official bulk archives instead of date-splitting"
            )
        request = {
            "filters": self.window.filters,
            "columns": REQUESTED_COLUMNS,
            "file_format": "csv",
            "limit": int(count_response["maximum_limit"]),
        }
        response = self._post(TRANSACTION_DOWNLOAD_ENDPOINT, request)
        status_url = response.get("status_url")
        if not isinstance(status_url, str):
            raise FeasibilityError("download response is missing status_url")
        self._validate_remote_url(status_url, {API_HOST})
        return request, response

    def request_bulk_download(self) -> tuple[dict[str, Any], dict[str, Any]]:
        request = {
            "filters": {
                "prime_award_types": AWARD_TYPE_CODES,
                "date_type": "action_date",
                "date_range": {
                    "start_date": self.window.start.isoformat(),
                    "end_date": self.window.end.isoformat(),
                },
                "agencies": [{"type": "awarding", "tier": "toptier", "name": "all"}],
            },
            "file_format": "csv",
            "columns": REQUESTED_COLUMNS,
        }
        response = self._post(BULK_DOWNLOAD_ENDPOINT, request)
        status_url = response.get("status_url")
        if not isinstance(status_url, str):
            raise FeasibilityError("bulk download response is missing status_url")
        self._validate_remote_url(status_url, {API_HOST})
        return request, response

    def poll_download(self, status_url: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        self._validate_remote_url(status_url, {API_HOST})
        deadline = time.monotonic() + self.poll_timeout_seconds
        interval = self.poll_initial_seconds
        observations: list[dict[str, Any]] = []
        while True:
            if time.monotonic() >= deadline:
                raise FeasibilityError("timed out waiting for USAspending export")
            request = self.client.build_request("GET", status_url)
            response = self._send_with_validated_redirects(request, {API_HOST})
            try:
                response.raise_for_status()
                payload = response.json()
                retry_after = response.headers.get("Retry-After")
            finally:
                response.close()
            if not isinstance(payload, dict):
                raise FeasibilityError("status endpoint returned a non-object response")
            observations.append(payload)
            self._checkpoint("status_observations", observations)
            status_value = str(payload.get("status", "")).lower()
            if status_value == "finished":
                return observations, payload
            if status_value == "failed":
                message = str(payload.get("message") or "no failure message")
                raise FeasibilityError(f"USAspending export reported failed status: {message}")
            if status_value not in {"ready", "running"}:
                raise FeasibilityError(f"unexpected export status: {status_value or 'missing'}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise FeasibilityError("timed out waiting for USAspending export")
            server_delay = _retry_after_seconds(retry_after)
            backoff_delay = min(interval, self.poll_max_seconds)
            requested_delay = max(backoff_delay, server_delay or 0.0)
            self.sleeper(min(requested_delay, remaining))
            interval = min(self.poll_max_seconds, max(interval * 1.5, self.poll_initial_seconds))

    def download_zip(self, file_url: str) -> tuple[Path, dict[str, Any]]:
        self._validate_remote_url(file_url, {FILE_HOST})
        self.output_dir.mkdir(parents=True, exist_ok=True)
        destination = self.output_dir / "transactions.zip"
        started = time.perf_counter()
        downloaded = 0
        request = self.client.build_request("GET", file_url)
        response = self._send_with_validated_redirects(request, {FILE_HOST}, stream=True)
        try:
            response.raise_for_status()
            with destination.open("wb") as stream:
                for chunk in response.iter_bytes():
                    stream.write(chunk)
                    downloaded += len(chunk)
        finally:
            response.close()
        elapsed = time.perf_counter() - started
        return destination, {
            "downloaded_bytes": downloaded,
            "seconds": elapsed,
            "bytes_per_second": downloaded / elapsed if elapsed else None,
        }

    def inspect_zip(self, archive_path: Path) -> dict[str, Any]:
        started = time.perf_counter()
        row_count = 0
        unique_transactions: set[str] = set()
        unique_awards: set[str] = set()
        duplicate_transaction_rows = 0
        missing_transaction_ids = 0
        invalid_action_dates = 0
        outside_window_rows = 0
        invalid_obligations = 0
        obligation_sum = Decimal("0")
        positive_sum = Decimal("0")
        negative_sum = Decimal("0")
        positive_rows = 0
        negative_rows = 0
        zero_rows = 0
        headers_by_file: dict[str, list[str]] = {}
        aliases_by_file: dict[str, dict[str, str | None]] = {}
        nulls: dict[str, int] = {semantic: 0 for semantic in FIELD_ALIASES}
        null_obligation_sums: dict[str, Decimal] = {
            semantic: Decimal("0") for semantic in FIELD_ALIASES
        }
        distinct_values: dict[str, set[str]] = {
            "award_type": set(),
            "awarding_agency": set(),
            "awarding_subagency": set(),
            "recipient_name": set(),
            "naics": set(),
            "psc": set(),
        }
        modeled_heap_bytes = 0
        modeled_transaction_index_bytes = 0
        modeled_award_index_bytes = 0
        csv_members: list[dict[str, Any]] = []

        with zipfile.ZipFile(archive_path) as archive:
            members = archive.infolist()
            if not members:
                raise FeasibilityError("download ZIP is empty")
            for member in members:
                path = PurePosixPath(member.filename)
                if path.is_absolute() or ".." in path.parts:
                    raise FeasibilityError(f"unsafe ZIP member path: {member.filename}")
                unix_mode = member.external_attr >> 16
                if stat.S_ISLNK(unix_mode):
                    raise FeasibilityError(f"ZIP member is a symbolic link: {member.filename}")
                if member.flag_bits & 0x1:
                    raise FeasibilityError(f"ZIP member is encrypted: {member.filename}")
                if member.is_dir():
                    continue
                if path.suffix.lower() != ".csv":
                    continue

                file_rows = 0
                with archive.open(member) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                    reader = csv.DictReader(text)
                    if reader.fieldnames is None:
                        raise FeasibilityError(f"CSV member has no header: {member.filename}")
                    headers = list(reader.fieldnames)
                    aliases = resolve_headers(headers)
                    missing = sorted(semantic for semantic in REQUIRED_SEMANTICS if aliases[semantic] is None)
                    if missing:
                        raise FeasibilityError(
                            f"CSV member {member.filename} is missing required semantics: {', '.join(missing)}"
                        )
                    headers_by_file[member.filename] = headers
                    aliases_by_file[member.filename] = aliases
                    for row in reader:
                        row_count += 1
                        file_rows += 1
                        values = {
                            semantic: (row.get(header, "") if header else "").strip()
                            for semantic, header in aliases.items()
                        }
                        for semantic, value in values.items():
                            if not value:
                                nulls[semantic] += 1
                        transaction_id = values["stable_transaction_id"]
                        if not transaction_id:
                            missing_transaction_ids += 1
                        elif transaction_id in unique_transactions:
                            duplicate_transaction_rows += 1
                        else:
                            unique_transactions.add(transaction_id)
                        if values["generated_award_id"]:
                            unique_awards.add(values["generated_award_id"])

                        try:
                            action_date = date.fromisoformat(values["action_date"][:10])
                            if not self.window.start <= action_date <= self.window.end:
                                outside_window_rows += 1
                        except ValueError:
                            invalid_action_dates += 1

                        obligation = _as_decimal(values["signed_obligation"])
                        if obligation is None:
                            invalid_obligations += 1
                        else:
                            obligation_sum += obligation
                            for semantic, value in values.items():
                                if not value:
                                    null_obligation_sums[semantic] += obligation
                            if obligation > 0:
                                positive_rows += 1
                                positive_sum += obligation
                            elif obligation < 0:
                                negative_rows += 1
                                negative_sum += obligation
                            else:
                                zero_rows += 1

                        for semantic in distinct_values:
                            if values[semantic]:
                                distinct_values[semantic].add(values[semantic])

                        encoded_value_bytes = sum(len(value.encode("utf-8")) for value in values.values())
                        nonempty_text_fields = sum(bool(value) for value in values.values())
                        # Transparent planning model, not a final PostgreSQL schema estimate:
                        # tuple header + null bitmap + varlena headers + observed UTF-8 payload.
                        row_heap = 24 + math.ceil(len(values) / 8) + 4 * nonempty_text_fields + encoded_value_bytes
                        modeled_heap_bytes += math.ceil(row_heap / 8) * 8
                        if transaction_id:
                            index_width = 16 + 4 + len(transaction_id.encode("utf-8"))
                            modeled_transaction_index_bytes += math.ceil(index_width / 8) * 8
                        award_id = values["generated_award_id"]
                        if award_id:
                            index_width = 16 + 4 + len(award_id.encode("utf-8"))
                            modeled_award_index_bytes += math.ceil(index_width / 8) * 8

                csv_members.append(
                    {
                        "name": member.filename,
                        "compressed_bytes": member.compress_size,
                        "uncompressed_bytes": member.file_size,
                        "rows": file_rows,
                        "crc32": f"{member.CRC:08x}",
                    }
                )

        if not csv_members:
            raise FeasibilityError("download ZIP contains no CSV members")
        elapsed = time.perf_counter() - started
        return {
            "archive_bytes": archive_path.stat().st_size,
            "csv_uncompressed_bytes": sum(item["uncompressed_bytes"] for item in csv_members),
            "members": csv_members,
            "headers_by_file": headers_by_file,
            "aliases_by_file": aliases_by_file,
            "rows": row_count,
            "unique_transaction_ids": len(unique_transactions),
            "duplicate_transaction_rows": duplicate_transaction_rows,
            "missing_transaction_ids": missing_transaction_ids,
            "unique_generated_award_ids": len(unique_awards),
            "invalid_action_dates": invalid_action_dates,
            "outside_window_rows": outside_window_rows,
            "invalid_obligations": invalid_obligations,
            "positive_rows": positive_rows,
            "negative_rows": negative_rows,
            "zero_rows": zero_rows,
            "gross_positive": positive_sum,
            "signed_deobligations": negative_sum,
            "deobligation_magnitude": -negative_sum,
            "net_obligations": obligation_sum,
            "null_counts": nulls,
            "null_obligation_sums": null_obligation_sums,
            "distinct_counts": {key: len(value) for key, value in distinct_values.items()},
            "parse_seconds": elapsed,
            "rows_per_second": row_count / elapsed if elapsed else None,
            "postgres_footprint_model": {
                "method": "observed UTF-8 payload plus documented tuple/varlena/index-entry assumptions; excludes table/page fill, WAL, TOAST, and future columns",
                "heap_bytes": modeled_heap_bytes,
                "stable_transaction_id_index_bytes": modeled_transaction_index_bytes,
                "generated_award_id_index_bytes": modeled_award_index_bytes,
                "total_modeled_bytes": modeled_heap_bytes
                + modeled_transaction_index_bytes
                + modeled_award_index_bytes,
            },
        }

    def aggregate_reconciliation(self) -> dict[str, Any]:
        common = {"filters": self.window.filters, "spending_level": "transactions"}
        time_request = {**common, "group": "month"}
        time_response = self._post(SPENDING_OVER_TIME_ENDPOINT, time_request)
        time_total = sum(
            (_as_decimal(str(row.get("aggregated_amount", ""))) or Decimal("0"))
            for row in time_response.get("results", [])
            if isinstance(row, dict)
        )
        categories: dict[str, Any] = {}
        for name, endpoint in CATEGORY_ENDPOINTS.items():
            page = 1
            amount = Decimal("0")
            result_count = 0
            page_count = 0
            while True:
                request = {**common, "limit": 100, "page": page}
                response = self._post(endpoint, request)
                results = response.get("results", [])
                if not isinstance(results, list):
                    raise FeasibilityError(f"{name} aggregate returned invalid results")
                for row in results:
                    if isinstance(row, dict):
                        amount += _as_decimal(str(row.get("amount", ""))) or Decimal("0")
                result_count += len(results)
                page_count += 1
                metadata = response.get("page_metadata") or {}
                if not metadata.get("hasNext"):
                    break
                page += 1
            categories[name] = {
                "endpoint": endpoint,
                "pages": page_count,
                "categories": result_count,
                "summed_amount": amount,
            }
        return {
            "spending_over_time": {
                "request": time_request,
                "result_count": len(time_response.get("results", [])),
                "summed_amount": time_total,
            },
            "categories": categories,
        }

    def run(self) -> dict[str, Any]:
        started_at = datetime.now().astimezone().isoformat()
        count_request, count_response = self.preflight()
        self._checkpoint("count", {"request": count_request, "response": count_response})
        bulk_request, bulk_response = self.evaluate_bulk_options()
        self._checkpoint("bulk_options", {"request": bulk_request, "response": bulk_response})
        result: dict[str, Any] = {
            "started_at": started_at,
            "window": {
                "start": self.window.start,
                "end": self.window.end,
                "fiscal_year": self.window.fiscal_year,
                "award_type_codes": AWARD_TYPE_CODES,
            },
            "count": {"request": count_request, "response": count_response},
            "bulk_options": {"request": bulk_request, "response": bulk_response},
        }
        if bool(count_response["rows_gt_limit"]):
            result["recommendation"] = {
                "decision": "NO-GO",
                "reason": "completed month exceeds the returned custom-export limit; use an approved bulk/archive path rather than limit-avoidance date partitions",
            }
            return result

        download_request, download_response = self.request_download(count_response)
        self._checkpoint("download_submission", {"request": download_request, "response": download_response})
        observations, final_status = self.poll_download(str(download_response["status_url"]))
        file_url = final_status.get("file_url") or download_response.get("file_url")
        if not isinstance(file_url, str):
            raise FeasibilityError("finished export did not provide a file URL")
        archive_path, download_metrics = self.download_zip(file_url)
        self._checkpoint("download_transfer", download_metrics)
        inspection = self.inspect_zip(archive_path)
        self._checkpoint("inspection", inspection)
        reconciliation = self.aggregate_reconciliation()
        csv_total = Decimal(str(inspection["net_obligations"]))
        reconciliation["deltas_from_csv_net"] = {
            "spending_over_time": csv_total - Decimal(str(reconciliation["spending_over_time"]["summed_amount"])),
            **{
                name: csv_total - Decimal(str(detail["summed_amount"]))
                for name, detail in reconciliation["categories"].items()
            },
        }
        reconciliation["category_residuals_after_unclassified"] = {
            "awarding_agency": reconciliation["deltas_from_csv_net"]["awarding_agency"]
            - Decimal(str(inspection["null_obligation_sums"]["awarding_agency"])),
            "naics": reconciliation["deltas_from_csv_net"]["naics"]
            - Decimal(str(inspection["null_obligation_sums"]["naics"])),
            "psc": reconciliation["deltas_from_csv_net"]["psc"]
            - Decimal(str(inspection["null_obligation_sums"]["psc"])),
        }
        expected = int(count_response["calculated_count"])
        exact_count_match = expected == inspection["rows"] == inspection["unique_transaction_ids"]
        structural_checks = _structural_checks_pass(inspection)
        exact_time_reconciliation = reconciliation["deltas_from_csv_net"]["spending_over_time"] == 0
        exact_category_reconciliation = all(
            residual == 0 for residual in reconciliation["category_residuals_after_unclassified"].values()
        )
        decision = (
            "GO"
            if exact_count_match
            and structural_checks
            and exact_time_reconciliation
            and exact_category_reconciliation
            else "NO-GO"
        )
        result.update(
            {
                "download": {
                    "request": download_request,
                    "response": download_response,
                    "status_observations": observations,
                    "final_status": final_status,
                    "transfer": download_metrics,
                },
                "inspection": inspection,
                "reconciliation": reconciliation,
                "recommendation": {
                    "decision": decision,
                    "exact_count_match": exact_count_match,
                    "structural_checks_pass": structural_checks,
                    "exact_spending_over_time_reconciliation": exact_time_reconciliation,
                    "exact_category_reconciliation_after_unclassified": exact_category_reconciliation,
                    "reason": (
                        "sample satisfies the Phase 0 count, structure, and net-obligation gates"
                        if decision == "GO"
                        else "one or more exact Phase 0 gates failed; inspect evidence before designing Phase 1"
                    ),
                },
            }
        )
        self._checkpoint("completed_result", result)
        return result

    def run_bulk(
        self,
        resume_status_url: str | None = None,
        resume_file_url: str | None = None,
    ) -> dict[str, Any]:
        started_at = datetime.now().astimezone().isoformat()
        count_request, count_response = self.preflight()
        self._checkpoint("count", {"request": count_request, "response": count_response})
        bulk_list_request, bulk_list_response = self.evaluate_bulk_options()
        self._checkpoint(
            "bulk_options",
            {"request": bulk_list_request, "response": bulk_list_response},
        )
        if resume_status_url:
            self._validate_remote_url(resume_status_url, {API_HOST})
            if not resume_file_url:
                raise FeasibilityError("--resume-file-url is required with --resume-status-url")
            self._validate_remote_url(resume_file_url, {FILE_HOST})
            bulk_request = {
                "filters": {
                    "prime_award_types": AWARD_TYPE_CODES,
                    "date_type": "action_date",
                    "date_range": {
                        "start_date": self.window.start.isoformat(),
                        "end_date": self.window.end.isoformat(),
                    },
                    "agencies": [{"type": "awarding", "tier": "toptier", "name": "all"}],
                },
                "file_format": "csv",
                "columns": REQUESTED_COLUMNS,
            }
            bulk_response = {
                "status_url": resume_status_url,
                "file_url": resume_file_url,
                "resumed_existing_job": True,
            }
        else:
            bulk_request, bulk_response = self.request_bulk_download()
        self._checkpoint(
            "bulk_fallback_submission",
            {"request": bulk_request, "response": bulk_response},
        )
        observations, final_status = self.poll_download(str(bulk_response["status_url"]))
        file_url = final_status.get("file_url") or bulk_response.get("file_url")
        if not isinstance(file_url, str):
            raise FeasibilityError("finished bulk export did not provide a file URL")
        archive_path, download_metrics = self.download_zip(file_url)
        self._checkpoint("download_transfer", download_metrics)
        inspection = self.inspect_zip(archive_path)
        self._checkpoint("inspection", inspection)
        reconciliation = self.aggregate_reconciliation()
        csv_total = Decimal(str(inspection["net_obligations"]))
        reconciliation["deltas_from_csv_net"] = {
            "spending_over_time": csv_total - Decimal(str(reconciliation["spending_over_time"]["summed_amount"])),
            **{
                name: csv_total - Decimal(str(detail["summed_amount"]))
                for name, detail in reconciliation["categories"].items()
            },
        }
        reconciliation["category_residuals_after_unclassified"] = {
            "awarding_agency": reconciliation["deltas_from_csv_net"]["awarding_agency"]
            - Decimal(str(inspection["null_obligation_sums"]["awarding_agency"])),
            "naics": reconciliation["deltas_from_csv_net"]["naics"]
            - Decimal(str(inspection["null_obligation_sums"]["naics"])),
            "psc": reconciliation["deltas_from_csv_net"]["psc"]
            - Decimal(str(inspection["null_obligation_sums"]["psc"])),
        }
        expected = int(count_response["calculated_count"])
        exact_count_match = expected == inspection["rows"] == inspection["unique_transaction_ids"]
        structural_checks = _structural_checks_pass(inspection)
        exact_time_reconciliation = reconciliation["deltas_from_csv_net"]["spending_over_time"] == 0
        exact_category_reconciliation = all(
            residual == 0 for residual in reconciliation["category_residuals_after_unclassified"].values()
        )
        decision = (
            "GO"
            if exact_count_match
            and structural_checks
            and exact_time_reconciliation
            and exact_category_reconciliation
            else "NO-GO"
        )
        result = {
            "started_at": started_at,
            "source_mode": "official_bulk_download",
            "window": {
                "start": self.window.start,
                "end": self.window.end,
                "fiscal_year": self.window.fiscal_year,
                "award_type_codes": AWARD_TYPE_CODES,
            },
            "count": {"request": count_request, "response": count_response},
            "bulk_options": {"request": bulk_list_request, "response": bulk_list_response},
            "download": {
                "request": bulk_request,
                "response": bulk_response,
                "status_observations": observations,
                "final_status": final_status,
                "transfer": download_metrics,
            },
            "inspection": inspection,
            "reconciliation": reconciliation,
            "recommendation": {
                "decision": decision,
                "exact_count_match": exact_count_match,
                "structural_checks_pass": structural_checks,
                "exact_spending_over_time_reconciliation": exact_time_reconciliation,
                "exact_category_reconciliation_after_unclassified": exact_category_reconciliation,
                "reason": (
                    "bulk sample satisfies the Phase 0 count, structure, and net-obligation gates"
                    if decision == "GO"
                    else "one or more exact Phase 0 bulk gates failed; inspect evidence before Phase 1"
                ),
            },
        }
        self._checkpoint("completed_result", result)
        return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the isolated USAspending Phase 0 feasibility probe.")
    parser.add_argument("--start-date", default=os.getenv("USASPENDING_SAMPLE_START", "2024-10-01"))
    parser.add_argument("--end-date", default=os.getenv("USASPENDING_SAMPLE_END", "2024-10-31"))
    parser.add_argument("--base-url", default=os.getenv("USASPENDING_BASE_URL", "https://api.usaspending.gov"))
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/govtracts-usaspending-phase0"))
    parser.add_argument("--poll-initial-seconds", type=float, default=10.0)
    parser.add_argument("--poll-max-seconds", type=float, default=60.0)
    parser.add_argument("--poll-timeout-seconds", type=float, default=3600.0)
    parser.add_argument("--mode", choices=("transactions", "bulk"), default="bulk")
    parser.add_argument("--resume-status-url")
    parser.add_argument("--resume-file-url")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    window = MonthWindow.from_strings(args.start_date, args.end_date)
    timeout = httpx.Timeout(connect=30.0, read=120.0, write=120.0, pool=30.0)
    with httpx.Client(base_url=args.base_url.rstrip("/"), timeout=timeout, follow_redirects=False) as client:
        harness = FeasibilityHarness(
            client=client,
            window=window,
            output_dir=args.output_dir,
            poll_initial_seconds=args.poll_initial_seconds,
            poll_max_seconds=args.poll_max_seconds,
            poll_timeout_seconds=args.poll_timeout_seconds,
        )
        try:
            if args.mode == "bulk":
                result = harness.run_bulk(args.resume_status_url, args.resume_file_url)
            else:
                if args.resume_status_url or args.resume_file_url:
                    raise FeasibilityError("resume URLs are supported only in bulk mode")
                result = harness.run()
        except Exception as exc:
            failure = {
                "decision": "NO-GO",
                "error_type": type(exc).__name__,
                "reason": str(exc),
                "journal": str(args.output_dir / "run-journal.json"),
            }
            args.output_dir.mkdir(parents=True, exist_ok=True)
            (args.output_dir / "failure.json").write_text(
                json.dumps(failure, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            print(json.dumps(failure, indent=2, sort_keys=True))
            return 2
    args.output_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = args.output_dir / "evidence.json"
    evidence_path.write_text(json.dumps(_json_safe(result), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(_json_safe(result["recommendation"]), indent=2, sort_keys=True))
    print(f"Evidence: {evidence_path}")
    return 0 if result["recommendation"]["decision"] == "GO" else 2


if __name__ == "__main__":
    raise SystemExit(main())
