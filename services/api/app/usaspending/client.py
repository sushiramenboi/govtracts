from __future__ import annotations

import asyncio
import math
import os
import tempfile
import time
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlparse

import httpx

from app.core.config import Settings


API_HOST = "api.usaspending.gov"
FILE_HOST = "files.usaspending.gov"
COUNT_ENDPOINT = "/api/v2/download/count/"
BULK_EXPORT_ENDPOINT = "/api/v2/bulk_download/awards/"
MAX_REDIRECTS = 5
DEFAULT_MAX_ARCHIVE_BYTES = 1024 * 1024 * 1024
DEFAULT_TRANSFER_TIMEOUT_SECONDS = 3600.0
PRIME_CONTRACT_AWARD_TYPES = ("A", "B", "C", "D")
BULK_TRANSACTION_FIELDS = (
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
AWARD_SEARCH_ENDPOINT = "/api/v2/search/spending_by_award/"
AWARD_FIELDS = [
    "Award ID",
    "Recipient Name",
    "Recipient DUNS Number",
    "Recipient UEI",
    "recipient_id",
    "Awarding Agency",
    "Awarding Agency Code",
    "Description",
    "Base Obligation Date",
    "Start Date",
    "End Date",
    "Award Amount",
    "Award Type",
    "Contract Award Type",
    "NAICS",
    "PSC",
    "Last Modified Date",
    "generated_internal_id",
]
REDIRECT_STATUS_CODES = frozenset({301, 302, 303, 307, 308})


class UsaSpendingError(RuntimeError):
    """A safe, categorised upstream error suitable for logs and run records."""


@dataclass(frozen=True)
class BulkExportJob:
    """Validated information returned when USAspending accepts a bulk export."""

    status_url: str
    file_url: str
    file_name: str


@dataclass(frozen=True)
class BulkExportStatus:
    """Validated terminal status suitable for later checkpoint persistence."""

    status: str
    status_url: str
    file_url: str
    file_name: str
    message: str | None
    seconds_elapsed: str | None


class UsaSpendingClient:
    def __init__(self, settings: Settings) -> None:
        self.base_url = settings.usaspending_base_url.rstrip("/")
        self.max_retries = settings.usaspending_max_retries
        self.timeout = httpx.Timeout(
            connect=settings.usaspending_connect_timeout_seconds,
            read=settings.usaspending_read_timeout_seconds,
            write=settings.usaspending_read_timeout_seconds,
            pool=settings.usaspending_connect_timeout_seconds,
        )

    def iter_award_pages(self, filters: dict[str, Any], page_size: int, max_pages: int | None = None) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
        page = 1
        while max_pages is None or page <= max_pages:
            request_body = {
                "filters": filters,
                "fields": AWARD_FIELDS,
                "limit": page_size,
                "page": page,
                "sort": "Award Amount",
                "order": "desc",
            }
            payload = self._post(AWARD_SEARCH_ENDPOINT, request_body)
            yield request_body, payload
            metadata = payload.get("page_metadata") or {}
            if not metadata.get("hasNext"):
                return
            page += 1

    def get_expected_transaction_count(self, start_date: date, end_date: date) -> int:
        """Return USAspending's expected transaction count for one action-date period."""
        self._validate_period(start_date, end_date)
        payload, _ = self._request_json(
            "POST",
            COUNT_ENDPOINT,
            body={
                "filters": {
                    "award_type_codes": list(PRIME_CONTRACT_AWARD_TYPES),
                    "time_period": [
                        {
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                            "date_type": "action_date",
                        }
                    ],
                },
                "spending_level": "transactions",
            },
            allowed_hosts={API_HOST},
            url_category="api",
        )
        count = payload.get("calculated_count")
        if (
            payload.get("spending_level") != "transactions"
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
        ):
            raise UsaSpendingError("invalid_count_response")
        return count

    def submit_bulk_export(self, start_date: date, end_date: date) -> BulkExportJob:
        """Submit the approved A-D prime-contract transaction export."""
        self._validate_period(start_date, end_date)
        payload, _ = self._request_json(
            "POST",
            BULK_EXPORT_ENDPOINT,
            body={
                "filters": {
                    "prime_award_types": list(PRIME_CONTRACT_AWARD_TYPES),
                    "date_type": "action_date",
                    "date_range": {
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                    "agencies": [{"type": "awarding", "tier": "toptier", "name": "all"}],
                },
                "file_format": "csv",
                "columns": list(BULK_TRANSACTION_FIELDS),
            },
            allowed_hosts={API_HOST},
            url_category="api",
        )
        status_url = payload.get("status_url")
        file_url = payload.get("file_url")
        file_name = payload.get("file_name")
        if not all(isinstance(value, str) and value for value in (status_url, file_url, file_name)):
            raise UsaSpendingError("invalid_bulk_export_response")
        self._validate_remote_url(status_url, {API_HOST}, "status")
        self._validate_remote_url(file_url, {FILE_HOST}, "file")
        return BulkExportJob(
            status_url=status_url,
            file_url=file_url,
            file_name=file_name,
        )

    async def poll_bulk_export(
        self,
        job: BulkExportJob,
        *,
        poll_initial_seconds: float = 10.0,
        poll_max_seconds: float = 60.0,
        poll_timeout_seconds: float = 3600.0,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        utcnow: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> BulkExportStatus:
        """Poll an accepted job under one cancellable wall-clock deadline."""
        self._validate_poll_configuration(
            poll_initial_seconds,
            poll_max_seconds,
            poll_timeout_seconds,
        )
        self._validate_remote_url(job.status_url, {API_HOST}, "status")
        interval = poll_initial_seconds

        try:
            async with asyncio.timeout(poll_timeout_seconds):
                while True:
                    payload, retry_after = await self._async_request_json(
                        "GET",
                        job.status_url,
                        allowed_hosts={API_HOST},
                        url_category="status",
                    )
                    raw_status = payload.get("status")
                    if not isinstance(raw_status, str) or not raw_status.strip():
                        raise UsaSpendingError("invalid_bulk_export_status")
                    status = raw_status.strip().lower()

                    if status == "finished":
                        file_url = payload.get("file_url") or job.file_url
                        file_name = payload.get("file_name") or job.file_name
                        message = payload.get("message")
                        seconds_elapsed = payload.get("seconds_elapsed")
                        if not isinstance(file_url, str) or not file_url:
                            raise UsaSpendingError("invalid_bulk_export_status")
                        if not isinstance(file_name, str) or not file_name:
                            raise UsaSpendingError("invalid_bulk_export_status")
                        if message is not None and not isinstance(message, str):
                            raise UsaSpendingError("invalid_bulk_export_status")
                        if seconds_elapsed is not None and not isinstance(seconds_elapsed, str):
                            raise UsaSpendingError("invalid_bulk_export_status")
                        self._validate_remote_url(file_url, {FILE_HOST}, "file")
                        return BulkExportStatus(
                            status=status,
                            status_url=job.status_url,
                            file_url=file_url,
                            file_name=file_name,
                            message=message,
                            seconds_elapsed=seconds_elapsed,
                        )
                    if status in {"failed", "rejected"}:
                        raise UsaSpendingError(f"bulk_export_{status}")
                    if status not in {"ready", "running"}:
                        raise UsaSpendingError("invalid_bulk_export_status")

                    server_delay = self._retry_after_seconds(retry_after, utcnow())
                    backoff_delay = min(interval, poll_max_seconds)
                    requested_delay = max(backoff_delay, server_delay or 0.0)
                    await sleeper(requested_delay)
                    interval = min(
                        poll_max_seconds,
                        max(interval * 1.5, poll_initial_seconds),
                    )
        except TimeoutError:
            raise UsaSpendingError("bulk_export_timeout") from None

    async def retrieve_bulk_export(
        self,
        completed: BulkExportStatus,
        destination: str | os.PathLike[str],
        *,
        max_archive_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES,
        transfer_timeout_seconds: float = DEFAULT_TRANSFER_TIMEOUT_SECONDS,
    ) -> int:
        """Download to a temporary sibling and atomically replace on success."""
        if completed.status != "finished":
            raise UsaSpendingError("bulk_export_not_finished")
        self._validate_transfer_configuration(max_archive_bytes, transfer_timeout_seconds)
        self._validate_remote_url(completed.file_url, {FILE_HOST}, "file")
        destination_path = Path(destination)

        try:
            temporary_fd, temporary_name = tempfile.mkstemp(
                prefix=f".{destination_path.name}.",
                suffix=".tmp",
                dir=destination_path.parent,
            )
        except OSError:
            raise UsaSpendingError("bulk_export_destination_error") from None

        temporary_path = Path(temporary_name)
        downloaded = 0
        try:
            try:
                async with asyncio.timeout(transfer_timeout_seconds):
                    async with self._async_http_client() as client:
                        request = client.build_request("GET", completed.file_url)
                        response = await self._async_send_with_validated_redirects(
                            client,
                            request,
                            {FILE_HOST},
                            "file",
                            stream=True,
                        )
                        try:
                            self._raise_for_status(response)
                            declared_size = self._content_length(response)
                            if declared_size is not None and declared_size > max_archive_bytes:
                                raise UsaSpendingError("bulk_export_archive_too_large")

                            try:
                                with os.fdopen(temporary_fd, "wb") as temporary_file:
                                    temporary_fd = -1
                                    async for chunk in response.aiter_bytes():
                                        if downloaded + len(chunk) > max_archive_bytes:
                                            raise UsaSpendingError("bulk_export_archive_too_large")
                                        self._write_chunk(temporary_file, chunk)
                                        downloaded += len(chunk)
                                    temporary_file.flush()
                                    os.fsync(temporary_file.fileno())
                            except OSError:
                                raise UsaSpendingError(
                                    "bulk_export_destination_write_failed"
                                ) from None
                        finally:
                            await response.aclose()
            except httpx.RequestError:
                raise UsaSpendingError("timeout_or_network") from None
            except TimeoutError:
                raise UsaSpendingError("bulk_export_transfer_timeout") from None

            try:
                os.replace(temporary_path, destination_path)
            except OSError:
                raise UsaSpendingError("bulk_export_destination_error") from None
            return downloaded
        finally:
            if temporary_fd >= 0:
                try:
                    os.close(temporary_fd)
                except OSError:
                    pass
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                raise UsaSpendingError("bulk_export_partial_cleanup_failed") from None

    def _http_client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )

    def _async_http_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        body: dict[str, Any] | None = None,
        allowed_hosts: set[str],
        url_category: str,
    ) -> tuple[dict[str, Any], str | None]:
        with self._http_client() as client:
            try:
                request = client.build_request(method, url, json=body)
                response = self._send_with_validated_redirects(
                    client,
                    request,
                    allowed_hosts,
                    url_category,
                )
                try:
                    self._raise_for_status(response)
                    try:
                        payload = response.json()
                    except ValueError:
                        raise UsaSpendingError("invalid_response") from None
                    retry_after = response.headers.get("Retry-After")
                finally:
                    response.close()
            except httpx.RequestError:
                raise UsaSpendingError("timeout_or_network") from None
        if not isinstance(payload, dict):
            raise UsaSpendingError("invalid_response")
        return payload, retry_after

    async def _async_request_json(
        self,
        method: str,
        url: str,
        *,
        body: dict[str, Any] | None = None,
        allowed_hosts: set[str],
        url_category: str,
    ) -> tuple[dict[str, Any], str | None]:
        async with self._async_http_client() as client:
            try:
                request = client.build_request(method, url, json=body)
                response = await self._async_send_with_validated_redirects(
                    client,
                    request,
                    allowed_hosts,
                    url_category,
                )
                try:
                    self._raise_for_status(response)
                    try:
                        payload = response.json()
                    except ValueError:
                        raise UsaSpendingError("invalid_response") from None
                    retry_after = response.headers.get("Retry-After")
                finally:
                    await response.aclose()
            except httpx.RequestError:
                raise UsaSpendingError("timeout_or_network") from None
        if not isinstance(payload, dict):
            raise UsaSpendingError("invalid_response")
        return payload, retry_after

    def _send_with_validated_redirects(
        self,
        client: httpx.Client,
        request: httpx.Request,
        allowed_hosts: set[str],
        url_category: str,
    ) -> httpx.Response:
        redirect_count = 0
        current_request = request
        while True:
            self._validate_remote_url(str(current_request.url), allowed_hosts, url_category)
            response = client.send(current_request, follow_redirects=False)
            if response.status_code not in REDIRECT_STATUS_CODES:
                return response
            next_request = response.next_request
            if next_request is None:
                response.close()
                raise UsaSpendingError("invalid_redirect")
            redirect_count += 1
            if redirect_count > MAX_REDIRECTS:
                response.close()
                raise UsaSpendingError("too_many_redirects")
            response.close()
            self._validate_remote_url(str(next_request.url), allowed_hosts, url_category)
            current_request = next_request

    async def _async_send_with_validated_redirects(
        self,
        client: httpx.AsyncClient,
        request: httpx.Request,
        allowed_hosts: set[str],
        url_category: str,
        *,
        stream: bool = False,
    ) -> httpx.Response:
        redirect_count = 0
        current_request = request
        while True:
            self._validate_remote_url(str(current_request.url), allowed_hosts, url_category)
            response = await client.send(
                current_request,
                stream=stream,
                follow_redirects=False,
            )
            if response.status_code not in REDIRECT_STATUS_CODES:
                return response
            next_request = response.next_request
            if next_request is None:
                await response.aclose()
                raise UsaSpendingError("invalid_redirect")
            redirect_count += 1
            if redirect_count > MAX_REDIRECTS:
                await response.aclose()
                raise UsaSpendingError("too_many_redirects")
            await response.aclose()
            self._validate_remote_url(str(next_request.url), allowed_hosts, url_category)
            current_request = next_request

    @staticmethod
    def _content_length(response: httpx.Response) -> int | None:
        raw_value = response.headers.get("Content-Length")
        if raw_value is None:
            return None
        try:
            content_length = int(raw_value)
        except ValueError:
            raise UsaSpendingError("invalid_content_length") from None
        if content_length < 0:
            raise UsaSpendingError("invalid_content_length")
        return content_length

    @staticmethod
    def _write_chunk(destination: BinaryIO, chunk: bytes) -> None:
        written = destination.write(chunk)
        if written != len(chunk):
            raise OSError("short archive write")

    @staticmethod
    def _validate_remote_url(url: str, allowed_hosts: set[str], category: str) -> None:
        try:
            parsed = urlparse(url)
            port = parsed.port
        except ValueError:
            raise UsaSpendingError(f"unsafe_{category}_url") from None
        if (
            parsed.scheme != "https"
            or parsed.hostname not in allowed_hosts
            or port not in {None, 443}
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise UsaSpendingError(f"unsafe_{category}_url")

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.status_code >= 400:
            raise UsaSpendingError(f"http_{response.status_code}")

    @staticmethod
    def _retry_after_seconds(value: str | None, now: datetime) -> float | None:
        if not value:
            return None
        stripped = value.strip()
        if stripped.isdigit():
            try:
                return float(stripped)
            except OverflowError:
                return None
        try:
            retry_at = parsedate_to_datetime(stripped)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - now).total_seconds())

    @staticmethod
    def _validate_period(start_date: date, end_date: date) -> None:
        if (
            not isinstance(start_date, date)
            or not isinstance(end_date, date)
            or end_date < start_date
        ):
            raise UsaSpendingError("invalid_action_date_period")

    @staticmethod
    def _validate_poll_configuration(
        initial_seconds: float,
        max_seconds: float,
        timeout_seconds: float,
    ) -> None:
        values = (initial_seconds, max_seconds, timeout_seconds)
        if (
            any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
                for value in values
            )
            or max_seconds < initial_seconds
        ):
            raise UsaSpendingError("invalid_poll_configuration")

    @staticmethod
    def _validate_transfer_configuration(
        max_archive_bytes: int,
        transfer_timeout_seconds: float,
    ) -> None:
        if (
            isinstance(max_archive_bytes, bool)
            or not isinstance(max_archive_bytes, int)
            or max_archive_bytes <= 0
            or isinstance(transfer_timeout_seconds, bool)
            or not isinstance(transfer_timeout_seconds, (int, float))
            or not math.isfinite(transfer_timeout_seconds)
            or transfer_timeout_seconds <= 0
        ):
            raise UsaSpendingError("invalid_transfer_configuration")

    def _post(self, endpoint: str, body: dict[str, Any]) -> dict[str, Any]:
        last_category = "upstream_error"
        for attempt in range(self.max_retries + 1):
            try:
                with httpx.Client(base_url=self.base_url, timeout=self.timeout, headers={"Accept": "application/json"}) as client:
                    response = client.post(endpoint, json=body)
                if response.status_code == 429 or response.status_code >= 500:
                    last_category = f"http_{response.status_code}"
                    if attempt < self.max_retries:
                        time.sleep(0.5 * (2**attempt))
                        continue
                    raise UsaSpendingError(last_category)
                if response.status_code >= 400:
                    raise UsaSpendingError(f"http_{response.status_code}")
                data = response.json()
                if not isinstance(data, dict):
                    raise UsaSpendingError("invalid_response")
                return data
            except (httpx.TimeoutException, httpx.NetworkError):
                last_category = "timeout_or_network"
                if attempt < self.max_retries:
                    time.sleep(0.5 * (2**attempt))
                    continue
            except ValueError:
                last_category = "invalid_response"
                break
        raise UsaSpendingError(last_category)
