import asyncio
import json
import time
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx
import pytest

from app.core.config import Settings
from app.usaspending.client import (
    BULK_TRANSACTION_FIELDS,
    BulkExportJob,
    BulkExportStatus,
    UsaSpendingClient,
    UsaSpendingError,
)


SyncHandler = Callable[[httpx.Request], httpx.Response]
AsyncHandler = Callable[[httpx.Request], Awaitable[httpx.Response]]
EXISTING_ARCHIVE = b"previously-completed-archive"


def settings() -> Settings:
    return Settings(database_url="postgresql+psycopg://test:password@db.invalid:5432/govtracts_test")


def client_with_transport(
    monkeypatch: pytest.MonkeyPatch,
    handler: SyncHandler,
) -> UsaSpendingClient:
    client = UsaSpendingClient(settings())
    transport = httpx.MockTransport(handler)

    def open_client() -> httpx.Client:
        return httpx.Client(
            base_url=client.base_url,
            timeout=client.timeout,
            headers={"Accept": "application/json"},
            follow_redirects=False,
            transport=transport,
        )

    monkeypatch.setattr(client, "_http_client", open_client)
    return client


def async_client_with_transport(
    monkeypatch: pytest.MonkeyPatch,
    handler: AsyncHandler,
) -> UsaSpendingClient:
    client = UsaSpendingClient(settings())
    transport = httpx.MockTransport(handler)

    def open_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=client.base_url,
            timeout=client.timeout,
            headers={"Accept": "application/json"},
            follow_redirects=False,
            transport=transport,
        )

    monkeypatch.setattr(client, "_async_http_client", open_client)
    return client


def run(coroutine: Awaitable[Any]) -> Any:
    return asyncio.run(coroutine)


def job(
    *,
    status_url: str = "https://api.usaspending.gov/api/v2/download/status?file_name=x.zip",
    file_url: str = "https://files.usaspending.gov/generated_downloads/x.zip",
) -> BulkExportJob:
    return BulkExportJob(status_url=status_url, file_url=file_url, file_name="x.zip")


def completed_status(*, file_url: str | None = None) -> BulkExportStatus:
    return BulkExportStatus(
        status="finished",
        status_url=job().status_url,
        file_url=file_url or job().file_url,
        file_name="x.zip",
        total_rows=1,
        total_columns=16,
        message=None,
        seconds_elapsed="5.0",
    )


def existing_destination(tmp_path: Path) -> Path:
    destination = tmp_path / "export.zip"
    destination.write_bytes(EXISTING_ARCHIVE)
    return destination


def assert_existing_destination_is_untouched(destination: Path) -> None:
    assert destination.read_bytes() == EXISTING_ARCHIVE
    assert list(destination.parent.glob(f".{destination.name}.*.tmp")) == []


class TrackingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.iterated = False

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        self.iterated = True
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        return None


class RaisingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, error: httpx.RequestError, *, partial: bytes = b"") -> None:
        self.error = error
        self.partial = partial

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        if self.partial:
            yield self.partial
        raise self.error

    async def aclose(self) -> None:
        return None


class BlockingAsyncStream(httpx.AsyncByteStream):
    async def __aiter__(self):  # type: ignore[no-untyped-def]
        await asyncio.Event().wait()
        yield b"unreachable"

    async def aclose(self) -> None:
        return None


class TricklingAsyncStream(httpx.AsyncByteStream):
    async def __aiter__(self):  # type: ignore[no-untyped-def]
        while True:
            await asyncio.sleep(0.005)
            yield b" "

    async def aclose(self) -> None:
        return None


def test_expected_transaction_count_uses_action_date_and_prime_contracts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "spending_level": "transactions",
                "calculated_count": 321,
                "maximum_limit": 500_000,
                "rows_gt_limit": False,
            },
        )

    client = client_with_transport(monkeypatch, handler)

    assert client.get_expected_transaction_count(date(2024, 10, 1), date(2024, 10, 31)) == 321
    assert captured == {
        "path": "/api/v2/download/count/",
        "body": {
            "filters": {
                "award_type_codes": ["A", "B", "C", "D"],
                "time_period": [
                    {
                        "start_date": "2024-10-01",
                        "end_date": "2024-10-31",
                        "date_type": "action_date",
                    }
                ],
            },
            "spending_level": "transactions",
        },
    }


def test_bulk_submission_uses_only_approved_fields_and_captures_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "status_url": job().status_url,
                "file_url": job().file_url,
                "file_name": "x.zip",
            },
        )

    client = client_with_transport(monkeypatch, handler)
    submitted = client.submit_bulk_export(date(2024, 10, 1), date(2024, 10, 31))

    assert submitted == job()
    assert captured == {
        "path": "/api/v2/bulk_download/awards/",
        "body": {
            "filters": {
                "prime_award_types": ["A", "B", "C", "D"],
                "date_type": "action_date",
                "date_range": {
                    "start_date": "2024-10-01",
                    "end_date": "2024-10-31",
                },
                "agencies": [{"type": "awarding", "tier": "toptier", "name": "all"}],
            },
            "file_format": "csv",
            "columns": list(BULK_TRANSACTION_FIELDS),
        },
    }


def test_sync_request_creation_error_is_translated(monkeypatch: pytest.MonkeyPatch) -> None:
    client = UsaSpendingClient(settings())

    class BuildFailureClient:
        def __enter__(self) -> "BuildFailureClient":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def build_request(self, *_: object, **__: object) -> httpx.Request:
            raise httpx.RemoteProtocolError("bad request construction")

    monkeypatch.setattr(client, "_http_client", BuildFailureClient)

    with pytest.raises(UsaSpendingError, match="timeout_or_network"):
        client.get_expected_transaction_count(date(2024, 10, 1), date(2024, 10, 31))


def test_polling_moves_from_pending_to_complete_and_respects_numeric_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        [
            httpx.Response(200, headers={"Retry-After": "12"}, json={"status": "running"}),
            httpx.Response(
                200,
                json={
                    "status": "finished",
                    "file_url": job().file_url,
                    "file_name": "x.zip",
                    "total_rows": 490381,
                    "total_columns": 16,
                    "message": None,
                    "seconds_elapsed": "12.0",
                },
            ),
        ]
    )
    sleeps: list[float] = []

    async def handler(_: httpx.Request) -> httpx.Response:
        return next(responses)

    async def sleeper(seconds: float) -> None:
        sleeps.append(seconds)

    client = async_client_with_transport(monkeypatch, handler)
    completed = run(
        client.poll_bulk_export(
            job(),
            poll_initial_seconds=1,
            poll_max_seconds=5,
            poll_timeout_seconds=30,
            sleeper=sleeper,
        )
    )

    assert completed.status == "finished"
    assert completed.file_url == job().file_url
    assert completed.total_rows == 490381
    assert completed.total_columns == 16
    assert completed.seconds_elapsed == "12.0"
    assert sleeps == [12.0]


def test_polling_respects_http_date_retry_after_beyond_backoff_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
    responses = iter(
        [
            httpx.Response(
                200,
                headers={"Retry-After": format_datetime(now + timedelta(seconds=120), usegmt=True)},
                json={"status": "ready"},
            ),
            httpx.Response(
                200,
                json={
                    "status": "finished",
                    "file_url": job().file_url,
                    "total_rows": 1,
                },
            ),
        ]
    )
    sleeps: list[float] = []

    async def handler(_: httpx.Request) -> httpx.Response:
        return next(responses)

    async def sleeper(seconds: float) -> None:
        sleeps.append(seconds)

    client = async_client_with_transport(monkeypatch, handler)
    run(
        client.poll_bulk_export(
            job(),
            poll_initial_seconds=1,
            poll_max_seconds=5,
            poll_timeout_seconds=180,
            sleeper=sleeper,
            utcnow=lambda: now,
        )
    )

    assert sleeps == [120.0]


@pytest.mark.parametrize("status", ["failed", "rejected"])
def test_polling_rejects_terminal_failure_states(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": status, "message": "remote detail"})

    client = async_client_with_transport(monkeypatch, handler)

    with pytest.raises(UsaSpendingError, match=f"bulk_export_{status}"):
        run(client.poll_bulk_export(job()))


@pytest.mark.parametrize("stream_type", [BlockingAsyncStream, TricklingAsyncStream])
def test_polling_deadline_cancels_blocking_and_trickling_response_bodies(
    monkeypatch: pytest.MonkeyPatch,
    stream_type: type[httpx.AsyncByteStream],
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream_type())

    client = async_client_with_transport(monkeypatch, handler)
    started = time.monotonic()

    with pytest.raises(UsaSpendingError, match="bulk_export_timeout"):
        run(client.poll_bulk_export(job(), poll_timeout_seconds=0.05))

    assert time.monotonic() - started < 0.5


def test_polling_rejects_malformed_status(monkeypatch: pytest.MonkeyPatch) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "unknown"})

    client = async_client_with_transport(monkeypatch, handler)

    with pytest.raises(UsaSpendingError, match="invalid_bulk_export_status"):
        run(client.poll_bulk_export(job()))


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"total_rows": -1},
        {"total_rows": True},
        {"total_rows": "490381"},
        {"total_rows": 500001},
    ],
)
def test_polling_rejects_invalid_total_rows(
    monkeypatch: pytest.MonkeyPatch,
    metadata: dict[str, object],
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "finished",
                "file_url": job().file_url,
                **metadata,
            },
        )

    client = async_client_with_transport(monkeypatch, handler)

    with pytest.raises(UsaSpendingError, match="invalid_bulk_export_total_rows"):
        run(client.poll_bulk_export(job()))


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [
        ({}, None),
        ({"total_columns": 16}, 16),
    ],
)
def test_polling_accepts_optional_strict_total_columns(
    monkeypatch: pytest.MonkeyPatch,
    metadata: dict[str, object],
    expected: int | None,
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "finished",
                "file_url": job().file_url,
                "total_rows": 490381,
                **metadata,
            },
        )

    client = async_client_with_transport(monkeypatch, handler)
    completed = run(client.poll_bulk_export(job()))

    assert completed.total_columns == expected


@pytest.mark.parametrize("total_columns", [True, "16", 15, 17])
def test_polling_rejects_invalid_total_columns(
    monkeypatch: pytest.MonkeyPatch,
    total_columns: object,
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "finished",
                "file_url": job().file_url,
                "total_rows": 490381,
                "total_columns": total_columns,
            },
        )

    client = async_client_with_transport(monkeypatch, handler)

    with pytest.raises(UsaSpendingError, match="invalid_bulk_export_total_columns"):
        run(client.poll_bulk_export(job()))


def test_polling_does_not_infer_counts_from_total_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "finished",
                "file_url": job().file_url,
                "total_rows": 490381,
                "total_size": "not-a-count",
            },
        )

    client = async_client_with_transport(monkeypatch, handler)
    completed = run(client.poll_bulk_export(job()))

    assert completed.total_rows == 490381
    assert completed.total_columns is None


def test_submission_rejects_malformed_job(monkeypatch: pytest.MonkeyPatch) -> None:
    client = client_with_transport(
        monkeypatch,
        lambda _: httpx.Response(200, json={"file_name": "x.zip"}),
    )

    with pytest.raises(UsaSpendingError, match="invalid_bulk_export_response"):
        client.submit_bulk_export(date(2024, 10, 1), date(2024, 10, 31))


def test_async_request_creation_error_is_translated(monkeypatch: pytest.MonkeyPatch) -> None:
    client = UsaSpendingClient(settings())

    class AsyncBuildFailureClient:
        async def __aenter__(self) -> "AsyncBuildFailureClient":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        def build_request(self, *_: object, **__: object) -> httpx.Request:
            raise httpx.DecodingError("bad request construction")

    monkeypatch.setattr(client, "_async_http_client", AsyncBuildFailureClient)

    with pytest.raises(UsaSpendingError, match="timeout_or_network"):
        run(client.poll_bulk_export(job()))


@pytest.mark.parametrize(
    "error",
    [
        httpx.RemoteProtocolError("status protocol failure"),
        httpx.DecodingError("status decoding failure"),
    ],
)
def test_status_response_consumption_request_errors_are_translated(
    monkeypatch: pytest.MonkeyPatch,
    error: httpx.RequestError,
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=RaisingAsyncStream(error))

    client = async_client_with_transport(monkeypatch, handler)

    with pytest.raises(UsaSpendingError, match="timeout_or_network"):
        run(client.poll_bulk_export(job()))


def test_redirect_request_error_is_translated(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(302, headers={"Location": "/api/v2/download/status?next=1"})
        raise httpx.RemoteProtocolError("redirect failed", request=request)

    client = async_client_with_transport(monkeypatch, handler)

    with pytest.raises(UsaSpendingError, match="timeout_or_network"):
        run(client.poll_bulk_export(job()))

    assert calls == 2


def test_malicious_initial_status_and_file_urls_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def unexpected_request(_: httpx.Request) -> httpx.Response:
        raise AssertionError("unsafe URL must be rejected before any request")

    client = async_client_with_transport(monkeypatch, unexpected_request)
    unsafe_job = job(status_url="https://attacker.example/status")
    unsafe_status = completed_status(file_url="https://attacker.example/archive.zip")

    with pytest.raises(UsaSpendingError, match="unsafe_status_url"):
        run(client.poll_bulk_export(unsafe_job))
    with pytest.raises(UsaSpendingError, match="unsafe_file_url"):
        run(client.retrieve_bulk_export(unsafe_status, tmp_path / "export.zip"))

    assert list(tmp_path.iterdir()) == []


def test_status_redirect_destination_is_validated_before_following(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_hosts: list[str | None] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested_hosts.append(request.url.host)
        return httpx.Response(302, headers={"Location": "https://attacker.example/status"})

    client = async_client_with_transport(monkeypatch, handler)

    with pytest.raises(UsaSpendingError, match="unsafe_status_url"):
        run(client.poll_bulk_export(job()))

    assert requested_hosts == ["api.usaspending.gov"]


def test_file_redirect_destination_is_validated_before_following(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    requested_hosts: list[str | None] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested_hosts.append(request.url.host)
        return httpx.Response(302, headers={"Location": "https://attacker.example/archive.zip"})

    client = async_client_with_transport(monkeypatch, handler)
    destination = existing_destination(tmp_path)

    with pytest.raises(UsaSpendingError, match="unsafe_file_url"):
        run(client.retrieve_bulk_export(completed_status(), destination))

    assert requested_hosts == ["files.usaspending.gov"]
    assert_existing_destination_is_untouched(destination)


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "https://api.usaspending.gov.attacker/status",
        "https://user@api.usaspending.gov/status",
        "https://api.usaspending.gov:8443/status",
        "http://api.usaspending.gov/status",
    ],
)
def test_status_url_bypass_forms_are_rejected_before_request(
    monkeypatch: pytest.MonkeyPatch,
    unsafe_url: str,
) -> None:
    async def unexpected_request(_: httpx.Request) -> httpx.Response:
        raise AssertionError("unsafe URL must be rejected before any request")

    client = async_client_with_transport(monkeypatch, unexpected_request)

    with pytest.raises(UsaSpendingError, match="unsafe_status_url"):
        run(client.poll_bulk_export(job(status_url=unsafe_url)))


@pytest.mark.parametrize(
    "unsafe_redirect",
    [
        "https://api.usaspending.gov.attacker/status",
        "https://user@api.usaspending.gov/status",
        "https://api.usaspending.gov:8443/status",
        "http://api.usaspending.gov/status",
    ],
)
def test_unsafe_status_redirect_forms_are_rejected_before_following(
    monkeypatch: pytest.MonkeyPatch,
    unsafe_redirect: str,
) -> None:
    requested_urls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        return httpx.Response(302, headers={"Location": unsafe_redirect})

    client = async_client_with_transport(monkeypatch, handler)

    with pytest.raises(UsaSpendingError, match="unsafe_status_url"):
        run(client.poll_bulk_export(job()))

    assert len(requested_urls) == 1


def test_same_host_relative_status_redirect_is_followed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested_paths.append(request.url.path)
        if len(requested_paths) == 1:
            return httpx.Response(
                302,
                headers={"Location": "/api/v2/download/status?file_name=redirected.zip"},
            )
        return httpx.Response(
            200,
            json={
                "status": "finished",
                "file_url": job().file_url,
                "file_name": "redirected.zip",
                "total_rows": 1,
            },
        )

    client = async_client_with_transport(monkeypatch, handler)
    completed = run(client.poll_bulk_export(job()))

    assert completed.file_name == "redirected.zip"
    assert requested_paths == ["/api/v2/download/status", "/api/v2/download/status"]


def test_completed_zip_atomically_replaces_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    archive = b"PK\x03\x04mock-zip"

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/zip"},
            content=archive,
        )

    client = async_client_with_transport(monkeypatch, handler)
    destination = existing_destination(tmp_path)

    assert run(client.retrieve_bulk_export(completed_status(), destination)) == len(archive)
    assert destination.read_bytes() == archive
    assert list(tmp_path.glob(f".{destination.name}.*.tmp")) == []


def test_same_host_relative_file_redirect_is_followed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if len(paths) == 1:
            return httpx.Response(302, headers={"Location": "/generated_downloads/redirected.zip"})
        return httpx.Response(200, content=b"PK\x03\x04redirected")

    client = async_client_with_transport(monkeypatch, handler)
    destination = tmp_path / "export.zip"

    run(client.retrieve_bulk_export(completed_status(), destination))

    assert destination.read_bytes() == b"PK\x03\x04redirected"
    assert paths == ["/generated_downloads/x.zip", "/generated_downloads/redirected.zip"]


def test_declared_oversized_archive_preserves_existing_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stream = TrackingAsyncStream([b"not-read"])

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Length": "11"}, stream=stream)

    client = async_client_with_transport(monkeypatch, handler)
    destination = existing_destination(tmp_path)

    with pytest.raises(UsaSpendingError, match="bulk_export_archive_too_large"):
        run(client.retrieve_bulk_export(completed_status(), destination, max_archive_bytes=10))

    assert not stream.iterated
    assert_existing_destination_is_untouched(destination)


@pytest.mark.parametrize("content_length", [None, "4"])
def test_streamed_archive_limit_preserves_existing_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    content_length: str | None,
) -> None:
    headers = {} if content_length is None else {"Content-Length": content_length}

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers=headers,
            stream=TrackingAsyncStream([b"PK\x03\x04", b"12345"]),
        )

    client = async_client_with_transport(monkeypatch, handler)
    destination = existing_destination(tmp_path)

    with pytest.raises(UsaSpendingError, match="bulk_export_archive_too_large"):
        run(client.retrieve_bulk_export(completed_status(), destination, max_archive_bytes=8))

    assert_existing_destination_is_untouched(destination)


def test_archive_request_failure_preserves_existing_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection failed", request=request)

    client = async_client_with_transport(monkeypatch, handler)
    destination = existing_destination(tmp_path)

    with pytest.raises(UsaSpendingError, match="timeout_or_network"):
        run(client.retrieve_bulk_export(completed_status(), destination))

    assert_existing_destination_is_untouched(destination)


@pytest.mark.parametrize(
    "error",
    [
        httpx.RemoteProtocolError("archive protocol failure"),
        httpx.DecodingError("archive decoding failure"),
    ],
)
def test_archive_protocol_and_decoding_failures_preserve_existing_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    error: httpx.RequestError,
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            stream=RaisingAsyncStream(error, partial=b"PK\x03\x04partial"),
        )

    client = async_client_with_transport(monkeypatch, handler)
    destination = existing_destination(tmp_path)

    with pytest.raises(UsaSpendingError, match="timeout_or_network"):
        run(client.retrieve_bulk_export(completed_status(), destination))

    assert_existing_destination_is_untouched(destination)


def test_archive_write_failure_preserves_existing_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"PK\x03\x04new")

    def fail_write(*_: object) -> None:
        raise OSError("disk full")

    client = async_client_with_transport(monkeypatch, handler)
    monkeypatch.setattr(client, "_write_chunk", fail_write)
    destination = existing_destination(tmp_path)

    with pytest.raises(UsaSpendingError, match="bulk_export_destination_write_failed"):
        run(client.retrieve_bulk_export(completed_status(), destination))

    assert_existing_destination_is_untouched(destination)


@pytest.mark.parametrize("stream_type", [BlockingAsyncStream, TricklingAsyncStream])
def test_archive_deadline_cancels_blocking_and_trickling_bodies_and_preserves_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    stream_type: type[httpx.AsyncByteStream],
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream_type())

    client = async_client_with_transport(monkeypatch, handler)
    destination = existing_destination(tmp_path)
    started = time.monotonic()

    with pytest.raises(UsaSpendingError, match="bulk_export_transfer_timeout"):
        run(
            client.retrieve_bulk_export(
                completed_status(),
                destination,
                max_archive_bytes=1024 * 1024,
                transfer_timeout_seconds=0.05,
            )
        )

    assert time.monotonic() - started < 0.5
    assert_existing_destination_is_untouched(destination)


def test_invalid_content_length_preserves_existing_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Length": "not-a-number"}, content=b"zip")

    client = async_client_with_transport(monkeypatch, handler)
    destination = existing_destination(tmp_path)

    with pytest.raises(UsaSpendingError, match="invalid_content_length"):
        run(client.retrieve_bulk_export(completed_status(), destination))

    assert_existing_destination_is_untouched(destination)
