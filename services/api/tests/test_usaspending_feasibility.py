from __future__ import annotations

import io
import json
import zipfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from email.utils import format_datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from tools.usaspending_feasibility import (
    FeasibilityError,
    FeasibilityHarness,
    MonthWindow,
    build_parser,
    resolve_headers,
)


def sample_window() -> MonthWindow:
    return MonthWindow.from_strings("2024-10-01", "2024-10-31", today=date(2026, 1, 1))


def response(request: httpx.Request, payload: dict[str, Any], status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload, request=request)


def make_client(handler: Any) -> httpx.Client:
    return httpx.Client(
        base_url="https://api.usaspending.gov",
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    )


def build_zip(path: Path, *, unsafe_name: str | None = None) -> None:
    headers = [
        "usaspending_unique_transaction_id",
        "generated_unique_award_id",
        "piid",
        "type",
        "action_date",
        "federal_action_obligation",
        "recipient_name",
        "recipient_uei",
        "parent_recipient_name",
        "parent_uei",
        "awarding_toptier_agency_name",
        "awarding_subtier_agency_name",
        "naics_code",
        "product_or_service_code",
        "transaction_description",
        "last_modified_date",
    ]
    rows = [
        ["tx-1", "award-1", "PIID-1", "A", "2024-10-01", "125.50", "Vendor", "UEI", "Parent", "PUEI", "Agency", "Sub", "541512", "D310", "Work", "2024-10-02"],
        ["tx-2", "award-1", "PIID-1", "A", "2024-10-02", "-25.50", "Vendor", "UEI", "Parent", "PUEI", "Agency", "Sub", "541512", "D310", "Correction", "2024-10-03"],
    ]
    stream = io.StringIO(newline="")
    writer = __import__("csv").writer(stream)
    writer.writerow(headers)
    writer.writerows(rows)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(unsafe_name or "Contracts_PrimeTransactions.csv", stream.getvalue())


def stub_completed_run(
    monkeypatch: pytest.MonkeyPatch,
    harness: FeasibilityHarness,
    tmp_path: Path,
    missing_semantic: str,
) -> None:
    null_counts = {
        "stable_transaction_id": 0,
        "generated_award_id": 0,
        "display_award_id": 0,
        "award_type": 0,
        "action_date": 0,
        "signed_obligation": 0,
        "recipient_name": 0,
        "recipient_uei": 0,
        "parent_recipient_name": 0,
        "parent_recipient_uei": 0,
        "awarding_agency": 0,
        "awarding_subagency": 0,
        "naics": 0,
        "psc": 0,
        "description": 0,
        "last_modified": 0,
    }
    null_counts[missing_semantic] = 1
    inspection = {
        "rows": 1,
        "unique_transaction_ids": 1,
        "missing_transaction_ids": 0,
        "duplicate_transaction_rows": 0,
        "invalid_action_dates": 0,
        "outside_window_rows": 0,
        "invalid_obligations": 0,
        "net_obligations": Decimal("10.00"),
        "null_counts": null_counts,
        "null_obligation_sums": {
            "awarding_agency": Decimal("0"),
            "naics": Decimal("0"),
            "psc": Decimal("0"),
        },
    }
    reconciliation = {
        "spending_over_time": {"summed_amount": Decimal("10.00")},
        "categories": {
            name: {"summed_amount": Decimal("10.00")}
            for name in ("awarding_agency", "naics", "psc")
        },
    }
    count = {
        "calculated_count": 1,
        "maximum_limit": 500000,
        "rows_gt_limit": False,
        "spending_level": "transactions",
    }
    api_status = "https://api.usaspending.gov/api/v2/download/status?file_name=x.zip"
    file_url = "https://files.usaspending.gov/generated_downloads/x.zip"
    monkeypatch.setattr(harness, "preflight", lambda: ({}, count))
    monkeypatch.setattr(harness, "evaluate_bulk_options", lambda: ({}, {"monthly_files": []}))
    monkeypatch.setattr(
        harness,
        "request_download",
        lambda _: ({}, {"status_url": api_status, "file_url": file_url}),
    )
    monkeypatch.setattr(
        harness,
        "request_bulk_download",
        lambda: ({}, {"status_url": api_status, "file_url": file_url}),
    )
    monkeypatch.setattr(
        harness,
        "poll_download",
        lambda _: ([], {"status": "finished", "file_url": file_url}),
    )
    monkeypatch.setattr(harness, "download_zip", lambda _: (tmp_path / "unused.zip", {}))
    monkeypatch.setattr(harness, "inspect_zip", lambda _: inspection)
    monkeypatch.setattr(harness, "aggregate_reconciliation", lambda: reconciliation)


def test_month_window_requires_one_completed_calendar_month() -> None:
    window = sample_window()
    assert window.fiscal_year == 2025
    assert window.filters["time_period"][0]["date_type"] == "action_date"
    with pytest.raises(FeasibilityError, match="first day"):
        MonthWindow.from_strings("2024-10-02", "2024-10-31")
    with pytest.raises(FeasibilityError, match="final day"):
        MonthWindow.from_strings("2024-10-01", "2024-10-30")


def test_header_aliases_accept_export_names() -> None:
    aliases = resolve_headers(
        [
            "Contract Transaction Unique Key",
            "generated_unique_award_id",
            "award_id_piid",
            "Federal Action Obligation",
        ]
    )
    assert aliases["stable_transaction_id"] == "Contract Transaction Unique Key"
    assert aliases["generated_award_id"] == "generated_unique_award_id"
    assert aliases["display_award_id"] == "award_id_piid"
    assert aliases["signed_obligation"] == "Federal Action Obligation"


def test_over_limit_preflight_does_not_submit_or_partition(tmp_path: Path) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path.endswith("/download/count/"):
            return response(
                request,
                {
                    "calculated_count": 500001,
                    "maximum_limit": 500000,
                    "rows_gt_limit": True,
                    "spending_level": "transactions",
                },
            )
        if request.url.path.endswith("/bulk_download/list_monthly_files/"):
            return response(request, {"monthly_files": []})
        raise AssertionError(f"unexpected request: {request.url}")

    with make_client(handler) as client:
        result = FeasibilityHarness(client, sample_window(), tmp_path).run()
    assert result["recommendation"]["decision"] == "NO-GO"
    assert not any(path.endswith("/download/transactions/") for path in paths)
    assert paths.count("/api/v2/download/count/") == 1
    assert paths.count("/api/v2/bulk_download/list_monthly_files/") == 1


def test_poll_uses_returned_status_url_until_finished(tmp_path: Path) -> None:
    statuses = iter(["ready", "running", "finished"])

    def handler(request: httpx.Request) -> httpx.Response:
        status = next(statuses)
        payload: dict[str, Any] = {"status": status}
        if status == "finished":
            payload["file_url"] = "https://files.usaspending.gov/generated_downloads/example.zip"
        return response(request, payload)

    with make_client(handler) as client:
        harness = FeasibilityHarness(client, sample_window(), tmp_path, sleeper=lambda _: None)
        observations, final = harness.poll_download(
            "https://api.usaspending.gov/api/v2/download/status?file_name=example.zip"
        )
    assert [item["status"] for item in observations] == ["ready", "running", "finished"]
    assert final["file_url"].startswith("https://files.usaspending.gov/")


def test_poll_rejects_unexpected_status_host(tmp_path: Path) -> None:
    with make_client(lambda request: response(request, {})) as client:
        harness = FeasibilityHarness(client, sample_window(), tmp_path)
        with pytest.raises(FeasibilityError, match="unexpected remote URL"):
            harness.poll_download("https://example.com/status")


def test_redirect_target_must_stay_on_status_allowlist(tmp_path: Path) -> None:
    requested_hosts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_hosts.append(request.url.host)
        if request.url.host == "api.usaspending.gov":
            return httpx.Response(
                302,
                headers={"Location": "https://example.com/status"},
                request=request,
            )
        return response(
            request,
            {
                "status": "finished",
                "file_url": "https://files.usaspending.gov/generated_downloads/example.zip",
            },
        )

    with make_client(handler) as client:
        harness = FeasibilityHarness(client, sample_window(), tmp_path)
        with pytest.raises(FeasibilityError, match="unexpected remote URL"):
            harness.poll_download(
                "https://api.usaspending.gov/api/v2/download/status?file_name=example.zip"
            )
    assert requested_hosts == ["api.usaspending.gov"]


def test_allowed_status_redirect_is_followed_after_validation(tmp_path: Path) -> None:
    requested_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_paths.append(request.url.path)
        if request.url.path.endswith("/status"):
            return httpx.Response(302, headers={"Location": "/finished"}, request=request)
        return response(
            request,
            {
                "status": "finished",
                "file_url": "https://files.usaspending.gov/generated_downloads/example.zip",
            },
        )

    with make_client(handler) as client:
        harness = FeasibilityHarness(client, sample_window(), tmp_path)
        _, final = harness.poll_download("https://api.usaspending.gov/status")
    assert final["status"] == "finished"
    assert requested_paths == ["/status", "/finished"]


def test_retry_after_exceeding_backoff_cap_is_respected(tmp_path: Path) -> None:
    statuses = iter(["running", "finished"])
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        status = next(statuses)
        payload: dict[str, Any] = {"status": status}
        headers = {"Retry-After": "120"} if status == "running" else {}
        if status == "finished":
            payload["file_url"] = "https://files.usaspending.gov/generated_downloads/example.zip"
        return httpx.Response(200, json=payload, headers=headers, request=request)

    with make_client(handler) as client:
        FeasibilityHarness(
            client,
            sample_window(),
            tmp_path,
            poll_initial_seconds=10,
            poll_max_seconds=60,
            poll_timeout_seconds=300,
            sleeper=sleeps.append,
        ).poll_download("https://api.usaspending.gov/status")
    assert sleeps == [120.0]


def test_retry_after_http_date_is_respected_without_exceeding_timeout(tmp_path: Path) -> None:
    statuses = iter(["running", "finished"])
    sleeps: list[float] = []
    retry_at = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=120), usegmt=True)

    def handler(request: httpx.Request) -> httpx.Response:
        status = next(statuses)
        payload: dict[str, Any] = {"status": status}
        headers = {"Retry-After": retry_at} if status == "running" else {}
        if status == "finished":
            payload["file_url"] = "https://files.usaspending.gov/generated_downloads/example.zip"
        return httpx.Response(200, json=payload, headers=headers, request=request)

    with make_client(handler) as client:
        FeasibilityHarness(
            client,
            sample_window(),
            tmp_path,
            poll_initial_seconds=10,
            poll_max_seconds=60,
            poll_timeout_seconds=90,
            sleeper=sleeps.append,
        ).poll_download("https://api.usaspending.gov/status")
    assert 89 <= sleeps[0] <= 90


def test_failed_poll_is_journaled_with_remote_message(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return response(request, {"status": "failed", "message": "generation failed"})

    with make_client(handler) as client:
        harness = FeasibilityHarness(client, sample_window(), tmp_path)
        with pytest.raises(FeasibilityError, match="generation failed"):
            harness.poll_download("https://api.usaspending.gov/api/v2/download/status?file_name=x.zip")
    journal = json.loads((tmp_path / "run-journal.json").read_text())
    assert journal["status_observations"][-1]["status"] == "failed"


def test_zip_inspection_preserves_signed_obligations_and_distinct_awards(tmp_path: Path) -> None:
    archive = tmp_path / "sample.zip"
    build_zip(archive)
    with make_client(lambda request: response(request, {})) as client:
        result = FeasibilityHarness(client, sample_window(), tmp_path).inspect_zip(archive)
    assert result["rows"] == 2
    assert result["unique_transaction_ids"] == 2
    assert result["unique_generated_award_ids"] == 1
    assert result["gross_positive"] == 125.50
    assert result["signed_deobligations"] == -25.50
    assert result["deobligation_magnitude"] == 25.50
    assert result["net_obligations"] == 100
    assert result["negative_rows"] == 1
    assert all(value == 0 for value in result["null_obligation_sums"].values())
    assert result["postgres_footprint_model"]["total_modeled_bytes"] > 0


def test_zip_inspection_rejects_path_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.zip"
    build_zip(archive, unsafe_name="../escape.csv")
    with make_client(lambda request: response(request, {})) as client:
        with pytest.raises(FeasibilityError, match="unsafe ZIP member"):
            FeasibilityHarness(client, sample_window(), tmp_path).inspect_zip(archive)


def test_download_request_reuses_exact_preflight_filters(tmp_path: Path) -> None:
    bodies: dict[str, dict[str, Any]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        bodies[request.url.path] = body
        if request.url.path.endswith("/download/count/"):
            return response(
                request,
                {
                    "calculated_count": 2,
                    "maximum_limit": 500000,
                    "rows_gt_limit": False,
                    "spending_level": "transactions",
                },
            )
        return response(
            request,
            {
                "status_url": "https://api.usaspending.gov/api/v2/download/status?file_name=x.zip",
                "file_url": "https://files.usaspending.gov/generated_downloads/x.zip",
            },
        )

    with make_client(handler) as client:
        harness = FeasibilityHarness(client, sample_window(), tmp_path)
        _, count = harness.preflight()
        harness.request_download(count)
    assert bodies["/api/v2/download/count/"]["filters"] == bodies["/api/v2/download/transactions/"]["filters"]
    assert bodies["/api/v2/download/transactions/"]["limit"] == 500000


def test_bulk_request_keeps_whole_month_and_prime_contract_types(tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return response(
            request,
            {
                "status_url": "https://api.usaspending.gov/api/v2/download/status?file_name=bulk.zip",
                "file_url": "https://files.usaspending.gov/generated_downloads/bulk.zip",
            },
        )

    with make_client(handler) as client:
        request, _ = FeasibilityHarness(client, sample_window(), tmp_path).request_bulk_download()
    assert request["filters"]["prime_award_types"] == ["A", "B", "C", "D"]
    assert request["filters"]["date_range"] == {
        "start_date": "2024-10-01",
        "end_date": "2024-10-31",
    }
    assert request["filters"]["date_type"] == "action_date"
    assert request["columns"][0] == "contract_transaction_unique_key"
    assert request["columns"][1] == "contract_award_unique_key"
    assert captured == request


@pytest.mark.parametrize("mode", ["run", "run_bulk"])
@pytest.mark.parametrize(
    "missing_semantic",
    [
        "generated_award_id",
        "display_award_id",
        "recipient_uei",
        "awarding_agency",
        "awarding_subagency",
    ],
)
def test_go_gates_require_all_identifiers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    missing_semantic: str,
) -> None:
    with make_client(lambda request: response(request, {})) as client:
        harness = FeasibilityHarness(client, sample_window(), tmp_path)
        stub_completed_run(monkeypatch, harness, tmp_path, missing_semantic)
        result = getattr(harness, mode)()
    assert result["recommendation"]["structural_checks_pass"] is False
    assert result["recommendation"]["decision"] == "NO-GO"


def test_bulk_mode_is_the_cli_default() -> None:
    assert build_parser().parse_args([]).mode == "bulk"
