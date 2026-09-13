from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import httpx

from app.core.config import Settings


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


class UsaSpendingError(RuntimeError):
    """A safe, categorised upstream error suitable for logs and run records."""


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
