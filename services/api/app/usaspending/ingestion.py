from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4

from sqlalchemy import Engine, and_, select
from sqlalchemy.dialects.postgresql import insert

from app.core.config import Settings
from app.db.models import Agency, Award, IngestionRun, UpstreamCache, Vendor
from app.usaspending.client import AWARD_SEARCH_ENDPOINT, UsaSpendingClient, UsaSpendingError
from app.usaspending.preset import CyberItPreset, get_preset


UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).casefold()


def parse_date(value: object) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def parse_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


class UsaSpendingIngestion:
    def __init__(self, engine: Engine, settings: Settings, client: UsaSpendingClient | None = None) -> None:
        self.engine = engine
        self.settings = settings
        self.client = client or UsaSpendingClient(settings)

    def run(self, start_date: date, end_date: date, page_size: int = 100, max_pages: int | None = None, preset_id: str = "cyber-it-v1") -> UUID:
        if end_date < start_date:
            raise ValueError("end_date must not be before start_date")
        if not 1 <= page_size <= 100:
            raise ValueError("page_size must be between 1 and 100")

        preset = get_preset(preset_id)
        run_id = uuid4()
        started_at = utc_now()
        with self.engine.begin() as connection:
            connection.execute(insert(IngestionRun).values(id=run_id, source="usaspending", job_type=preset.identifier, status="running", started_at=started_at))

        pages_processed = 0
        records_processed = 0
        latest_source_time: datetime | None = None
        try:
            for filters in self._rule_filters(preset, start_date, end_date):
                for request_body, response in self.client.iter_award_pages(filters, page_size, max_pages):
                    if max_pages is not None and pages_processed >= max_pages:
                        break
                    fetched_at = utc_now()
                    records = response.get("results") if isinstance(response.get("results"), list) else []
                    with self.engine.begin() as connection:
                        self._cache_response(connection, request_body, response, fetched_at)
                        for record in records:
                            if not isinstance(record, dict):
                                continue
                            source_time = self._upsert_record(connection, record, fetched_at)
                            if source_time and (latest_source_time is None or source_time > latest_source_time):
                                latest_source_time = source_time
                            records_processed += 1
                        pages_processed += 1
                        connection.execute(
                            IngestionRun.__table__.update()
                            .where(IngestionRun.id == run_id)
                            .values(pages_processed=pages_processed, records_processed=records_processed)
                        )
                if max_pages is not None and pages_processed >= max_pages:
                    break
        except UsaSpendingError as error:
            self._finish_run(run_id, "failed", pages_processed, records_processed, None, str(error))
            raise
        except Exception:
            self._finish_run(run_id, "failed", pages_processed, records_processed, None, "ingestion_error")
            raise RuntimeError("USAspending ingestion failed") from None

        self._finish_run(run_id, "succeeded", pages_processed, records_processed, latest_source_time or utc_now(), None)
        return run_id

    def _rule_filters(self, preset: CyberItPreset, start_date: date, end_date: date) -> list[dict[str, Any]]:
        base = {"award_type_codes": list(preset.award_type_codes), "time_period": [{"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}]}
        return [
            {**base, "naics_codes": [code]} for code in preset.naics_codes
        ] + [{**base, "psc_codes": [code]} for code in preset.psc_codes] + [{**base, "keywords": [keyword]} for keyword in preset.keywords]

    def _finish_run(self, run_id: UUID, status: str, pages: int, records: int, freshness: datetime | None, error: str | None) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                IngestionRun.__table__.update()
                .where(IngestionRun.id == run_id)
                .values(status=status, pages_processed=pages, records_processed=records, finished_at=utc_now(), data_freshness_at=freshness, error_summary=error)
            )

    def _cache_response(self, connection: Any, request_body: dict[str, Any], response: dict[str, Any], fetched_at: datetime) -> None:
        fingerprint = hashlib.sha256(canonical_json(request_body)).hexdigest()
        response_hash = hashlib.sha256(canonical_json(response)).hexdigest()
        values = {
            "id": uuid4(), "source": "usaspending", "endpoint": AWARD_SEARCH_ENDPOINT,
            "request_fingerprint": fingerprint, "response_payload": response, "response_hash": response_hash,
            "fetched_at": fetched_at, "expires_at": fetched_at + timedelta(hours=24), "last_error": None,
            "created_at": fetched_at, "updated_at": fetched_at,
        }
        statement = insert(UpstreamCache).values(**values).on_conflict_do_update(
            constraint="uq_upstream_cache_request",
            set_={key: values[key] for key in ("response_payload", "response_hash", "fetched_at", "expires_at", "last_error", "updated_at")},
        )
        connection.execute(statement)

    def _upsert_record(self, connection: Any, record: dict[str, Any], fetched_at: datetime) -> datetime | None:
        generated_id = record.get("generated_internal_id")
        award_id = record.get("Award ID")
        if not isinstance(generated_id, str) or not generated_id or not isinstance(award_id, str) or not award_id:
            return None
        agency_id = self._upsert_agency(connection, record, fetched_at)
        vendor_id = self._upsert_vendor(connection, record, fetched_at)
        source_updated_at = parse_datetime(record.get("Last Modified Date"))
        values = {
            "id": uuid4(), "usa_generated_id": generated_id, "award_id": award_id, "vendor_id": vendor_id,
            "awarding_agency_id": agency_id, "naics_code": self._string(record.get("NAICS"), 6),
            "psc_code": self._string(record.get("PSC"), 4), "description": self._string(record.get("Description")),
            "base_obligation_date": parse_date(record.get("Base Obligation Date")), "start_date": parse_date(record.get("Start Date")),
            "end_date": parse_date(record.get("End Date")), "obligation_amount": parse_decimal(record.get("Award Amount")),
            "award_type": self._string(record.get("Contract Award Type")) or self._string(record.get("Award Type")),
            "source_url": f"{self.settings.usaspending_base_url.rstrip('/')}/api/v2/awards/{quote(generated_id, safe='')}/",
            "raw_payload": record, "source_updated_at": source_updated_at, "fetched_at": fetched_at,
            "created_at": fetched_at, "updated_at": fetched_at,
        }
        statement = insert(Award).values(**values).on_conflict_do_update(
            constraint="uq_awards_usa_generated_id",
            set_={key: values[key] for key in values if key not in {"id", "usa_generated_id", "created_at"}},
        )
        connection.execute(statement)
        return source_updated_at

    def _upsert_agency(self, connection: Any, record: dict[str, Any], fetched_at: datetime) -> UUID | None:
        code, name = self._string(record.get("Awarding Agency Code")), self._string(record.get("Awarding Agency"))
        if not code or not name:
            return None
        values = {"id": uuid4(), "source": "usaspending", "external_code": code, "name": name, "tier": "toptier", "created_at": fetched_at, "updated_at": fetched_at}
        connection.execute(insert(Agency).values(**values).on_conflict_do_update(constraint="uq_agencies_source_code_tier", set_={"name": name, "updated_at": fetched_at}))
        return connection.scalar(select(Agency.id).where(and_(Agency.source == "usaspending", Agency.external_code == code, Agency.tier == "toptier")))

    def _upsert_vendor(self, connection: Any, record: dict[str, Any], fetched_at: datetime) -> UUID | None:
        name = self._string(record.get("Recipient Name"))
        recipient_id = self._string(record.get("recipient_id"))
        uei = self._string(record.get("Recipient UEI"), 12)
        duns = self._string(record.get("Recipient DUNS Number"), 9) or self._string(record.get("Recipient DUNS"), 9)
        key = recipient_id or (f"uei:{uei}" if uei else f"duns:{duns}" if duns else None)
        if not name or not key:
            return None
        values = {"id": uuid4(), "source": "usaspending", "source_recipient_key": key, "canonical_name": name, "normalized_name": normalize_name(name), "uei": uei, "duns": duns, "created_at": fetched_at, "updated_at": fetched_at}
        connection.execute(insert(Vendor).values(**values).on_conflict_do_update(constraint="uq_vendors_source_recipient_key", set_={key: values[key] for key in ("canonical_name", "normalized_name", "uei", "duns", "updated_at")}))
        return connection.scalar(select(Vendor.id).where(and_(Vendor.source == "usaspending", Vendor.source_recipient_key == key)))

    @staticmethod
    def _string(value: object, limit: int | None = None) -> str | None:
        if not isinstance(value, str):
            return None
        value = value.strip()
        if not value:
            return None
        return value[:limit] if limit else value
