from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


jsonb_type = JSON().with_variant(JSONB, "postgresql")


class TimestampedModel:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class Agency(TimestampedModel, Base):
    __tablename__ = "agencies"
    __table_args__ = (
        UniqueConstraint("source", "external_code", "tier", name="uq_agencies_source_code_tier"),
        Index("ix_agencies_parent_id", "parent_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    external_code: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    tier: Mapped[str] = mapped_column(String(32), nullable=False)
    parent_id: Mapped[UUID | None] = mapped_column(
        Uuid, ForeignKey("agencies.id", ondelete="SET NULL"), nullable=True
    )


class Vendor(TimestampedModel, Base):
    __tablename__ = "vendors"
    __table_args__ = (
        UniqueConstraint("source", "source_recipient_key", name="uq_vendors_source_recipient_key"),
        Index("uq_vendors_uei_not_null", "uei", unique=True, postgresql_where=text("uei IS NOT NULL")),
        Index("uq_vendors_duns_not_null", "duns", unique=True, postgresql_where=text("duns IS NOT NULL")),
        Index("ix_vendors_normalized_name", "normalized_name"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_recipient_key: Mapped[str] = mapped_column(String(255), nullable=False)
    canonical_name: Mapped[str] = mapped_column(String(512), nullable=False)
    normalized_name: Mapped[str] = mapped_column(String(512), nullable=False)
    uei: Mapped[str | None] = mapped_column(String(12), nullable=True)
    duns: Mapped[str | None] = mapped_column(String(9), nullable=True)


class Award(TimestampedModel, Base):
    __tablename__ = "awards"
    __table_args__ = (
        UniqueConstraint("usa_generated_id", name="uq_awards_usa_generated_id"),
        Index("ix_awards_agency_obligation_date", "awarding_agency_id", "base_obligation_date"),
        Index("ix_awards_vendor_obligation_date", "vendor_id", "base_obligation_date"),
        Index("ix_awards_naics_obligation_date", "naics_code", "base_obligation_date"),
        Index("ix_awards_obligation_amount", "obligation_amount"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    usa_generated_id: Mapped[str] = mapped_column(String(512), nullable=False)
    award_id: Mapped[str] = mapped_column(String(255), nullable=False)
    vendor_id: Mapped[UUID | None] = mapped_column(Uuid, ForeignKey("vendors.id", ondelete="SET NULL"), nullable=True)
    awarding_agency_id: Mapped[UUID | None] = mapped_column(
        Uuid, ForeignKey("agencies.id", ondelete="SET NULL"), nullable=True
    )
    naics_code: Mapped[str | None] = mapped_column(String(6), nullable=True)
    psc_code: Mapped[str | None] = mapped_column(String(4), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    base_obligation_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    obligation_amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    award_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(jsonb_type, nullable=True)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class Opportunity(TimestampedModel, Base):
    __tablename__ = "opportunities"
    __table_args__ = (
        Index("ix_opportunities_active_deadline", "active", "response_deadline"),
        Index("ix_opportunities_naics_code", "naics_code"),
        Index("ix_opportunities_set_aside_code", "set_aside_code"),
        Index("ix_opportunities_state", "place_of_performance_state"),
        Index("ix_opportunities_organization_path", "organization_path"),
    )

    notice_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    solicitation_number: Mapped[str | None] = mapped_column(String(255), nullable=True)
    title: Mapped[str] = mapped_column(String(1024), nullable=False)
    agency_id: Mapped[UUID | None] = mapped_column(Uuid, ForeignKey("agencies.id", ondelete="SET NULL"), nullable=True)
    organization_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    organization_code: Mapped[str | None] = mapped_column(String(512), nullable=True)
    opportunity_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    set_aside_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    set_aside_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    naics_code: Mapped[str | None] = mapped_column(String(6), nullable=True)
    posted_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    response_deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    place_of_performance_state: Mapped[str | None] = mapped_column(String(2), nullable=True)
    source_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(jsonb_type, nullable=True)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class UpstreamCache(TimestampedModel, Base):
    __tablename__ = "upstream_cache"
    __table_args__ = (
        UniqueConstraint("source", "endpoint", "request_fingerprint", name="uq_upstream_cache_request"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(512), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    response_payload: Mapped[dict[str, Any] | None] = mapped_column(jsonb_type, nullable=True)
    response_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    job_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    pages_processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    records_processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    data_freshness_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
