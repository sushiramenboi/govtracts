from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
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


class UsaSpendingIngestionCheckpoint(TimestampedModel, Base):
    __tablename__ = "usaspending_ingestion_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "period_start",
            "period_end",
            name="uq_usaspending_checkpoints_source_period",
        ),
        CheckConstraint("period_end >= period_start", name="period_order"),
        CheckConstraint("expected_rows IS NULL OR expected_rows >= 0", name="expected_rows_nonnegative"),
        CheckConstraint("loaded_rows >= 0", name="loaded_rows_nonnegative"),
        Index("ix_usaspending_checkpoints_fiscal_period", "fiscal_year", "period_start", "period_end"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="usaspending")
    fiscal_year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    status_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    remote_file_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    archive_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expected_rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    loaded_rows: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class UsaSpendingTransaction(TimestampedModel, Base):
    __tablename__ = "usaspending_transactions"
    __table_args__ = (
        CheckConstraint("award_type_code IN ('A', 'B', 'C', 'D')", name="prime_award_type"),
        CheckConstraint("fiscal_year BETWEEN 2000 AND 9999", name="fiscal_year_range"),
        Index("ix_usaspending_transactions_checkpoint", "ingestion_checkpoint_id"),
        Index("ix_usaspending_transactions_fiscal_action", "fiscal_year", "action_date"),
        Index("ix_usaspending_transactions_award_action", "generated_award_id", "action_date"),
        Index("ix_usaspending_transactions_fiscal_agency", "fiscal_year", "awarding_agency_name"),
        Index("ix_usaspending_transactions_fiscal_recipient", "fiscal_year", "recipient_uei"),
        Index("ix_usaspending_transactions_fiscal_naics", "fiscal_year", "naics_code"),
        Index("ix_usaspending_transactions_fiscal_psc", "fiscal_year", "psc_code"),
    )

    stable_transaction_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    ingestion_checkpoint_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("usaspending_ingestion_checkpoints.id", name="fk_usaspending_tx_checkpoint", ondelete="RESTRICT"),
        nullable=False,
    )
    generated_award_id: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
    )
    display_award_id: Mapped[str] = mapped_column(String(255), nullable=False)
    award_type_code: Mapped[str] = mapped_column(String(1), nullable=False)
    action_date: Mapped[date] = mapped_column(Date, nullable=False)
    fiscal_year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    federal_action_obligation: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    recipient_name: Mapped[str] = mapped_column(String(512), nullable=False)
    recipient_uei: Mapped[str] = mapped_column(String(12), nullable=False)
    recipient_parent_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    recipient_parent_uei: Mapped[str | None] = mapped_column(String(12), nullable=True)
    awarding_agency_name: Mapped[str] = mapped_column(String(255), nullable=False)
    awarding_subagency_name: Mapped[str] = mapped_column(String(255), nullable=False)
    naics_code: Mapped[str | None] = mapped_column(String(6), nullable=True)
    psc_code: Mapped[str | None] = mapped_column(String(4), nullable=True)
    transaction_description: Mapped[str] = mapped_column(Text, nullable=False)
    source_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class UsaSpendingTransactionPresetMatch(Base):
    __tablename__ = "usaspending_transaction_preset_matches"
    __table_args__ = (
        CheckConstraint("preset_version > 0", name="version_positive"),
        CheckConstraint("match_basis IN ('naics', 'psc', 'both')", name="match_basis"),
        Index(
            "ix_usaspending_preset_matches_preset_transaction",
            "preset_key",
            "preset_version",
            "stable_transaction_id",
        ),
    )

    stable_transaction_id: Mapped[str] = mapped_column(
        String(512),
        ForeignKey(
            "usaspending_transactions.stable_transaction_id",
            name="fk_usaspending_preset_match_transaction",
            ondelete="CASCADE",
        ),
        primary_key=True,
    )
    preset_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    preset_version: Mapped[int] = mapped_column(Integer, primary_key=True)
    match_basis: Mapped[str] = mapped_column(String(16), nullable=False)
    matched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class UsaSpendingPeriodAggregate(TimestampedModel, Base):
    __tablename__ = "usaspending_period_aggregates"
    __table_args__ = (
        UniqueConstraint(
            "fiscal_year",
            "period_start",
            "period_end",
            "preset_key",
            "preset_version",
            "dimension_type",
            "dimension_key",
            name="uq_usaspending_period_aggregates_grain",
        ),
        CheckConstraint("period_end >= period_start", name="period_order"),
        CheckConstraint("preset_version > 0", name="preset_version_positive"),
        CheckConstraint("gross_positive_obligations >= 0", name="gross_positive_nonnegative"),
        CheckConstraint("signed_deobligations <= 0", name="signed_deob_nonpositive"),
        CheckConstraint(
            "net_obligations = gross_positive_obligations + signed_deobligations",
            name="net_obligations_formula",
        ),
        CheckConstraint("transaction_count >= 0", name="transaction_count_nonnegative"),
        CheckConstraint("distinct_award_count >= 0", name="award_count_nonnegative"),
        Index(
            "ix_usaspending_period_aggregates_ranking",
            "fiscal_year",
            "preset_key",
            "preset_version",
            "dimension_type",
            "net_obligations",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    fiscal_year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    preset_key: Mapped[str] = mapped_column(String(64), nullable=False)
    preset_version: Mapped[int] = mapped_column(Integer, nullable=False)
    dimension_type: Mapped[str] = mapped_column(String(32), nullable=False)
    dimension_key: Mapped[str] = mapped_column(String(512), nullable=False)
    gross_positive_obligations: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    signed_deobligations: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    net_obligations: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    transaction_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    distinct_award_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
