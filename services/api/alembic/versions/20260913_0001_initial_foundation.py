"""Create Govtracts foundation tables.

Revision ID: 20260913_0001
Revises:
Create Date: 2026-09-13
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "20260913_0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "agencies",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("external_code", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("tier", sa.String(length=32), nullable=False),
        sa.Column("parent_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["parent_id"], ["agencies.id"], name="fk_agencies_parent_id_agencies", ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_agencies"),
        sa.UniqueConstraint("source", "external_code", "tier", name="uq_agencies_source_code_tier"),
    )
    op.create_index("ix_agencies_parent_id", "agencies", ["parent_id"], unique=False)

    op.create_table(
        "vendors",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("source_recipient_key", sa.String(length=255), nullable=False),
        sa.Column("canonical_name", sa.String(length=512), nullable=False),
        sa.Column("normalized_name", sa.String(length=512), nullable=False),
        sa.Column("uei", sa.String(length=12), nullable=True),
        sa.Column("duns", sa.String(length=9), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_vendors"),
        sa.UniqueConstraint("source", "source_recipient_key", name="uq_vendors_source_recipient_key"),
    )
    op.create_index("ix_vendors_normalized_name", "vendors", ["normalized_name"], unique=False)
    op.create_index(
        "uq_vendors_uei_not_null", "vendors", ["uei"], unique=True, postgresql_where=sa.text("uei IS NOT NULL")
    )
    op.create_index(
        "uq_vendors_duns_not_null", "vendors", ["duns"], unique=True, postgresql_where=sa.text("duns IS NOT NULL")
    )

    op.create_table(
        "awards",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("usa_generated_id", sa.String(length=512), nullable=False),
        sa.Column("award_id", sa.String(length=255), nullable=False),
        sa.Column("vendor_id", sa.Uuid(), nullable=True),
        sa.Column("awarding_agency_id", sa.Uuid(), nullable=True),
        sa.Column("naics_code", sa.String(length=6), nullable=True),
        sa.Column("psc_code", sa.String(length=4), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("base_obligation_date", sa.Date(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("obligation_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("award_type", sa.String(length=64), nullable=True),
        sa.Column("source_url", sa.String(length=2048), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["awarding_agency_id"], ["agencies.id"], name="fk_awards_awarding_agency_id_agencies", ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["vendor_id"], ["vendors.id"], name="fk_awards_vendor_id_vendors", ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_awards"),
        sa.UniqueConstraint("usa_generated_id", name="uq_awards_usa_generated_id"),
    )
    op.create_index("ix_awards_agency_obligation_date", "awards", ["awarding_agency_id", "base_obligation_date"], unique=False)
    op.create_index("ix_awards_vendor_obligation_date", "awards", ["vendor_id", "base_obligation_date"], unique=False)
    op.create_index("ix_awards_naics_obligation_date", "awards", ["naics_code", "base_obligation_date"], unique=False)
    op.create_index("ix_awards_obligation_amount", "awards", ["obligation_amount"], unique=False)

    op.create_table(
        "opportunities",
        sa.Column("notice_id", sa.String(length=64), nullable=False),
        sa.Column("solicitation_number", sa.String(length=255), nullable=True),
        sa.Column("title", sa.String(length=1024), nullable=False),
        sa.Column("agency_id", sa.Uuid(), nullable=True),
        sa.Column("organization_path", sa.String(length=2048), nullable=True),
        sa.Column("organization_code", sa.String(length=512), nullable=True),
        sa.Column("opportunity_type", sa.String(length=128), nullable=True),
        sa.Column("set_aside_code", sa.String(length=32), nullable=True),
        sa.Column("set_aside_label", sa.String(length=255), nullable=True),
        sa.Column("naics_code", sa.String(length=6), nullable=True),
        sa.Column("posted_date", sa.Date(), nullable=True),
        sa.Column("response_deadline", sa.DateTime(timezone=True), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("place_of_performance_state", sa.String(length=2), nullable=True),
        sa.Column("source_url", sa.String(length=2048), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["agency_id"], ["agencies.id"], name="fk_opportunities_agency_id_agencies", ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("notice_id", name="pk_opportunities"),
    )
    op.create_index("ix_opportunities_active_deadline", "opportunities", ["active", "response_deadline"], unique=False)
    op.create_index("ix_opportunities_naics_code", "opportunities", ["naics_code"], unique=False)
    op.create_index("ix_opportunities_set_aside_code", "opportunities", ["set_aside_code"], unique=False)
    op.create_index("ix_opportunities_state", "opportunities", ["place_of_performance_state"], unique=False)
    op.create_index("ix_opportunities_organization_path", "opportunities", ["organization_path"], unique=False)

    op.create_table(
        "upstream_cache",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("endpoint", sa.String(length=512), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("response_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("response_hash", sa.String(length=64), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_upstream_cache"),
        sa.UniqueConstraint("source", "endpoint", "request_fingerprint", name="uq_upstream_cache_request"),
    )

    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("pages_processed", sa.Integer(), nullable=False),
        sa.Column("records_processed", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("data_freshness_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_ingestion_runs"),
    )


def downgrade() -> None:
    op.drop_table("ingestion_runs")
    op.drop_table("upstream_cache")
    op.drop_index("ix_opportunities_organization_path", table_name="opportunities")
    op.drop_index("ix_opportunities_state", table_name="opportunities")
    op.drop_index("ix_opportunities_set_aside_code", table_name="opportunities")
    op.drop_index("ix_opportunities_naics_code", table_name="opportunities")
    op.drop_index("ix_opportunities_active_deadline", table_name="opportunities")
    op.drop_table("opportunities")
    op.drop_index("ix_awards_obligation_amount", table_name="awards")
    op.drop_index("ix_awards_naics_obligation_date", table_name="awards")
    op.drop_index("ix_awards_vendor_obligation_date", table_name="awards")
    op.drop_index("ix_awards_agency_obligation_date", table_name="awards")
    op.drop_table("awards")
    op.drop_index("uq_vendors_duns_not_null", table_name="vendors")
    op.drop_index("uq_vendors_uei_not_null", table_name="vendors")
    op.drop_index("ix_vendors_normalized_name", table_name="vendors")
    op.drop_table("vendors")
    op.drop_index("ix_agencies_parent_id", table_name="agencies")
    op.drop_table("agencies")
