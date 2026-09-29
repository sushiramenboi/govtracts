"""Add restartable USAspending transaction-ingestion attempts.

Revision ID: 20260927_0004
Revises: 20260927_0003
Create Date: 2026-09-27
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260927_0004"
down_revision: Union[str, Sequence[str], None] = "20260927_0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


ACTIVE_ATTEMPT_PREDICATE = "status NOT IN ('completed', 'failed')"


def upgrade() -> None:
    op.create_table(
        "usaspending_transaction_ingestion_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("fiscal_year", sa.SmallInteger(), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("expected_rows", sa.BigInteger(), nullable=True),
        sa.Column("status_url", sa.String(length=2048), nullable=True),
        sa.Column("file_url", sa.String(length=2048), nullable=True),
        sa.Column("remote_file_name", sa.String(length=512), nullable=True),
        sa.Column("archive_sha256", sa.String(length=64), nullable=True),
        sa.Column("archive_bytes", sa.BigInteger(), nullable=True),
        sa.Column("loaded_rows", sa.BigInteger(), nullable=True),
        sa.Column(
            "signed_obligation_total",
            sa.Numeric(precision=20, scale=2),
            nullable=True,
        ),
        sa.Column("checkpoint_id", sa.Uuid(), nullable=True),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column("last_error_detail", sa.Text(), nullable=True),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('created', 'counted', 'submitting', 'submitted', "
            "'export_finished', 'archive_hashed', 'loading', 'completed', "
            "'failed', 'submission_unknown')",
            name="ck_usaspending_transaction_ingestion_attempts_status_allowed",
        ),
        sa.CheckConstraint(
            "period_end >= period_start",
            name="ck_usaspending_transaction_ingestion_attempts_period_order",
        ),
        sa.CheckConstraint(
            "fiscal_year BETWEEN 2000 AND 9999",
            name="ck_usaspending_transaction_ingestion_attempts_fiscal_year_range",
        ),
        sa.CheckConstraint(
            "(expected_rows IS NULL OR expected_rows >= 0) AND "
            "(archive_bytes IS NULL OR archive_bytes >= 0) AND "
            "(loaded_rows IS NULL OR loaded_rows >= 0) AND failure_count >= 0",
            name="ck_usaspending_transaction_ingestion_attempts_counts_nonneg",
        ),
        sa.CheckConstraint(
            "status NOT IN ('counted', 'submitting', 'submitted', "
            "'export_finished', 'archive_hashed', 'loading', 'completed', "
            "'submission_unknown') OR expected_rows IS NOT NULL",
            name="ck_usaspending_transaction_ingestion_attempts_count_metadata",
        ),
        sa.CheckConstraint(
            "archive_sha256 IS NULL OR ("
            "length(archive_sha256) = 64 AND "
            "archive_sha256 = lower(archive_sha256) AND "
            "replace(replace(replace(replace(replace(replace(replace(replace("
            "replace(replace(replace(replace(replace(replace(replace(replace("
            "archive_sha256, '0', ''), '1', ''), '2', ''), '3', ''), "
            "'4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), "
            "'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
            name="ck_usaspending_transaction_ingestion_attempts_hash_lower_hex",
        ),
        sa.CheckConstraint(
            "status NOT IN ('submitted', 'export_finished', 'archive_hashed', "
            "'loading', 'completed') OR "
            "(status_url IS NOT NULL AND file_url IS NOT NULL "
            "AND remote_file_name IS NOT NULL)",
            name="ck_usaspending_transaction_ingestion_attempts_job_metadata",
        ),
        sa.CheckConstraint(
            "status NOT IN ('archive_hashed', 'loading', 'completed') OR "
            "(archive_sha256 IS NOT NULL AND archive_bytes IS NOT NULL)",
            name="ck_usaspending_transaction_ingestion_attempts_archive_metadata",
        ),
        sa.CheckConstraint(
            "((status IN ('completed', 'failed')) = (completed_at IS NOT NULL)) AND "
            "(status != 'completed' OR "
            "(checkpoint_id IS NOT NULL AND expected_rows IS NOT NULL "
            "AND loaded_rows IS NOT NULL AND expected_rows = loaded_rows "
            "AND signed_obligation_total IS NOT NULL))",
            name="ck_usaspending_transaction_ingestion_attempts_terminal_fields",
        ),
        sa.CheckConstraint(
            "status NOT IN ('failed', 'submission_unknown') "
            "OR last_error_code IS NOT NULL",
            name="ck_usaspending_transaction_ingestion_attempts_error_metadata",
        ),
        sa.ForeignKeyConstraint(
            ["checkpoint_id"],
            ["usaspending_ingestion_checkpoints.id"],
            name="fk_usaspending_attempt_checkpoint",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name="pk_usaspending_transaction_ingestion_attempts",
        ),
    )
    op.create_index(
        "uq_usaspending_attempts_active_period",
        "usaspending_transaction_ingestion_attempts",
        ["source", "period_start", "period_end"],
        unique=True,
        postgresql_where=sa.text(ACTIVE_ATTEMPT_PREDICATE),
    )
    op.create_index(
        "ix_usaspending_attempts_period_status",
        "usaspending_transaction_ingestion_attempts",
        ["source", "period_start", "period_end", "status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_usaspending_attempts_period_status",
        table_name="usaspending_transaction_ingestion_attempts",
    )
    op.drop_index(
        "uq_usaspending_attempts_active_period",
        table_name="usaspending_transaction_ingestion_attempts",
    )
    op.drop_table("usaspending_transaction_ingestion_attempts")
