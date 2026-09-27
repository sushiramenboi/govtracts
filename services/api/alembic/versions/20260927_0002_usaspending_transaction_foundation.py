"""Add the minimal USAspending transaction foundation.

Revision ID: 20260927_0002
Revises: 20260913_0001
Create Date: 2026-09-27
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260927_0002"
down_revision: Union[str, Sequence[str], None] = "20260913_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "usaspending_ingestion_checkpoints",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("fiscal_year", sa.SmallInteger(), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("status_url", sa.String(length=2048), nullable=True),
        sa.Column("remote_file_name", sa.String(length=512), nullable=True),
        sa.Column("archive_sha256", sa.String(length=64), nullable=True),
        sa.Column("expected_rows", sa.BigInteger(), nullable=True),
        sa.Column("loaded_rows", sa.BigInteger(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "expected_rows IS NULL OR expected_rows >= 0",
            name="ck_usaspending_ingestion_checkpoints_expected_rows_nonnegative",
        ),
        sa.CheckConstraint(
            "loaded_rows >= 0",
            name="ck_usaspending_ingestion_checkpoints_loaded_rows_nonnegative",
        ),
        sa.CheckConstraint(
            "period_end >= period_start",
            name="ck_usaspending_ingestion_checkpoints_period_order",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_usaspending_ingestion_checkpoints"),
        sa.UniqueConstraint(
            "source",
            "period_start",
            "period_end",
            name="uq_usaspending_checkpoints_source_period",
        ),
    )
    op.create_index(
        "ix_usaspending_checkpoints_fiscal_period",
        "usaspending_ingestion_checkpoints",
        ["fiscal_year", "period_start", "period_end"],
        unique=False,
    )

    op.create_table(
        "usaspending_transactions",
        sa.Column("stable_transaction_id", sa.String(length=512), nullable=False),
        sa.Column("ingestion_checkpoint_id", sa.Uuid(), nullable=False),
        sa.Column("generated_award_id", sa.String(length=512), nullable=False),
        sa.Column("display_award_id", sa.String(length=255), nullable=False),
        sa.Column("award_type_code", sa.String(length=1), nullable=False),
        sa.Column("action_date", sa.Date(), nullable=False),
        sa.Column("fiscal_year", sa.SmallInteger(), nullable=False),
        sa.Column("federal_action_obligation", sa.Numeric(precision=20, scale=2), nullable=False),
        sa.Column("recipient_name", sa.String(length=512), nullable=False),
        sa.Column("recipient_uei", sa.String(length=12), nullable=False),
        sa.Column("recipient_parent_name", sa.String(length=512), nullable=True),
        sa.Column("recipient_parent_uei", sa.String(length=12), nullable=True),
        sa.Column("awarding_agency_name", sa.String(length=255), nullable=False),
        sa.Column("awarding_subagency_name", sa.String(length=255), nullable=False),
        sa.Column("naics_code", sa.String(length=6), nullable=True),
        sa.Column("psc_code", sa.String(length=4), nullable=True),
        sa.Column("transaction_description", sa.Text(), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "fiscal_year BETWEEN 2000 AND 9999",
            name="ck_usaspending_transactions_fiscal_year_range",
        ),
        sa.CheckConstraint(
            "award_type_code IN ('A', 'B', 'C', 'D')",
            name="ck_usaspending_transactions_prime_award_type",
        ),
        sa.ForeignKeyConstraint(
            ["generated_award_id"],
            ["awards.usa_generated_id"],
            name="fk_usaspending_tx_award",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["ingestion_checkpoint_id"],
            ["usaspending_ingestion_checkpoints.id"],
            name="fk_usaspending_tx_checkpoint",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "stable_transaction_id",
            name="pk_usaspending_transactions",
        ),
    )
    op.create_index(
        "ix_usaspending_transactions_award_action",
        "usaspending_transactions",
        ["generated_award_id", "action_date"],
        unique=False,
    )
    op.create_index(
        "ix_usaspending_transactions_checkpoint",
        "usaspending_transactions",
        ["ingestion_checkpoint_id"],
        unique=False,
    )
    op.create_index(
        "ix_usaspending_transactions_fiscal_action",
        "usaspending_transactions",
        ["fiscal_year", "action_date"],
        unique=False,
    )
    op.create_index(
        "ix_usaspending_transactions_fiscal_agency",
        "usaspending_transactions",
        ["fiscal_year", "awarding_agency_name"],
        unique=False,
    )
    op.create_index(
        "ix_usaspending_transactions_fiscal_recipient",
        "usaspending_transactions",
        ["fiscal_year", "recipient_uei"],
        unique=False,
    )
    op.create_index(
        "ix_usaspending_transactions_fiscal_naics",
        "usaspending_transactions",
        ["fiscal_year", "naics_code"],
        unique=False,
    )
    op.create_index(
        "ix_usaspending_transactions_fiscal_psc",
        "usaspending_transactions",
        ["fiscal_year", "psc_code"],
        unique=False,
    )

    op.create_table(
        "usaspending_transaction_preset_matches",
        sa.Column("stable_transaction_id", sa.String(length=512), nullable=False),
        sa.Column("preset_key", sa.String(length=64), nullable=False),
        sa.Column("preset_version", sa.Integer(), nullable=False),
        sa.Column("match_basis", sa.String(length=16), nullable=False),
        sa.Column("matched_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "match_basis IN ('naics', 'psc', 'both')",
            name="ck_usaspending_transaction_preset_matches_match_basis",
        ),
        sa.CheckConstraint(
            "preset_version > 0",
            name="ck_usaspending_transaction_preset_matches_version_positive",
        ),
        sa.ForeignKeyConstraint(
            ["stable_transaction_id"],
            ["usaspending_transactions.stable_transaction_id"],
            name="fk_usaspending_preset_match_transaction",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "stable_transaction_id",
            "preset_key",
            "preset_version",
            name="pk_usaspending_transaction_preset_matches",
        ),
    )
    op.create_index(
        "ix_usaspending_preset_matches_preset_transaction",
        "usaspending_transaction_preset_matches",
        ["preset_key", "preset_version", "stable_transaction_id"],
        unique=False,
    )

    op.create_table(
        "usaspending_period_aggregates",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("fiscal_year", sa.SmallInteger(), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("preset_key", sa.String(length=64), nullable=False),
        sa.Column("preset_version", sa.Integer(), nullable=False),
        sa.Column("dimension_type", sa.String(length=32), nullable=False),
        sa.Column("dimension_key", sa.String(length=512), nullable=False),
        sa.Column("gross_positive_obligations", sa.Numeric(precision=20, scale=2), nullable=False),
        sa.Column("signed_deobligations", sa.Numeric(precision=20, scale=2), nullable=False),
        sa.Column("net_obligations", sa.Numeric(precision=20, scale=2), nullable=False),
        sa.Column("transaction_count", sa.BigInteger(), nullable=False),
        sa.Column("distinct_award_count", sa.BigInteger(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "distinct_award_count >= 0",
            name="ck_usaspending_period_aggregates_award_count_nonnegative",
        ),
        sa.CheckConstraint(
            "gross_positive_obligations >= 0",
            name="ck_usaspending_period_aggregates_gross_positive_nonnegative",
        ),
        sa.CheckConstraint(
            "net_obligations = gross_positive_obligations + signed_deobligations",
            name="ck_usaspending_period_aggregates_net_obligations_formula",
        ),
        sa.CheckConstraint(
            "period_end >= period_start",
            name="ck_usaspending_period_aggregates_period_order",
        ),
        sa.CheckConstraint(
            "preset_version > 0",
            name="ck_usaspending_period_aggregates_preset_version_positive",
        ),
        sa.CheckConstraint(
            "signed_deobligations <= 0",
            name="ck_usaspending_period_aggregates_signed_deob_nonpositive",
        ),
        sa.CheckConstraint(
            "transaction_count >= 0",
            name="ck_usaspending_period_aggregates_transaction_count_nonnegative",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_usaspending_period_aggregates"),
        sa.UniqueConstraint(
            "fiscal_year",
            "period_start",
            "period_end",
            "preset_key",
            "preset_version",
            "dimension_type",
            "dimension_key",
            name="uq_usaspending_period_aggregates_grain",
        ),
    )
    op.create_index(
        "ix_usaspending_period_aggregates_ranking",
        "usaspending_period_aggregates",
        [
            "fiscal_year",
            "preset_key",
            "preset_version",
            "dimension_type",
            "net_obligations",
        ],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_usaspending_period_aggregates_ranking",
        table_name="usaspending_period_aggregates",
    )
    op.drop_table("usaspending_period_aggregates")
    op.drop_index(
        "ix_usaspending_preset_matches_preset_transaction",
        table_name="usaspending_transaction_preset_matches",
    )
    op.drop_table("usaspending_transaction_preset_matches")
    op.drop_index(
        "ix_usaspending_transactions_fiscal_psc",
        table_name="usaspending_transactions",
    )
    op.drop_index(
        "ix_usaspending_transactions_fiscal_naics",
        table_name="usaspending_transactions",
    )
    op.drop_index(
        "ix_usaspending_transactions_fiscal_recipient",
        table_name="usaspending_transactions",
    )
    op.drop_index(
        "ix_usaspending_transactions_fiscal_agency",
        table_name="usaspending_transactions",
    )
    op.drop_index(
        "ix_usaspending_transactions_fiscal_action",
        table_name="usaspending_transactions",
    )
    op.drop_index(
        "ix_usaspending_transactions_checkpoint",
        table_name="usaspending_transactions",
    )
    op.drop_index(
        "ix_usaspending_transactions_award_action",
        table_name="usaspending_transactions",
    )
    op.drop_table("usaspending_transactions")
    op.drop_index(
        "ix_usaspending_checkpoints_fiscal_period",
        table_name="usaspending_ingestion_checkpoints",
    )
    op.drop_table("usaspending_ingestion_checkpoints")
