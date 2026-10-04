"""Persist USAspending export-bound row and column counts.

Revision ID: 20261003_0005
Revises: 20260927_0004
Create Date: 2026-10-03
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20261003_0005"
down_revision: Union[str, Sequence[str], None] = "20260927_0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


TABLE_NAME = "usaspending_transaction_ingestion_attempts"
COUNTS_CONSTRAINT = "ck_usaspending_transaction_ingestion_attempts_counts_nonneg"
EXPORT_CONSTRAINT = "ck_usaspending_transaction_ingestion_attempts_export_metadata"
TERMINAL_CONSTRAINT = "ck_usaspending_transaction_ingestion_attempts_terminal_fields"

COUNTS_CHECK = (
    "(expected_rows IS NULL OR expected_rows >= 0) AND "
    "(export_rows IS NULL OR export_rows BETWEEN 0 AND 500000) AND "
    "(export_columns IS NULL OR export_columns = 16) AND "
    "(archive_bytes IS NULL OR archive_bytes >= 0) AND "
    "(loaded_rows IS NULL OR loaded_rows >= 0) AND failure_count >= 0"
)
EXPORT_METADATA_CHECK = (
    "status NOT IN ('export_finished', 'archive_hashed', 'loading', "
    "'completed') OR export_rows IS NOT NULL"
)
TERMINAL_FIELDS_CHECK = (
    "((status IN ('completed', 'failed')) = (completed_at IS NOT NULL)) AND "
    "(status != 'completed' OR "
    "(checkpoint_id IS NOT NULL AND expected_rows IS NOT NULL "
    "AND export_rows IS NOT NULL AND loaded_rows IS NOT NULL "
    "AND export_rows = loaded_rows "
    "AND signed_obligation_total IS NOT NULL))"
)

OLD_COUNTS_CHECK = (
    "(expected_rows IS NULL OR expected_rows >= 0) AND "
    "(archive_bytes IS NULL OR archive_bytes >= 0) AND "
    "(loaded_rows IS NULL OR loaded_rows >= 0) AND failure_count >= 0"
)
OLD_TERMINAL_FIELDS_CHECK = (
    "((status IN ('completed', 'failed')) = (completed_at IS NOT NULL)) AND "
    "(status != 'completed' OR "
    "(checkpoint_id IS NOT NULL AND expected_rows IS NOT NULL "
    "AND loaded_rows IS NOT NULL AND expected_rows = loaded_rows "
    "AND signed_obligation_total IS NOT NULL))"
)


def upgrade() -> None:
    op.add_column(TABLE_NAME, sa.Column("export_rows", sa.BigInteger(), nullable=True))
    op.add_column(
        TABLE_NAME,
        sa.Column("export_columns", sa.SmallInteger(), nullable=True),
    )
    op.execute(
        sa.text(
            f"UPDATE {TABLE_NAME} "
            "SET export_rows = loaded_rows "
            "WHERE status = 'completed'"
        )
    )
    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM {TABLE_NAME}
                    WHERE status IN ('export_finished', 'archive_hashed', 'loading')
                      AND export_rows IS NULL
                ) THEN
                    RAISE EXCEPTION USING
                        MESSAGE = 'unreconstructable active USAspending export count';
                END IF;
            END
            $$
            """
        )
    )

    op.drop_constraint(COUNTS_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(TERMINAL_CONSTRAINT, TABLE_NAME, type_="check")
    op.create_check_constraint(COUNTS_CONSTRAINT, TABLE_NAME, COUNTS_CHECK)
    op.create_check_constraint(EXPORT_CONSTRAINT, TABLE_NAME, EXPORT_METADATA_CHECK)
    op.create_check_constraint(
        TERMINAL_CONSTRAINT,
        TABLE_NAME,
        TERMINAL_FIELDS_CHECK,
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM {TABLE_NAME}
                    WHERE status IN ('export_finished', 'archive_hashed', 'loading')
                ) THEN
                    RAISE EXCEPTION USING
                        MESSAGE = 'active USAspending export state cannot be downgraded safely';
                END IF;
                IF EXISTS (
                    SELECT 1
                    FROM {TABLE_NAME}
                    WHERE status = 'completed'
                      AND expected_rows IS DISTINCT FROM loaded_rows
                ) THEN
                    RAISE EXCEPTION USING
                        MESSAGE = 'completed USAspending count drift cannot be downgraded safely';
                END IF;
                IF EXISTS (
                    SELECT 1
                    FROM {TABLE_NAME}
                    WHERE status = 'failed'
                      AND (
                          export_columns IS NOT NULL
                          OR (
                              export_rows IS NOT NULL
                              AND export_rows IS DISTINCT FROM expected_rows
                          )
                      )
                ) THEN
                    RAISE EXCEPTION USING
                        MESSAGE = 'failed USAspending export metadata cannot be downgraded safely';
                END IF;
            END
            $$
            """
        )
    )

    op.drop_constraint(COUNTS_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(EXPORT_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(TERMINAL_CONSTRAINT, TABLE_NAME, type_="check")
    op.create_check_constraint(COUNTS_CONSTRAINT, TABLE_NAME, OLD_COUNTS_CHECK)
    op.create_check_constraint(
        TERMINAL_CONSTRAINT,
        TABLE_NAME,
        OLD_TERMINAL_FIELDS_CHECK,
    )
    op.drop_column(TABLE_NAME, "export_columns")
    op.drop_column(TABLE_NAME, "export_rows")
