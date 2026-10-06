"""Add durable USAspending backfill attempt subdivisions.

Revision ID: 20261004_0006
Revises: 20261003_0005
Create Date: 2026-10-04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20261004_0006"
down_revision: Union[str, Sequence[str], None] = "20261003_0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


TABLE_NAME = "usaspending_transaction_ingestion_attempts"
STATUS_CONSTRAINT = "ck_usaspending_transaction_ingestion_attempts_status_allowed"
COUNT_CONSTRAINT = "ck_usaspending_transaction_ingestion_attempts_count_metadata"
TERMINAL_CONSTRAINT = "ck_usaspending_transaction_ingestion_attempts_terminal_fields"
SELF_PARENT_CONSTRAINT = "ck_usaspending_transaction_ingestion_attempts_not_self_parent"
SUBDIVISION_CONSTRAINT = "ck_usaspending_transaction_ingestion_attempts_subdivision"
PARENT_FOREIGN_KEY = "fk_usaspending_attempt_parent"
PARENT_UNIQUE = "uq_usaspending_attempts_parent_period"
PARENT_INDEX = "ix_usaspending_attempts_parent"

STATUS_CHECK = (
    "status IN ('created', 'counted', 'submitting', 'submitted', "
    "'export_finished', 'archive_hashed', 'loading', 'completed', "
    "'failed', 'submission_unknown', 'subdivided')"
)
COUNT_CHECK = (
    "status NOT IN ('counted', 'submitting', 'submitted', "
    "'export_finished', 'archive_hashed', 'loading', 'completed', "
    "'submission_unknown', 'subdivided') OR expected_rows IS NOT NULL"
)
TERMINAL_CHECK = (
    "((status IN ('completed', 'failed', 'subdivided')) = "
    "(completed_at IS NOT NULL)) AND "
    "(status != 'completed' OR "
    "(checkpoint_id IS NOT NULL AND expected_rows IS NOT NULL "
    "AND export_rows IS NOT NULL AND loaded_rows IS NOT NULL "
    "AND export_rows = loaded_rows "
    "AND signed_obligation_total IS NOT NULL))"
)
SUBDIVISION_CHECK = (
    "status != 'subdivided' OR ("
    "expected_rows > 500000 AND export_rows IS NULL "
    "AND export_columns IS NULL AND status_url IS NULL "
    "AND file_url IS NULL AND remote_file_name IS NULL "
    "AND archive_sha256 IS NULL AND archive_bytes IS NULL "
    "AND loaded_rows IS NULL AND signed_obligation_total IS NULL "
    "AND checkpoint_id IS NULL AND last_error_code IS NULL "
    "AND last_error_detail IS NULL)"
)

OLD_STATUS_CHECK = (
    "status IN ('created', 'counted', 'submitting', 'submitted', "
    "'export_finished', 'archive_hashed', 'loading', 'completed', "
    "'failed', 'submission_unknown')"
)
OLD_COUNT_CHECK = (
    "status NOT IN ('counted', 'submitting', 'submitted', "
    "'export_finished', 'archive_hashed', 'loading', 'completed', "
    "'submission_unknown') OR expected_rows IS NOT NULL"
)
OLD_TERMINAL_CHECK = (
    "((status IN ('completed', 'failed')) = (completed_at IS NOT NULL)) AND "
    "(status != 'completed' OR "
    "(checkpoint_id IS NOT NULL AND expected_rows IS NOT NULL "
    "AND export_rows IS NOT NULL AND loaded_rows IS NOT NULL "
    "AND export_rows = loaded_rows "
    "AND signed_obligation_total IS NOT NULL))"
)


def upgrade() -> None:
    op.add_column(TABLE_NAME, sa.Column("parent_attempt_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        PARENT_FOREIGN_KEY,
        TABLE_NAME,
        TABLE_NAME,
        ["parent_attempt_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        SELF_PARENT_CONSTRAINT,
        TABLE_NAME,
        "parent_attempt_id IS NULL OR parent_attempt_id != id",
    )
    op.create_index(
        PARENT_UNIQUE,
        TABLE_NAME,
        ["parent_attempt_id", "period_start", "period_end"],
        unique=True,
        postgresql_where=sa.text(
            "parent_attempt_id IS NOT NULL AND status != 'failed'"
        ),
    )
    op.create_index(PARENT_INDEX, TABLE_NAME, ["parent_attempt_id"], unique=False)

    op.drop_constraint(STATUS_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(COUNT_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(TERMINAL_CONSTRAINT, TABLE_NAME, type_="check")
    op.create_check_constraint(STATUS_CONSTRAINT, TABLE_NAME, STATUS_CHECK)
    op.create_check_constraint(COUNT_CONSTRAINT, TABLE_NAME, COUNT_CHECK)
    op.create_check_constraint(TERMINAL_CONSTRAINT, TABLE_NAME, TERMINAL_CHECK)
    op.create_check_constraint(
        SUBDIVISION_CONSTRAINT,
        TABLE_NAME,
        SUBDIVISION_CHECK,
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM {TABLE_NAME}
                    WHERE status = 'subdivided' OR parent_attempt_id IS NOT NULL
                ) THEN
                    RAISE EXCEPTION USING
                        MESSAGE = 'used USAspending subdivisions cannot be downgraded safely';
                END IF;
            END
            $$
            """
        )
    )

    op.drop_constraint(SUBDIVISION_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(STATUS_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(COUNT_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(TERMINAL_CONSTRAINT, TABLE_NAME, type_="check")
    op.create_check_constraint(STATUS_CONSTRAINT, TABLE_NAME, OLD_STATUS_CHECK)
    op.create_check_constraint(COUNT_CONSTRAINT, TABLE_NAME, OLD_COUNT_CHECK)
    op.create_check_constraint(
        TERMINAL_CONSTRAINT,
        TABLE_NAME,
        OLD_TERMINAL_CHECK,
    )

    op.drop_index(PARENT_INDEX, table_name=TABLE_NAME)
    op.drop_index(PARENT_UNIQUE, table_name=TABLE_NAME)
    op.drop_constraint(SELF_PARENT_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(PARENT_FOREIGN_KEY, TABLE_NAME, type_="foreignkey")
    op.drop_column(TABLE_NAME, "parent_attempt_id")
