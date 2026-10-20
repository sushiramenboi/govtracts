"""Add immutable lineage for same-job USAspending recovery attempts.

Revision ID: 20261017_0007
Revises: 20261004_0006
Create Date: 2026-10-17
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20261017_0007"
down_revision: Union[str, Sequence[str], None] = "20261004_0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


TABLE_NAME = "usaspending_transaction_ingestion_attempts"
RECOVERY_FOREIGN_KEY = "fk_usaspending_attempt_recovery_of"
SELF_RECOVERY_CONSTRAINT = (
    "ck_usaspending_transaction_ingestion_attempts_not_self_recovery"
)
RECOVERY_UNIQUE = "uq_usaspending_attempts_recovery_of"


def upgrade() -> None:
    op.add_column(
        TABLE_NAME,
        sa.Column("recovery_of_attempt_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        RECOVERY_FOREIGN_KEY,
        TABLE_NAME,
        TABLE_NAME,
        ["recovery_of_attempt_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        SELF_RECOVERY_CONSTRAINT,
        TABLE_NAME,
        "recovery_of_attempt_id IS NULL OR recovery_of_attempt_id != id",
    )
    op.create_index(
        RECOVERY_UNIQUE,
        TABLE_NAME,
        ["recovery_of_attempt_id"],
        unique=True,
        postgresql_where=sa.text("recovery_of_attempt_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM {TABLE_NAME}
                    WHERE recovery_of_attempt_id IS NOT NULL
                ) THEN
                    RAISE EXCEPTION USING
                        MESSAGE = 'used USAspending recovery lineage cannot be downgraded safely';
                END IF;
            END
            $$
            """
        )
    )

    op.drop_index(RECOVERY_UNIQUE, table_name=TABLE_NAME)
    op.drop_constraint(SELF_RECOVERY_CONSTRAINT, TABLE_NAME, type_="check")
    op.drop_constraint(RECOVERY_FOREIGN_KEY, TABLE_NAME, type_="foreignkey")
    op.drop_column(TABLE_NAME, "recovery_of_attempt_id")
