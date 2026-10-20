"""Normalize the recovery lineage check constraint name.

Revision ID: 20261020_0008
Revises: 20261017_0007
Create Date: 2026-10-20
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20261020_0008"
down_revision: Union[str, Sequence[str], None] = "20261017_0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


GENERATED_NAME = "ck_usaspending_transaction_ingestion_attempts_ck_usaspe_ca28"
MODEL_NAME = "ck_usaspending_transaction_ingestion_attempts_not_self_recovery"
TABLE_NAME = "usaspending_transaction_ingestion_attempts"


def _rename(source: str, target: str) -> None:
    # Raw PostgreSQL DDL avoids applying the metadata naming convention again.
    # The identifiers are fixed, reviewed constants, not runtime input.
    op.execute(
        sa.text(
            f'ALTER TABLE "{TABLE_NAME}" RENAME CONSTRAINT "{source}" TO "{target}"'
        )
    )


def upgrade() -> None:
    _rename(GENERATED_NAME, MODEL_NAME)


def downgrade() -> None:
    _rename(MODEL_NAME, GENERATED_NAME)
