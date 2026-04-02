"""flow_snapshot: is_current/is_draft -> role (draft/published/archived)

Revision ID: c91a04f2b8e3
Revises: a1b2c3d4e5f6
Create Date: 2026-04-02

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "c91a04f2b8e3"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "workflow"
TABLE = "flow_snapshot"


def upgrade() -> None:
    op.add_column(
        TABLE,
        sa.Column("role", sa.String(length=20), nullable=True),
        schema=SCHEMA,
    )

    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.{TABLE} SET role = CASE
                WHEN is_current THEN 'published'
                WHEN is_draft THEN 'draft'
                ELSE 'archived'
            END
            """
        )
    )

    # 플로당 published는 최대 version 한 행만 유지
    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.{TABLE} fs
            SET role = 'archived'
            WHERE fs.role = 'published'
              AND EXISTS (
                SELECT 1 FROM {SCHEMA}.{TABLE} fs2
                WHERE fs2.flow_id = fs.flow_id
                  AND fs2.role = 'published'
                  AND fs2.version > fs.version
              )
            """
        )
    )

    # 플로당 draft는 최대 version 한 행만 유지
    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.{TABLE} fs
            SET role = 'archived'
            WHERE fs.role = 'draft'
              AND EXISTS (
                SELECT 1 FROM {SCHEMA}.{TABLE} fs2
                WHERE fs2.flow_id = fs.flow_id
                  AND fs2.role = 'draft'
                  AND fs2.version > fs.version
              )
            """
        )
    )

    # 공개본보다 낮은 버전의 draft -> archived
    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.{TABLE} d
            SET role = 'archived'
            FROM {SCHEMA}.{TABLE} p
            WHERE d.flow_id = p.flow_id
              AND p.role = 'published'
              AND d.role = 'draft'
              AND d.version < p.version
            """
        )
    )

    op.alter_column(
        TABLE,
        "role",
        existing_type=sa.String(length=20),
        nullable=False,
        server_default=sa.text("'archived'"),
        schema=SCHEMA,
    )

    op.drop_constraint("ck_current_xor_draft", TABLE, schema=SCHEMA, type_="check")
    op.drop_column(TABLE, "is_current", schema=SCHEMA)
    op.drop_column(TABLE, "is_draft", schema=SCHEMA)

    op.create_check_constraint(
        "ck_flow_snapshot_role",
        TABLE,
        "role IN ('draft', 'published', 'archived')",
        schema=SCHEMA,
    )
    op.create_index(
        "uq_flow_snapshot_one_draft",
        TABLE,
        ["flow_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("role = 'draft'"),
    )
    op.create_index(
        "uq_flow_snapshot_one_published",
        TABLE,
        ["flow_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("role = 'published'"),
    )


def downgrade() -> None:
    op.drop_index("uq_flow_snapshot_one_published", table_name=TABLE, schema=SCHEMA)
    op.drop_index("uq_flow_snapshot_one_draft", table_name=TABLE, schema=SCHEMA)
    op.drop_constraint("ck_flow_snapshot_role", TABLE, schema=SCHEMA, type_="check")

    op.add_column(
        TABLE,
        sa.Column("is_current", sa.Boolean(), server_default="false", nullable=False),
        schema=SCHEMA,
    )
    op.add_column(
        TABLE,
        sa.Column("is_draft", sa.Boolean(), server_default="false", nullable=False),
        schema=SCHEMA,
    )

    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.{TABLE} SET
                is_current = (role = 'published'),
                is_draft = (role = 'draft')
            """
        )
    )

    op.drop_column(TABLE, "role", schema=SCHEMA)

    op.create_check_constraint(
        "ck_current_xor_draft",
        TABLE,
        "NOT (is_current AND is_draft)",
        schema=SCHEMA,
    )
