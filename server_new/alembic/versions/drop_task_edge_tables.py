"""Drop task_input, edge, task tables (graph data in FlowSnapshot payload only)

Revision ID: a1b2c3d4e5f6
Revises: 458877c4da5b
Create Date: 2026-03-13

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "458877c4da5b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("ix_workflow_task_input_id", table_name="task_input", schema="workflow")
    op.drop_table("task_input", schema="workflow")
    op.drop_index("ix_workflow_edge_id", table_name="edge", schema="workflow")
    op.drop_table("edge", schema="workflow")
    op.drop_index("ix_workflow_task_id", table_name="task", schema="workflow")
    op.drop_table("task", schema="workflow")


def downgrade() -> None:
    op.create_table(
        "task",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("flow_id", sa.String(), nullable=False),
        sa.Column("variable_id", sa.String(), nullable=False),
        sa.Column("kind", sa.String(), nullable=True),
        sa.Column("code_string", sa.Text(), nullable=True),
        sa.Column("code_hash", sa.String(), nullable=True),
        sa.Column("python_libraries", sa.JSON(), nullable=True),
        sa.Column("system_function_id", sa.String(), nullable=True),
        sa.Column("impl_namespace", sa.String(), nullable=True),
        sa.Column("impl_callable", sa.String(), nullable=True),
        sa.Column("input_properties", sa.JSON(), nullable=True),
        sa.Column("output_properties", sa.JSON(), nullable=True),
        sa.Column("ui_type", sa.String(), nullable=True),
        sa.Column("ui_label", sa.String(), nullable=True),
        sa.Column("ui_position", sa.JSON(), nullable=True),
        sa.Column("ui_style", sa.JSON(), nullable=True),
        sa.Column("ui_class", sa.String(), nullable=True),
        sa.Column("ui_extra_data", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["flow_id"], ["workflow.flow.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["system_function_id"], ["workflow.system_function.id"]),
        sa.PrimaryKeyConstraint("id"),
        schema="workflow",
    )
    op.create_index("ix_workflow_task_id", "task", ["id"], unique=False, schema="workflow")
    op.create_table(
        "edge",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("flow_id", sa.String(), nullable=False),
        sa.Column("from_task_id", sa.String(), nullable=True),
        sa.Column("to_task_id", sa.String(), nullable=True),
        sa.Column("ui_type", sa.String(), nullable=True),
        sa.Column("ui_label", sa.String(), nullable=True),
        sa.Column("ui_labelStyle", sa.JSON(), nullable=True),
        sa.Column("ui_labelBgStyle", sa.JSON(), nullable=True),
        sa.Column("ui_labelBgPadding", sa.JSON(), nullable=True),
        sa.Column("ui_labelBgBorderRadius", sa.Float(), nullable=True),
        sa.Column("ui_style", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["flow_id"], ["workflow.flow.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["from_task_id"], ["workflow.task.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["to_task_id"], ["workflow.task.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        schema="workflow",
    )
    op.create_index("ix_workflow_edge_id", "edge", ["id"], unique=False, schema="workflow")
    op.create_table(
        "task_input",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("task_id", sa.String(), nullable=False),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("type", sa.String(), server_default="string", nullable=False),
        sa.Column("value", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["task_id"], ["workflow.task.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        schema="workflow",
    )
    op.create_index("ix_workflow_task_input_id", "task_input", ["id"], unique=False, schema="workflow")
