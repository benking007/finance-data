"""Keep only business fields in dataset tables."""

from alembic import op

from financedb.storage import create_schema


revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade():
    create_schema(op.get_bind(), upgrade=True)


def downgrade():
    raise RuntimeError("已删除旧版非业务列，仅可从迁移前备份恢复")
