"""Let inserts omit the legacy business payload column."""

from alembic import op

from financedb.business_tables import business_tables
from financedb.payload_schema import prepare_payload_defaults


revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade():
    prepare_payload_defaults(op.get_bind(), business_tables.values())


def downgrade():
    raise RuntimeError("旧版采集器依赖原始数据列，不能自动恢复该存储方式")
