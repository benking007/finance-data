"""Preserve the full precision and range of factor values on MySQL."""

from alembic import op
from sqlalchemy import inspect

from financedb.business_schema import ensure_factor_value_storage


revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    ensure_factor_value_storage(conn, inspect(conn).get_table_names(), upgrade=True)


def downgrade():
    raise RuntimeError("因子值可能超过旧列的范围，无法无损自动降级")
