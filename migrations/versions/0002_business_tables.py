"""Initialize the business-table database in an empty target."""

from alembic import op

from financedb.storage import create_schema


revision = "0002"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    create_schema(op.get_bind())


def downgrade():
    raise RuntimeError("业务表版本没有自动降级，请使用独立目标库")
