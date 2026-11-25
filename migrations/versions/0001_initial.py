"""Create the portable offline store."""

from alembic import op
from sqlalchemy import select
from financedb.storage import metadata, schema_versions, timestamp

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    metadata.create_all(conn)
    if conn.scalar(select(schema_versions.c.version).where(schema_versions.c.version == 1)) is None:
        conn.execute(schema_versions.insert().values(version=1, applied_at=timestamp()))


def downgrade():
    metadata.drop_all(op.get_bind())
