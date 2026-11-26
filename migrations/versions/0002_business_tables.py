"""Historical introduction of per-dataset business tables."""

revision = "0002"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    pass  # Revision 0004 creates the current schema or migrates an existing one.


def downgrade():
    raise RuntimeError("业务表版本没有自动降级，请使用独立目标库")
