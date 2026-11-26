"""Historical adjustment of the legacy payload column default."""

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade():
    pass  # Revision 0004 removes the column; its default no longer matters.


def downgrade():
    raise RuntimeError("旧版采集器依赖原始数据列，不能自动恢复该存储方式")
