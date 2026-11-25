import os
from alembic import context
from financedb.storage import DEFAULT_DATABASE_URL, Store, metadata


store = Store(os.getenv("FINANCEDB_DATABASE_URL", DEFAULT_DATABASE_URL))
try:
    with store.engine.connect() as connection:
        context.configure(connection=connection, target_metadata=metadata)
        with context.begin_transaction():
            context.run_migrations()
finally:
    store.close()
