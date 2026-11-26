import os
from alembic import context
from financedb.business_tables import business_metadata
from financedb.storage import DEFAULT_DATABASE_URL, Store, metadata, reject_legacy_schema


store = Store(os.getenv("FINANCEDB_DATABASE_URL", DEFAULT_DATABASE_URL))
try:
    with store.engine.begin() as connection:
        reject_legacy_schema(connection, allow_upgrade=True)
        context.configure(connection=connection, target_metadata=[metadata, business_metadata])
        with context.begin_transaction():
            context.run_migrations()
finally:
    store.close()
