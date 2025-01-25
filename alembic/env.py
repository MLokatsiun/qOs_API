from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from models import Base
from decouple import config

DATABASE_URL = config("DATABASE_URL")

fileConfig(context.config.config_file_name)

target_metadata = Base.metadata

def get_url():
    """Отримує URL підключення до бази даних з файлу .env."""
    return DATABASE_URL

def run_migrations_offline():
    """Запускає міграції в 'offline' режимі."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"}
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    """Запускає міграції в 'online' режимі."""
    connectable = engine_from_config(
        {"sqlalchemy.url": get_url()},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
