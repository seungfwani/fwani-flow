from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config
from sqlalchemy import pool

from config import Config
from core.database import BaseDB

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = BaseDB.metadata
target_metadata.schema = Config.DB_SCHEMA if Config.DB_TYPE == "postgresql" else None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.
if Config.DB_TYPE == "postgresql":
    config.set_main_option(
        "sqlalchemy.url",
        f"{Config.DB_TYPE}://{Config.DB_USERNAME}:{Config.DB_PASSWORD}@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
    )
else:  # sqlite
    config.set_main_option(
        "sqlalchemy.url",
        f"{Config.DB_TYPE}:///{Config.DB_NAME}"
    )

def include_name(name, type_, parent_names):
    """
    Alembic autogenerate 중 특정 스키마만 관리하도록 설정.
    """
    if type_ == "schema":
        return name == Config.DB_SCHEMA
    return True

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        # DB 타입에 따라 분기
        if Config.DB_TYPE == "postgresql":
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                include_schemas=True,
                include_name=include_name,
                version_table_schema=Config.DB_SCHEMA,
            )
        else:  # SQLite
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                include_schemas=False,
            )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
