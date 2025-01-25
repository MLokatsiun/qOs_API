from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from decouple import config
from contextlib import asynccontextmanager

URL_DATABASE_AS = config(
    "URL_DATABASE_AS")
URL_DATABASE_SYNC = config("DATABASE_URL")
engine = create_async_engine(URL_DATABASE_AS, echo=True, connect_args={"prepared_statement_cache_size": 0})

SessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


sync_engine = create_engine(
    URL_DATABASE_SYNC,
    echo=True
)

SyncSessionLocal = sessionmaker(
    bind=sync_engine,
    expire_on_commit=False
)

Base = declarative_base()

async def get_db():
    async with SessionLocal() as session:
        yield session

def get_sync_db():
    with SyncSessionLocal() as session:
        yield session