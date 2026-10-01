"""Exercise the optional Fleet package from its standalone source tree."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "packages" / "ecs-fleet"))

import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest_asyncio.fixture
async def fleet_database():
    uri = os.environ.get("TEST_POSTGRES_URI")
    if not uri:
        pytest.skip("requires TEST_POSTGRES_URI for real Fleet transactions")
    schema = "fleet_test_" + uuid.uuid4().hex
    admin = create_async_engine(uri)
    async with admin.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(uri, connect_args={"server_settings": {"search_path": schema}}, pool_size=8)
    try:
        yield engine, async_sessionmaker(engine, expire_on_commit=False), schema
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()
