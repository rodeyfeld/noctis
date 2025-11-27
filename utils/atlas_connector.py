"""
Atlas database connection utilities for Noctis flows.

Uses asyncpg for async PostgreSQL operations and connectorx for fast bulk reads.
"""

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import asyncpg
import connectorx as cx
import polars as pl
from prefect import get_run_logger
from prefect.blocks.system import Secret


@dataclass
class AtlasConfig:
    """Configuration for Atlas database connection."""

    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "AtlasConfig":
        """Load configuration from environment variables."""
        return cls(
            host=os.environ.get("ATLAS_DB_HOST", "localhost"),
            port=int(os.environ.get("ATLAS_DB_PORT", "5432")),
            database=os.environ.get("ATLAS_DB_NAME", "augur"),
            user=os.environ.get("ATLAS_DB_USER", "atlas_user"),
            password=os.environ.get("ATLAS_DB_PASSWORD", "atlas_password"),
        )

    @classmethod
    async def from_prefect_secret(cls, secret_name: str = "atlas-db-config") -> "AtlasConfig":
        """Load configuration from Prefect Secret block."""
        try:
            secret = await Secret.load(secret_name)
            config = secret.get()
            return cls(
                host=config["host"],
                port=int(config["port"]),
                database=config["database"],
                user=config["user"],
                password=config["password"],
            )
        except Exception:
            # Fall back to environment variables
            return cls.from_env()

    @property
    def dsn(self) -> str:
        """Return PostgreSQL DSN for asyncpg."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def connectorx_uri(self) -> str:
        """Return connection URI for connectorx (uses psycopg2 protocol)."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class AtlasConnector:
    """
    Async PostgreSQL connector for Atlas database.
    
    Uses asyncpg for async operations and connectorx for bulk reads.
    """

    def __init__(self, config: AtlasConfig):
        self.config = config
        self._pool: asyncpg.Pool | None = None

    async def create_pool(self, min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
        """Create a connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                min_size=min_size,
                max_size=max_size,
            )
        return self._pool

    async def close_pool(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def connection(self):
        """Get a connection from the pool."""
        pool = await self.create_pool()
        async with pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def transaction(self):
        """Get a connection with an active transaction."""
        async with self.connection() as conn:
            async with conn.transaction():
                yield conn

    async def execute(self, query: str, *args) -> str:
        """Execute a query and return status."""
        async with self.connection() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, args: list[tuple]) -> None:
        """Execute a query with multiple parameter sets."""
        async with self.connection() as conn:
            await conn.executemany(query, args)

    async def fetch(self, query: str, *args) -> list[asyncpg.Record]:
        """Fetch all rows from a query."""
        async with self.connection() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> asyncpg.Record | None:
        """Fetch a single row from a query."""
        async with self.connection() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args) -> Any:
        """Fetch a single value from a query."""
        async with self.connection() as conn:
            return await conn.fetchval(query, *args)

    def read_sql(self, query: str) -> pl.DataFrame:
        """
        Read SQL query results into a Polars DataFrame using connectorx.
        
        This is significantly faster than traditional methods for large datasets.
        Note: This is a synchronous operation as connectorx doesn't support async.
        """
        return cx.read_sql(self.config.connectorx_uri, query, return_type="polars")

    def read_sql_pandas(self, query: str):
        """
        Read SQL query results into a Pandas DataFrame using connectorx.
        """
        return cx.read_sql(self.config.connectorx_uri, query, return_type="pandas")


@asynccontextmanager
async def get_atlas_connector():
    """
    Context manager for Atlas database operations.
    
    Usage:
        async with get_atlas_connector() as atlas:
            await atlas.execute("INSERT INTO ...")
            df = atlas.read_sql("SELECT * FROM ...")
    """
    config = AtlasConfig.from_env()
    connector = AtlasConnector(config)
    try:
        await connector.create_pool()
        yield connector
    finally:
        await connector.close_pool()


async def get_connector() -> AtlasConnector:
    """Get an Atlas connector instance (caller manages lifecycle)."""
    config = AtlasConfig.from_env()
    connector = AtlasConnector(config)
    await connector.create_pool()
    return connector

