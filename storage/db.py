"""
Database connection pool and helper utilities for Neon PostgreSQL.

Uses psycopg2 with connection pooling. All queries go through
get_connection() as a context manager to ensure connections are
returned to the pool after use.
"""

import os
import psycopg2
from psycopg2 import pool, extras
from dotenv import load_dotenv
from contextlib import contextmanager

from observability.logging_config import get_logger

load_dotenv()
logger = get_logger("storage.db")

# Module-level connection pool (initialized lazily)
_connection_pool: pool.ThreadedConnectionPool | None = None


def _get_pool() -> pool.ThreadedConnectionPool:
    """Lazily initialize and return the connection pool."""
    global _connection_pool
    if _connection_pool is None or _connection_pool.closed:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError(
                "DATABASE_URL not set. Add your Neon connection string to .env"
            )
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=database_url,
        )
        logger.info("db_pool_initialized", min_conn=2, max_conn=10)
    return _connection_pool


@contextmanager
def get_connection():
    """
    Yield a database connection from the pool.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    conn = None
    try:
        conn = _get_pool().getconn()
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _get_pool().putconn(conn)


def execute_query(query: str, params: tuple = None) -> list[dict]:
    """Execute a SELECT query and return rows as a list of dicts."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


def execute_write(query: str, params: tuple = None) -> int:
    """Execute an INSERT/UPDATE/DELETE and return the row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount


def execute_batch(query: str, params_list: list[tuple]) -> int:
    """Execute a parameterized query for each tuple in params_list."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            extras.execute_batch(cur, query, params_list, page_size=100)
            return cur.rowcount


def init_schema(sql_file_path: str = "storage/init.sql"):
    """Run the schema initialization script against Neon."""
    with open(sql_file_path, "r") as f:
        sql = f.read()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    logger.info("schema_initialized", sql_file=sql_file_path)


def close_pool():
    """Close every connection in the pool. Call at shutdown."""
    global _connection_pool
    if _connection_pool and not _connection_pool.closed:
        _connection_pool.closeall()
        logger.info("db_pool_closed")
        _connection_pool = None
