"""SQLite in-memory database for query tracking."""

from __future__ import annotations

import aiosqlite

_db: aiosqlite.Connection | None = None


async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db


async def init_db() -> None:
    global _db
    _db = await aiosqlite.connect(":memory:")
    _db.row_factory = aiosqlite.Row
    await _db.execute(
        """
        CREATE TABLE IF NOT EXISTS queries (
            id INTEGER PRIMARY KEY,
            query_text TEXT NOT NULL,
            query_name TEXT NOT NULL DEFAULT ''
        )
        """
    )
    await _db.commit()


async def close_db() -> None:
    global _db
    if _db is not None:
        await _db.close()
        _db = None
