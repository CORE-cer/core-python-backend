"""GET/POST/DELETE /query — manage CORE queries."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException

from core_backend.db import get_db
from core_backend.schemas import CreateQueryRequest

if TYPE_CHECKING:
    from core_backend.engine import CoreEngine

router = APIRouter()

_engine: CoreEngine | None = None


def init_query_routes(engine: CoreEngine) -> None:
    global _engine
    _engine = engine


@router.get("/query")
async def get_queries():
    """Return all active queries, enriching with stored query_text from DB."""
    queries = _engine.list_all_queries()
    db = await get_db()

    for q in queries:
        cursor = await db.execute(
            "SELECT query_text, query_name FROM queries WHERE id = ?",
            (q["result_handler_identifier"],),
        )
        stored = await cursor.fetchone()
        if stored:
            q["query_string"] = stored["query_text"]
            q["query_name"] = stored["query_name"]

    return queries


@router.post("/query")
async def add_query(body: CreateQueryRequest):
    """Add a query to the CORE engine and track it in SQLite."""
    try:
        query_id, port = _engine.add_query(body.query, body.query_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    db = await get_db()
    await db.execute(
        "INSERT OR REPLACE INTO queries (id, query_text, query_name) VALUES (?, ?, ?)",
        (port, body.query, body.query_name),
    )
    await db.commit()

    return query_id


@router.delete("/query/{query_id}")
async def inactivate_query(query_id: int):
    """Inactivate a query in the CORE engine."""
    try:
        _engine.inactivate_query(query_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
