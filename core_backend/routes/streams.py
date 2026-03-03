"""Stream routes — list and declare streams."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse

if TYPE_CHECKING:
    from core_backend.engine import CoreEngine

router = APIRouter()

_engine: CoreEngine | None = None


def init_stream_routes(engine: CoreEngine) -> None:
    global _engine
    _engine = engine


@router.get("/stream")
async def get_streams():
    return _engine.list_all_streams()


@router.post("/declare-stream")
async def declare_stream(request: Request):
    """Declare a stream in the CORE engine. Used by data streamers."""
    body = await request.json()
    declaration = body.get("declaration", "")
    try:
        stream_info = _engine.declare_stream(declaration)
        events = []
        for e in stream_info.events_info:
            attrs = [{"name": a.name, "value_type": a.value_type.value} for a in e.attributes_info]
            events.append({"id": e.id, "name": e.name, "attributes_info": attrs})
        return {"id": stream_info.id, "name": stream_info.name, "events_info": events}
    except Exception as e:
        return PlainTextResponse(str(e), status_code=400)


@router.post("/declare-option")
async def declare_option(request: Request):
    """Declare an option in the CORE engine. Used by data streamers."""
    body = await request.json()
    option = body.get("option", "")
    try:
        _engine.declare_option(option)
        return {"status": "ok"}
    except Exception as e:
        return PlainTextResponse(str(e), status_code=400)
