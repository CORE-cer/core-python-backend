"""Stream routes — list and declare streams, serve live stats."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse

from core_backend.streamers.abstract_streamer import AbstractStreamer

if TYPE_CHECKING:
    from core_backend.engine import CoreEngine

router = APIRouter()

_engine: CoreEngine | None = None
_streamers: Sequence[AbstractStreamer[Any]] = []


def init_stream_routes(engine: CoreEngine, streamers: Sequence[AbstractStreamer[Any]] | None = None) -> None:
    global _engine, _streamers
    _engine = engine
    if streamers is not None:
        _streamers = streamers


@router.get("/stream")
async def get_streams():
    assert _engine is not None
    return _engine.list_all_streams()


@router.get("/stream/stats")
async def get_stream_stats():
    return [s.get_stats() for s in _streamers]


@router.post("/declare-stream")
async def declare_stream(request: Request):
    """Declare a stream in the CORE engine. Used by data streamers."""
    assert _engine is not None
    body = await request.json()
    declaration = body.get("declaration", "")
    try:
        stream_info = _engine.declare_stream(declaration)
        events: list[dict[str, Any]] = []
        for e in stream_info.events_info:
            attrs: list[dict[str, Any]] = [
                {"name": a.name, "value_type": a.value_type.value} for a in e.attributes_info
            ]
            events.append({"id": e.id, "name": e.name, "attributes_info": attrs})
        return {"id": stream_info.id, "name": stream_info.name, "events_info": events}
    except Exception as e:
        return PlainTextResponse(str(e), status_code=400)


@router.post("/declare-option")
async def declare_option(request: Request):
    """Declare an option in the CORE engine. Used by data streamers."""
    assert _engine is not None
    body = await request.json()
    option = body.get("option", "")
    try:
        _engine.declare_option(option)
        return {"status": "ok"}
    except Exception as e:
        return PlainTextResponse(str(e), status_code=400)
