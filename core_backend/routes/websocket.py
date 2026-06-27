"""WebSocket routes — query results and raw stream events."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from core_backend.streamers.abstract_streamer import AbstractStreamer

if TYPE_CHECKING:
    from core_backend.engine import CoreEngine

logger = logging.getLogger(__name__)

router = APIRouter()

_engine: CoreEngine | None = None
_streamers: Sequence[AbstractStreamer[Any]] = []


def init_websocket_routes(engine: CoreEngine, streamers: Sequence[AbstractStreamer[Any]] | None = None) -> None:
    global _engine, _streamers
    _engine = engine
    if streamers is not None:
        _streamers = streamers


@router.websocket("/{query_id}")
async def websocket_endpoint(websocket: WebSocket, query_id: int):
    assert _engine is not None
    await websocket.accept()
    queue = _engine.subscribe_client(query_id)
    try:
        while True:
            result = await queue.get()
            await websocket.send_json(result)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("WebSocket error for query_id=%d", query_id)
    finally:
        _engine.unsubscribe_client(query_id, queue)


@router.websocket("/stream/events/{stream_name}")
async def stream_events(websocket: WebSocket, stream_name: str):
    streamer = next((s for s in _streamers if s.name == stream_name), None)
    if streamer is None:
        await websocket.close(code=4004, reason=f"Stream '{stream_name}' not found")
        return
    await websocket.accept()
    queue = streamer.subscribe_events()
    try:
        while True:
            event = await queue.get()
            await websocket.send_json(event)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("Stream events WebSocket error for stream=%s", stream_name)
    finally:
        streamer.unsubscribe_events(queue)
