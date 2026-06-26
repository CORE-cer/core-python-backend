"""WS /{query_id} — stream live complex event results."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

if TYPE_CHECKING:
    from core_backend.engine import CoreEngine

logger = logging.getLogger(__name__)

router = APIRouter()

_engine: CoreEngine | None = None


def init_websocket_routes(engine: CoreEngine) -> None:
    global _engine
    _engine = engine


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
