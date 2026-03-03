"""FastAPI application — single backend replacing NestJS middleware + core_server_py."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .db import close_db, init_db
from .engine import CoreEngine
from .routes.queries import init_query_routes, router as query_router
from .routes.streams import init_stream_routes, router as stream_router
from .routes.websocket import init_websocket_routes, router as ws_router

logger = logging.getLogger(__name__)

engine: CoreEngine | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    router_port = getattr(app.state, "router_port", 5000)
    stream_listener_port = getattr(app.state, "stream_listener_port", 5001)
    starting_query_port = getattr(app.state, "starting_query_port", 5002)

    await init_db()

    engine = CoreEngine(
        router_port=router_port,
        stream_listener_port=stream_listener_port,
        starting_query_port=starting_query_port,
    )
    engine.set_event_loop(asyncio.get_running_loop())

    init_query_routes(engine)
    init_stream_routes(engine)
    init_websocket_routes(engine)

    logger.info(
        "CORE engine started — router=%d, stream_listener=%d",
        router_port,
        stream_listener_port,
    )

    yield

    await close_db()
    engine = None


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,
)

app.include_router(query_router)
app.include_router(stream_router)
app.include_router(ws_router)
