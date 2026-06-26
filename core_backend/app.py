"""FastAPI application — single backend replacing NestJS middleware + core_server_py."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

import pycer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .db import close_db, init_db
from .engine import CoreEngine
from .routes.queries import init_query_routes
from .routes.queries import router as query_router
from .routes.streams import init_stream_routes
from .routes.streams import router as stream_router
from .routes.websocket import init_websocket_routes
from .routes.websocket import router as ws_router
from .streamers.abstract_streamer import AbstractStreamer
from .streamers.bluesky.create_post import CreatePostStreamer
from .streamers.coinbase.ticker import TickerStreamer

logger = logging.getLogger(__name__)

engine: CoreEngine | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    router_port = getattr(app.state, "router_port", 5000)
    stream_listener_port = getattr(app.state, "stream_listener_port", 5001)
    starting_query_port = getattr(app.state, "starting_query_port", 5002)

    await init_db()

    with CoreEngine(
        router_port=router_port,
        stream_listener_port=stream_listener_port,
        starting_query_port=starting_query_port,
    ) as eng:
        engine = eng
        engine.set_event_loop(asyncio.get_running_loop())

        init_query_routes(engine)
        init_websocket_routes(engine)

        # Start data streamers
        with (
            pycer.PyClient("tcp://localhost", router_port) as streamer_client,
            pycer.PyStreamer("tcp://localhost", stream_listener_port) as py_streamer,
        ):
            streamers = [
                TickerStreamer(streamer_client, py_streamer),
                CreatePostStreamer(streamer_client, py_streamer),
            ]
            init_stream_routes(engine, streamers)

            async def run_streamer(streamer: AbstractStreamer[Any]) -> None:
                try:
                    await streamer.start()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Streamer %s crashed", streamer.name)

            streamer_tasks: list[asyncio.Task[None]] = []
            for s in streamers:
                task = asyncio.create_task(run_streamer(s), name=f"streamer-{s.name}")
                streamer_tasks.append(task)

            # Let streamers declare their streams, then build event mappings
            await asyncio.sleep(1)
            engine.rebuild_event_mappings()

            logger.info(
                "CORE engine started — router=%d, stream_listener=%d, streamers=%d",
                router_port,
                stream_listener_port,
                len(streamers),
            )

            yield

            # Cancel streamer tasks
            for task in streamer_tasks:
                task.cancel()
            await asyncio.gather(*streamer_tasks, return_exceptions=True)

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
app.include_router(ws_router, prefix="/ws")
