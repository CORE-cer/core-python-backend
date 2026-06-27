# CLAUDE.md

## WebSocket endpoints

All WebSocket endpoints must be registered on the `ws_router` (in `routes/websocket.py`), not on `stream_router` or other routers. The `ws_router` is mounted at `/ws` prefix in `app.py`. The deployment reverse proxy is configured to handle WebSocket upgrades only on the `/ws/` path.
