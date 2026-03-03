"""CLI entry point for the CORE Python backend."""

import argparse

import uvicorn

from .app import app


def main() -> None:
    parser = argparse.ArgumentParser(description="CORE Python Backend")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument(
        "--router-port",
        type=int,
        default=5000,
        help="ZMQ router port for the embedded CORE server",
    )
    parser.add_argument(
        "--stream-listener-port",
        type=int,
        default=5001,
        help="ZMQ port for receiving streamer events",
    )
    parser.add_argument(
        "--starting-query-port",
        type=int,
        default=5002,
        help="Starting ZMQ port for query result broadcasting",
    )
    args = parser.parse_args()

    app.state.router_port = args.router_port
    app.state.stream_listener_port = args.stream_listener_port
    app.state.starting_query_port = args.starting_query_port

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
