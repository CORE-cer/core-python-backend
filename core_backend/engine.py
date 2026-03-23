"""Wrapper around pycer that manages an embedded C++ CORE server and client."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import pycer

logger = logging.getLogger(__name__)


@dataclass
class QuerySubscription:
    query_id: int
    port: int
    query_string: str
    query_name: str


class CoreEngine:
    """Embeds the C++ CORE OnlineServer and connects via PyClient, all in-process."""

    def __init__(
        self,
        router_port: int = 5000,
        stream_listener_port: int = 5001,
        starting_query_port: int = 5002,
    ) -> None:
        self._server = pycer.PyOnlineServer(
            router_port=router_port,
            stream_listener_port=stream_listener_port,
            starting_query_port=starting_query_port,
        )
        self._client = pycer.PyClient("tcp://localhost", router_port)
        self._subscriptions: dict[int, QuerySubscription] = {}
        self._next_query_id: int = 0
        self._result_queues: dict[int, list[asyncio.Queue]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._event_to_stream: dict[int, int] = {}
        self._event_to_name: dict[int, str] = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._client.shutdown()
        self._server.shutdown()

    @property
    def stream_listener_port(self) -> int:
        """Port that PyStreamer should send events to."""
        return self._server.stream_listener_port

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def declare_stream(self, declaration: str) -> pycer.PyStreamInfo:
        return self._client.declare_stream(declaration)

    def declare_option(self, option: str) -> None:
        self._client.declare_option(option)

    def add_query(self, query: str, query_name: str = "") -> tuple[int, int]:
        """Add a query. Returns (query_id, port).

        The port doubles as the result_handler_identifier used by the
        frontend to open WebSocket subscriptions, so we key
        _result_queues by port so that subscribe_client(port) finds
        the right queue.
        """
        port = self._client.add_query(query)
        query_id = self._next_query_id
        self._next_query_id += 1
        self._result_queues[port] = []

        callback = self._create_result_callback(port)
        self._subscriptions[query_id] = QuerySubscription(
            query_id=query_id,
            port=port,
            query_string=query,
            query_name=query_name,
        )
        self._client.subscribe_to_complex_event(callback, port)
        return query_id, port

    def inactivate_query(self, query_id: int) -> None:
        self._client.inactivate_query(query_id)
        sub = self._subscriptions.pop(query_id, None)
        if sub and sub.port in self._result_queues:
            del self._result_queues[sub.port]

    def _rebuild_event_mappings(self) -> None:
        """Rebuild event_type_id → stream_id and event_type_id → event_name mappings."""
        streams = self._client.list_all_streams()
        for s in streams:
            for e in s.events_info:
                self._event_to_stream[e.id] = s.id
                self._event_to_name[e.id] = e.name

    def list_all_streams(self) -> list[dict]:
        """Return stream info in the format the frontend expects."""
        streams = self._client.list_all_streams()
        result = []
        for s in streams:
            events = []
            for e in s.events_info:
                self._event_to_stream[e.id] = s.id
                self._event_to_name[e.id] = e.name
                attrs = [{"name": a.name, "value_type": a.value_type.value} for a in e.attributes_info]
                events.append({"id": e.id, "name": e.name, "attributes_info": attrs})
            result.append({"id": s.id, "name": s.name, "events_info": events})
        return result

    def list_all_queries(self) -> list[dict]:
        """Return query info in the format the frontend expects."""
        queries = self._client.list_all_queries()
        port_to_qid = {sub.port: sub.query_id for sub in self._subscriptions.values()}
        result = []
        for q in queries:
            port = int(q.result_handler_identifier)
            result.append({
                "query_id": port_to_qid.get(port),
                "result_handler_identifier": port,
                "result_handler_type": q.result_handler_type.value,
                "query_string": q.query_string,
                "query_name": q.query_name,
                "active": q.active,
                "attribute_projection_stream_event": q.get_attribute_projection_stream_event(),
                "attribute_projection_variable": dict(q.attribute_projection_variable),
            })
        return result

    def subscribe_client(self, query_id: int) -> asyncio.Queue:
        """Register a WebSocket client to receive results for a query."""
        sub = self._subscriptions.get(query_id)
        port = sub.port if sub else query_id
        queue: asyncio.Queue = asyncio.Queue()
        if port not in self._result_queues:
            self._result_queues[port] = []
        self._result_queues[port].append(queue)
        return queue

    def unsubscribe_client(self, query_id: int, queue: asyncio.Queue) -> None:
        """Unregister a WebSocket client."""
        sub = self._subscriptions.get(query_id)
        port = sub.port if sub else query_id
        if port in self._result_queues:
            try:
                self._result_queues[port].remove(queue)
            except ValueError:
                pass

    def _create_result_callback(self, query_id: int):
        """Create a per-query callback that forwards to asyncio queues."""

        def on_result(enumerator: pycer.PyEnumerator) -> None:
            result = self._enumerator_to_json(enumerator)
            if not result:
                return
            if self._loop and query_id in self._result_queues:
                for queue in self._result_queues[query_id]:
                    self._loop.call_soon_threadsafe(queue.put_nowait, result)

        return on_result

    def _enumerator_to_json(self, enumerator: pycer.PyEnumerator) -> list[dict]:
        """Convert enumerator to the JSON format the frontend expects.

        The C++ convert_enumerator now resolves variable names from the
        marked_variables bitset and applies attribute projections, so each
        Event already carries its variable_name and projected attributes.
        """
        results = []
        for ce in enumerator:
            events_list = []
            for event in ce.events:
                event_type_id = event.get_event_type_id()
                event_key = event.variable_name or self._event_to_name.get(
                    event_type_id, str(event_type_id)
                )
                event_data = {
                    "event_type_id": event_type_id,
                    "stream_type_id": self._event_to_stream.get(event_type_id, 0),
                    "attributes": event.get_attributes_as_list(),
                }
                events_list.append({event_key: event_data})
            results.append({
                "start": ce.start,
                "end": ce.end,
                "events": events_list,
            })
        return results
