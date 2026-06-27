import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

logger = logging.getLogger(__name__)

RATE_WINDOW_SECONDS = 5
STALE_THRESHOLD_SECONDS = 10


class AbstractStreamer(ABC, Generic[T]):
    """
    Base class for all streamers.
    """

    def __init__(self, py_client: Any, py_streamer: Any) -> None:
        self.py_client = py_client
        self.py_streamer = py_streamer
        self.event_name_to_unique_id: dict[str, int] = {}
        self._event_count: int = 0
        self._last_event_time: float | None = None
        self._event_timestamps: deque[float] = deque()
        self._event_queues: list[asyncio.Queue[dict[str, Any]]] = []
        self._loop: asyncio.AbstractEventLoop | None = None

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"

    def __str__(self):
        return self.name

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AbstractStreamer):
            return self.name == other.name
        return False

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def stream_declaration(self) -> str:
        pass

    @property
    def option_declaration(self) -> str | None:
        return None

    @abstractmethod
    def parse_message_json(self, message: str) -> T | None:
        pass

    @abstractmethod
    def get_event_id_from_model(self, model: T) -> int:
        pass

    @abstractmethod
    def create_event(self, model: T) -> Any:
        pass

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe_events(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._event_queues.append(queue)
        return queue

    def unsubscribe_events(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        try:
            self._event_queues.remove(queue)
        except ValueError:
            pass

    def process_message(self, message: str, stream_id: int) -> None:
        model = self.parse_message_json(message)
        if not model:
            return
        event = self.create_event(model)
        self.py_streamer.send_stream(stream_id, event)
        now = time.monotonic()
        self._event_count += 1
        self._last_event_time = now
        self._event_timestamps.append(now)
        if self._loop and self._event_queues:
            unique_id_to_name = {v: k for k, v in self.event_name_to_unique_id.items()}
            event_type_id = event.get_event_type_id()
            event_data: dict[str, Any] = {
                "event_type": unique_id_to_name.get(event_type_id, str(event_type_id)),
                "attributes": event.get_attributes_as_list(),
            }
            for queue in self._event_queues:
                self._loop.call_soon_threadsafe(queue.put_nowait, event_data)

    def get_stats(self) -> dict[str, Any]:
        now = time.monotonic()
        cutoff = now - RATE_WINDOW_SECONDS
        while self._event_timestamps and self._event_timestamps[0] < cutoff:
            self._event_timestamps.popleft()
        count_in_window = len(self._event_timestamps)
        events_per_sec = round(count_in_window / RATE_WINDOW_SECONDS, 1)

        if self._last_event_time is not None:
            seconds_ago = now - self._last_event_time
            status = "live" if seconds_ago <= STALE_THRESHOLD_SECONDS else "stale"
        else:
            seconds_ago = None
            status = "inactive"

        event_types = list(self.event_name_to_unique_id.keys())

        return {
            "name": self.name,
            "events_per_sec": events_per_sec,
            "total_events": self._event_count,
            "last_event_seconds_ago": round(seconds_ago, 1) if seconds_ago is not None else None,
            "status": status,
            "event_types": event_types,
        }

    @abstractmethod
    async def setup_and_receive(self, stream_id: int):
        pass

    async def start(self):
        stream_info = self.py_client.declare_stream(self.stream_declaration)
        if self.option_declaration:
            self.py_client.declare_option(self.option_declaration)
        stream_id = stream_info.id
        events_info = stream_info.events_info
        for event_info in events_info:
            self.event_name_to_unique_id[event_info.name] = event_info.id

        print(f"Starting streamer: {self.name}")
        await self.setup_and_receive(stream_id)

    def stop(self):
        print(f"Stopping streamer: {self.name}")
