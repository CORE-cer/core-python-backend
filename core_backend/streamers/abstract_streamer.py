from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Optional, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class AbstractStreamer(ABC, Generic[T]):
    """
    Base class for all streamers.
    """

    event_name_to_unique_id: Dict[str, int] = {}

    def __init__(self, py_client, py_streamer):
        self.py_client = py_client
        self.py_streamer = py_streamer

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"

    def __str__(self):
        return self.name

    def __eq__(self, other):
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
    def option_declaration(self) -> Optional[str]:
        return None

    @abstractmethod
    def parse_message_json(self, message: str) -> Optional[T]:
        pass

    @abstractmethod
    def get_event_id_from_model(self, model: T) -> int:
        pass

    @abstractmethod
    def create_event(self, model: T) -> Any:
        pass

    def process_message(self, message: str, stream_id: int) -> None:
        model = self.parse_message_json(message)
        if not model:
            return
        event = self.create_event(model)
        self.py_streamer.send_stream(stream_id, event)

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
