import json
from typing import final

import pycer

from ..abstract_streamer_websocket import AbstractStreamerWebsocket
from .models.ticker import TickerModel, subscription_message


@final
class TickerStreamer(AbstractStreamerWebsocket[TickerModel]):
    @property
    def name(self) -> str:
        return "coinbase"

    @property
    def stream_declaration(self) -> str:
        return """CREATE STREAM TICKER
                { \n
                EVENT Buy { product_id:string, price:double, open24h:double, volume_24h:double, low_24h:double, \
                high_24h:double, volume_30d:double, best_bid:double, best_bid_size:double, best_ask:double, best_ask_size:double, \
                last_size:double, time:primary_time } \n,
                EVENT Sell { product_id:string, price:double, open24h:double, volume_24h:double, low_24h:double, \
                high_24h:double, volume_30d:double, best_bid:double, best_bid_size:double, best_ask:double, best_ask_size:double, \
                last_size:double, time:primary_time } \n
                }
                """

    @property
    def option_declaration(self) -> str | None:
        return """
                    CREATE QUARANTINE
                    { \n
                    FIXED_TIME 2 seconds {TICKER} \n
                    }
                    """

    @property
    def URI(self) -> str:
        return "wss://ws-feed.exchange.coinbase.com"

    @property
    def subscribe_message_json(self) -> str:
        return json.dumps(subscription_message)

    def parse_message_json(self, message: str) -> TickerModel:
        return TickerModel.model_validate_json(message)

    def get_event_id_from_model(self, model: TickerModel) -> int:
        event_id = self.event_name_to_unique_id.get(model.side.capitalize())
        assert event_id is not None, f"Unknown side: {model.side}"
        return event_id

    def create_event(self, model: TickerModel):
        product_id = pycer.PyStringValue(model.product_id)
        price = pycer.PyDoubleValue(model.price)
        open_24h = pycer.PyDoubleValue(model.open_24h)
        volume_24h = pycer.PyDoubleValue(model.volume_24h)
        low_24h = pycer.PyDoubleValue(model.low_24h)
        high_24h = pycer.PyDoubleValue(model.high_24h)
        volume_30d = pycer.PyDoubleValue(model.volume_30d)
        best_bid = pycer.PyDoubleValue(model.best_bid)
        best_bid_size = pycer.PyDoubleValue(model.best_bid_size)
        best_ask = pycer.PyDoubleValue(model.best_ask)
        best_ask_size = pycer.PyDoubleValue(model.best_ask_size)
        last_size = pycer.PyDoubleValue(model.last_size)
        time = pycer.PyIntValue(int(model.time.timestamp() * 1e9))
        attributes = [
            product_id,
            price,
            open_24h,
            volume_24h,
            low_24h,
            high_24h,
            volume_30d,
            best_bid,
            best_bid_size,
            best_ask,
            best_ask_size,
            last_size,
            time,
        ]
        event_id = self.get_event_id_from_model(model)
        event = pycer.PyEvent(event_id, attributes, time)
        return event
