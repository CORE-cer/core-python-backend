from datetime import datetime
from typing import Literal

from pydantic import BaseModel

subscription_message = {
    "type": "subscribe",
    "product_ids": [
        "BTC-USD",
        "ETH-USD",
        "BTC-USDT",
        "ETH-USDT",
        "SOL-ETH",
        "SOL-USDT",
        "SOL-BTC",
        "LTC-USD",
        "LTC-BTC",
    ],
    "channels": ["ticker"],
}


class TickerModel(BaseModel):
    type: Literal["ticker"]
    sequence: int
    product_id: str
    price: float
    open_24h: float
    volume_24h: float
    low_24h: float
    high_24h: float
    volume_30d: float
    best_bid: float
    best_bid_size: float
    best_ask: float
    best_ask_size: float
    side: Literal["buy", "sell"]
    time: datetime
    trade_id: int
    last_size: float
