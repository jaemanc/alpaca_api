"""데이터 모델 — Kafka 메시지 페이로드"""

import json
from dataclasses import dataclass, asdict


@dataclass
class Quote:
    """실시간 시세 (Alpaca WebSocket → Kafka)"""
    symbol: str
    bid_price: float
    ask_price: float
    bid_size: int
    ask_size: int
    timestamp: str

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "Quote":
        return cls(**json.loads(raw))
