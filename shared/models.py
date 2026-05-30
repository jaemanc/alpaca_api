"""
데이터 모델 정의

시스템에서 사용하는 핵심 데이터 구조.
JSON 직렬화를 통해 Kafka 메시지 페이로드로 사용된다.
"""

import json
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class Quote:
    """
    실시간 시세 데이터

    Alpaca WebSocket → Kafka market-quotes 토픽 페이로드.
    """
    symbol: str           # 종목 심볼 (예: "AAPL")
    bid_price: float      # 매수 호가
    ask_price: float      # 매도 호가
    bid_size: int         # 매수 수량
    ask_size: int         # 매도 수량
    timestamp: str        # 수신 시각 (ISO 8601)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "Quote":
        return cls(**json.loads(raw))


@dataclass
class AlertRule:
    """사용자 알림 규칙 — Redis 해시 구조로 저장"""
    rule_id: str
    user_id: str
    symbol: str
    alert_type: str       # price_above | price_below | price_change | ob_change | ob_imbalance
    threshold: float
    channel: str          # push | email | both
    is_active: bool = True

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "AlertRule":
        return cls(**json.loads(raw))


@dataclass
class AlertEvent:
    """알림 이벤트 — Alert_Engine → Notification_Service 전달용"""
    event_id: str
    rule_id: str
    user_id: str
    symbol: str
    alert_type: str
    current_price: float
    threshold: float
    channel: str
    triggered_at: str
    change_pct: Optional[float] = None
    direction: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "AlertEvent":
        return cls(**json.loads(raw))
