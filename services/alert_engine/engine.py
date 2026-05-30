"""
Alert Engine - 가격 알림 규칙 평가

Kafka에서 시세 소비 → Redis에서 규칙 조회 → 조건 평가 → ntfy 발송

Redis 연동:
- 규칙 조회: get_rules_by_symbol()
- 쿨다운 확인/설정: is_cooldown_active(), set_cooldown()
- 시세 캐시: cache_quote()

Redis 미연결 시 인메모리 폴백으로 동작한다.
"""

import logging
import json
from confluent_kafka import Consumer, KafkaError

from shared.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_QUOTES
from shared.models import Quote
from services.notification.ntfy_sender import send_price_alert

logger = logging.getLogger(__name__)

# Redis 사용 가능 여부 플래그
_use_redis = True
try:
    from shared.redis_client import (
        get_rules_by_symbol,
        is_cooldown_active,
        set_cooldown,
        cache_quote,
        get_redis,
    )
    # 연결 테스트
    get_redis().ping()
    logger.info("Redis 연결 성공")
except Exception:
    _use_redis = False
    logger.warning("Redis 미연결 — 인메모리 모드로 동작")

# 인메모리 폴백 (Redis 없을 때)
_memory_rules: dict[str, list[dict]] = {}
_memory_cooldown: dict[str, float] = {}


class AlertEngine:
    """
    가격 알림 엔진

    Kafka Consumer → 규칙 평가 → ntfy 발송
    """

    def __init__(self):
        self._consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "alert-engine",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        logger.info(f"AlertEngine 초기화 (Redis: {'ON' if _use_redis else 'OFF'})")

    def add_rule(self, rule: dict):
        """인메모리 규칙 등록 (Redis 미사용 시 또는 초기 설정용)"""
        symbol = rule["symbol"]
        if symbol not in _memory_rules:
            _memory_rules[symbol] = []
        _memory_rules[symbol].append(rule)
        logger.info(f"규칙 등록: {symbol} {rule['alert_type']} @ {rule['threshold']}")

    def _get_rules(self, symbol: str) -> list[dict]:
        """종목별 활성 규칙 조회 (Redis 우선, 폴백: 인메모리)"""
        if _use_redis:
            try:
                return get_rules_by_symbol(symbol)
            except Exception:
                pass
        return _memory_rules.get(symbol, [])

    def _check_cooldown(self, rule_id: str) -> bool:
        """쿨다운 확인 (True면 억제)"""
        if _use_redis:
            try:
                return is_cooldown_active(rule_id)
            except Exception:
                pass
        # 인메모리 폴백
        import time
        last = _memory_cooldown.get(rule_id, 0)
        return (time.time() - last) < 300

    def _set_cooldown(self, rule_id: str):
        """쿨다운 설정"""
        if _use_redis:
            try:
                set_cooldown(rule_id, "price")
                return
            except Exception:
                pass
        import time
        _memory_cooldown[rule_id] = time.time()

    def _evaluate(self, quote: Quote):
        """Quote와 규칙 비교 → 조건 충족 시 알림 발송"""
        # Redis에 시세 캐시
        if _use_redis:
            try:
                cache_quote(quote.symbol, quote.to_json())
            except Exception:
                pass

        rules = self._get_rules(quote.symbol)
        if not rules:
            return

        price = quote.bid_price
        if price <= 0:
            return

        for rule in rules:
            if not rule.get("is_active", True):
                continue

            rule_id = rule["rule_id"]
            if self._check_cooldown(rule_id):
                continue

            alert_type = rule["alert_type"]
            threshold = rule["threshold"]
            triggered = False

            if alert_type == "price_above" and price >= threshold:
                triggered = True
            elif alert_type == "price_below" and price <= threshold:
                triggered = True
            elif alert_type == "price_change":
                if threshold > 0:
                    change_pct = abs((price - threshold) / threshold * 100)
                    if change_pct >= 5.0:
                        triggered = True

            if triggered:
                logger.info(f"ALERT: {quote.symbol} ${price:.2f} ({alert_type} @ {threshold})")
                send_price_alert(quote.symbol, price, alert_type, threshold)
                self._set_cooldown(rule_id)

    def run(self):
        """Kafka Consumer 루프 (블로킹)"""
        self._consumer.subscribe([KAFKA_TOPIC_QUOTES])
        logger.info(f"Kafka 소비 시작: {KAFKA_TOPIC_QUOTES}")

        try:
            while True:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka: {msg.error()}")
                    continue

                try:
                    quote = Quote.from_json(msg.value().decode("utf-8"))
                    self._evaluate(quote)
                except Exception as e:
                    logger.error(f"파싱 오류: {e}")

        except KeyboardInterrupt:
            logger.info("종료")
        finally:
            self._consumer.close()

    def stop(self):
        self._consumer.close()
