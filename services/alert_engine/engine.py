"""
Alert Engine - 가격 알림 규칙 평가

Kafka market-quotes 토픽에서 시세 데이터를 소비하고,
Redis에 저장된 사용자 Alert_Rule과 비교하여
조건 충족 시 ntfy 푸시 알림을 발송한다.

동작 흐름:
1. Kafka Consumer로 Quote 메시지 수신
2. Redis에서 해당 종목의 활성 Alert_Rule 조회
3. 현재가와 조건값 비교
4. 쿨다운 확인 (중복 알림 방지)
5. 조건 충족 시 ntfy로 푸시 발송
"""

import time
import logging
import json
from confluent_kafka import Consumer, KafkaError

from shared.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_QUOTES,
    REDIS_HOST,
    REDIS_PORT,
    COOLDOWN_PRICE_ALERT,
)
from shared.models import Quote
from services.notification.ntfy_sender import send_price_alert

logger = logging.getLogger(__name__)

# 인메모리 쿨다운 저장소 (Redis 없이도 동작하도록)
# key: rule_id, value: 마지막 알림 발송 시각 (unix timestamp)
_cooldown_map: dict[str, float] = {}


class AlertEngine:
    """
    가격 알림 엔진

    Kafka에서 Quote를 소비하고, 등록된 규칙과 비교하여 알림을 발송한다.
    현재는 Redis 없이 인메모리 규칙 저장소를 사용한다.
    (추후 Redis 연동 시 교체)
    """

    def __init__(self):
        # 인메모리 규칙 저장소: {symbol: [rule_dict, ...]}
        self._rules: dict[str, list[dict]] = {}

        # Kafka Consumer 설정
        self._consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "alert-engine",
            "auto.offset.reset": "latest",  # 최신 메시지부터 소비
            "enable.auto.commit": True,
        })

        logger.info("AlertEngine 초기화 완료")

    def add_rule(self, rule: dict):
        """
        알림 규칙 등록

        Args:
            rule: {
                "rule_id": "...",
                "symbol": "AAPL",
                "alert_type": "price_above" | "price_below" | "price_change",
                "threshold": 190.0,
                "is_active": True
            }
        """
        symbol = rule["symbol"]
        if symbol not in self._rules:
            self._rules[symbol] = []
        self._rules[symbol].append(rule)
        logger.info(f"규칙 등록: {symbol} {rule['alert_type']} @ {rule['threshold']}")

    def _check_cooldown(self, rule_id: str) -> bool:
        """
        쿨다운 확인

        마지막 알림 발송 후 COOLDOWN_PRICE_ALERT(5분) 이내면 True(억제).
        """
        last_sent = _cooldown_map.get(rule_id)
        if last_sent is None:
            return False
        return (time.time() - last_sent) < COOLDOWN_PRICE_ALERT

    def _evaluate_quote(self, quote: Quote):
        """
        Quote 데이터와 규칙 비교

        해당 종목에 등록된 모든 활성 규칙을 순회하며 조건 충족 여부를 판단한다.
        """
        rules = self._rules.get(quote.symbol, [])
        if not rules:
            return

        current_price = quote.bid_price  # 매수호가를 현재가로 사용

        # 무효 가격 스킵
        if current_price <= 0:
            logger.debug(f"[{quote.symbol}] 무효 가격: {current_price}")
            return

        for rule in rules:
            if not rule.get("is_active", True):
                continue

            rule_id = rule["rule_id"]
            alert_type = rule["alert_type"]
            threshold = rule["threshold"]

            # 쿨다운 중이면 스킵
            if self._check_cooldown(rule_id):
                continue

            triggered = False

            if alert_type == "price_above" and current_price >= threshold:
                triggered = True
            elif alert_type == "price_below" and current_price <= threshold:
                triggered = True
            elif alert_type == "price_change":
                # threshold를 기준가로 사용, 변동률 계산
                change_pct = abs((current_price - threshold) / threshold * 100)
                # 변동률이 5% 이상이면 알림 (기본 임계값)
                if change_pct >= 5.0:
                    triggered = True

            if triggered:
                logger.info(
                    f"🔔 알림 트리거: {quote.symbol} "
                    f"${current_price:.2f} ({alert_type} @ {threshold})"
                )
                # ntfy 푸시 발송
                send_price_alert(quote.symbol, current_price, alert_type, threshold)
                # 쿨다운 기록
                _cooldown_map[rule_id] = time.time()

    def run(self):
        """
        Kafka Consumer 루프 시작 (블로킹)

        market-quotes 토픽을 구독하고, 수신된 Quote마다 규칙을 평가한다.
        """
        self._consumer.subscribe([KAFKA_TOPIC_QUOTES])
        logger.info(f"Kafka 소비 시작: {KAFKA_TOPIC_QUOTES}")

        try:
            while True:
                msg = self._consumer.poll(1.0)  # 1초 대기

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka 오류: {msg.error()}")
                    continue

                # 메시지 역직렬화
                try:
                    quote = Quote.from_json(msg.value().decode("utf-8"))
                    self._evaluate_quote(quote)
                except Exception as e:
                    logger.error(f"메시지 파싱 오류: {e}")

        except KeyboardInterrupt:
            logger.info("Alert Engine 종료")
        finally:
            self._consumer.close()

    def stop(self):
        """Consumer 종료"""
        self._consumer.close()
