"""
Alert Engine - 가격 알림 규칙 평가

Kafka에서 시세 소비 → 4단계 알림 제어 → ntfy 발송

알림 빈도 제어 (4개 레이어):
1. 규칙별 쿨다운: 같은 규칙 5분 내 재트리거 방지
2. 종목별 쿨다운: 같은 종목 1시간에 최대 1건
3. 글로벌 레이트 리밋: 분당 최대 5건
4. 일일 한도: 하루 최대 100건
"""

import logging
import time
from confluent_kafka import Consumer, KafkaError

from shared.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_QUOTES
from shared.models import Quote
from services.notification.ntfy_sender import send_price_alert

logger = logging.getLogger(__name__)

# Redis 사용 가능 여부
_use_redis = True
try:
    from shared.redis_client import (
        get_rules_by_symbol,
        is_cooldown_active,
        set_cooldown,
        is_symbol_cooldown_active,
        set_symbol_cooldown,
        check_rate_limit,
        check_daily_limit,
        cache_quote,
        get_redis,
    )
    get_redis().ping()
    logger.info("Redis 연결 성공")
except Exception:
    _use_redis = False
    logger.warning("Redis 미연결 — 인메모리 모드")

# 인메모리 폴백
_memory_rules: dict[str, list[dict]] = {}
_memory_cooldown: dict[str, float] = {}
_memory_symbol_cooldown: dict[str, float] = {}
_memory_minute_count = {"count": 0, "reset_at": 0.0}
_memory_daily_count = {"count": 0, "date": ""}


class AlertEngine:
    """Kafka Consumer + 규칙 평가 + 4단계 알림 제어"""

    def __init__(self):
        self._consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "alert-engine",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        logger.info(f"AlertEngine 초기화 (Redis: {'ON' if _use_redis else 'OFF'})")

    def add_rule(self, rule: dict):
        """인메모리 규칙 등록"""
        symbol = rule["symbol"]
        if symbol not in _memory_rules:
            _memory_rules[symbol] = []
        _memory_rules[symbol].append(rule)
        logger.info(f"규칙: {symbol} {rule['alert_type']} @ {rule['threshold']}")

    # ========================================================
    # 4단계 알림 제어
    # ========================================================

    def _check_rule_cooldown(self, rule_id: str) -> bool:
        """레이어 1: 규칙별 쿨다운 (5분)"""
        if _use_redis:
            try:
                return is_cooldown_active(rule_id)
            except Exception:
                pass
        last = _memory_cooldown.get(rule_id, 0)
        return (time.time() - last) < 300

    def _check_symbol_cooldown(self, symbol: str) -> bool:
        """레이어 2: 종목별 쿨다운 (1시간)"""
        if _use_redis:
            try:
                return is_symbol_cooldown_active(symbol)
            except Exception:
                pass
        last = _memory_symbol_cooldown.get(symbol, 0)
        return (time.time() - last) < 3600

    def _check_rate_limit(self) -> bool:
        """레이어 3: 글로벌 레이트 리밋 (5건/분). True=발송 가능"""
        if _use_redis:
            try:
                return check_rate_limit()
            except Exception:
                pass
        # 인메모리 폴백
        now = time.time()
        if now > _memory_minute_count["reset_at"]:
            _memory_minute_count["count"] = 0
            _memory_minute_count["reset_at"] = now + 60
        if _memory_minute_count["count"] >= 5:
            return False
        _memory_minute_count["count"] += 1
        return True

    def _check_daily_limit(self) -> bool:
        """레이어 4: 일일 한도 (100건/일). True=발송 가능"""
        if _use_redis:
            try:
                return check_daily_limit()
            except Exception:
                pass
        # 인메모리 폴백
        from datetime import date
        today = str(date.today())
        if _memory_daily_count["date"] != today:
            _memory_daily_count["date"] = today
            _memory_daily_count["count"] = 0
        if _memory_daily_count["count"] >= 100:
            return False
        _memory_daily_count["count"] += 1
        return True

    def _set_cooldowns(self, rule_id: str, symbol: str):
        """알림 발송 후 쿨다운 설정"""
        if _use_redis:
            try:
                set_cooldown(rule_id)
                set_symbol_cooldown(symbol)
                return
            except Exception:
                pass
        _memory_cooldown[rule_id] = time.time()
        _memory_symbol_cooldown[symbol] = time.time()

    # ========================================================
    # 규칙 평가
    # ========================================================

    def _get_rules(self, symbol: str) -> list[dict]:
        if _use_redis:
            try:
                return get_rules_by_symbol(symbol)
            except Exception:
                pass
        return _memory_rules.get(symbol, [])

    def _evaluate(self, quote: Quote):
        """Quote와 규칙 비교 → 4단계 제어 통과 시 알림 발송"""
        # 시세 캐시
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
            alert_type = rule["alert_type"]
            threshold = rule["threshold"]

            # 조건 충족 여부 확인
            triggered = False
            if alert_type == "price_above" and price >= threshold:
                triggered = True
            elif alert_type == "price_below" and price <= threshold:
                triggered = True
            elif alert_type == "price_change" and threshold > 0:
                change_pct = abs((price - threshold) / threshold * 100)
                if change_pct >= 5.0:
                    triggered = True

            if not triggered:
                continue

            # === 4단계 알림 제어 ===

            # 1. 규칙별 쿨다운 (5분)
            if self._check_rule_cooldown(rule_id):
                logger.debug(f"[억제] 규칙 쿨다운: {rule_id}")
                continue

            # 2. 종목별 쿨다운 (1시간)
            if self._check_symbol_cooldown(quote.symbol):
                logger.debug(f"[억제] 종목 쿨다운: {quote.symbol}")
                continue

            # 3. 글로벌 레이트 리밋 (5건/분)
            if not self._check_rate_limit():
                logger.warning(f"[억제] 분당 한도 초과")
                continue

            # 4. 일일 한도 (100건/일)
            if not self._check_daily_limit():
                logger.warning(f"[억제] 일일 한도 초과")
                continue

            # === 모든 제어 통과 → 알림 발송 ===
            logger.info(
                f"ALERT: {quote.symbol} ${price:.2f} ({alert_type} @ {threshold})"
            )
            send_price_alert(quote.symbol, price, alert_type, threshold)
            self._set_cooldowns(rule_id, quote.symbol)

            # 한 종목에 대해 한 번만 알림 (다른 규칙은 종목 쿨다운으로 차단됨)
            break

    # ========================================================
    # Kafka Consumer 루프
    # ========================================================

    def run(self):
        """Kafka 소비 시작 (블로킹)"""
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
