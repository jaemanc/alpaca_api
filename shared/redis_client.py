"""Redis 클라이언트 — 캐시, 쿨다운, 레이트리밋"""

import json
import logging
import redis
from datetime import datetime, timezone

from shared.config import (
    REDIS_HOST, REDIS_PORT,
    COOLDOWN_PRICE_ALERT, COOLDOWN_SYMBOL,
    RATE_LIMIT_PER_MINUTE, DAILY_ALERT_LIMIT,
    TIMESERIES_RETENTION_DAYS,
)

logger = logging.getLogger(__name__)
_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=_pool)


# --- 시세 캐시 ---

def cache_quote(symbol: str, quote_json: str):
    get_redis().setex(f"quote:{symbol}", 60, quote_json)


def get_cached_quote(symbol: str) -> str | None:
    """캐시된 시세 조회 (없으면 None)"""
    return get_redis().get(f"quote:{symbol}")


# --- 3일치 시계열 (분당 1포인트) ---

def record_timeseries(symbol: str, price: float, ts_epoch: float):
    """
    시계열 기록 (5분 버킷).

    5분 단위로 샘플링하고, 3일 이전 데이터는 자동 삭제.
    """
    r = get_redis()
    key = f"ts:{symbol}"
    bucket = int(ts_epoch // 300) * 300  # 5분 단위로 내림

    r.zremrangebyscore(key, bucket, bucket)
    r.zadd(key, {f"{bucket}:{price}": bucket})

    # 3일 이전 삭제
    cutoff = ts_epoch - TIMESERIES_RETENTION_DAYS * 86400
    r.zremrangebyscore(key, 0, cutoff)


def get_timeseries(symbol: str) -> list[tuple[float, float]]:
    """시계열 조회 → [(epoch, price), ...] 시간순 정렬"""
    r = get_redis()
    raw = r.zrange(f"ts:{symbol}", 0, -1)
    result = []
    for m in raw:
        epoch_str, price_str = m.split(":", 1)
        result.append((float(epoch_str), float(price_str)))
    return result


# --- 쿨다운 ---

def is_cooldown_active(rule_id: str) -> bool:
    return get_redis().exists(f"cooldown:{rule_id}") > 0


def set_cooldown(rule_id: str):
    get_redis().setex(f"cooldown:{rule_id}", COOLDOWN_PRICE_ALERT, "1")


def is_symbol_cooldown_active(symbol: str) -> bool:
    return get_redis().exists(f"cooldown:symbol:{symbol}") > 0


def set_symbol_cooldown(symbol: str):
    get_redis().setex(f"cooldown:symbol:{symbol}", COOLDOWN_SYMBOL, "1")


# --- 레이트 리밋 ---

def check_rate_limit() -> bool:
    """분당 N건 제한. True=발송 가능"""
    r = get_redis()
    key = "ratelimit:global"
    count = r.get(key)
    if count is None:
        r.setex(key, 60, 1)
        return True
    if int(count) >= RATE_LIMIT_PER_MINUTE:
        return False
    r.incr(key)
    return True


def check_daily_limit(user_id: str = "default") -> bool:
    """일일 N건 제한. True=발송 가능"""
    r = get_redis()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"daily:{user_id}:{today}"
    count = r.get(key)
    if count is None:
        now = datetime.now(timezone.utc)
        secs_left = max(1, int((now.replace(hour=23, minute=59, second=59) - now).total_seconds()))
        r.setex(key, secs_left, 1)
        return True
    if int(count) >= DAILY_ALERT_LIMIT:
        return False
    r.incr(key)
    return True


# --- 규칙 저장소 ---

def save_rule(user_id: str, rule_id: str, rule_json: str):
    get_redis().hset(f"rules:{user_id}", rule_id, rule_json)


def get_rules_by_user(user_id: str) -> list[dict]:
    raw = get_redis().hgetall(f"rules:{user_id}")
    return [json.loads(v) for v in raw.values()]


def get_rules_by_symbol(symbol: str) -> list[dict]:
    r = get_redis()
    rules = []
    for key in r.scan_iter("rules:*"):
        for v in r.hgetall(key).values():
            rule = json.loads(v)
            if rule.get("symbol") == symbol and rule.get("is_active", True):
                rules.append(rule)
    return rules


def delete_rule(user_id: str, rule_id: str):
    r = get_redis()
    r.hdel(f"rules:{user_id}", rule_id)
    r.delete(f"cooldown:{rule_id}")


def count_rules(user_id: str) -> int:
    return get_redis().hlen(f"rules:{user_id}")
