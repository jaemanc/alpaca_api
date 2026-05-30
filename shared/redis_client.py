"""
Redis 클라이언트

시세 캐시, Alert_Rule 저장, 쿨다운 상태를 관리한다.
- 시세 캐시: TTL 60초
- 쿨다운: Redis TTL로 자동 만료 (별도 타이머 불필요)
- 규칙 저장: 사용자별 해시 구조
"""

import json
import logging
import redis

from shared.config import (
    REDIS_HOST,
    REDIS_PORT,
    COOLDOWN_PRICE_ALERT,
    COOLDOWN_OB_ALERT,
)

logger = logging.getLogger(__name__)

# Redis 연결 (싱글톤)
_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_redis() -> redis.Redis:
    """Redis 클라이언트 인스턴스 반환"""
    return redis.Redis(connection_pool=_pool)


# ============================================================
# 시세 캐시
# ============================================================

def cache_quote(symbol: str, quote_json: str):
    """시세 캐시 저장 (TTL 60초)"""
    r = get_redis()
    r.setex(f"quote:{symbol}", 60, quote_json)


def get_cached_quote(symbol: str) -> str | None:
    """캐시된 시세 조회 (없으면 None)"""
    r = get_redis()
    return r.get(f"quote:{symbol}")


# ============================================================
# 쿨다운 관리
# ============================================================

def is_cooldown_active(rule_id: str) -> bool:
    """쿨다운 중인지 확인 (키가 존재하면 쿨다운 중)"""
    r = get_redis()
    return r.exists(f"cooldown:{rule_id}") > 0


def set_cooldown(rule_id: str, alert_type: str = "price"):
    """
    쿨다운 설정

    가격 알림: 5분, 호가 알림: 60초
    Redis TTL로 자동 만료되므로 별도 정리 불필요.
    """
    r = get_redis()
    ttl = COOLDOWN_PRICE_ALERT if alert_type == "price" else COOLDOWN_OB_ALERT
    r.setex(f"cooldown:{rule_id}", ttl, "1")


# ============================================================
# Alert Rule 저장소
# ============================================================

def save_rule(user_id: str, rule_id: str, rule_json: str):
    """규칙 저장 (해시 구조: user별 규칙 목록)"""
    r = get_redis()
    r.hset(f"rules:{user_id}", rule_id, rule_json)


def get_rules_by_user(user_id: str) -> list[dict]:
    """사용자의 모든 규칙 조회"""
    r = get_redis()
    raw = r.hgetall(f"rules:{user_id}")
    return [json.loads(v) for v in raw.values()]


def get_rules_by_symbol(symbol: str) -> list[dict]:
    """
    특정 종목에 대한 모든 활성 규칙 조회

    모든 사용자의 규칙을 스캔한다.
    (규모가 커지면 별도 인덱스 필요하지만, 현재는 충분)
    """
    r = get_redis()
    rules = []
    # rules:* 패턴의 모든 키를 스캔
    for key in r.scan_iter("rules:*"):
        raw = r.hgetall(key)
        for v in raw.values():
            rule = json.loads(v)
            if rule.get("symbol") == symbol and rule.get("is_active", True):
                rules.append(rule)
    return rules


def delete_rule(user_id: str, rule_id: str):
    """규칙 삭제 + 쿨다운 초기화"""
    r = get_redis()
    r.hdel(f"rules:{user_id}", rule_id)
    r.delete(f"cooldown:{rule_id}")


def count_rules(user_id: str) -> int:
    """사용자의 규칙 수 조회"""
    r = get_redis()
    return r.hlen(f"rules:{user_id}")
