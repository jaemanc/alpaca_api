"""
Alert Rule 관리 모듈

사용자 알림 규칙의 CRUD를 담당한다.
- 사용자당 최대 20개 규칙
- 유효성 검증 (심볼, 조건값)
- Redis 해시 구조로 저장
"""

import uuid
import json
import logging
from shared.redis_client import (
    save_rule,
    get_rules_by_user,
    delete_rule,
    count_rules,
    get_redis,
)
from shared.config import WATCH_SYMBOLS

logger = logging.getLogger(__name__)

MAX_RULES_PER_USER = 20
VALID_ALERT_TYPES = ["price_above", "price_below", "price_change", "ob_change", "ob_imbalance"]
VALID_CHANNELS = ["push", "email", "both"]


class RuleValidationError(Exception):
    """규칙 유효성 검증 실패"""
    pass


def create_rule(
    user_id: str,
    symbol: str,
    alert_type: str,
    threshold: float,
    channel: str = "push",
) -> dict:
    """
    알림 규칙 생성

    Args:
        user_id: 사용자 ID
        symbol: 종목 심볼
        alert_type: 알림 유형
        threshold: 조건값
        channel: 알림 채널 (push/email/both)

    Returns:
        생성된 규칙 dict

    Raises:
        RuleValidationError: 유효성 검증 실패 시
    """
    # 유효성 검증
    if not symbol or symbol not in WATCH_SYMBOLS:
        raise RuleValidationError(f"유효하지 않은 심볼: {symbol}")
    if threshold <= 0:
        raise RuleValidationError(f"조건값은 0보다 커야 합니다: {threshold}")
    if alert_type not in VALID_ALERT_TYPES:
        raise RuleValidationError(f"유효하지 않은 알림 유형: {alert_type}")
    if channel not in VALID_CHANNELS:
        raise RuleValidationError(f"유효하지 않은 채널: {channel}")

    # 규칙 수 상한 확인
    current_count = count_rules(user_id)
    if current_count >= MAX_RULES_PER_USER:
        raise RuleValidationError(f"규칙 상한 초과 (최대 {MAX_RULES_PER_USER}개)")

    rule = {
        "rule_id": str(uuid.uuid4()),
        "user_id": user_id,
        "symbol": symbol,
        "alert_type": alert_type,
        "threshold": threshold,
        "channel": channel,
        "is_active": True,
    }

    save_rule(user_id, rule["rule_id"], json.dumps(rule))
    logger.info(f"규칙 생성: {user_id} → {symbol} {alert_type} @ {threshold}")
    return rule


def list_rules(user_id: str) -> list[dict]:
    """사용자의 모든 규칙 조회"""
    return get_rules_by_user(user_id)


def remove_rule(user_id: str, rule_id: str):
    """규칙 삭제 + 쿨다운 초기화"""
    delete_rule(user_id, rule_id)
    logger.info(f"규칙 삭제: {user_id} / {rule_id}")


def toggle_rule(user_id: str, rule_id: str, is_active: bool):
    """규칙 활성/비활성 토글"""
    r = get_redis()
    raw = r.hget(f"rules:{user_id}", rule_id)
    if not raw:
        raise RuleValidationError(f"규칙을 찾을 수 없음: {rule_id}")

    rule = json.loads(raw)
    rule["is_active"] = is_active
    save_rule(user_id, rule_id, json.dumps(rule))
    logger.info(f"규칙 {'활성화' if is_active else '비활성화'}: {rule_id}")
