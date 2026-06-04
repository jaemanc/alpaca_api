"""
시장 시간 스케줄러

미국 주식시장 시간을 ET(동부시간)로 계산하고, 로그는 KST로 표시.
- 연결 시작: 09:20 ET (KST 22:20 서머타임 / 23:20 비서머타임)
- 폐장: 16:00 ET (KST 05:00 / 06:00)
- 주말: 대기
"""

import time
import logging
from datetime import datetime, timedelta

import pytz

logger = logging.getLogger(__name__)

ET = pytz.timezone("US/Eastern")
KST = pytz.timezone("Asia/Seoul")

# 시장 시간 (ET 기준)
CONNECT_HOUR, CONNECT_MINUTE = 9, 20
CLOSE_HOUR, CLOSE_MINUTE = 16, 0


def now_et() -> datetime:
    return datetime.now(ET)


def now_kst() -> datetime:
    return datetime.now(KST)


def is_market_hours() -> bool:
    """09:20~16:00 ET, 평일만"""
    now = now_et()
    if now.weekday() >= 5:
        return False
    start = now.replace(hour=CONNECT_HOUR, minute=CONNECT_MINUTE, second=0, microsecond=0)
    end = now.replace(hour=CLOSE_HOUR, minute=CLOSE_MINUTE, second=0, microsecond=0)
    return start <= now <= end


def seconds_until_market_start() -> int:
    """다음 09:20 ET까지 남은 초 (주말 건너뜀)"""
    now = now_et()
    target = now.replace(hour=CONNECT_HOUR, minute=CONNECT_MINUTE, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return max(0, int((target - now).total_seconds()))


def seconds_until_market_close() -> int:
    """16:00 ET까지 남은 초"""
    now = now_et()
    close = now.replace(hour=CLOSE_HOUR, minute=CLOSE_MINUTE, second=0, microsecond=0)
    return max(0, int((close - now).total_seconds()))


def wait_for_market():
    """시장 시작까지 대기 (60초 간격 체크)"""
    if is_market_hours():
        logger.info("시장 활성 — 즉시 시작")
        return

    remaining = seconds_until_market_start()
    h, m = remaining // 3600, (remaining % 3600) // 60
    logger.info(f"시장 대기. 시작까지 {h}시간 {m}분 (KST {now_kst().strftime('%m/%d %H:%M')} / ET {now_et().strftime('%H:%M')})")

    while not is_market_hours():
        time.sleep(60)
        remaining = seconds_until_market_start()
        if remaining > 0 and remaining % 600 < 60:
            h, m = remaining // 3600, (remaining % 3600) // 60
            logger.info(f"대기 중... {h}시간 {m}분 남음 (KST {now_kst().strftime('%H:%M')})")

    logger.info(f"시장 시작! (KST {now_kst().strftime('%H:%M')})")
