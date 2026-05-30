"""
시장 시간 스케줄러

미국 주식시장 개장/폐장 시간에 맞춰 WebSocket 연결을 제어한다.
- 개장: 09:30 ET → WebSocket 연결
- 폐장: 16:00 ET → WebSocket 해제 + 대기 모드

대기 모드에서는 CPU를 거의 사용하지 않는다 (sleep 루프).
"""

import time
import logging
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)

ET = pytz.timezone("US/Eastern")


def is_market_open() -> bool:
    """현재 미국 주식시장이 개장 중인지 확인"""
    now = datetime.now(ET)

    # 주말 체크 (토=5, 일=6)
    if now.weekday() >= 5:
        return False

    # 시간 체크: 09:30 ~ 16:00
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_open <= now <= market_close


def wait_for_market_open():
    """
    시장 개장까지 대기

    폐장 중에는 60초 간격으로 체크하며 대기한다.
    CPU 사용률을 최소화한다.
    """
    if is_market_open():
        return

    logger.info("시장 폐장 중. 개장 대기...")
    while not is_market_open():
        now = datetime.now(ET)
        logger.debug(f"대기 중... (현재 ET: {now.strftime('%H:%M')})")
        time.sleep(60)

    logger.info("시장 개장! WebSocket 연결 시작.")


def seconds_until_close() -> int:
    """폐장까지 남은 초 (폐장 후면 0 반환)"""
    now = datetime.now(ET)
    close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    diff = (close - now).total_seconds()
    return max(0, int(diff))
