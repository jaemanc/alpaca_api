"""
시장 시간 스케줄러

미국 주식시장 개장/폐장에 맞춰 시스템을 자동 제어한다.
- 개장 10분 전 (09:20 ET): WebSocket 연결 시작
- 폐장 (16:00 ET): WebSocket 해제 + 대기 모드
- 주말/공휴일: 대기

대기 모드에서는 60초 간격으로 시간만 체크하므로 CPU를 거의 안 씀.
"""

import time
import logging
from datetime import datetime, timedelta

import pytz

logger = logging.getLogger(__name__)

ET = pytz.timezone("US/Eastern")

# 개장 10분 전에 연결 시작
CONNECT_HOUR = 9
CONNECT_MINUTE = 20

# 폐장 시간
CLOSE_HOUR = 16
CLOSE_MINUTE = 0


def now_et() -> datetime:
    """현재 미국 동부시간"""
    return datetime.now(ET)


def is_weekday() -> bool:
    """평일인지 확인 (토=5, 일=6)"""
    return now_et().weekday() < 5


def is_market_hours() -> bool:
    """
    현재 시장 활성 시간인지 확인

    09:20 ET ~ 16:00 ET (평일만)
    """
    if not is_weekday():
        return False

    now = now_et()
    start = now.replace(hour=CONNECT_HOUR, minute=CONNECT_MINUTE, second=0, microsecond=0)
    end = now.replace(hour=CLOSE_HOUR, minute=CLOSE_MINUTE, second=0, microsecond=0)

    return start <= now <= end


def seconds_until_market_start() -> int:
    """
    다음 시장 시작(09:20 ET)까지 남은 초

    주말이면 월요일 09:20까지 계산.
    """
    now = now_et()

    # 오늘 09:20
    target = now.replace(hour=CONNECT_HOUR, minute=CONNECT_MINUTE, second=0, microsecond=0)

    # 이미 지났으면 내일로
    if now >= target:
        target += timedelta(days=1)

    # 주말 건너뛰기
    while target.weekday() >= 5:
        target += timedelta(days=1)

    diff = (target - now).total_seconds()
    return max(0, int(diff))


def seconds_until_market_close() -> int:
    """폐장(16:00 ET)까지 남은 초"""
    now = now_et()
    close = now.replace(hour=CLOSE_HOUR, minute=CLOSE_MINUTE, second=0, microsecond=0)
    diff = (close - now).total_seconds()
    return max(0, int(diff))


def wait_for_market():
    """
    시장 시작까지 대기

    60초 간격으로 체크하며 대기한다.
    대기 중 남은 시간을 로그로 출력.
    """
    if is_market_hours():
        logger.info("시장 활성 시간 — 즉시 시작")
        return

    remaining = seconds_until_market_start()
    hours = remaining // 3600
    minutes = (remaining % 3600) // 60

    logger.info(
        f"시장 대기 중. 다음 시작까지 약 {hours}시간 {minutes}분 "
        f"(현재 ET: {now_et().strftime('%a %H:%M')})"
    )

    while not is_market_hours():
        time.sleep(60)

        # 10분마다 상태 로그
        remaining = seconds_until_market_start()
        if remaining > 0 and remaining % 600 < 60:
            h = remaining // 3600
            m = (remaining % 3600) // 60
            logger.info(f"대기 중... 시작까지 {h}시간 {m}분")

    logger.info("시장 시작! 연결 개시.")
