"""
과거 데이터를 Alpaca Snapshot에서 조회하여 Redis 시계열에 적재.

IEX 무료 플랜에서 사용 가능한 데이터:
- Snapshot API의 daily_bar (오늘 또는 마지막 거래일)
- Snapshot API의 previous_daily_bar (전일 종가)

각 종목에 대해 2포인트(전일종가, 최신호가)를 시계열에 기록한다.
장 시간에 실시간 데이터가 쌓이면 3일치 그래프가 완성된다.

실행: python scripts/backfill_timeseries.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
from datetime import datetime, timedelta, timezone

from shared.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, WATCH_SYMBOLS
from shared.redis_client import record_timeseries, get_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("backfill")


def backfill():
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockSnapshotRequest

    get_redis().ping()
    logger.info("Redis 연결 OK")

    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    request = StockSnapshotRequest(symbol_or_symbols=WATCH_SYMBOLS, feed="iex")
    snaps = client.get_stock_snapshot(request)

    total = 0
    now = datetime.now(timezone.utc)

    for sym, snap in snaps.items():
        # 전일 종가 → 어제 16:00 ET 기준 시각으로 기록
        if snap.previous_daily_bar:
            prev_ts = (now - timedelta(days=1)).replace(hour=20, minute=0).timestamp()
            record_timeseries(sym, float(snap.previous_daily_bar.close), prev_ts)
            total += 1

        # 당일 일봉 (마지막 종가 또는 현재 시세)
        if snap.daily_bar:
            daily_ts = now.replace(hour=14, minute=0).timestamp()
            record_timeseries(sym, float(snap.daily_bar.close), daily_ts)
            total += 1

        # 최신 호가
        if snap.latest_quote and snap.latest_quote.bid_price:
            record_timeseries(sym, float(snap.latest_quote.bid_price), now.timestamp())
            total += 1

    logger.info(f"적재 완료: {total}건 ({len(snaps)}종목)")


if __name__ == "__main__":
    backfill()
