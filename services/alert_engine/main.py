"""
Alert Engine — 전일 종가 대비 ±5% 변동 시 알림

시작 시:
1. SQLite에서 전일 종가 로드 (있으면 사용)
2. 없거나 오래됐으면 Alpaca Snapshot API로 조회 후 SQLite에 저장
3. 전일 종가를 기준으로 규칙 자동 등록

실행: python -m services.alert_engine.main
"""

import sys
import signal
import logging
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, WATCH_SYMBOLS
from shared.db import save_prev_close, load_prev_close, get_updated_date
from services.alert_engine.engine import AlertEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("alert_engine")

CHANGE_THRESHOLD_PCT = 5.0


def fetch_and_save_prev_close() -> dict[str, float]:
    """Alpaca Snapshot에서 전일 종가 조회 → SQLite 저장"""
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockSnapshotRequest

    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    snaps = client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=WATCH_SYMBOLS, feed="iex"))

    prev_close = {}
    for sym, snap in snaps.items():
        if snap.previous_daily_bar:
            prev_close[sym] = float(snap.previous_daily_bar.close)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    save_prev_close(prev_close, today)
    return prev_close


def get_prev_close() -> dict[str, float]:
    """SQLite에서 로드. 오늘 날짜가 아니면 API에서 갱신."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    updated = get_updated_date()

    if updated == today:
        data = load_prev_close()
        if data:
            logger.info(f"SQLite에서 전일 종가 로드: {len(data)}종목")
            return data

    # 갱신 필요
    logger.info("전일 종가 갱신 중 (Alpaca API)...")
    try:
        return fetch_and_save_prev_close()
    except Exception as e:
        logger.error(f"API 조회 실패: {e}")
        # 폴백: 기존 SQLite 데이터라도 사용
        return load_prev_close()


def main():
    logger.info(f"Alert Engine | ±{CHANGE_THRESHOLD_PCT}% 변동 알림")

    engine = AlertEngine()
    prev_close = get_prev_close()
    logger.info(f"기준가 로드 완료: {len(prev_close)}종목")

    for sym, close in prev_close.items():
        engine.add_rule({
            "rule_id": f"auto-{sym}-change",
            "symbol": sym,
            "alert_type": "price_change",
            "threshold": close,
            "is_active": True,
        })

    def shutdown(signum, frame):
        engine.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    engine.run()


if __name__ == "__main__":
    main()
