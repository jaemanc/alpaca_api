"""
Alert Engine 메인 엔트리포인트

시작 시 Snapshot API로 전일 종가를 조회하고,
전일 대비 ±5% 변동 시 알림을 발송하는 규칙을 자동 등록한다.

실행: python -m services.alert_engine.main
"""

import sys
import signal
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, WATCH_SYMBOLS
from services.alert_engine.engine import AlertEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("alert_engine")

# 변동률 임계값 (전일 종가 대비 이 비율 이상 변동 시 알림)
CHANGE_THRESHOLD_PCT = 5.0


def load_prev_close() -> dict[str, float]:
    """Snapshot API로 전일 종가 조회"""
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockSnapshotRequest

        client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        request = StockSnapshotRequest(symbol_or_symbols=WATCH_SYMBOLS, feed="iex")
        snapshots = client.get_stock_snapshot(request)

        prev_close = {}
        for sym, snap in snapshots.items():
            if snap.previous_daily_bar:
                prev_close[sym] = float(snap.previous_daily_bar.close)
        return prev_close
    except Exception as e:
        logger.error(f"전일 종가 조회 실패: {e}")
        return {}


def main():
    logger.info("=" * 50)
    logger.info("Alert Engine 시작")
    logger.info(f"  변동률 임계값: ±{CHANGE_THRESHOLD_PCT}%")
    logger.info("=" * 50)

    engine = AlertEngine()

    # 전일 종가 조회 → 자동 규칙 등록
    prev_close = load_prev_close()
    logger.info(f"전일 종가 로드: {len(prev_close)}종목")

    for sym, close in prev_close.items():
        # price_change 규칙: threshold에 전일 종가를 넣고,
        # engine에서 변동률 계산하여 5% 초과 시 알림
        engine.add_rule({
            "rule_id": f"auto-{sym}-change",
            "symbol": sym,
            "alert_type": "price_change",
            "threshold": close,  # 전일 종가가 기준가
            "is_active": True,
        })

    logger.info(f"규칙 등록 완료: {len(prev_close)}개 (전일 대비 ±{CHANGE_THRESHOLD_PCT}%)")

    def shutdown(signum, frame):
        logger.info("종료 중...")
        engine.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    engine.run()


if __name__ == "__main__":
    main()
