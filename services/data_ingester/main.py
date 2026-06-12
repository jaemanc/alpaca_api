"""
Data Ingester — Alpaca WebSocket → Kafka

시장 시간 스케줄러 통합: 09:20~16:00 ET 자동 사이클.
실행: python -m services.data_ingester.main
"""

import sys
import signal
import logging
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.config import WATCH_SYMBOLS, ALPACA_API_KEY, ALPACA_SECRET_KEY
from shared.models import Quote
from shared.kafka_producer import KafkaQuoteProducer
from services.data_ingester.ingester import DataIngester
from services.data_ingester.scheduler import wait_for_market, seconds_until_market_close, now_et
from services.telegram_bot.notifier import send_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("data_ingester")

producer = KafkaQuoteProducer()
quote_count = 0
_shutdown = False


def send_startup_notification():
    """시작 알림 — 변동성 상위 15종목, 한글, 한 줄 2개"""
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockSnapshotRequest
        from datetime import datetime
        from shared.symbol_names import get_name

        client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        snaps = client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=WATCH_SYMBOLS, feed="iex"))

        changes = []
        for sym, s in snaps.items():
            price = float(s.latest_quote.bid_price) if s.latest_quote and s.latest_quote.bid_price else 0
            prev = float(s.previous_daily_bar.close) if s.previous_daily_bar else 0
            if price > 0 and prev > 0:
                changes.append((sym, price, ((price - prev) / prev) * 100))

        changes.sort(key=lambda x: abs(x[2]), reverse=True)
        top = changes[:15]

        lines = [f"📡 시작 {datetime.now().strftime('%m/%d %H:%M')} | {len(WATCH_SYMBOLS)}종목 감시중", ""]
        for i in range(0, len(top), 2):
            pair = top[i:i+2]
            lines.append(" | ".join(f"{get_name(s)} {'▲' if p >= 0 else '▼'}{abs(p):.1f}%" for s, _, p in pair))

        send_message("\n".join(lines))
        logger.info("시작 알림 발송 완료")
    except Exception as e:
        logger.warning(f"시작 알림 실패: {e}")


def on_quote(quote: Quote):
    global quote_count
    quote_count += 1
    producer.send_quote(quote.symbol, quote.to_json())
    if quote_count % 100 == 1:
        logger.info(f"[{quote.symbol}] ${quote.bid_price:.2f}/{quote.ask_price:.2f} (총 {quote_count}건)")


def hourly_report():
    """1시간마다 변동성 상위 10종목 리포트"""
    import time
    while not _shutdown:
        time.sleep(3600)
        if _shutdown:
            break
        try:
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.data.requests import StockSnapshotRequest
            from shared.symbol_names import get_name

            client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
            snaps = client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=WATCH_SYMBOLS, feed="iex"))

            changes = []
            for sym, s in snaps.items():
                price = float(s.latest_quote.bid_price) if s.latest_quote and s.latest_quote.bid_price else 0
                prev = float(s.previous_daily_bar.close) if s.previous_daily_bar else 0
                if price > 0 and prev > 0:
                    changes.append((sym, ((price - prev) / prev) * 100))

            changes.sort(key=lambda x: abs(x[1]), reverse=True)
            top = changes[:10]

            lines = [f"🕐 {now_et().strftime('%H:%M ET')} | {quote_count}건 처리", ""]
            for i in range(0, len(top), 2):
                pair = top[i:i+2]
                lines.append(" | ".join(f"{get_name(s)} {'▲' if p >= 0 else '▼'}{abs(p):.1f}%" for s, p in pair))

            send_message("\n".join(lines))
        except Exception as e:
            logger.warning(f"리포트 실패: {e}")


def run_session():
    """거래 세션 1회 실행"""
    global quote_count
    quote_count = 0

    ingester = DataIngester(symbols=WATCH_SYMBOLS, on_quote=on_quote)

    close_secs = seconds_until_market_close()
    logger.info(f"폐장까지 {close_secs // 60}분")

    # 폐장 타이머
    def close_timer():
        import time
        time.sleep(close_secs)
        if not _shutdown:
            logger.info("폐장. WebSocket 종료.")
            ingester.stop()

    threading.Thread(target=close_timer, daemon=True).start()
    threading.Thread(target=hourly_report, daemon=True).start()

    ingester.start()
    producer.flush()

    send_message(f"🌙 폐장. 오늘 {quote_count}건 처리.")
    logger.info(f"세션 종료. {quote_count}건.")


def main():
    global _shutdown

    logger.info(f"Alpaca Stock Alert | {len(WATCH_SYMBOLS)}종목 | {now_et().strftime('%a %m/%d %H:%M ET')}")

    def shutdown(signum, frame):
        global _shutdown
        _shutdown = True
        producer.flush()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    send_startup_notification()

    while not _shutdown:
        wait_for_market()
        if _shutdown:
            break
        run_session()
        if not _shutdown:
            import time
            time.sleep(5)


if __name__ == "__main__":
    main()
