"""
Data Ingester 메인 엔트리포인트

시장 시간 스케줄러 통합:
- 개장 10분 전(09:20 ET)에 WebSocket 연결
- 폐장(16:00 ET)에 자동 종료
- 다음 거래일까지 대기 후 반복

실행: python -m services.data_ingester.main
(한 번 실행하면 24시간 상주하며 자동으로 개장/폐장 사이클을 반복)
"""

import sys
import signal
import logging
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.config import WATCH_SYMBOLS, KAFKA_BOOTSTRAP_SERVERS, ALPACA_API_KEY, ALPACA_SECRET_KEY
from shared.models import Quote
from shared.kafka_producer import KafkaQuoteProducer
from services.data_ingester.ingester import DataIngester
from services.data_ingester.scheduler import (
    is_market_hours,
    wait_for_market,
    seconds_until_market_close,
    now_et,
)
from services.notification.ntfy_sender import send_push

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("data_ingester")

# Kafka Producer
producer = KafkaQuoteProducer()
quote_count = 0
_shutdown = False


def send_startup_notification():
    """시작 알림 — 변동성 상위 15종목, 한글 이름, 한 줄에 2개"""
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockSnapshotRequest
        from datetime import datetime
        from shared.symbol_names import get_name

        client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

        request = StockSnapshotRequest(symbol_or_symbols=WATCH_SYMBOLS, feed="iex")
        snapshots = client.get_stock_snapshot(request)

        # 변동률 계산
        changes = []
        for sym, snap in snapshots.items():
            price = float(snap.latest_quote.bid_price) if snap.latest_quote and snap.latest_quote.bid_price else 0
            prev = float(snap.previous_daily_bar.close) if snap.previous_daily_bar else 0
            if price > 0 and prev > 0:
                pct = ((price - prev) / prev) * 100
                changes.append((sym, price, pct))

        # 변동성(절대값) 큰 순으로 정렬, 상위 15개
        changes.sort(key=lambda x: abs(x[2]), reverse=True)
        top15 = changes[:15]

        now = datetime.now().strftime("%m/%d %H:%M")
        lines = [f"{now} | {len(WATCH_SYMBOLS)}종목 감시중", ""]

        # 한 줄에 2개씩
        for i in range(0, len(top15), 2):
            pair = top15[i:i+2]
            parts = []
            for sym, price, pct in pair:
                arrow = "▲" if pct >= 0 else "▼"
                name = get_name(sym)
                parts.append(f"{name} {arrow}{abs(pct):.1f}%")
            lines.append(" | ".join(parts))

        send_push(
            message="\n".join(lines),
            title="Stock Alert - System Online",
            tags="white_check_mark,chart_with_upwards_trend",
            priority=2,
        )
        logger.info("시작 알림 발송 완료")
    except Exception as e:
        logger.warning(f"시작 알림 실패: {e}")


def send_close_notification():
    """폐장 알림"""
    global quote_count
    try:
        send_push(
            message=f"Market closed. Processed {quote_count} quotes today.\nSleeping until next trading day.",
            title="Stock Alert - Market Closed",
            tags="zzz,moon",
            priority=1,
        )
    except Exception:
        pass


def on_quote(quote: Quote):
    """Quote 수신 → Kafka 발행"""
    global quote_count
    quote_count += 1
    producer.send_quote(quote.symbol, quote.to_json())

    if quote_count % 100 == 1:
        logger.info(
            f"[{quote.symbol}] ${quote.bid_price:.2f}/{quote.ask_price:.2f} "
            f"(총 {quote_count}건)"
        )


def run_session():
    """
    하나의 거래 세션 실행

    WebSocket 연결 → 폐장까지 수신 → 연결 해제
    1시간마다 헬스체크 알림 발송.
    """
    global quote_count
    quote_count = 0

    send_startup_notification()

    ingester = DataIngester(symbols=WATCH_SYMBOLS, on_quote=on_quote)

    # 폐장 타이머: 폐장 시간에 자동으로 ingester 종료
    close_seconds = seconds_until_market_close()
    logger.info(f"폐장까지 {close_seconds // 60}분. 타이머 설정.")

    def close_timer():
        import time
        time.sleep(close_seconds)
        if not _shutdown:
            logger.info("폐장 시간 도달. WebSocket 종료.")
            ingester.stop()

    timer = threading.Thread(target=close_timer, daemon=True)
    timer.start()

    # 1시간마다 변동성 리포트 발송
    def hourly_report_loop():
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
                req = StockSnapshotRequest(symbol_or_symbols=WATCH_SYMBOLS, feed="iex")
                snaps = client.get_stock_snapshot(req)

                changes = []
                for sym, snap in snaps.items():
                    price = float(snap.latest_quote.bid_price) if snap.latest_quote and snap.latest_quote.bid_price else 0
                    prev = float(snap.previous_daily_bar.close) if snap.previous_daily_bar else 0
                    if price > 0 and prev > 0:
                        pct = ((price - prev) / prev) * 100
                        changes.append((sym, price, pct))

                changes.sort(key=lambda x: abs(x[2]), reverse=True)
                top10 = changes[:10]

                et_now = now_et().strftime("%H:%M ET")
                lines = [f"{et_now} | {quote_count}건 처리", ""]
                for i in range(0, len(top10), 2):
                    pair = top10[i:i+2]
                    parts = []
                    for sym, price, pct in pair:
                        arrow = "▲" if pct >= 0 else "▼"
                        parts.append(f"{get_name(sym)} {arrow}{abs(pct):.1f}%")
                    lines.append(" | ".join(parts))

                send_push(
                    message="\n".join(lines),
                    title="Stock Alert - Hourly Report",
                    tags="clock1,chart_with_upwards_trend",
                    priority=1,
                )
                logger.info(f"1시간 리포트 발송: {quote_count}건")
            except Exception as e:
                logger.warning(f"리포트 실패: {e}")
    hc_thread = threading.Thread(target=hourly_report_loop, daemon=True)
    hc_thread.start()

    # WebSocket 시작 (블로킹 — 폐장 타이머가 stop() 호출하면 반환됨)
    ingester.start()

    # 세션 종료
    producer.flush()
    send_close_notification()
    logger.info(f"세션 종료. 총 {quote_count}건 처리.")


def main():
    global _shutdown

    logger.info("=" * 50)
    logger.info("Alpaca Stock Alert - Scheduled Mode")
    logger.info(f"  Symbols: {len(WATCH_SYMBOLS)} stocks")
    logger.info(f"  Schedule: 09:20~16:00 ET (weekdays)")
    logger.info(f"  Current ET: {now_et().strftime('%a %m/%d %H:%M')}")
    logger.info("=" * 50)

    def shutdown(signum, frame):
        global _shutdown
        _shutdown = True
        logger.info("종료 신호 수신. 프로세스 종료.")
        producer.flush()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # 시작 즉시 알림 발송 (연결 확인용)
    send_startup_notification()

    # 무한 루프: 대기 → 세션 실행 → 대기 → ...
    while not _shutdown:
        # 시장 시작까지 대기 (주말/폐장 후 자동 대기)
        wait_for_market()

        if _shutdown:
            break

        # 거래 세션 실행
        run_session()

        # 세션 종료 후 잠시 대기 (다음 루프에서 wait_for_market이 처리)
        if not _shutdown:
            logger.info("다음 거래일까지 대기...")
            import time
            time.sleep(5)


if __name__ == "__main__":
    main()
