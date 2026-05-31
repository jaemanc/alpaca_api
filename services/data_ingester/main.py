"""
Data Ingester 메인 엔트리포인트

Alpaca WebSocket → Kafka 파이프라인.
시작 시 주요 종목 전일 종가를 ntfy로 발송하여 연결 상태를 확인한다.

실행: python -m services.data_ingester.main
"""

import sys
import signal
import logging
from pathlib import Path
from datetime import datetime

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.config import WATCH_SYMBOLS, KAFKA_BOOTSTRAP_SERVERS, ALPACA_API_KEY, ALPACA_SECRET_KEY
from shared.models import Quote
from shared.kafka_producer import KafkaQuoteProducer
from services.data_ingester.ingester import DataIngester
from services.notification.ntfy_sender import send_push

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("data_ingester")

# Kafka Producer 초기화
producer = KafkaQuoteProducer()
quote_count = 0


def send_startup_notification():
    """
    시작 알림 발송

    주요 종목의 최신 호가(전일 종가)를 조회하여 ntfy로 발송한다.
    시스템이 정상 연결되었는지 확인하는 용도.
    """
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockLatestQuoteRequest

        client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

        # 상위 5종목만 조회 (시작 알림용)
        top_symbols = WATCH_SYMBOLS[:5]
        request = StockLatestQuoteRequest(symbol_or_symbols=top_symbols)
        quotes = client.get_stock_latest_quote(request)

        # 메시지 구성
        now = datetime.now().strftime("%m/%d %H:%M")
        lines = [f"System started at {now}", f"Watching {len(WATCH_SYMBOLS)} symbols", ""]

        for sym, q in quotes.items():
            bid = q.bid_price if q.bid_price else 0
            ask = q.ask_price if q.ask_price else 0
            mid = (bid + ask) / 2 if ask > 0 else bid
            lines.append(f"{sym}: ${mid:.2f}")

        lines.append(f"\nKafka: {KAFKA_BOOTSTRAP_SERVERS}")

        message = "\n".join(lines)
        send_push(
            message=message,
            title="Stock Alert - System Online",
            tags="white_check_mark,satellite",
            priority=2,  # 낮은 우선순위 (시작 알림이므로)
        )
        logger.info("시작 알림 발송 완료")

    except Exception as e:
        logger.warning(f"시작 알림 발송 실패 (무시): {e}")


def on_quote(quote: Quote):
    """
    Quote 수신 콜백 — Kafka로 발행
    """
    global quote_count
    quote_count += 1

    # Kafka로 발행
    producer.send_quote(quote.symbol, quote.to_json())

    # 50건마다 로그 출력
    if quote_count % 50 == 1:
        logger.info(
            f"[{quote.symbol}] ${quote.bid_price:.2f}/{quote.ask_price:.2f} "
            f"(총 {quote_count}건, 버퍼: {producer.buffer_size})"
        )


def main():
    logger.info("=" * 50)
    logger.info("Alpaca -> Kafka pipeline starting")
    logger.info(f"  Symbols: {len(WATCH_SYMBOLS)} stocks")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info("=" * 50)

    # 시작 알림 발송 (전일 종가 포함)
    send_startup_notification()

    ingester = DataIngester(symbols=WATCH_SYMBOLS, on_quote=on_quote)

    def shutdown(signum, frame):
        logger.info("종료 중...")
        ingester.stop()
        producer.flush()
        logger.info(f"완료. 총 {quote_count}건 처리.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    ingester.start()


if __name__ == "__main__":
    main()
