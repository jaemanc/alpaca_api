"""
Data Ingester 메인 엔트리포인트

Alpaca WebSocket → Kafka 파이프라인.
수신된 Quote 데이터를 Kafka market-quotes 토픽으로 발행한다.

실행: python -m services.data_ingester.main
"""

import sys
import signal
import logging
from pathlib import Path

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.config import WATCH_SYMBOLS, KAFKA_BOOTSTRAP_SERVERS
from shared.models import Quote
from shared.kafka_producer import KafkaQuoteProducer
from services.data_ingester.ingester import DataIngester

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


def on_quote(quote: Quote):
    """
    Quote 수신 콜백 — Kafka로 발행

    수신된 시세 데이터를 JSON 직렬화하여 Kafka market-quotes 토픽에 발행한다.
    파티션 키는 종목 심볼이므로 동일 종목의 메시지 순서가 보장된다.
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
    logger.info("Alpaca → Kafka 파이프라인 시작")
    logger.info(f"  종목: {WATCH_SYMBOLS}")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info("=" * 50)

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
