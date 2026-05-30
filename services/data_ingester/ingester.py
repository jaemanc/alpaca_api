"""
Data Ingester - Alpaca 실시간 데이터 수신기

Alpaca WebSocket → 내부 Quote 모델 변환 → Kafka 발행

재연결: 지수 백오프 (1초→30초, 최대 5회)
"""

import time
import logging
from typing import Callable, Optional

from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

from shared.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, WATCH_SYMBOLS
from shared.models import Quote

logger = logging.getLogger(__name__)


class DataIngester:
    """
    Alpaca WebSocket 실시간 수신기

    수신된 Quote를 콜백 함수로 전달한다.
    콜백에서 Kafka 발행, 로깅 등 원하는 처리를 수행한다.
    """

    def __init__(self, symbols: list[str] = None, on_quote: Optional[Callable] = None):
        self._symbols = symbols or WATCH_SYMBOLS
        self._on_quote = on_quote
        self._stream: Optional[StockDataStream] = None
        self._reconnect_count = 0

    async def _handle_quote(self, data):
        """Alpaca Quote → 내부 모델 변환 → 콜백 호출"""
        try:
            quote = Quote(
                symbol=data.symbol,
                bid_price=float(data.bid_price or 0),
                ask_price=float(data.ask_price or 0),
                bid_size=int(data.bid_size or 0),
                ask_size=int(data.ask_size or 0),
                timestamp=data.timestamp.isoformat() if data.timestamp else "",
            )
            if self._on_quote:
                self._on_quote(quote)
        except Exception as e:
            logger.error(f"Quote 처리 오류: {e}")

    def start(self):
        """WebSocket 연결 및 스트림 시작 (블로킹)"""
        logger.info(f"수신 시작: {self._symbols}")

        self._stream = StockDataStream(
            ALPACA_API_KEY, ALPACA_SECRET_KEY, feed=DataFeed.IEX
        )
        self._stream.subscribe_quotes(self._handle_quote, *self._symbols)

        try:
            self._stream.run()
        except KeyboardInterrupt:
            logger.info("사용자 중단")
        except Exception as e:
            logger.error(f"스트림 오류: {e}")
            self._reconnect()

    def _reconnect(self):
        """지수 백오프 재연결 (최대 5회)"""
        if self._reconnect_count >= 5:
            logger.critical("재연결 5회 실패. 종료.")
            return

        self._reconnect_count += 1
        wait = min(2 ** (self._reconnect_count - 1), 30)
        logger.warning(f"재연결 {self._reconnect_count}/5 ({wait}초 후)")
        time.sleep(wait)
        self.start()

    def stop(self):
        """연결 해제"""
        if self._stream:
            self._stream.stop()
            logger.info("WebSocket 해제 완료")
