"""
Kafka Producer

Alpaca에서 수신한 시세 데이터를 Kafka 토픽으로 발행한다.
- 파티션 키: 종목 심볼 (동일 종목 메시지 순서 보장)
- 발행 실패 시 로컬 버퍼에 보관 후 재전송 시도
"""

import logging
from collections import deque
from confluent_kafka import Producer, KafkaError

from shared.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_QUOTES

logger = logging.getLogger(__name__)

# 로컬 버퍼: Kafka 장애 시 메시지 임시 보관 (최대 1000건)
_buffer: deque = deque(maxlen=1000)


def _delivery_callback(err, msg):
    """
    Kafka 메시지 전송 결과 콜백

    발행 성공/실패를 로그로 기록한다.
    실패 시 버퍼에 메시지를 보관한다.
    """
    if err:
        logger.error(f"Kafka 발행 실패: {err} | 토픽: {msg.topic()}")
        # 실패한 메시지를 버퍼에 보관
        _buffer.append((msg.topic(), msg.key(), msg.value()))
    else:
        logger.debug(f"Kafka 발행 성공: {msg.topic()} [{msg.partition()}]")


class KafkaQuoteProducer:
    """
    시세 데이터 Kafka Producer

    confluent-kafka 기반. 종목 심볼을 파티션 키로 사용하여
    동일 종목의 메시지 순서를 보장한다.
    """

    def __init__(self, bootstrap_servers: str = None):
        """
        Producer 초기화

        Args:
            bootstrap_servers: Kafka 브로커 주소 (기본값: config에서 로드)
        """
        servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self._producer = Producer({
            "bootstrap.servers": servers,
            "acks": "all",                    # 모든 복제본 확인 후 성공 처리
            "retries": 3,                     # 자동 재시도 3회
            "retry.backoff.ms": 500,          # 재시도 간격 500ms
            "linger.ms": 5,                   # 배치 전송 대기 (5ms)
            "batch.size": 16384,              # 배치 크기 16KB
            "compression.type": "snappy",     # 압축으로 네트워크 절약
        })
        self._topic = KAFKA_TOPIC_QUOTES
        logger.info(f"KafkaProducer 초기화 완료. 브로커: {servers}")

    def send_quote(self, symbol: str, payload: str):
        """
        Quote 데이터를 Kafka로 발행

        Args:
            symbol: 종목 심볼 (파티션 키로 사용)
            payload: JSON 직렬화된 Quote 데이터
        """
        try:
            self._producer.produce(
                topic=self._topic,
                key=symbol.encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=_delivery_callback,
            )
            # 비동기 전송 큐 처리 (논블로킹)
            self._producer.poll(0)
        except BufferError:
            # Producer 내부 큐가 가득 찬 경우
            logger.warning("Kafka Producer 큐 포화. 버퍼에 보관.")
            _buffer.append((self._topic, symbol.encode("utf-8"), payload.encode("utf-8")))
        except Exception as e:
            logger.error(f"Kafka 발행 예외: {e}")
            _buffer.append((self._topic, symbol.encode("utf-8"), payload.encode("utf-8")))

    def flush(self):
        """미전송 메시지를 모두 전송 완료할 때까지 대기"""
        self._producer.flush()

    def flush_buffer(self):
        """
        로컬 버퍼에 보관된 메시지를 재전송

        Kafka 연결 복구 후 호출하여 버퍼링된 메시지를 순서대로 발행한다.
        """
        count = len(_buffer)
        if count == 0:
            return

        logger.info(f"버퍼 재전송 시작: {count}건")
        sent = 0
        while _buffer:
            topic, key, value = _buffer.popleft()
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    callback=_delivery_callback,
                )
                sent += 1
            except Exception as e:
                logger.error(f"버퍼 재전송 실패: {e}")
                # 실패한 메시지를 다시 앞에 넣음
                _buffer.appendleft((topic, key, value))
                break

        self._producer.flush()
        logger.info(f"버퍼 재전송 완료: {sent}/{count}건")

    @property
    def buffer_size(self) -> int:
        """현재 로컬 버퍼에 보관된 메시지 수"""
        return len(_buffer)
