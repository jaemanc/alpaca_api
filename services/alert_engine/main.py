"""
Alert Engine 메인 엔트리포인트

Kafka에서 시세를 소비하고, 등록된 규칙에 따라 ntfy 푸시 알림을 발송한다.

실행: python -m services.alert_engine.main
"""

import sys
import signal
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from services.alert_engine.engine import AlertEngine

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("alert_engine")


def main():
    logger.info("=" * 50)
    logger.info("Alert Engine 시작")
    logger.info("=" * 50)

    engine = AlertEngine()

    # 예시 규칙 등록 (추후 Redis/API에서 로드)
    engine.add_rule({
        "rule_id": "rule-aapl-above-200",
        "symbol": "AAPL",
        "alert_type": "price_above",
        "threshold": 200.0,
        "is_active": True,
    })
    engine.add_rule({
        "rule_id": "rule-tsla-below-400",
        "symbol": "TSLA",
        "alert_type": "price_below",
        "threshold": 400.0,
        "is_active": True,
    })
    engine.add_rule({
        "rule_id": "rule-nvda-above-130",
        "symbol": "NVDA",
        "alert_type": "price_above",
        "threshold": 130.0,
        "is_active": True,
    })

    def shutdown(signum, frame):
        logger.info("종료 중...")
        engine.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Kafka Consumer 루프 시작
    engine.run()


if __name__ == "__main__":
    main()
