"""
공통 설정

환경변수에서 모든 설정값을 로드한다.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Alpaca API
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

# 구독 종목 (쉼표 구분)
WATCH_SYMBOLS = os.getenv("WATCH_SYMBOLS", "AAPL,TSLA,NVDA").split(",")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_QUOTES = "market-quotes"
KAFKA_TOPIC_ORDERBOOK = "order-book"

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# 쿨다운 (초)
COOLDOWN_PRICE_ALERT = 300   # 가격 알림: 5분
COOLDOWN_OB_ALERT = 60       # 호가 알림: 60초

# ntfy 푸시 알림
NTFY_TOPIC_URL = os.getenv("NTFY_TOPIC_URL", "https://ntfy.sh/GirinDev")
NTFY_TITLE = os.getenv("NTFY_TITLE", "Stock Alert")
