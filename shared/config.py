"""공통 설정 — 환경변수에서 로드"""

import os
import ssl
import urllib3
import requests
from dotenv import load_dotenv

load_dotenv()

# SSL 검증 우회 (회사 프록시 환경)
os.environ["PYTHONHTTPSVERIFY"] = "0"
os.environ["CURL_CA_BUNDLE"] = ""
os.environ["REQUESTS_CA_BUNDLE"] = ""
ssl._create_default_https_context = ssl._create_unverified_context
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
_orig_request = requests.Session.request
def _no_verify(self, *a, **kw):
    kw.setdefault("verify", False)
    return _orig_request(self, *a, **kw)
requests.Session.request = _no_verify

# Alpaca API
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

# 구독 종목
_env_symbols = os.getenv("WATCH_SYMBOLS", "")
if _env_symbols:
    WATCH_SYMBOLS = _env_symbols.split(",")
else:
    from shared.symbols import ALL_SYMBOLS
    WATCH_SYMBOLS = ALL_SYMBOLS

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_QUOTES = "market-quotes"

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# 쿨다운 (초)
COOLDOWN_PRICE_ALERT = 300    # 규칙별: 5분
COOLDOWN_SYMBOL = 3600        # 종목별: 1시간

# 레이트 리밋
RATE_LIMIT_PER_MINUTE = 5
DAILY_ALERT_LIMIT = 100

# ntfy
NTFY_TOPIC_URL = os.getenv("NTFY_TOPIC_URL", "https://ntfy.sh/GirinDev")
NTFY_TITLE = os.getenv("NTFY_TITLE", "Stock Alert")
