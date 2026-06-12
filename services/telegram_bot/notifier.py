"""
텔레그램 알림 발송 (동기)

등록된 모든 chat_id로 메시지를 보낸다.
Alert Engine, Data Ingester 등에서 호출한다 (ntfy 대체).
"""

import logging
import requests
from shared.config import TELEGRAM_BOT_TOKEN
from shared.db import get_chat_ids

logger = logging.getLogger(__name__)

_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


def send_message(text: str) -> bool:
    """등록된 모든 chat으로 텍스트 발송"""
    chat_ids = get_chat_ids()
    if not chat_ids:
        logger.warning("등록된 텔레그램 chat이 없습니다. /start 필요.")
        return False

    ok = True
    for chat_id in chat_ids:
        try:
            resp = requests.post(
                f"{_API}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=10,
            )
            if resp.status_code != 200:
                logger.error(f"텔레그램 발송 실패 ({chat_id}): HTTP {resp.status_code}")
                ok = False
        except Exception as e:
            logger.error(f"텔레그램 발송 예외 ({chat_id}): {e}")
            ok = False
    return ok


def send_price_alert(symbol: str, current_price: float, alert_type: str, threshold: float):
    """가격 알림 (Alert Engine에서 호출)"""
    from shared.symbol_names import get_name
    name = get_name(symbol)

    if alert_type == "price_change":
        pct = abs((current_price - threshold) / threshold * 100)
        direction = "급등 📈" if current_price > threshold else "급락 📉"
        text = f"🚨 {name}({symbol})\n${current_price:.2f} — {direction} {pct:.1f}%"
    elif alert_type == "price_above":
        text = f"📈 {name}({symbol})\n${current_price:.2f} — 목표가 ${threshold:.2f} 도달"
    elif alert_type == "price_below":
        text = f"📉 {name}({symbol})\n${current_price:.2f} — 하한가 ${threshold:.2f} 이탈"
    else:
        text = f"🔔 {name}({symbol}) ${current_price:.2f}"

    return send_message(text)
