"""ntfy 푸시 알림 발송"""

import logging
import requests
from shared.config import NTFY_TOPIC_URL, NTFY_TITLE

logger = logging.getLogger(__name__)


def send_push(message: str, title: str = None, tags: str = None, priority: int = 3):
    """ntfy로 푸시 발송. 성공 시 True."""
    headers = {"Priority": str(priority)}

    safe_title = (title or NTFY_TITLE).encode("ascii", "ignore").decode("ascii").strip() or "Stock Alert"
    headers["Title"] = safe_title

    if tags:
        headers["Tags"] = tags

    try:
        resp = requests.post(NTFY_TOPIC_URL, data=message.encode("utf-8"), headers=headers, timeout=10)
        if resp.status_code == 200:
            logger.info(f"푸시 발송 성공: {message[:50]}")
            return True
        logger.error(f"푸시 실패 (HTTP {resp.status_code})")
        return False
    except Exception as e:
        logger.error(f"푸시 예외: {e}")
        return False


def send_price_alert(symbol: str, current_price: float, alert_type: str, threshold: float):
    """가격 알림 발송"""
    from shared.symbol_names import get_name
    name = get_name(symbol)

    if alert_type == "price_above":
        msg = f"{name}({symbol}) ${current_price:.2f} — 목표가 ${threshold:.2f} 도달 ↑"
        tags, priority = "chart_with_upwards_trend,moneybag", 4
    elif alert_type == "price_below":
        msg = f"{name}({symbol}) ${current_price:.2f} — 하한가 ${threshold:.2f} 이탈 ↓"
        tags, priority = "chart_with_downwards_trend,warning", 4
    elif alert_type == "price_change":
        pct = abs((current_price - threshold) / threshold * 100)
        direction = "급등" if current_price > threshold else "급락"
        msg = f"{name}({symbol}) ${current_price:.2f} — {direction} {pct:.1f}%"
        tags = "rotating_light,chart_with_upwards_trend" if current_price > threshold else "rotating_light,chart_with_downwards_trend"
        priority = 5
    else:
        msg = f"{name}({symbol}) ${current_price:.2f}"
        tags, priority = "bell", 3

    return send_push(msg, tags=tags, priority=priority)
