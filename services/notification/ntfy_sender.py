"""
ntfy 푸시 알림 발송 모듈

ntfy.sh를 통해 iOS/Android 기기로 푸시 알림을 보낸다.
HTTP POST 한 번으로 잠금화면 알림이 도착한다.

커스터마이징 가능 항목 (HTTP 헤더로 제어):
- Title: 알림 제목 (앱에서 굵은 글씨로 표시)
- Tags: 이모지 태그 (제목 앞에 아이콘으로 표시)
- Priority: 1(최소) ~ 5(긴급, 소리+진동 반복)
- Click: 알림 클릭 시 열릴 URL
- Icon: 알림 아이콘 이미지 URL (Android만 지원)
"""

import logging
import requests
from shared.config import NTFY_TOPIC_URL, NTFY_TITLE

logger = logging.getLogger(__name__)


def send_push(
    message: str,
    title: str = None,
    tags: str = None,
    priority: int = 3,
    click_url: str = None,
):
    """
    ntfy로 푸시 알림 발송

    Args:
        message: 알림 본문 (잠금화면에 표시되는 내용)
        title: 알림 제목 (기본값: config의 NTFY_TITLE)
        tags: 이모지 태그 (쉼표 구분, 예: "chart_with_upwards_trend,dollar")
              전체 목록: https://docs.ntfy.sh/emojis/
        priority: 우선순위 1~5 (3=기본, 4=높음, 5=긴급)
        click_url: 알림 탭 시 열릴 URL

    Returns:
        bool: 발송 성공 여부
    """
    headers = {
        "Priority": str(priority),
    }

    # 제목은 ASCII가 아닌 경우가 있으므로 별도 처리
    # ntfy는 UTF-8 body를 지원하지만 헤더는 ASCII만 허용
    # 제목에 이모지가 있으면 제거하고 태그로 대체
    safe_title = (title or NTFY_TITLE).encode("ascii", "ignore").decode("ascii").strip()
    if safe_title:
        headers["Title"] = safe_title
    else:
        headers["Title"] = "Stock Alert"

    # 태그 설정 (이모지 아이콘)
    if tags:
        headers["Tags"] = tags

    # 클릭 URL 설정
    if click_url:
        headers["Click"] = click_url

    try:
        resp = requests.post(
            NTFY_TOPIC_URL,
            data=message.encode("utf-8"),
            headers=headers,
            timeout=10,
        )

        if resp.status_code == 200:
            logger.info(f"푸시 발송 성공: {message[:50]}")
            return True
        else:
            logger.error(f"푸시 발송 실패 (HTTP {resp.status_code}): {resp.text}")
            return False

    except requests.exceptions.Timeout:
        logger.error("푸시 발송 타임아웃 (10초)")
        return False
    except Exception as e:
        logger.error(f"푸시 발송 예외: {e}")
        return False


def send_price_alert(symbol: str, current_price: float, alert_type: str, threshold: float):
    """
    가격 알림 발송

    Args:
        symbol: 종목 심볼
        current_price: 현재가
        alert_type: 알림 유형 (price_above, price_below, price_change)
        threshold: 설정된 조건값
    """
    # 알림 유형별 메시지 구성
    if alert_type == "price_above":
        msg = f"{symbol} ${current_price:.2f} — 목표가 ${threshold:.2f} 도달 ↑"
        tags = "chart_with_upwards_trend,moneybag"
        priority = 4
    elif alert_type == "price_below":
        msg = f"{symbol} ${current_price:.2f} — 하한가 ${threshold:.2f} 이탈 ↓"
        tags = "chart_with_downwards_trend,warning"
        priority = 4
    elif alert_type == "price_change":
        change_pct = abs((current_price - threshold) / threshold * 100)
        direction = "급등" if current_price > threshold else "급락"
        msg = f"{symbol} ${current_price:.2f} — {direction} {change_pct:.1f}%"
        tags = "rotating_light,chart_with_upwards_trend" if current_price > threshold else "rotating_light,chart_with_downwards_trend"
        priority = 5
    else:
        msg = f"{symbol} ${current_price:.2f} — 알림 조건 충족"
        tags = "bell"
        priority = 3

    return send_push(message=msg, tags=tags, priority=priority)


def send_orderbook_alert(symbol: str, alert_type: str, details: str):
    """
    호가창 변동 알림 발송

    Args:
        symbol: 종목 심볼
        alert_type: ob_change 또는 ob_imbalance
        details: 상세 내용 문자열
    """
    if alert_type == "ob_imbalance":
        msg = f"{symbol} 수급 불균형 감지\n{details}"
        tags = "scales,warning"
    else:
        msg = f"{symbol} 호가 잔량 급변\n{details}"
        tags = "bar_chart,zap"

    return send_push(message=msg, tags=tags, priority=4)
