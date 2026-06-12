"""
3일치 시계열 변동률 그래프 생성

Redis 시계열 데이터로 matplotlib 차트를 그려 PNG 바이트로 반환한다.
"""

import io
import logging
from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # GUI 없는 환경용 백엔드
import matplotlib.pyplot as plt

from shared.redis_client import get_timeseries

logger = logging.getLogger(__name__)


def build_chart(symbol: str) -> bytes | None:
    """
    종목의 3일치 시간별 변동률 그래프 PNG 생성.

    변동률 = (각 시점 가격 - 첫 시점 가격) / 첫 시점 가격 * 100

    Returns:
        PNG 바이트. 데이터 없으면 None.
    """
    series = get_timeseries(symbol)
    if len(series) < 2:
        return None

    base = series[0][1]
    if base <= 0:
        return None

    times = [datetime.fromtimestamp(e) for e, _ in series]
    pct = [((p - base) / base) * 100 for _, p in series]

    fig, ax = plt.subplots(figsize=(8, 4))
    color = "#e74c3c" if pct[-1] < 0 else "#2ecc71"
    ax.plot(times, pct, color=color, linewidth=1.5)
    ax.axhline(0, color="#999", linewidth=0.8, linestyle="--")
    ax.fill_between(times, pct, 0, alpha=0.15, color=color)

    ax.set_title(f"{symbol} — 3-Day Change ({pct[-1]:+.2f}%)")
    ax.set_ylabel("Change (%)")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=80)
    plt.close(fig)
    buf.seek(0)
    return buf.read()
