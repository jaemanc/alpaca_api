"""
텔레그램 봇 — 조회 제어

명령어:
  /start         — chat 등록 (알림 수신 시작)
  /list          — 감시 종목을 버튼으로 표시 → 클릭 시 3일 그래프
  /price SYMBOL  — 특정 종목 현재가 + 변동률

실행: python -m services.telegram_bot.bot
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

from shared.config import TELEGRAM_BOT_TOKEN, WATCH_SYMBOLS
from shared.db import save_chat_id
from shared.symbol_names import get_name, find_symbol
from shared.redis_client import get_cached_quote
from shared.models import Quote
from services.telegram_bot.chart import build_chart

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("telegram_bot")


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """chat 등록"""
    chat_id = update.effective_chat.id
    save_chat_id(chat_id)
    await update.message.reply_text(
        "✅ 등록 완료! 이제 알림을 받습니다.\n\n"
        "/list — 종목 리스트 (클릭하면 3일 그래프)\n"
        "/price SYMBOL — 특정 종목 조회 (예: /price AAPL)"
    )


async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """감시 종목을 인라인 버튼으로 표시 (한 줄에 3개)"""
    buttons = []
    row = []
    for i, sym in enumerate(WATCH_SYMBOLS, 1):
        row.append(InlineKeyboardButton(get_name(sym), callback_data=f"chart:{sym}"))
        if i % 3 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    await update.message.reply_text(
        "종목을 선택하면 3일치 변동률 그래프를 보여드립니다:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """버튼 클릭 → 3일 그래프 전송"""
    query = update.callback_query
    await query.answer()

    if not query.data.startswith("chart:"):
        return

    symbol = query.data.split(":", 1)[1]
    png = build_chart(symbol)

    if png is None:
        await query.message.reply_text(f"{get_name(symbol)}({symbol}) — 아직 그래프를 그릴 데이터가 부족합니다.")
        return

    await ctx.bot.send_photo(
        chat_id=query.message.chat_id,
        photo=png,
        caption=f"{get_name(symbol)}({symbol}) 3일 변동률",
    )


async def cmd_price(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """특정 종목 현재가 + 변동률"""
    if not ctx.args:
        await update.message.reply_text("사용법: /price AAPL")
        return

    symbol = ctx.args[0].upper()
    await _reply_price(update, symbol)


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    자연어 메시지 처리

    "애플 시세 알려줘", "테슬라 얼마야", "NVDA" 등을 인식한다.
    """
    text = update.message.text
    symbol = find_symbol(text)

    if symbol:
        # 그래프 데이터가 있으면 그래프도 전송
        png = build_chart(symbol)
        if png:
            await ctx.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=png,
                caption=f"{get_name(symbol)}({symbol}) 3일 변동률",
            )
        else:
            await _reply_price(update, symbol)
    else:
        await update.message.reply_text(
            "종목을 찾지 못했습니다.\n"
            "예: 애플 시세, 테슬라 얼마야, NVDA\n"
            "/list 로 종목 리스트를 볼 수 있습니다."
        )


async def _reply_price(update: Update, symbol: str):
    """종목 시세 응답 (캐시 또는 Snapshot API)"""
    cached = get_cached_quote(symbol)
    if cached:
        q = Quote.from_json(cached)
        await update.message.reply_text(
            f"{get_name(symbol)}({symbol})\n매수 ${q.bid_price:.2f} / 매도 ${q.ask_price:.2f}"
        )
    else:
        # 캐시 없으면 Alpaca Snapshot에서 직접 조회
        try:
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.data.requests import StockSnapshotRequest
            from shared.config import ALPACA_API_KEY, ALPACA_SECRET_KEY

            client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
            snaps = client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=[symbol], feed="iex"))
            snap = snaps.get(symbol)
            if snap and snap.latest_quote:
                price = float(snap.latest_quote.bid_price or 0)
                prev = float(snap.previous_daily_bar.close) if snap.previous_daily_bar else 0
                if prev > 0:
                    pct = ((price - prev) / prev) * 100
                    arrow = "▲" if pct >= 0 else "▼"
                    await update.message.reply_text(
                        f"{get_name(symbol)}({symbol})\n${price:.2f} {arrow}{abs(pct):.1f}% (전일 대비)"
                    )
                else:
                    await update.message.reply_text(f"{get_name(symbol)}({symbol})\n${price:.2f}")
            else:
                await update.message.reply_text(f"{symbol} — 데이터를 가져올 수 없습니다.")
        except Exception as e:
            await update.message.reply_text(f"{symbol} — 조회 실패: {e}")


def main():
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN 미설정")
        sys.exit(1)

    # Python 3.12+ 에서 메인 스레드 이벤트 루프 보장
    import asyncio
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("price", cmd_price))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    logger.info("텔레그램 봇 시작 (polling)")
    app.run_polling()


if __name__ == "__main__":
    main()
