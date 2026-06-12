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
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from shared.config import TELEGRAM_BOT_TOKEN, WATCH_SYMBOLS
from shared.db import save_chat_id
from shared.symbol_names import get_name
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
    cached = get_cached_quote(symbol)

    if not cached:
        await update.message.reply_text(f"{symbol} — 캐시된 시세가 없습니다 (장 시간에 다시 시도).")
        return

    q = Quote.from_json(cached)
    await update.message.reply_text(
        f"{get_name(symbol)}({symbol})\n"
        f"매수 ${q.bid_price:.2f} / 매도 ${q.ask_price:.2f}"
    )


def main():
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN 미설정")
        sys.exit(1)

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("price", cmd_price))
    app.add_handler(CallbackQueryHandler(on_button))

    logger.info("텔레그램 봇 시작 (polling)")
    app.run_polling()


if __name__ == "__main__":
    main()
