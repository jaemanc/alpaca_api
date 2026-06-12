"""SQLite 전일 종가 저장소 — 매일 갱신, 이력 없이 최신 1행만 유지"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "prev_close.db")


def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prev_close (
            symbol TEXT PRIMARY KEY,
            close_price REAL NOT NULL,
            updated_date TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def save_prev_close(data: dict[str, float], date_str: str):
    """전일 종가 일괄 저장 (UPSERT — 기존 값 덮어쓰기)"""
    conn = _connect()
    conn.executemany(
        "INSERT OR REPLACE INTO prev_close (symbol, close_price, updated_date) VALUES (?, ?, ?)",
        [(sym, price, date_str) for sym, price in data.items()]
    )
    conn.commit()
    conn.close()


def load_prev_close() -> dict[str, float]:
    """저장된 전일 종가 전체 로드"""
    conn = _connect()
    rows = conn.execute("SELECT symbol, close_price FROM prev_close").fetchall()
    conn.close()
    return {sym: price for sym, price in rows}


def get_updated_date() -> str | None:
    """마지막 갱신 날짜 반환"""
    conn = _connect()
    row = conn.execute("SELECT updated_date FROM prev_close LIMIT 1").fetchone()
    conn.close()
    return row[0] if row else None


# --- 텔레그램 chat_id 저장 ---

def _ensure_chat_table(conn: sqlite3.Connection):
    conn.execute("CREATE TABLE IF NOT EXISTS telegram_chats (chat_id TEXT PRIMARY KEY)")
    conn.commit()


def save_chat_id(chat_id: str):
    """텔레그램 chat_id 등록 (중복 무시)"""
    conn = _connect()
    _ensure_chat_table(conn)
    conn.execute("INSERT OR IGNORE INTO telegram_chats (chat_id) VALUES (?)", (str(chat_id),))
    conn.commit()
    conn.close()


def get_chat_ids() -> list[str]:
    """등록된 모든 chat_id 반환"""
    conn = _connect()
    _ensure_chat_table(conn)
    rows = conn.execute("SELECT chat_id FROM telegram_chats").fetchall()
    conn.close()
    return [r[0] for r in rows]
