"""종목 심볼 → 한글 이름 매핑"""

NAMES = {
    "AAPL": "애플",
    "MSFT": "마소",
    "NVDA": "엔비디아",
    "AMZN": "아마존",
    "GOOGL": "구글",
    "META": "메타",
    "TSLA": "테슬라",
    "AVGO": "브로드컴",
    "NFLX": "넷플릭스",
    "AMD": "AMD",
    "ADBE": "어도비",
    "QCOM": "퀄컴",
    "INTC": "인텔",
    "MU": "마이크론",
    "COIN": "코인베이스",
    "PLTR": "팔란티어",
    "SOFI": "소파이",
    "ARM": "ARM",
    "SMCI": "슈퍼마이크로",
    "IONQ": "아이온큐",
    "MARA": "마라홀딩스",
    "RIOT": "라이엇",
    "HOOD": "로빈후드",
    "RKLB": "로켓랩",
    "SOUN": "사운드하운드",
    "MSTR": "마이크로스트래티지",
    "SNOW": "스노우플레이크",
    "NET": "클라우드플레어",
    "DKNG": "드래프트킹스",
    "SQ": "블록(스퀘어)",
}


def get_name(symbol: str) -> str:
    """심볼의 한글 이름 반환 (없으면 심볼 그대로)"""
    return NAMES.get(symbol, symbol)


# 역방향 매핑 (한글→심볼)
_REVERSE = {v.lower(): k for k, v in NAMES.items()}
# 심볼 자체도 매핑에 포함 (대소문자 무관)
_REVERSE.update({k.lower(): k for k in NAMES.keys()})


def find_symbol(text: str) -> str | None:
    """
    텍스트에서 종목을 찾는다.

    "애플 시세 알려줘" → "AAPL"
    "TSLA 얼마야" → "TSLA"
    못 찾으면 None.
    """
    text_lower = text.lower().strip()
    # 정확히 심볼이거나 한글명인 단어를 찾음
    for key, sym in _REVERSE.items():
        if key in text_lower:
            return sym
    return None
