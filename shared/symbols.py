"""
구독 종목 리스트 (총 300개)

나스닥 상장 종목 중심으로 3개 카테고리로 구성:
1. 거래량/시총 상위 100개 (대형주, 유동성 높음)
2. 최근 인기/모멘텀 100개 (소셜미디어, 뉴스 빈도 높음)
3. 미래 성장 잠재력 100개 (AI, 반도체, 바이오, 클린에너지 등)

중복 제거 후 300개로 맞춤.
"""

# ============================================================
# 카테고리 1: 거래량/시가총액 상위 100 (나스닥 대형주)
# ============================================================
TOP_VOLUME = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG", "META", "TSLA", "AVGO", "COST",
    "NFLX", "AMD", "ADBE", "PEP", "CSCO", "TMUS", "INTC", "INTU", "CMCSA", "QCOM",
    "TXN", "AMGN", "AMAT", "ISRG", "HON", "BKNG", "LRCX", "VRTX", "MU", "ADI",
    "REGN", "KLAC", "PANW", "SNPS", "MDLZ", "CDNS", "MELI", "ASML", "CRWD", "ABNB",
    "FTNT", "MAR", "ORLY", "CTAS", "DASH", "WDAY", "MRVL", "ADSK", "CSX", "CHTR",
    "PCAR", "DXCM", "MNST", "NXPI", "CPRT", "ROP", "PAYX", "AEP", "MCHP", "ODFL",
    "FAST", "KDP", "ROST", "LULU", "KHC", "EA", "VRSK", "CTSH", "GEHC", "EXC",
    "IDXX", "CSGP", "BKR", "FANG", "DDOG", "ON", "ANSS", "TEAM", "ZS", "GFS",
    "CDW", "TTWO", "BIIB", "ILMN", "WBD", "DLTR", "XEL", "WBA", "SIRI", "LCID",
    "RIVN", "PARA", "PYPL", "COIN", "HOOD", "MARA", "RIOT", "SOFI", "PLTR", "ARM",
]

# ============================================================
# 카테고리 2: 최근 인기/모멘텀 종목 100 (2024-2025 화제)
# ============================================================
TRENDING = [
    "SMCI", "IONQ", "RGTI", "QUBT", "QBTS", "SOUN", "BBAI", "RKLB", "LUNR", "ASTS",
    "APLD", "CORZ", "CLSK", "CIFR", "BTBT", "HUT", "BITF", "WULF", "IREN", "MSTR",
    "MDB", "SNOW", "NET", "DKNG", "ROKU", "PINS", "SNAP", "SPOT", "TTD", "RBLX",
    "U", "DUOL", "HIMS", "CELH", "MNDY", "GLBE", "TOST", "BROS", "CAVA", "BIRK",
    "APP", "RDDT", "CART", "IBKR", "AFRM", "UPST", "SQ", "NU", "GRAB", "SE",
    "SHOP", "MELI", "BABA", "JD", "PDD", "BIDU", "NIO", "XPEV", "LI", "ZM",
    "DOCU", "OKTA", "TWLO", "BILL", "PCOR", "CFLT", "ESTC", "GTLB", "PATH", "AI",
    "BIGC", "FIVN", "ASAN", "DOCN", "DLO", "PAYO", "RELY", "FLYW", "BRZE", "CWAN",
    "TMDX", "AXON", "TW", "FOUR", "PAYC", "WIX", "CYBR", "TENB", "QLYS", "RPD",
    "VRNS", "SAIL", "S", "CRDO", "SMMT", "RXRX", "DNLI", "BEAM", "NTLA", "CRSP",
]

# ============================================================
# 카테고리 3: 미래 성장 잠재력 100 (AI, 반도체, 바이오, 에너지)
# ============================================================
GROWTH_POTENTIAL = [
    # AI/ML 인프라
    "DELL", "HPE", "PSTG", "NTAP", "WEKA", "CIEN", "ANET", "LITE", "COHR", "II",
    # 반도체/칩
    "MPWR", "ALGM", "WOLF", "ACLS", "RMBS", "CRUS", "SLAB", "DIOD", "POWI", "SITM",
    # 로보틱스/자동화
    "TER", "ISRG", "NOVT", "BRKS", "CGNX", "OUST", "AEVA", "LAZR", "INVZ", "LIDR",
    # 바이오테크/유전자
    "MRNA", "BNTX", "SGEN", "ALNY", "BMRN", "RARE", "IONS", "SRPT", "EXAS", "TWST",
    # 클린에너지/EV
    "ENPH", "SEDG", "FSLR", "RUN", "NOVA", "ARRY", "STEM", "CHPT", "EVGO", "BLNK",
    # 우주/방산
    "KTOS", "RKLB", "MNTS", "SPCE", "ASTR", "RDW", "BWXT", "LDOS", "CACI", "MRCY",
    # 핀테크
    "FIS", "FISV", "GPN", "NDAQ", "VRSN", "LPLA", "MKTX", "CBOE", "CME", "ICE",
    # 사이버보안
    "FTNT", "PANW", "CRWD", "ZS", "CYBR", "TENB", "QLYS", "RPD", "VRNS", "SAIL",
    # 디지털헬스
    "VEEV", "DOCS", "HCAT", "GDRX", "OSCR", "ACCD", "TALK", "AMWL", "TDOC", "CERT",
    # 메타버스/게임
    "RBLX", "U", "TTWO", "EA", "ATVI", "ZNGA", "PLTK", "SKLZ", "DNUT", "MTTR",
    # 추가 성장주 (300개 맞춤)
    "UBER", "LYFT", "ABNB", "DASH", "DDOG", "DATADOG", "ZI", "CFLT", "SUMO", "NEWR",
    "DT", "ESTC", "MDB", "SNOW", "SPLK", "NOW", "CRM",
    "WDAY", "VEEV", "ZEN", "HUBS", "PCTY", "PAYC", "MANH",
    "AZPN", "APPF", "ALTR",
]

# ============================================================
# 중복 제거 후 최종 300개 리스트
# ============================================================
def get_all_symbols() -> list[str]:
    """중복 제거된 전체 종목 리스트 반환 (최대 300개)"""
    seen = set()
    result = []
    for sym in TOP_VOLUME + TRENDING + GROWTH_POTENTIAL:
        if sym not in seen:
            seen.add(sym)
            result.append(sym)
        if len(result) >= 300:
            break
    return result


# 전체 종목 리스트
ALL_SYMBOLS = get_all_symbols()

# 카테고리별 개수 확인용
if __name__ == "__main__":
    all_syms = get_all_symbols()
    print(f"총 종목 수: {len(all_syms)}")
    print(f"처음 10개: {all_syms[:10]}")
    print(f"마지막 10개: {all_syms[-10:]}")
