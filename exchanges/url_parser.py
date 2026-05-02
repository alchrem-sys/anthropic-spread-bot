"""Parse exchange trade URLs into (exchange_name, symbol, market_type)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse


@dataclass
class ExchangeInfo:
    exchange: str       # "hyperliquid" | "gate" | "binance" | "bybit" | "okx" |
                        # "mexc" | "kucoin" | "bitget" | "aster"
    symbol: str         # normalised symbol as the exchange API expects it
    market_type: str    # "futures" | "spot"
    display: str        # human-readable  e.g. "Hyperliquid · vntl:ANTHROPIC"


def parse_url(url: str) -> Optional[ExchangeInfo]:
    """Return ExchangeInfo for a recognised trade URL, or None."""
    url = url.strip().rstrip("/")
    p = urlparse(url)
    host = p.netloc.lower().lstrip("www.")
    path = p.path

    for fn in _PARSERS:
        result = fn(host, path, url)
        if result:
            return result
    return None


# ──────────────────────────────────────────── individual parsers

def _hyperliquid(host: str, path: str, _url: str) -> Optional[ExchangeInfo]:
    # https://app.hyperliquid.xyz/trade/vntl:ANTHROPIC
    if "hyperliquid.xyz" not in host:
        return None
    m = re.search(r"/trade/([^/?#]+)", path)
    if not m:
        return None
    sym = m.group(1)
    return ExchangeInfo("hyperliquid", sym, "futures",
                        f"Hyperliquid · {sym}")


def _gate(host: str, path: str, _url: str) -> Optional[ExchangeInfo]:
    # futures: https://www.gate.io/futures/USDT/ANTHROPIC_USDT
    #          https://futures.gate.io/trade/ANTHROPIC_USDT
    # spot:    https://www.gate.io/trade/ANTHROPIC_USDT
    if "gate.io" not in host and "gateio" not in host:
        return None
    if "futures" in host or "futures" in path:
        m = re.search(r"/(?:futures/USDT/|trade/)([A-Z0-9_]+)", path, re.I)
        sym = m.group(1).upper() if m else None
        if not sym:
            return None
        return ExchangeInfo("gate", sym, "futures", f"Gate.io Futures · {sym}")
    else:
        m = re.search(r"/trade/([A-Z0-9_]+)", path, re.I)
        sym = m.group(1).upper() if m else None
        if not sym:
            return None
        return ExchangeInfo("gate", sym, "spot", f"Gate.io Spot · {sym}")


def _binance(host: str, path: str, _url: str) -> Optional[ExchangeInfo]:
    # futures: https://www.binance.com/en/futures/ANTHROPICUSDT
    # spot:    https://www.binance.com/en/trade/ANTHROPIC_USDT
    if "binance.com" not in host:
        return None
    if "futures" in path or "delivery" in path:
        m = re.search(r"/futures/([A-Z0-9]+)", path, re.I)
        sym = m.group(1).upper() if m else None
        if not sym:
            return None
        return ExchangeInfo("binance", sym, "futures", f"Binance Futures · {sym}")
    else:
        m = re.search(r"/trade/([A-Z0-9_]+)", path, re.I)
        raw = m.group(1).upper().replace("_", "") if m else None
        if not raw:
            return None
        return ExchangeInfo("binance", raw, "spot", f"Binance Spot · {raw}")


def _bybit(host: str, path: str, _url: str) -> Optional[ExchangeInfo]:
    # linear perp: https://www.bybit.com/trade/usdt/ANTHROPICUSDT
    # spot:         https://www.bybit.com/en/trade/spot/ANTHROPIC/USDT
    if "bybit.com" not in host:
        return None
    if "/trade/usdt/" in path.lower() or "/trade/usdc/" in path.lower():
        m = re.search(r"/trade/(?:usdt|usdc)/([A-Z0-9]+)", path, re.I)
        sym = m.group(1).upper() if m else None
        if not sym:
            return None
        return ExchangeInfo("bybit", sym, "futures", f"Bybit Perp · {sym}")
    elif "/trade/spot/" in path.lower():
        m = re.search(r"/trade/spot/([A-Z0-9]+)/([A-Z0-9]+)", path, re.I)
        sym = (m.group(1) + m.group(2)).upper() if m else None
        if not sym:
            return None
        return ExchangeInfo("bybit", sym, "spot", f"Bybit Spot · {sym}")
    return None


def _okx(host: str, path: str, _url: str) -> Optional[ExchangeInfo]:
    # swap/perp: https://www.okx.com/trade-swap/anthropic-usdt-swap
    # futures:   https://www.okx.com/trade-futures/anthropic-usdt-220930
    # spot:      https://www.okx.com/trade-spot/anthropic-usdt
    if "okx.com" not in host:
        return None
    m = re.search(r"/trade-(?:swap|futures|spot)/([a-z0-9-]+)", path, re.I)
    if not m:
        return None
    raw = m.group(1).upper()          # e.g. ANTHROPIC-USDT-SWAP
    if "swap" in path.lower() or "futures" in path.lower():
        # ensure -SWAP suffix for perpetuals
        inst_id = raw if raw.endswith("-SWAP") else raw + "-SWAP"
        return ExchangeInfo("okx", inst_id, "futures", f"OKX Perp · {inst_id}")
    else:
        return ExchangeInfo("okx", raw, "spot", f"OKX Spot · {raw}")


def _mexc(host: str, path: str, _url: str) -> Optional[ExchangeInfo]:
    # futures: https://futures.mexc.com/trade/ANTHROPIC_USDT
    # spot:    https://www.mexc.com/exchange/ANTHROPIC_USDT
    if "mexc.com" not in host:
        return None
    m = re.search(r"/(?:trade|exchange)/([A-Z0-9_]+)", path, re.I)
    sym = m.group(1).upper() if m else None
    if not sym:
        return None
    if "futures" in host:
        return ExchangeInfo("mexc", sym, "futures", f"MEXC Futures · {sym}")
    else:
        return ExchangeInfo("mexc", sym, "spot", f"MEXC Spot · {sym}")


def _kucoin(host: str, path: str, _url: str) -> Optional[ExchangeInfo]:
    # futures: https://www.kucoin.com/futures/trade/ANTHROPICUSDTM
    # spot:    https://www.kucoin.com/trade/ANTHROPIC-USDT
    if "kucoin.com" not in host:
        return None
    if "futures" in host or "futures" in path:
        m = re.search(r"/trade/([A-Z0-9]+)", path, re.I)
        sym = m.group(1).upper() if m else None
        if not sym:
            return None
        # KuCoin futures symbols end in M (XBTUSDTM), add if missing
        if not sym.endswith("M"):
            sym = sym + "M"
        return ExchangeInfo("kucoin", sym, "futures", f"KuCoin Futures · {sym}")
    else:
        m = re.search(r"/trade/([A-Z0-9-]+)", path, re.I)
        sym = m.group(1).upper() if m else None
        if not sym:
            return None
        return ExchangeInfo("kucoin", sym, "spot", f"KuCoin Spot · {sym}")


def _bitget(host: str, path: str, _url: str) -> Optional[ExchangeInfo]:
    # futures: https://www.bitget.com/futures/usdt/ANTHROPICUSDT
    # spot:    https://www.bitget.com/spot/ANTHROPICUSDT
    if "bitget.com" not in host:
        return None
    if "futures" in path:
        m = re.search(r"/futures/(?:usdt|usdc|coin)/([A-Z0-9]+)", path, re.I)
        sym = m.group(1).upper() if m else None
        if not sym:
            return None
        return ExchangeInfo("bitget", sym, "futures", f"Bitget Futures · {sym}")
    else:
        m = re.search(r"/spot/([A-Z0-9]+)", path, re.I)
        sym = m.group(1).upper() if m else None
        if not sym:
            return None
        return ExchangeInfo("bitget", sym, "spot", f"Bitget Spot · {sym}")


def _aster(host: str, path: str, _url: str) -> Optional[ExchangeInfo]:
    # https://asterdex.com/futures/ANTHROPICUSDT
    # https://www.asterdex.com/ru/trade/pro/futures/UBUSDT  ← localized path
    if "aster" not in host:
        return None
    # Prioritise /futures/SYMBOL — must match before /trade/SYMBOL to avoid
    # catching path segments like "pro" in /trade/pro/futures/UBUSDT
    m = re.search(r"/futures/([A-Z0-9_]+)", path, re.I)
    if not m:
        # Fall back: /trade/SYMBOL only when SYMBOL is the last path segment
        m = re.search(r"/trade/([A-Z0-9_]+)$", path, re.I)
    sym = m.group(1).upper() if m else None
    if not sym:
        return None
    return ExchangeInfo("aster", sym, "futures", f"Aster · {sym}")


_PARSERS = [
    _hyperliquid, _gate, _binance, _bybit,
    _okx, _mexc, _kucoin, _bitget, _aster,
]
