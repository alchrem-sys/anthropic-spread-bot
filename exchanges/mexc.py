"""MEXC exchange adapters (futures + spot)."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import aiohttp

from .base import Exchange


def _mexc_futures_symbol(symbol: str) -> str:
    """Normalize to MEXC futures contract format: BARUSDT → BAR_USDT.
    Already-formatted symbols (contain '_') are returned as-is.
    """
    if "_" in symbol:
        return symbol
    for quote in ("USDT", "USDC", "BTC", "ETH", "USD"):
        if symbol.endswith(quote):
            base = symbol[: -len(quote)]
            return f"{base}_{quote}"
    return symbol


_MEXC_TICKER_URL = "https://futures.mexc.com/api/v1/contract/ticker"
_MEXC_TICKER_POLL_S = 1.0


class MEXCFutures(Exchange):
    """MEXC USDT-margined perpetual futures.

    Uses REST ticker polling instead of WS because contract.mexc.com
    WS is geo-blocked from many hosting providers. The /ticker endpoint
    on futures.mexc.com returns all contracts in one request and is not blocked.
    """

    name = "mexc"
    market_type = "futures"

    def add_symbol(self, symbol: str) -> None:
        super().add_symbol(_mexc_futures_symbol(symbol))

    # ── Override start(): no WS, only REST ticker polling ──

    async def start(self, session: aiohttp.ClientSession) -> None:
        if self._tasks:
            return
        self._session = session
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._ticker_loop(), name=f"{self.name}_futures_ticker"),
        ]

    async def _ticker_loop(self) -> None:
        """Poll futures.mexc.com/api/v1/contract/ticker every ~1s."""
        fail_count = 0
        while not self._stop.is_set():
            try:
                assert self._session is not None
                async with self._session.get(
                    _MEXC_TICKER_URL,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}")
                    body = await resp.json(content_type=None)

                if not isinstance(body, dict) or not body.get("success"):
                    raise RuntimeError(f"bad response: {str(body)[:100]}")

                tickers = body.get("data") or []
                now = time.monotonic()
                async with self._lock:
                    for t in tickers:
                        if not isinstance(t, dict):
                            continue
                        sym = t.get("symbol")
                        if sym not in self._state:
                            continue
                        s = self._state[sym]
                        bid = t.get("bid1")
                        ask = t.get("ask1")
                        fr = t.get("fundingRate")
                        hold = t.get("holdVol")
                        fair = t.get("fairPrice")
                        if bid is not None:
                            s["bid"] = float(bid)
                            s["bids"] = [{"px": float(bid), "sz": 0}]
                        if ask is not None:
                            s["ask"] = float(ask)
                            s["asks"] = [{"px": float(ask), "sz": 0}]
                        if bid is not None or ask is not None:
                            s["ts"] = now
                        if fr is not None:
                            try:
                                s["funding"] = float(fr)
                            except (TypeError, ValueError):
                                pass
                        if hold is not None and fair is not None:
                            try:
                                s["oi_usd"] = float(hold) * float(fair)
                            except (TypeError, ValueError):
                                pass
                fail_count = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                fail_count += 1
                if fail_count <= 3 or fail_count % 60 == 0:
                    self.log.warning("mexc ticker error #%d: %s", fail_count, e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_MEXC_TICKER_POLL_S)
            except asyncio.TimeoutError:
                pass

    # Unused but required by base class
    def _ws_url(self) -> str:
        return ""

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return []

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        pass

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        return {}  # handled by _ticker_loop


class MEXCSpot(Exchange):
    """MEXC spot market."""

    name = "mexc"
    market_type = "spot"

    def _ws_url(self) -> str:
        return "wss://wbs.mexc.com/ws"

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        # MEXC spot WS uses subscription topic format
        return [{"method": "SUBSCRIPTION", "params": [f"spot@public.limit.depth.v3.api@{symbol}@20"]}]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        # Topic is like "spot@public.limit.depth.v3.api@BTCUSDT@20"
        topic: str = msg.get("c", "")
        if "@" in topic:
            parts = topic.split("@")
            if len(parts) >= 3:
                return parts[2]
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        data = msg.get("d") or {}
        if not isinstance(data, dict):
            return
        raw_bids = data.get("bids") or []
        raw_asks = data.get("asks") or []
        # MEXC spot: [{"p": price, "v": qty}, ...]
        bids = [{"px": float(b["p"]), "sz": float(b["v"])} for b in raw_bids if isinstance(b, dict)]
        asks = [{"px": float(a["p"]), "sz": float(a["v"])} for a in raw_asks if isinstance(a, dict)]
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            "https://api.mexc.com/api/v3/depth",
            params={"symbol": symbol, "limit": "20"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        if not isinstance(body, dict):
            return {}
        bids = self._parse_levels_list(body.get("bids") or [])
        asks = self._parse_levels_list(body.get("asks") or [])
        result: dict = {}
        if bids:
            result["bids"] = bids
            result["bid"] = bids[0]["px"]
        if asks:
            result["asks"] = asks
            result["ask"] = asks[0]["px"]
        if result:
            result["ts"] = time.monotonic()
        return result
