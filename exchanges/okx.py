"""OKX exchange adapters (swap/perp + spot)."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from .base import Exchange

_OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
_OKX_REST = "https://www.okx.com/api/v5/market/books"
_OKX_FUND = "https://www.okx.com/api/v5/public/funding-rate"


def _okx_inst_id(symbol: str, futures: bool) -> str:
    """Normalize user symbol → OKX instId.
    'LABUSDT' → 'LAB-USDT-SWAP' (futures) or 'LAB-USDT' (spot).
    Already-formatted symbols (contain '-') are returned as-is.
    """
    if "-" in symbol:
        return symbol
    for quote in ("USDT", "USDC", "BTC", "ETH", "USD"):
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return f"{base}-{quote}-SWAP" if futures else f"{base}-{quote}"
    return symbol


class OKXFutures(Exchange):
    """OKX perpetual swap futures (instId ends with -SWAP)."""

    name = "okx"
    market_type = "futures"

    def add_symbol(self, symbol: str) -> None:
        normalized = _okx_inst_id(symbol, futures=True)
        self._sym_aliases[symbol] = normalized
        super().add_symbol(normalized)

    async def start(self, session: aiohttp.ClientSession) -> None:
        if self._tasks:
            return
        self._session = session
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._ws_loop(), name=f"{self.name}_ws"),
            asyncio.create_task(self._rest_loop(), name=f"{self.name}_rest"),
            asyncio.create_task(self._funding_loop(), name=f"{self.name}_funding"),
        ]

    def _ws_url(self) -> str:
        return _OKX_WS

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{"op": "subscribe", "args": [{"channel": "books", "instId": symbol}]}]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        arg = msg.get("arg") or {}
        return arg.get("instId")

    def _parse(self, msg: dict, state: dict) -> None:
        data_list = msg.get("data") or []
        if not data_list:
            return
        data = data_list[0] if isinstance(data_list, list) else data_list
        if not isinstance(data, dict):
            return
        bids = self._parse_levels_list(data.get("bids") or [])
        asks = self._parse_levels_list(data.get("asks") or [])
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            _OKX_REST,
            params={"instId": symbol, "sz": "20"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        data_list = (body.get("data") or []) if isinstance(body, dict) else []
        if not data_list:
            return {}
        data = data_list[0]
        bids = self._parse_levels_list(data.get("bids") or [])
        asks = self._parse_levels_list(data.get("asks") or [])
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

    async def _funding_loop(self) -> None:
        while not self._stop.is_set():
            for sym, state in list(self._state.items()):
                try:
                    assert self._session is not None
                    async with self._session.get(
                        _OKX_FUND,
                        params={"instId": sym},
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as resp:
                        body = await resp.json()
                    data_list = (body.get("data") or []) if isinstance(body, dict) else []
                    if data_list:
                        fr = data_list[0].get("fundingRate")
                        if fr is not None:
                            async with self._lock:
                                try:
                                    state["funding"] = float(fr)
                                except (TypeError, ValueError):
                                    pass
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.log.debug("okx funding error for %s: %s", sym, e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                pass


class OKXSpot(Exchange):
    """OKX spot market."""

    name = "okx"
    market_type = "spot"

    def add_symbol(self, symbol: str) -> None:
        normalized = _okx_inst_id(symbol, futures=False)
        self._sym_aliases[symbol] = normalized
        super().add_symbol(normalized)

    def _ws_url(self) -> str:
        return _OKX_WS

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{"op": "subscribe", "args": [{"channel": "books", "instId": symbol}]}]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        arg = msg.get("arg") or {}
        return arg.get("instId")

    def _parse(self, msg: dict, state: dict) -> None:
        data_list = msg.get("data") or []
        if not data_list:
            return
        data = data_list[0] if isinstance(data_list, list) else data_list
        if not isinstance(data, dict):
            return
        bids = self._parse_levels_list(data.get("bids") or [])
        asks = self._parse_levels_list(data.get("asks") or [])
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            _OKX_REST,
            params={"instId": symbol, "sz": "20"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        data_list = (body.get("data") or []) if isinstance(body, dict) else []
        if not data_list:
            return {}
        data = data_list[0]
        bids = self._parse_levels_list(data.get("bids") or [])
        asks = self._parse_levels_list(data.get("asks") or [])
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
