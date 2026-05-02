"""Aster (AsterDEX) exchange adapter — Binance-compatible futures API."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from .base import Exchange

# AsterDEX uses Binance-compatible fstream endpoints
_WS_BASE = "wss://fstream.asterdex.com/stream"
_REST_BASE = "https://fstream.asterdex.com/fapi/v1"


class AsterFutures(Exchange):
    """AsterDEX perpetual futures (Binance-compatible API)."""

    name = "aster"
    market_type = "futures"

    def _ws_url(self) -> str:
        streams = []
        for sym in sorted(self._symbols):
            s = sym.lower()
            streams.extend([f"{s}@depth20@100ms", f"{s}@markPrice@1s"])
        return f"{_WS_BASE}?streams={'/' .join(streams)}" if streams else _WS_BASE

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return []  # subscriptions encoded in URL

    def add_symbol(self, symbol: str) -> None:
        already = symbol in self._symbols
        super().add_symbol(symbol)
        if not already and self._tasks:
            asyncio.ensure_future(self._force_reconnect())

    async def _force_reconnect(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        stream: str = msg.get("stream", "")
        if "@" in stream:
            return stream.split("@")[0].upper()
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        stream: str = msg.get("stream", "")
        data = msg.get("data") or {}
        if "depth" in stream:
            # combined stream depth uses "b"/"a" keys, REST snapshot uses "bids"/"asks"
            bids = self._parse_levels_list(data.get("b") or data.get("bids") or [])
            asks = self._parse_levels_list(data.get("a") or data.get("asks") or [])
            self._set_book(state, bids, asks)
        elif "markPrice" in stream:
            fr = data.get("r")
            if fr is not None:
                try:
                    state["funding"] = float(fr)
                except (TypeError, ValueError):
                    pass

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            f"{_REST_BASE}/depth",
            params={"symbol": symbol, "limit": "20"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            body = await resp.json(content_type=None)
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
