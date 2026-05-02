"""Binance exchange adapters (USDM futures + spot)."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from .base import Exchange

_FAPI = "https://fstream.binance.com/fapi/v1"
_API = "https://api.binance.com/api/v3"


class BinanceFutures(Exchange):
    """Binance USDM perpetual futures."""

    name = "binance"
    market_type = "futures"

    def _ws_url(self) -> str:
        streams = []
        for sym in sorted(self._symbols):
            s = sym.lower()
            streams.extend([f"{s}@depth20@100ms", f"{s}@markPrice@1s"])
        base = "wss://fstream.binance.com/stream"
        return f"{base}?streams={'/' .join(streams)}" if streams else base

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return []  # subscriptions encoded in URL; reconnect adds new symbols

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
            bids = self._parse_levels_list(data.get("bids") or [])
            asks = self._parse_levels_list(data.get("asks") or [])
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
            f"{_FAPI}/depth",
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


class BinanceSpot(Exchange):
    """Binance spot market."""

    name = "binance"
    market_type = "spot"

    def _ws_url(self) -> str:
        streams = [f"{s.lower()}@depth20@100ms" for s in sorted(self._symbols)]
        base = "wss://stream.binance.com:9443/stream"
        return f"{base}?streams={'/' .join(streams)}" if streams else base

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return []

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
        data = msg.get("data") or {}
        bids = self._parse_levels_list(data.get("bids") or [])
        asks = self._parse_levels_list(data.get("asks") or [])
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            f"{_API}/depth",
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
