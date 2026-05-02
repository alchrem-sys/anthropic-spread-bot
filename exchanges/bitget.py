"""Bitget exchange adapters (USDT futures + spot)."""

from __future__ import annotations

import time
from typing import Optional

import aiohttp

from .base import Exchange

_WS = "wss://ws.bitget.com/v2/ws/public"
_REST = "https://api.bitget.com/api/v2/mix/market/orderbook"
_REST_SPOT = "https://api.bitget.com/api/v2/spot/market/orderbook"


class BitgetFutures(Exchange):
    """Bitget USDT-margined perpetual futures."""

    name = "bitget"
    market_type = "futures"

    def _ws_url(self) -> str:
        return _WS

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{
            "op": "subscribe",
            "args": [{"instType": "USDT-FUTURES", "channel": "books", "instId": symbol}],
        }]

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
        # Bitget books: {"bids": [[px, sz], ...], "asks": [[px, sz], ...]}
        bids = self._parse_levels_list(data.get("bids") or [])
        asks = self._parse_levels_list(data.get("asks") or [])
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            _REST,
            params={"symbol": symbol, "productType": "usdt-futures", "limit": "20"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        data = (body.get("data") or {}) if isinstance(body, dict) else {}
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


class BitgetSpot(Exchange):
    """Bitget spot market."""

    name = "bitget"
    market_type = "spot"

    def _ws_url(self) -> str:
        return _WS

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{
            "op": "subscribe",
            "args": [{"instType": "SPOT", "channel": "books", "instId": symbol}],
        }]

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
            _REST_SPOT,
            params={"symbol": symbol, "limit": "20"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        data = (body.get("data") or {}) if isinstance(body, dict) else {}
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
