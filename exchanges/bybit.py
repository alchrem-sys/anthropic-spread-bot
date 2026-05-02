"""Bybit exchange adapters (linear perp + spot)."""

from __future__ import annotations

import time
from typing import Optional

import aiohttp

from .base import Exchange

_BASE = "https://api.bybit.com/v5/market"


def _bybit_rest(category: str):
    async def _snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            f"{_BASE}/orderbook",
            params={"category": category, "symbol": symbol, "limit": "50"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        data = (body.get("result") or {}) if isinstance(body, dict) else {}
        bids = self._parse_levels_list(data.get("b") or [])
        asks = self._parse_levels_list(data.get("a") or [])
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
    return _snapshot


class BybitLinear(Exchange):
    """Bybit linear (USDT/USDC) perpetual futures."""

    name = "bybit"
    market_type = "futures"

    def _ws_url(self) -> str:
        return "wss://stream.bybit.com/v5/public/linear"

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{"op": "subscribe", "args": [f"orderbook.50.{symbol}", f"tickers.{symbol}"]}]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        topic: str = msg.get("topic", "")
        if "." in topic:
            return topic.rsplit(".", 1)[-1]
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        topic: str = msg.get("topic", "")
        data = msg.get("data") or {}
        if topic.startswith("orderbook"):
            bids = self._parse_levels_list(data.get("b") or [])
            asks = self._parse_levels_list(data.get("a") or [])
            self._set_book(state, bids, asks)
        elif topic.startswith("tickers"):
            fr = data.get("fundingRate")
            if fr is not None:
                try:
                    state["funding"] = float(fr)
                except (TypeError, ValueError):
                    pass

    _rest_snapshot = _bybit_rest("linear")  # type: ignore[assignment]


class BybitSpot(Exchange):
    """Bybit spot market."""

    name = "bybit"
    market_type = "spot"

    def _ws_url(self) -> str:
        return "wss://stream.bybit.com/v5/public/spot"

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{"op": "subscribe", "args": [f"orderbook.50.{symbol}"]}]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        topic: str = msg.get("topic", "")
        if "." in topic:
            return topic.rsplit(".", 1)[-1]
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        data = msg.get("data") or {}
        bids = self._parse_levels_list(data.get("b") or [])
        asks = self._parse_levels_list(data.get("a") or [])
        self._set_book(state, bids, asks)

    _rest_snapshot = _bybit_rest("spot")  # type: ignore[assignment]
