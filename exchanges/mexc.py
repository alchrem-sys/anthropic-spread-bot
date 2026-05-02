"""MEXC exchange adapters (futures + spot)."""

from __future__ import annotations

import time
from typing import Optional

import aiohttp

from .base import Exchange


class MEXCFutures(Exchange):
    """MEXC USDT-margined perpetual futures."""

    name = "mexc"
    market_type = "futures"

    def _ws_url(self) -> str:
        return "wss://contract.mexc.com/edge"

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{"method": "sub.depth", "param": {"contract": symbol}}]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        data = msg.get("data") or {}
        return data.get("contract") if isinstance(data, dict) else None

    def _parse(self, msg: dict, state: dict) -> None:
        if msg.get("channel") != "push.depth":
            return
        data = msg.get("data") or {}
        if not isinstance(data, dict):
            return
        raw_bids = data.get("bids") or []
        raw_asks = data.get("asks") or []
        # MEXC futures depth: {"p": price, "v": volume, "a": num_orders}
        bids = [{"px": float(b["p"]), "sz": float(b["v"])} for b in raw_bids if isinstance(b, dict)]
        asks = [{"px": float(a["p"]), "sz": float(a["v"])} for a in raw_asks if isinstance(a, dict)]
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            "https://contract.mexc.com/api/v1/contract/depth",
            params={"symbol": symbol, "limit": "20"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        data = (body.get("data") or {}) if isinstance(body, dict) else {}
        raw_bids = data.get("bids") or []
        raw_asks = data.get("asks") or []
        # REST format is [[price, qty], ...]
        bids = self._parse_levels_list(raw_bids)
        asks = self._parse_levels_list(raw_asks)
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
