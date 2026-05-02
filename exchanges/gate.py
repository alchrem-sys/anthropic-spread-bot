"""Gate.io exchange adapters (futures USDT-margined + spot)."""

from __future__ import annotations

import time
from typing import Optional

import aiohttp

from .base import Exchange


class GateFutures(Exchange):
    """Gate.io USDT-margined perpetual futures."""

    name = "gate"
    market_type = "futures"

    def _ws_url(self) -> str:
        return "wss://fx-ws.gateio.ws/v4/ws/usdt"

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        t = int(time.time())
        return [
            {
                "time": t, "channel": "futures.order_book",
                "event": "subscribe", "payload": [symbol, "100ms", "20"],
            },
            {
                "time": t, "channel": "futures.tickers",
                "event": "subscribe", "payload": [symbol],
            },
        ]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        result = msg.get("result")
        if isinstance(result, dict):
            return result.get("contract")
        if isinstance(result, list) and result and isinstance(result[0], dict):
            return result[0].get("contract")
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        channel = msg.get("channel")
        if msg.get("event") in ("subscribe", "unsubscribe"):
            return
        result = msg.get("result")
        if channel == "futures.order_book" and isinstance(result, dict):
            raw_bids = result.get("bids") or []
            raw_asks = result.get("asks") or []
            bids = [{"px": float(b["p"]), "sz": float(b["s"])} for b in raw_bids]
            asks = [{"px": float(a["p"]), "sz": float(a["s"])} for a in raw_asks]
            self._set_book(state, bids, asks)
        elif channel == "futures.tickers":
            entries = result if isinstance(result, list) else ([result] if isinstance(result, dict) else [])
            for t in entries:
                if not isinstance(t, dict):
                    continue
                fr = t.get("funding_rate")
                if fr is not None:
                    try:
                        state["funding"] = float(fr)
                    except (TypeError, ValueError):
                        pass

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            "https://api.gateio.ws/api/v4/futures/usdt/order_book",
            params={"contract": symbol, "limit": "20"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        if not isinstance(body, dict):
            return {}
        raw_bids = body.get("bids") or []
        raw_asks = body.get("asks") or []
        bids = [{"px": float(b["p"]), "sz": float(b["s"])} for b in raw_bids]
        asks = [{"px": float(a["p"]), "sz": float(a["s"])} for a in raw_asks]
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


class GateSpot(Exchange):
    """Gate.io spot market."""

    name = "gate"
    market_type = "spot"

    def _ws_url(self) -> str:
        return "wss://api.gateio.ws/ws/v4/"

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        t = int(time.time())
        return [
            {
                "time": t, "channel": "spot.order_book",
                "event": "subscribe", "payload": [symbol, "20", "100ms"],
            },
        ]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        result = msg.get("result")
        if isinstance(result, dict):
            # Gate spot uses "s" (currency_pair) field
            return result.get("s") or result.get("currency_pair")
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        if msg.get("channel") != "spot.order_book":
            return
        if msg.get("event") in ("subscribe", "unsubscribe"):
            return
        result = msg.get("result")
        if not isinstance(result, dict):
            return
        raw_bids = result.get("bids") or []
        raw_asks = result.get("asks") or []
        bids = self._parse_levels_list(raw_bids)
        asks = self._parse_levels_list(raw_asks)
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            "https://api.gateio.ws/api/v4/spot/order_book",
            params={"currency_pair": symbol, "limit": "20"},
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
