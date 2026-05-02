"""KuCoin exchange adapters (futures + spot).

KuCoin requires a public token obtained via REST before connecting to WS.
The token is fetched inside the WS loop on each (re)connect.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import aiohttp

from .base import (
    Exchange, RECONNECT_MIN_S, RECONNECT_MAX_S, REST_POLL_INTERVAL_S, STALE_AFTER_S
)

_FAPI = "https://api-futures.kucoin.com"
_SAPI = "https://api.kucoin.com"


async def _get_kucoin_ws_url(session: aiohttp.ClientSession, futures: bool) -> str:
    base = _FAPI if futures else _SAPI
    async with session.post(
        f"{base}/api/v1/bullet-public",
        timeout=aiohttp.ClientTimeout(total=10),
    ) as resp:
        body = await resp.json()
    data = body.get("data") or {}
    token = data.get("token", "")
    servers = data.get("instanceServers") or []
    endpoint = servers[0].get("endpoint") if servers else "wss://ws-api-futures.kucoin.com"
    return f"{endpoint}?token={token}&connectId={int(time.time() * 1000)}"


class KuCoinFutures(Exchange):
    """KuCoin USDT-margined perpetual futures."""

    name = "kucoin"
    market_type = "futures"

    def _ws_url(self) -> str:
        # Not used directly; overridden in _ws_loop
        return ""

    async def _ws_loop(self) -> None:
        """Override to fetch token before each connection attempt."""
        backoff = RECONNECT_MIN_S
        while not self._stop.is_set():
            try:
                assert self._session is not None
                url = await _get_kucoin_ws_url(self._session, futures=True)
                import websockets
                from websockets.exceptions import ConnectionClosed
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20,
                ) as ws:
                    self._ws = ws
                    for sym in list(self._symbols):
                        for msg in self._subscribe_msgs(sym):
                            await ws.send(json.dumps(msg))
                    backoff = RECONNECT_MIN_S
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        sym = self._msg_symbol(msg)
                        if sym and sym in self._state:
                            async with self._lock:
                                self._parse(msg, self._state[sym])
                        elif sym is None:
                            async with self._lock:
                                for st in self._state.values():
                                    self._parse(msg, st)
            except (OSError, asyncio.TimeoutError) as e:
                self.log.warning("ws disconnected: %s", e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.exception("ws error: %s", e)
            finally:
                self._ws = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_S)

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{
            "id": str(int(time.time() * 1000)),
            "type": "subscribe",
            "topic": f"/contractMarket/level2Depth50:{symbol}",
            "response": True,
        }]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        topic: str = msg.get("topic", "")
        if ":" in topic:
            return topic.split(":")[-1]
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        if msg.get("type") != "message":
            return
        data = msg.get("data") or {}
        if not isinstance(data, dict):
            return
        bids = self._parse_levels_list(data.get("bids") or [])
        asks = self._parse_levels_list(data.get("asks") or [])
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            f"{_FAPI}/api/v1/level2/depth100",
            params={"symbol": symbol},
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


class KuCoinSpot(Exchange):
    """KuCoin spot market."""

    name = "kucoin"
    market_type = "spot"

    def _ws_url(self) -> str:
        return ""

    async def _ws_loop(self) -> None:
        backoff = RECONNECT_MIN_S
        while not self._stop.is_set():
            try:
                assert self._session is not None
                url = await _get_kucoin_ws_url(self._session, futures=False)
                import websockets
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20,
                ) as ws:
                    self._ws = ws
                    for sym in list(self._symbols):
                        for msg in self._subscribe_msgs(sym):
                            await ws.send(json.dumps(msg))
                    backoff = RECONNECT_MIN_S
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        sym = self._msg_symbol(msg)
                        if sym and sym in self._state:
                            async with self._lock:
                                self._parse(msg, self._state[sym])
                        elif sym is None:
                            async with self._lock:
                                for st in self._state.values():
                                    self._parse(msg, st)
            except (OSError, asyncio.TimeoutError) as e:
                self.log.warning("ws disconnected: %s", e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.exception("ws error: %s", e)
            finally:
                self._ws = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_S)

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{
            "id": str(int(time.time() * 1000)),
            "type": "subscribe",
            "topic": f"/spotMarket/level2Depth50:{symbol}",
            "response": True,
        }]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        topic: str = msg.get("topic", "")
        if ":" in topic:
            return topic.split(":")[-1]
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        if msg.get("type") != "message":
            return
        data = msg.get("data") or {}
        if not isinstance(data, dict):
            return
        bids = self._parse_levels_list(data.get("bids") or [])
        asks = self._parse_levels_list(data.get("asks") or [])
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.get(
            f"{_SAPI}/api/v3/market/orderbook/level2_100",
            params={"symbol": symbol},
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
