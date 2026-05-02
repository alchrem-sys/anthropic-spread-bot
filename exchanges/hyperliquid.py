"""Hyperliquid exchange adapter (perps only)."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from .base import Exchange, BOOK_DEPTH

HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
HL_INFO_URL = "https://api.hyperliquid.xyz/info"
HL_FUNDING_INTERVAL_S = 10.0


class HyperliquidFutures(Exchange):
    """Hyperliquid perpetual futures (supports VNTL builder DEX and main universe)."""

    name = "hyperliquid"
    market_type = "futures"

    def __init__(self, dex: str = "") -> None:
        super().__init__()
        self._dex = dex  # "vntl" for VNTL DEX, "" for main universe

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

    def _ws_connect_kwargs(self) -> dict:
        # HL doesn't support WS-level ping frames; it uses JSON {"method":"ping"}
        return {"ping_interval": None, "ping_timeout": None}

    async def _on_ws_msg(self, ws, msg: dict) -> None:
        if msg.get("method") == "ping":
            import json as _json
            await ws.send(_json.dumps({"method": "pong"}))

    def _ws_url(self) -> str:
        return HL_WS_URL

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        return [{"method": "subscribe", "subscription": {"type": "l2Book", "coin": symbol}}]

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        if msg.get("channel") == "l2Book":
            return (msg.get("data") or {}).get("coin")
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        if msg.get("channel") != "l2Book":
            return
        data = msg.get("data") or {}
        levels = data.get("levels") or []
        if len(levels) < 2:
            return
        bids = self._parse_levels(levels[0])
        asks = self._parse_levels(levels[1])
        self._set_book(state, bids, asks)

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        async with session.post(
            HL_INFO_URL,
            json={"type": "l2Book", "coin": symbol},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        if not isinstance(body, dict):
            return {}
        levels = body.get("levels") or []
        if len(levels) < 2:
            return {}
        bids = self._parse_levels(levels[0])
        asks = self._parse_levels(levels[1])
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
            try:
                assert self._session is not None
                payload: dict = {"type": "metaAndAssetCtxs"}
                if self._dex:
                    payload["dex"] = self._dex
                async with self._session.post(
                    HL_INFO_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    body = await resp.json()
                if isinstance(body, list) and len(body) >= 2:
                    meta, ctxs = body[0], body[1]
                    universe = meta.get("universe", []) if isinstance(meta, dict) else []
                    for sym, state in list(self._state.items()):
                        idx = next(
                            (i for i, u in enumerate(universe)
                             if isinstance(u, dict) and u.get("name") == sym),
                            None,
                        )
                        if idx is not None and idx < len(ctxs):
                            ctx = ctxs[idx] if isinstance(ctxs[idx], dict) else {}
                            async with self._lock:
                                try:
                                    fr = ctx.get("funding")
                                    oi_raw = ctx.get("openInterest")
                                    mark_raw = ctx.get("markPx")
                                    if fr is not None:
                                        state["funding"] = float(fr)
                                    if oi_raw is not None and mark_raw is not None:
                                        state["oi_usd"] = float(oi_raw) * float(mark_raw)
                                except (TypeError, ValueError):
                                    pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.warning("funding poll error: %s", e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=HL_FUNDING_INTERVAL_S)
            except asyncio.TimeoutError:
                pass
