"""Shared price feed for Gate.io futures and Hyperliquid perps.

Both spread_monitor.py and alert_bot.py instantiate a PriceFeed and call
await feed.start(); the feed maintains its own WS connections and REST
fallback in background asyncio tasks.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

GATE_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
GATE_REST_ORDER_BOOK = "https://api.gateio.ws/api/v4/futures/usdt/order_book"
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

STALE_AFTER_S = 3.0
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 30.0
REST_FALLBACK_INTERVAL_S = 0.5
HL_FUNDING_INTERVAL_S = 10.0

log = logging.getLogger("feeds")


class PriceFeed:
    def __init__(
        self,
        symbol_gate: str = "ANTHROPIC_USDT",
        coin_hl: str = "vntl:ANTHROPIC",
        dex_hl: str = "vntl",
    ) -> None:
        self.symbol_gate = symbol_gate
        self.coin_hl = coin_hl
        self.dex_hl = dex_hl
        self._lock = asyncio.Lock()
        self._prices: dict[str, dict[str, Any]] = {
            "gate": {"bid": None, "ask": None, "funding": None, "ts": None},
            "hl": {"bid": None, "ask": None, "funding": None, "oi": None, "mark_px": None,
                   "bids": [], "ts": None},
        }
        self._tasks: list[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._tasks:
            return
        self._session = aiohttp.ClientSession()
        self._tasks = [
            asyncio.create_task(self._run_gate_ws(), name="gate_ws"),
            asyncio.create_task(self._run_hl_ws(), name="hl_ws"),
            asyncio.create_task(self._run_hl_funding_poll(), name="hl_funding"),
            asyncio.create_task(self._run_rest_fallback(), name="rest_fallback"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks = []
        if self._session:
            await self._session.close()
            self._session = None

    def is_stale(self, venue: str) -> bool:
        ts = self._prices[venue]["ts"]
        if ts is None:
            return True
        return (time.monotonic() - ts) > STALE_AFTER_S

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            gate = dict(self._prices["gate"])
            hl = dict(self._prices["hl"])
        snap: dict[str, Any] = {
            "gate": gate,
            "hl": hl,
            "in_spread_usd": None,
            "in_spread_pct": None,
            "out_spread_usd": None,
            "out_spread_pct": None,
            "ts": time.time(),
            "gate_stale": self.is_stale("gate"),
            "hl_stale": self.is_stale("hl"),
        }
        if gate["bid"] is not None and hl["ask"] is not None and hl["ask"] > 0:
            snap["in_spread_usd"] = gate["bid"] - hl["ask"]
            snap["in_spread_pct"] = (gate["bid"] - hl["ask"]) / hl["ask"] * 100.0
        if hl["bid"] is not None and gate["ask"] is not None and gate["ask"] > 0:
            snap["out_spread_usd"] = hl["bid"] - gate["ask"]
            snap["out_spread_pct"] = (hl["bid"] - gate["ask"]) / gate["ask"] * 100.0
        oi = hl.get("oi")
        mark = hl.get("mark_px")
        snap["hl_oi_usd"] = float(oi) * float(mark) if oi is not None and mark is not None else None
        snap["hl_bids"] = list(hl.get("bids") or [])
        return snap

    async def _set(
        self,
        venue: str,
        *,
        bid: Optional[float] = None,
        ask: Optional[float] = None,
        funding: Optional[float] = None,
        oi: Optional[float] = None,
        mark_px: Optional[float] = None,
        bids: Optional[list] = None,
        touch_ts: bool = True,
    ) -> None:
        async with self._lock:
            p = self._prices[venue]
            if bid is not None:
                p["bid"] = bid
            if ask is not None:
                p["ask"] = ask
            if funding is not None:
                p["funding"] = funding
            if oi is not None:
                p["oi"] = oi
            if mark_px is not None:
                p["mark_px"] = mark_px
            if bids is not None:
                p["bids"] = bids
            if touch_ts and (bid is not None or ask is not None):
                p["ts"] = time.monotonic()


    # ------------------------------------------------------------------ Gate

    async def _run_gate_ws(self) -> None:
        backoff = RECONNECT_MIN_S
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    GATE_WS_URL, ping_interval=20, ping_timeout=20
                ) as ws:
                    sub_ob = {
                        "time": int(time.time()),
                        "channel": "futures.order_book",
                        "event": "subscribe",
                        "payload": [self.symbol_gate, "100ms", "1"],
                    }
                    sub_tk = {
                        "time": int(time.time()),
                        "channel": "futures.tickers",
                        "event": "subscribe",
                        "payload": [self.symbol_gate],
                    }
                    await ws.send(json.dumps(sub_ob))
                    await ws.send(json.dumps(sub_tk))
                    backoff = RECONNECT_MIN_S
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        await self._handle_gate_msg(raw)
            except (ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning("gate ws disconnected: %s", e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("gate ws error: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_S)

    async def _handle_gate_msg(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        channel = msg.get("channel")
        event = msg.get("event")
        result = msg.get("result")
        if event in ("subscribe", "unsubscribe", None):
            return
        if channel == "futures.order_book" and isinstance(result, dict):
            asks = result.get("asks") or []
            bids = result.get("bids") or []
            bid = float(bids[0]["p"]) if bids else None
            ask = float(asks[0]["p"]) if asks else None
            if bid is not None or ask is not None:
                await self._set("gate", bid=bid, ask=ask)
        elif channel == "futures.tickers":
            entries = result if isinstance(result, list) else [result]
            for t in entries:
                if not isinstance(t, dict):
                    continue
                if t.get("contract") != self.symbol_gate:
                    continue
                fr = t.get("funding_rate")
                if fr is not None:
                    try:
                        await self._set("gate", funding=float(fr), touch_ts=False)
                    except (TypeError, ValueError):
                        pass

    # -------------------------------------------------------------- Hyperliquid

    async def _run_hl_ws(self) -> None:
        backoff = RECONNECT_MIN_S
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    HL_WS_URL, ping_interval=20, ping_timeout=20
                ) as ws:
                    sub = {
                        "method": "subscribe",
                        "subscription": {"type": "l2Book", "coin": self.coin_hl},
                    }
                    await ws.send(json.dumps(sub))
                    backoff = RECONNECT_MIN_S
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        await self._handle_hl_msg(raw)
            except (ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning("hl ws disconnected: %s", e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("hl ws error: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_S)

    async def _handle_hl_msg(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if msg.get("channel") != "l2Book":
            return
        data = msg.get("data") or {}
        levels = data.get("levels") or []
        if len(levels) < 2:
            return
        raw_bids = levels[0]
        asks = levels[1]
        bid = float(raw_bids[0]["px"]) if raw_bids else None
        ask = float(asks[0]["px"]) if asks else None
        parsed_bids = [{"px": float(b["px"]), "sz": float(b["sz"])} for b in raw_bids]
        if bid is not None or ask is not None:
            await self._set("hl", bid=bid, ask=ask, bids=parsed_bids)

    async def _run_hl_funding_poll(self) -> None:
        while not self._stop.is_set():
            try:
                assert self._session is not None
                payload: dict[str, Any] = {"type": "metaAndAssetCtxs"}
                if self.dex_hl:
                    payload["dex"] = self.dex_hl
                async with self._session.post(
                    HL_INFO_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    body = await resp.json()
                if isinstance(body, list) and len(body) >= 2:
                    meta, ctxs = body[0], body[1]
                    universe = meta.get("universe", []) if isinstance(meta, dict) else []
                    idx = next(
                        (
                            i
                            for i, u in enumerate(universe)
                            if isinstance(u, dict) and u.get("name") == self.coin_hl
                        ),
                        None,
                    )
                    if idx is not None and idx < len(ctxs):
                        ctx = ctxs[idx] if isinstance(ctxs[idx], dict) else {}
                        fr = ctx.get("funding")
                        oi_raw = ctx.get("openInterest")
                        mark_raw = ctx.get("markPx")
                        kw: dict[str, Any] = {"touch_ts": False}
                        try:
                            if fr is not None:
                                kw["funding"] = float(fr)
                            if oi_raw is not None:
                                kw["oi"] = float(oi_raw)
                            if mark_raw is not None:
                                kw["mark_px"] = float(mark_raw)
                            await self._set("hl", **kw)
                        except (TypeError, ValueError):
                            pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("hl funding poll error: %s", e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=HL_FUNDING_INTERVAL_S)
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------- REST fallback

    async def _run_rest_fallback(self) -> None:
        while not self._stop.is_set():
            try:
                if self.is_stale("gate"):
                    await self._poll_gate_rest()
                if self.is_stale("hl"):
                    await self._poll_hl_rest()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("rest fallback error: %s", e)
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=REST_FALLBACK_INTERVAL_S
                )
            except asyncio.TimeoutError:
                pass

    async def _poll_gate_rest(self) -> None:
        assert self._session is not None
        params = {"contract": self.symbol_gate, "limit": "1"}
        async with self._session.get(
            GATE_REST_ORDER_BOOK,
            params=params,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        if not isinstance(body, dict):
            return
        asks = body.get("asks") or []
        bids = body.get("bids") or []
        # Gate REST returns {"p": "...", "s": ...}
        bid = float(bids[0]["p"]) if bids else None
        ask = float(asks[0]["p"]) if asks else None
        if bid is not None or ask is not None:
            await self._set("gate", bid=bid, ask=ask)

    async def _poll_hl_rest(self) -> None:
        assert self._session is not None
        async with self._session.post(
            HL_INFO_URL,
            json={"type": "l2Book", "coin": self.coin_hl},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            body = await resp.json()
        if not isinstance(body, dict):
            return
        levels = body.get("levels") or []
        if len(levels) < 2:
            return
        raw_bids = levels[0]
        asks = levels[1]
        bid = float(raw_bids[0]["px"]) if raw_bids else None
        ask = float(asks[0]["px"]) if asks else None
        parsed_bids = [{"px": float(b["px"]), "sz": float(b["sz"])} for b in raw_bids]
        if bid is not None or ask is not None:
            await self._set("hl", bid=bid, ask=ask, bids=parsed_bids)

    # ----------------------------------------------------------------- depth util

    @staticmethod
    def bid_depth_within_pct(bids: list[dict], range_pct: float) -> float:
        """Sum of bid sizes where price >= best_bid * (1 - range_pct/100)."""
        if not bids:
            return 0.0
        best = bids[0]["px"]
        cutoff = best * (1.0 - range_pct / 100.0)
        return sum(b["sz"] for b in bids if b["px"] >= cutoff)

    # ----------------------------------------------------------------- smoke test

    async def _smoke_test(self, seconds: float = 8.0) -> None:
        logging.basicConfig(level=logging.INFO)
        await self.start()
        try:
            for _ in range(int(seconds)):
                await asyncio.sleep(1)
                snap = await self.snapshot()
                print(
                    f"gate bid={snap['gate']['bid']} ask={snap['gate']['ask']} "
                    f"fund={snap['gate']['funding']} stale={snap['gate_stale']} | "
                    f"hl bid={snap['hl']['bid']} ask={snap['hl']['ask']} "
                    f"fund={snap['hl']['funding']} oi_usd={snap['hl_oi_usd']} stale={snap['hl_stale']} | "
                    f"in={snap['in_spread_pct']} out={snap['out_spread_pct']}"
                )
        finally:
            await self.stop()


if __name__ == "__main__":
    asyncio.run(PriceFeed()._smoke_test())
