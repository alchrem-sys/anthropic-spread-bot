"""Base class and shared types for all exchange adapters."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

STALE_AFTER_S = 3.0
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 30.0
REST_POLL_INTERVAL_S = 0.5
BOOK_DEPTH = 20
WS_IDLE_TIMEOUT_S = 30.0   # reconnect if WS connected but silent this long


@dataclass
class Snapshot:
    bid: Optional[float] = None
    ask: Optional[float] = None
    funding: Optional[float] = None   # per-funding-period rate; None for spot
    oi_usd: Optional[float] = None    # open interest in USD; None for spot
    bids: list[dict] = field(default_factory=list)  # [{"px": float, "sz": float}]
    asks: list[dict] = field(default_factory=list)
    ts: float = 0.0                   # monotonic timestamp of last price update

    @property
    def stale(self) -> bool:
        return (time.monotonic() - self.ts) > STALE_AFTER_S if self.ts else True


def _empty_sym_state() -> dict:
    return {"bid": None, "ask": None, "funding": None, "oi_usd": None,
            "bids": [], "asks": [], "ts": 0.0}


class Exchange:
    """
    One instance per (exchange, market_type) pair.
    Manages a single WS connection; multiple symbols can be subscribed.

    Subclasses must implement:
        _ws_url() -> str
        _subscribe_msgs(symbol) -> list[dict]  (JSON-serializable dicts to send)
        _parse(msg: dict, state: dict) -> None  (mutate state in-place)
        _rest_snapshot(symbol, session) -> dict  ({"bid","ask","bids","asks",...})
    """

    name: str = "base"
    market_type: str = "futures"   # "futures" | "spot"
    log: logging.Logger

    def __init__(self) -> None:
        self._state: dict[str, dict] = {}   # symbol -> sym_state
        self._lock = asyncio.Lock()
        self._symbols: set[str] = set()
        self._ws: Any = None
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        self._session: Optional[aiohttp.ClientSession] = None
        self.log = logging.getLogger(f"ex.{self.name}")

    # ------------------------------------------------------------------ public

    async def start(self, session: aiohttp.ClientSession) -> None:
        if self._tasks:
            return
        self._session = session
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._ws_loop(), name=f"{self.name}_ws"),
            asyncio.create_task(self._rest_loop(), name=f"{self.name}_rest"),
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

    def add_symbol(self, symbol: str) -> None:
        self._symbols.add(symbol)
        if symbol not in self._state:
            self._state[symbol] = _empty_sym_state()

    def snapshot(self, symbol: str) -> Snapshot:
        s = self._state.get(symbol) or _empty_sym_state()
        return Snapshot(
            bid=s["bid"], ask=s["ask"],
            funding=s["funding"], oi_usd=s["oi_usd"],
            bids=list(s["bids"]), asks=list(s["asks"]),
            ts=s["ts"],
        )

    # ---------------------------------------------------------------- WS loop

    async def _ws_loop(self) -> None:
        backoff = RECONNECT_MIN_S
        while not self._stop.is_set():
            try:
                url = self._ws_url()
                self.log.info("ws connecting: %s…", url[:80])
                async with websockets.connect(
                    url,
                    additional_headers=self._ws_headers(),
                    **{"ping_interval": 20, "ping_timeout": 20,
                       **self._ws_connect_kwargs()},
                ) as ws:
                    self._ws = ws
                    self.log.info("ws connected: %s  symbols=%s", self.name, list(self._symbols))
                    for sym in list(self._symbols):
                        for msg in self._subscribe_msgs(sym):
                            await ws.send(__import__("json").dumps(msg))
                    backoff = RECONNECT_MIN_S
                    # Start optional keepalive task (e.g. MEXC needs active pings)
                    keepalive_task = None
                    keepalive_interval = self._ws_keepalive_interval()
                    if keepalive_interval:
                        async def _keepalive(ws=ws, interval=keepalive_interval):
                            try:
                                while True:
                                    await asyncio.sleep(interval)
                                    await self._ws_send_keepalive(ws)
                            except asyncio.CancelledError:
                                pass
                            except Exception:
                                pass
                        keepalive_task = asyncio.create_task(_keepalive())
                    try:
                        while not self._stop.is_set():
                            try:
                                raw = await asyncio.wait_for(
                                    ws.recv(), timeout=WS_IDLE_TIMEOUT_S
                                )
                            except asyncio.TimeoutError:
                                self.log.warning(
                                    "ws idle [%s]: no data for %.0fs, reconnecting",
                                    self.name, WS_IDLE_TIMEOUT_S,
                                )
                                break
                            if self._stop.is_set():
                                break
                            try:
                                msg = __import__("json").loads(raw)
                            except Exception:
                                continue
                            await self._on_ws_msg(ws, msg)
                            sym = self._msg_symbol(msg)
                            if sym and sym in self._state:
                                async with self._lock:
                                    self._parse(msg, self._state[sym])
                            elif sym is None:
                                # broadcast message — try all symbols
                                async with self._lock:
                                    for st in self._state.values():
                                        self._parse(msg, st)
                    finally:
                        if keepalive_task:
                            keepalive_task.cancel()
            except (ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                self.log.warning("ws disconnected [%s]: %s  (retry in %.0fs)",
                                 self.name, e, backoff)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.exception("ws error [%s]: %s", self.name, e)
            finally:
                self._ws = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_S)

    # ---------------------------------------------------------------- REST fallback

    async def _rest_loop(self) -> None:
        # track consecutive REST failures per symbol to avoid log spam
        _fail_counts: dict[str, int] = {}
        while not self._stop.is_set():
            for sym in list(self._symbols):
                s = self._state.get(sym)
                if s is None:
                    continue
                now = time.monotonic()
                if (now - s["ts"]) > STALE_AFTER_S:
                    try:
                        assert self._session is not None
                        data = await self._rest_snapshot(sym, self._session)
                        if data:
                            async with self._lock:
                                s.update(data)
                            _fail_counts[sym] = 0
                        else:
                            _fail_counts[sym] = _fail_counts.get(sym, 0) + 1
                            if _fail_counts[sym] <= 3:
                                self.log.warning("rest empty response [%s/%s]", self.name, sym)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        _fail_counts[sym] = _fail_counts.get(sym, 0) + 1
                        # log first 3 failures verbosely, then every 30th
                        if _fail_counts[sym] <= 3 or _fail_counts[sym] % 30 == 0:
                            self.log.warning("rest error [%s/%s] #%d: %s",
                                             self.name, sym, _fail_counts[sym], e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=REST_POLL_INTERVAL_S)
            except asyncio.TimeoutError:
                pass

    # ---------------------------------------------------------------- override these

    def _ws_url(self) -> str:
        raise NotImplementedError

    def _ws_headers(self) -> dict:
        return {}

    def _ws_connect_kwargs(self) -> dict:
        """Extra kwargs for websockets.connect (overrides defaults)."""
        return {}

    async def _on_ws_msg(self, ws, msg: dict) -> None:
        """Hook called for every incoming WS message. Override for JSON ping/pong etc."""
        pass

    def _ws_keepalive_interval(self) -> Optional[float]:
        """Return seconds between active keepalive pings, or None to disable."""
        return None

    async def _ws_send_keepalive(self, ws) -> None:
        """Called every _ws_keepalive_interval() seconds. Override to send custom ping."""
        pass

    def _subscribe_msgs(self, symbol: str) -> list[dict]:
        raise NotImplementedError

    def _msg_symbol(self, msg: dict) -> Optional[str]:
        """Return the symbol this message belongs to, or None if unknown/broadcast."""
        return None

    def _parse(self, msg: dict, state: dict) -> None:
        raise NotImplementedError

    async def _rest_snapshot(self, symbol: str, session: aiohttp.ClientSession) -> dict:
        raise NotImplementedError

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def _touch(state: dict) -> None:
        state["ts"] = time.monotonic()

    @staticmethod
    def _set_book(state: dict, bids: list[dict], asks: list[dict]) -> None:
        if bids:
            state["bids"] = bids
            state["bid"] = bids[0]["px"]
        if asks:
            state["asks"] = asks
            state["ask"] = asks[0]["px"]
        if bids or asks:
            Exchange._touch(state)

    @staticmethod
    def _parse_levels(raw: list, px_key: str = "px", sz_key: str = "sz") -> list[dict]:
        return [{"px": float(r[px_key]), "sz": float(r[sz_key])} for r in raw if r]

    @staticmethod
    def _parse_levels_list(raw: list) -> list[dict]:
        """For [[px, sz, ...], ...] format."""
        return [{"px": float(r[0]), "sz": float(r[1])} for r in raw if r]
