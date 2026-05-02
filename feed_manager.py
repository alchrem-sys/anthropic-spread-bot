"""FeedManager — owns all Exchange instances and exposes per-spread snapshots.

Usage:
    mgr = FeedManager()
    session = aiohttp.ClientSession()
    await mgr.start(session)

    info_a = parse_url("https://www.gate.io/futures/USDT/ANTHROPIC_USDT")
    info_b = parse_url("https://app.hyperliquid.xyz/trade/vntl:ANTHROPIC")
    await mgr.subscribe(info_a)
    await mgr.subscribe(info_b)

    snap = mgr.spread_snapshot(info_a, info_b)
    # snap.in_spread_pct, snap.out_spread_pct, ...
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

from exchanges import (
    Exchange, Snapshot, ExchangeInfo,
    HyperliquidFutures,
    GateFutures, GateSpot,
    BinanceFutures, BinanceSpot,
    BybitLinear, BybitSpot,
    OKXFutures, OKXSpot,
    MEXCFutures, MEXCSpot,
    KuCoinFutures, KuCoinSpot,
    BitgetFutures, BitgetSpot,
    AsterFutures,
)

# DEX prefix → dex parameter for Hyperliquid
_HL_DEX_MAP: dict[str, str] = {
    "vntl:": "vntl",
    # add more builder DEXes here as needed
}


@dataclass
class SpreadSnapshot:
    """Computed spread between two exchange legs."""
    leg_a: Snapshot
    leg_b: Snapshot
    # IN  = enter by longing leg_b / shorting leg_a: favourable when leg_a.bid > leg_b.ask
    in_spread_usd: Optional[float] = None
    in_spread_pct: Optional[float] = None
    # OUT = exit by longing leg_a / shorting leg_b: favourable when leg_b.bid > leg_a.ask
    out_spread_usd: Optional[float] = None
    out_spread_pct: Optional[float] = None
    ts: float = field(default_factory=time.time)

    @property
    def stale(self) -> bool:
        return self.leg_a.stale or self.leg_b.stale

    # ---------------------------------------------------------------- depth helpers

    @staticmethod
    def side_depth(
        levels: list[dict],
        reference_price: float,
        range_pct: float,
        side: str,          # "bid" or "ask"
    ) -> float:
        """Coins within range_pct% of the exit spread zone.

        For bid-side (selling): px >= reference * (1 - range_pct/100)
        For ask-side (buying):  px <= reference / (1 - range_pct/100)
        """
        if not levels or reference_price <= 0:
            return 0.0
        factor = 1.0 - range_pct / 100.0
        if side == "bid":
            cutoff = reference_price * factor
            return sum(lvl["sz"] for lvl in levels if lvl["px"] >= cutoff)
        else:
            cutoff = reference_price / factor
            return sum(lvl["sz"] for lvl in levels if lvl["px"] <= cutoff)

    def out_depth_a(self, range_pct: float) -> float:
        """Leg-A ask-side coins in OUT exit zone (buying leg_a to close short)."""
        return self.side_depth(self.leg_a.asks, self.leg_b.bid or 0.0, range_pct, "ask")

    def out_depth_b(self, range_pct: float) -> float:
        """Leg-B bid-side coins in OUT exit zone (selling leg_b to close long)."""
        return self.side_depth(self.leg_b.bids, self.leg_a.ask or 0.0, range_pct, "bid")


def _make_exchange(exchange: str, market_type: str, symbol: str) -> Exchange:
    """Instantiate the correct Exchange subclass."""
    key = (exchange, market_type)
    if key == ("hyperliquid", "futures"):
        # Derive DEX from symbol prefix
        dex = ""
        for prefix, dex_name in _HL_DEX_MAP.items():
            if symbol.startswith(prefix):
                dex = dex_name
                break
        return HyperliquidFutures(dex=dex)
    if key == ("gate", "futures"):
        return GateFutures()
    if key == ("gate", "spot"):
        return GateSpot()
    if key == ("binance", "futures"):
        return BinanceFutures()
    if key == ("binance", "spot"):
        return BinanceSpot()
    if key == ("bybit", "futures"):
        return BybitLinear()
    if key == ("bybit", "spot"):
        return BybitSpot()
    if key == ("okx", "futures"):
        return OKXFutures()
    if key == ("okx", "spot"):
        return OKXSpot()
    if key == ("mexc", "futures"):
        return MEXCFutures()
    if key == ("mexc", "spot"):
        return MEXCSpot()
    if key == ("kucoin", "futures"):
        return KuCoinFutures()
    if key == ("kucoin", "spot"):
        return KuCoinSpot()
    if key == ("bitget", "futures"):
        return BitgetFutures()
    if key == ("bitget", "spot"):
        return BitgetSpot()
    if key == ("aster", "futures"):
        return AsterFutures()
    raise ValueError(f"Unknown exchange/market_type: {exchange}/{market_type}")


class FeedManager:
    """Manages all active Exchange instances. One instance per (exchange, market_type)."""

    def __init__(self) -> None:
        # (exchange, market_type) → Exchange instance
        self._exchanges: dict[tuple[str, str], Exchange] = {}
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def stop(self) -> None:
        for ex in self._exchanges.values():
            await ex.stop()
        self._exchanges.clear()

    async def subscribe(self, info: ExchangeInfo) -> None:
        """Subscribe to an exchange+symbol. Starts the exchange if not yet running."""
        key = (info.exchange, info.market_type)
        if key not in self._exchanges:
            self._exchanges[key] = _make_exchange(info.exchange, info.market_type, info.symbol)

        ex = self._exchanges[key]
        ex.add_symbol(info.symbol)

        if not ex._tasks and self._session is not None:
            await ex.start(self._session)

    def snapshot(self, info: ExchangeInfo) -> Snapshot:
        """Return the latest Snapshot for a single leg."""
        key = (info.exchange, info.market_type)
        ex = self._exchanges.get(key)
        if ex is None:
            return Snapshot()
        return ex.snapshot(info.symbol)

    def spread_snapshot(self, leg_a: ExchangeInfo, leg_b: ExchangeInfo) -> SpreadSnapshot:
        """Compute and return a SpreadSnapshot for the two legs."""
        snap_a = self.snapshot(leg_a)
        snap_b = self.snapshot(leg_b)

        ss = SpreadSnapshot(leg_a=snap_a, leg_b=snap_b)

        # IN spread: leg_a.bid − leg_b.ask  (short A / long B)
        if snap_a.bid is not None and snap_b.ask is not None and snap_b.ask > 0:
            ss.in_spread_usd = snap_a.bid - snap_b.ask
            ss.in_spread_pct = ss.in_spread_usd / snap_b.ask * 100.0

        # OUT spread: leg_b.bid − leg_a.ask  (close: sell B / buy A)
        if snap_b.bid is not None and snap_a.ask is not None and snap_a.ask > 0:
            ss.out_spread_usd = snap_b.bid - snap_a.ask
            ss.out_spread_pct = ss.out_spread_usd / snap_a.ask * 100.0

        return ss
