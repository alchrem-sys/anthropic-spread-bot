"""Multi-spread terminal UI.

Reads the active spread from Redis key `monitor:active_spread` (set by the
Telegram bot's "📺 Terminal" button), then subscribes feeds and renders live.

Run:
    python spread_monitor.py [--spread <spread_id>] [--in-threshold 0.3] [--out-threshold 0.3]

If --spread is omitted the monitor polls Redis for `monitor:active_spread`.
Hotkeys: q quit | i set IN threshold | o set OUT threshold | s switch spread
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import signal
import sys
import termios
import time
import tty
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from redis.asyncio import from_url as redis_from_url
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from exchanges import ExchangeInfo, parse_url
from feed_manager import FeedManager, SpreadSnapshot

load_dotenv()

REFRESH_HZ = 5
ALERT_TICK_S = 0.2
REALERT_S = 1.0
HYSTERESIS = 0.8
BEEP_INTERVAL_S = 0.3


@dataclass
class AlertState:
    active: bool = False
    last_fired: float = 0.0
    count: int = 0
    last_ts: str = "--"


@dataclass
class MonitorState:
    in_threshold: float
    out_threshold: float
    in_alert: AlertState = field(default_factory=AlertState)
    out_alert: AlertState = field(default_factory=AlertState)
    log_path: str = "alerts.log"
    msgs: list[str] = field(default_factory=list)
    # active spread
    leg_a: Optional[ExchangeInfo] = None
    leg_b: Optional[ExchangeInfo] = None


# ─────────────────────────────────────────── rendering

def _fp(v: Optional[float], w: int = 9) -> str:
    return f"${v:,.4f}".rjust(w) if v is not None else "--".rjust(w)


def _pct(v: Optional[float]) -> str:
    if v is None:
        return "  --    "
    return f"{v:+.3f}%"


def _fund(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    return f"{v*100:+.4f}%"


def render(ss: SpreadSnapshot, state: MonitorState) -> Panel:
    a, b = ss.leg_a, ss.leg_b
    la = state.leg_a
    lb = state.leg_b

    t = Table.grid(padding=(0, 1), expand=True)
    t.add_column(ratio=1)

    a_label = la.display if la else "Leg A"
    b_label = lb.display if lb else "Leg B"

    a_status = Text("STALE", style="bold yellow") if a.stale else Text("LIVE", style="bold green")
    b_status = Text("STALE", style="bold yellow") if b.stale else Text("LIVE", style="bold green")

    row_a = Text.assemble(
        (f"{a_label:<28}", "bold cyan"),
        f"bid {_fp(a.bid)}  ask {_fp(a.ask)}  fund {_fund(a.funding)}  ",
        a_status,
    )
    row_b = Text.assemble(
        (f"{b_label:<28}", "bold magenta"),
        f"bid {_fp(b.bid)}  ask {_fp(b.ask)}  fund {_fund(b.funding)}  ",
        b_status,
    )

    in_pct  = ss.in_spread_pct
    out_pct = ss.out_spread_pct
    in_usd  = ss.in_spread_usd
    out_usd = ss.out_spread_usd

    in_blink  = in_pct  is not None and in_pct  >= state.in_threshold
    out_blink = out_pct is not None and out_pct >= state.out_threshold

    in_arb  = Text("ARB OPEN", "bold green") if (in_usd or 0) > 0 else Text("NO ARB ", "bold red")
    out_arb = Text("ARB OPEN", "bold green") if (out_usd or 0) > 0 else Text("NO ARB ", "bold red")

    in_row = Text.assemble(
        ("IN  SPREAD ", "bold blink" if in_blink else "bold"),
        Text("(A.bid − B.ask)  ", "dim"),
        Text(f"{_pct(in_pct)}  ({_fp(in_usd, 10)})  ", "bold blink" if in_blink else "bold"),
        in_arb,
    )
    out_row = Text.assemble(
        ("OUT SPREAD ", "bold blink" if out_blink else "bold"),
        Text("(B.bid − A.ask)  ", "dim"),
        Text(f"{_pct(out_pct)}  ({_fp(out_usd, 10)})  ", "bold blink" if out_blink else "bold"),
        out_arb,
    )

    th_row = Text(
        f"IN thr {state.in_threshold:.3f}%  alerts {state.in_alert.count}  last {state.in_alert.last_ts}"
        f"    |    OUT thr {state.out_threshold:.3f}%  alerts {state.out_alert.count}  last {state.out_alert.last_ts}",
        style="white",
    )
    footer = Text("[Q] quit   [I] IN threshold   [O] OUT threshold   [S] switch spread", style="dim")

    sep = Text("─" * 70, style="dim")
    t.add_row(row_a)
    t.add_row(row_b)
    t.add_row(sep)
    t.add_row(in_row)
    t.add_row(out_row)
    t.add_row(sep)
    t.add_row(th_row)
    t.add_row(sep)
    t.add_row(footer)
    if state.msgs:
        for m in state.msgs[-5:]:
            t.add_row(Text(m, style="bold yellow"))

    spread_name = f"{a_label} ↔ {b_label}" if la else "SPREAD MONITOR"
    return Panel(t, title=spread_name, border_style="cyan")


# ─────────────────────────────────────────── alert / beeper

def _append_csv(path: str, ss: SpreadSnapshot, kind: str) -> None:
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow([
                "iso_ts", "a_bid", "a_ask", "b_bid", "b_ask",
                "in_usd", "in_pct", "out_usd", "out_pct", "type",
            ])
        w.writerow([
            datetime.now(timezone.utc).isoformat(),
            ss.leg_a.bid, ss.leg_a.ask, ss.leg_b.bid, ss.leg_b.ask,
            ss.in_spread_usd, ss.in_spread_pct,
            ss.out_spread_usd, ss.out_spread_pct,
            kind,
        ])


def _maybe_fire(kind: str, alert: AlertState, pct: float,
                ss: SpreadSnapshot, state: MonitorState, now: float, console: Console) -> None:
    if now - alert.last_fired < REALERT_S:
        return
    alert.last_fired = now
    alert.count += 1
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    alert.last_ts = ts
    usd = ss.in_spread_usd if kind == "IN" else ss.out_spread_usd
    msg = f"[{ts}] {kind} ALERT  {_pct(pct)}  ${usd:+.4f}" if usd is not None else f"[{ts}] {kind} ALERT  {_pct(pct)}"
    state.msgs.append(msg)
    state.msgs = state.msgs[-12:]
    console.log(msg, style="bold yellow")
    try:
        _append_csv(state.log_path, ss, kind)
    except Exception:
        pass


async def alert_loop(feed: FeedManager, state: MonitorState, console: Console) -> None:
    while True:
        try:
            la, lb = state.leg_a, state.leg_b
            if la and lb:
                ss = feed.spread_snapshot(la, lb)
                now = time.monotonic()
                for kind, alert, thr, pct in (
                    ("IN",  state.in_alert,  state.in_threshold,  ss.in_spread_pct),
                    ("OUT", state.out_alert, state.out_threshold, ss.out_spread_pct),
                ):
                    if pct is None:
                        continue
                    if pct >= thr:
                        alert.active = True
                        _maybe_fire(kind, alert, pct, ss, state, now, console)
                    elif alert.active and pct < thr * HYSTERESIS:
                        alert.active = False
        except Exception:
            pass
        await asyncio.sleep(ALERT_TICK_S)


async def beeper(state: MonitorState) -> None:
    while True:
        if state.in_alert.active or state.out_alert.active:
            sys.stdout.write("\a")
            sys.stdout.flush()
        await asyncio.sleep(BEEP_INTERVAL_S)


# ─────────────────────────────────────────── stdin / hotkeys

class StdinReader:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self._fd: Optional[int] = None
        self._old = None
        self._on = False

    def enable(self) -> None:
        if not sys.stdin.isatty():
            return
        self._fd = sys.stdin.fileno()
        self._old = termios.tcgetattr(self._fd)
        tty.setcbreak(self._fd)
        asyncio.get_event_loop().add_reader(self._fd, self._read)
        self._on = True

    def disable(self) -> None:
        if not self._on:
            return
        try:
            asyncio.get_event_loop().remove_reader(self._fd)
        except Exception:
            pass
        if self._fd is not None and self._old is not None:
            termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old)
        self._on = False

    def _read(self) -> None:
        try:
            ch = os.read(self._fd, 1).decode(errors="ignore")
            if ch:
                self.queue.put_nowait(ch)
        except OSError:
            pass


async def hotkey_loop(
    reader: StdinReader,
    feed: FeedManager,
    state: MonitorState,
    live: Live,
    stop: asyncio.Event,
    redis_client,
) -> None:
    while not stop.is_set():
        ch = await reader.queue.get()
        if ch.lower() == "q":
            stop.set()
            return
        if ch.lower() in ("i", "o"):
            label = "IN" if ch.lower() == "i" else "OUT"
            live.stop()
            reader.disable()
            try:
                sys.stdout.write(f"\nNew {label} threshold (%): ")
                sys.stdout.flush()
                line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
                val = float((line or "").strip())
                if ch.lower() == "i":
                    state.in_threshold = val
                else:
                    state.out_threshold = val
            except (ValueError, EOFError):
                sys.stdout.write("\n(invalid)\n")
                sys.stdout.flush()
            finally:
                reader.enable()
                live.start()
        if ch.lower() == "s" and redis_client:
            # reload active spread from Redis
            sid = await redis_client.get("monitor:active_spread")
            if sid:
                h = await redis_client.hgetall(f"spread:{sid}")
                if h:
                    try:
                        from exchanges import ExchangeInfo
                        new_a = ExchangeInfo(h["leg_a_ex"], h["leg_a_sym"], h["leg_a_mt"], h["leg_a_disp"])
                        new_b = ExchangeInfo(h["leg_b_ex"], h["leg_b_sym"], h["leg_b_mt"], h["leg_b_disp"])
                        await feed.subscribe(new_a)
                        await feed.subscribe(new_b)
                        state.leg_a = new_a
                        state.leg_b = new_b
                        state.in_alert = AlertState()
                        state.out_alert = AlertState()
                    except Exception as e:
                        sys.stdout.write(f"\nswitch error: {e}\n")


async def render_loop(
    feed: FeedManager, state: MonitorState, live: Live, stop: asyncio.Event
) -> None:
    interval = 1.0 / REFRESH_HZ
    while not stop.is_set():
        try:
            la, lb = state.leg_a, state.leg_b
            if la and lb:
                ss = feed.spread_snapshot(la, lb)
                live.update(render(ss, state))
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def redis_watcher(redis_client, feed: FeedManager, state: MonitorState) -> None:
    """Poll Redis for active spread changes every 2s."""
    last_sid: Optional[str] = None
    while True:
        try:
            sid = await redis_client.get("monitor:active_spread")
            if sid and sid != last_sid:
                h = await redis_client.hgetall(f"spread:{sid}")
                if h:
                    new_a = ExchangeInfo(h["leg_a_ex"], h["leg_a_sym"], h["leg_a_mt"], h["leg_a_disp"])
                    new_b = ExchangeInfo(h["leg_b_ex"], h["leg_b_sym"], h["leg_b_mt"], h["leg_b_disp"])
                    await feed.subscribe(new_a)
                    await feed.subscribe(new_b)
                    state.leg_a = new_a
                    state.leg_b = new_b
                    state.in_alert = AlertState()
                    state.out_alert = AlertState()
                    last_sid = sid
        except Exception:
            pass
        await asyncio.sleep(2.0)


# ─────────────────────────────────────────── main

async def main_async(args: argparse.Namespace) -> None:
    state = MonitorState(
        in_threshold=args.in_threshold,
        out_threshold=args.out_threshold,
        log_path=args.log,
    )

    redis_url = os.getenv("REDIS_URL", "").strip()
    redis_client = None
    if redis_url:
        try:
            redis_client = redis_from_url(redis_url, decode_responses=True)
            await redis_client.ping()
        except Exception as e:
            print(f"Redis unavailable: {e}. Spread switching disabled.")
            redis_client = None

    session = aiohttp.ClientSession()
    feed = FeedManager()
    await feed.start(session)

    # Determine initial spread
    if args.url_a and args.url_b:
        la = parse_url(args.url_a)
        lb = parse_url(args.url_b)
        if not la or not lb:
            print("ERROR: Could not parse one or both URLs.")
            await session.close()
            return
        await feed.subscribe(la)
        await feed.subscribe(lb)
        state.leg_a = la
        state.leg_b = lb
        if redis_client:
            sid_placeholder = "manual"
            await redis_client.hset(f"spread:{sid_placeholder}", mapping={
                "leg_a_ex": la.exchange, "leg_a_sym": la.symbol,
                "leg_a_mt": la.market_type, "leg_a_disp": la.display,
                "leg_b_ex": lb.exchange, "leg_b_sym": lb.symbol,
                "leg_b_mt": lb.market_type, "leg_b_disp": lb.display,
            })
            await redis_client.set("monitor:active_spread", sid_placeholder)
    elif redis_client:
        # Load active spread from Redis
        sid = await redis_client.get("monitor:active_spread")
        if sid:
            h = await redis_client.hgetall(f"spread:{sid}")
            if h:
                la = ExchangeInfo(h["leg_a_ex"], h["leg_a_sym"], h["leg_a_mt"], h["leg_a_disp"])
                lb = ExchangeInfo(h["leg_b_ex"], h["leg_b_sym"], h["leg_b_mt"], h["leg_b_disp"])
                await feed.subscribe(la)
                await feed.subscribe(lb)
                state.leg_a = la
                state.leg_b = lb

    console = Console()
    reader = StdinReader()
    reader.enable()
    stop = asyncio.Event()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    # Initial snapshot for first render
    if state.leg_a and state.leg_b:
        await asyncio.sleep(0.1)
        initial_ss = feed.spread_snapshot(state.leg_a, state.leg_b)
    else:
        from feed_manager import SpreadSnapshot
        from exchanges import Snapshot
        initial_ss = SpreadSnapshot(leg_a=Snapshot(), leg_b=Snapshot())

    live = Live(render(initial_ss, state), console=console,
                refresh_per_second=REFRESH_HZ, screen=False)
    live.start()

    tasks = [
        asyncio.create_task(render_loop(feed, state, live, stop)),
        asyncio.create_task(alert_loop(feed, state, console)),
        asyncio.create_task(beeper(state)),
        asyncio.create_task(hotkey_loop(reader, feed, state, live, stop, redis_client)),
    ]
    if redis_client:
        tasks.append(asyncio.create_task(redis_watcher(redis_client, feed, state)))

    try:
        await stop.wait()
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        try:
            live.stop()
        except Exception:
            pass
        reader.disable()
        await feed.stop()
        await session.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Multi-spread terminal monitor")
    p.add_argument("--url-a", default="", help="Leg A trade URL (overrides Redis)")
    p.add_argument("--url-b", default="", help="Leg B trade URL (overrides Redis)")
    p.add_argument("--spread", default="", help="Spread ID to load from Redis")
    p.add_argument("--in-threshold",  type=float, default=0.3)
    p.add_argument("--out-threshold", type=float, default=0.3)
    p.add_argument("--log", default="alerts.log")
    args = p.parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
