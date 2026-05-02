"""Telegram alert bot for the ANTHROPIC IN/OUT spread.

Reuses feeds.PriceFeed so the spread math is bit-identical to spread_monitor.py.
Configure via .env:

    TELEGRAM_BOT_TOKEN=...           # required
    REDIS_URL=redis://...            # optional; falls back to config.json

Deployable to Railway / Fly / any always-on host.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

from feeds import PriceFeed

log = logging.getLogger("alert_bot")

DEFAULT_IN_THRESHOLD = 0.3
DEFAULT_OUT_THRESHOLD = 0.3
DEFAULT_OUT_MAX_THRESHOLD = 20.0
DEFAULT_OI_THRESHOLD = 6_900_000.0
DEFAULT_OUT_MIN_DEPTH = 0.5        # min coins in HL bid book within depth range
OUT_DEPTH_RANGE_PCT = 4.5          # look this far below best HL bid
ALERT_POLL_INTERVAL_S = 1.0
ALERT_REFIRE_INTERVAL_S = 3.0  # spam cadence while breached


# ----------------------------------------------------------------- config store


@dataclass
class ChatConfig:
    chat_id: int
    in_threshold: float = DEFAULT_IN_THRESHOLD
    out_threshold: float = DEFAULT_OUT_THRESHOLD
    out_max_threshold: float = DEFAULT_OUT_MAX_THRESHOLD
    oi_threshold: float = DEFAULT_OI_THRESHOLD
    out_min_depth: float = DEFAULT_OUT_MIN_DEPTH
    alerts_on: bool = True


class ConfigStore:
    async def load(self) -> dict[int, ChatConfig]:
        raise NotImplementedError

    async def upsert(self, cfg: ChatConfig) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        pass


class JsonConfigStore(ConfigStore):
    def __init__(self, path: str = "config.json") -> None:
        self.path = path
        self._lock = asyncio.Lock()

    async def load(self) -> dict[int, ChatConfig]:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(None, self._load_sync)

    def _load_sync(self) -> dict[int, ChatConfig]:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
        out: dict[int, ChatConfig] = {}
        for k, v in (data.get("chats") or {}).items():
            try:
                cid = int(k)
                out[cid] = ChatConfig(
                    chat_id=cid,
                    in_threshold=float(v.get("in_threshold", DEFAULT_IN_THRESHOLD)),
                    out_threshold=float(v.get("out_threshold", DEFAULT_OUT_THRESHOLD)),
                    out_max_threshold=float(v.get("out_max_threshold", DEFAULT_OUT_MAX_THRESHOLD)),
                    oi_threshold=float(v.get("oi_threshold", DEFAULT_OI_THRESHOLD)),
                    out_min_depth=float(v.get("out_min_depth", DEFAULT_OUT_MIN_DEPTH)),
                    alerts_on=bool(v.get("alerts_on", True)),
                )
            except (TypeError, ValueError):
                continue
        return out

    async def upsert(self, cfg: ChatConfig) -> None:
        async with self._lock:
            await asyncio.get_running_loop().run_in_executor(None, self._upsert_sync, cfg)

    def _upsert_sync(self, cfg: ChatConfig) -> None:
        data: dict[str, Any] = {"chats": {}}
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                data = {"chats": {}}
        data.setdefault("chats", {})[str(cfg.chat_id)] = {
            "in_threshold": cfg.in_threshold,
            "out_threshold": cfg.out_threshold,
            "out_max_threshold": cfg.out_max_threshold,
            "oi_threshold": cfg.oi_threshold,
            "out_min_depth": cfg.out_min_depth,
            "alerts_on": cfg.alerts_on,
        }
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, self.path)


class RedisConfigStore(ConfigStore):
    def __init__(self, redis_client) -> None:
        self.r = redis_client

    @classmethod
    async def try_create(cls, url: str) -> Optional["RedisConfigStore"]:
        try:
            from redis.asyncio import from_url

            client = from_url(url, decode_responses=True)
            await client.ping()
            return cls(client)
        except Exception as e:
            log.warning("redis unavailable (%s); falling back to JSON", e)
            return None

    async def load(self) -> dict[int, ChatConfig]:
        ids = await self.r.smembers("chats")
        out: dict[int, ChatConfig] = {}
        for sid in ids:
            try:
                cid = int(sid)
            except ValueError:
                continue
            data = await self.r.hgetall(f"chat:{cid}")
            out[cid] = ChatConfig(
                chat_id=cid,
                in_threshold=float(data.get("in_threshold", DEFAULT_IN_THRESHOLD)),
                out_threshold=float(data.get("out_threshold", DEFAULT_OUT_THRESHOLD)),
                out_max_threshold=float(data.get("out_max_threshold", DEFAULT_OUT_MAX_THRESHOLD)),
                oi_threshold=float(data.get("oi_threshold", DEFAULT_OI_THRESHOLD)),
                out_min_depth=float(data.get("out_min_depth", DEFAULT_OUT_MIN_DEPTH)),
                alerts_on=data.get("alerts_on", "1") not in ("0", "false", "False"),
            )
        return out

    async def upsert(self, cfg: ChatConfig) -> None:
        await self.r.sadd("chats", cfg.chat_id)
        await self.r.hset(
            f"chat:{cfg.chat_id}",
            mapping={
                "in_threshold": str(cfg.in_threshold),
                "out_threshold": str(cfg.out_threshold),
                "out_max_threshold": str(cfg.out_max_threshold),
                "oi_threshold": str(cfg.oi_threshold),
                "out_min_depth": str(cfg.out_min_depth),
                "alerts_on": "1" if cfg.alerts_on else "0",
            },
        )

    async def close(self) -> None:
        try:
            await self.r.aclose()
        except Exception:
            pass


async def build_config_store() -> ConfigStore:
    url = os.getenv("REDIS_URL", "").strip()
    if not url:
        raise SystemExit("REDIS_URL is not set. Add it to Railway env vars (Upstash free tier works).")
    try:
        from redis.asyncio import from_url
        client = from_url(url, decode_responses=True)
        await client.ping()
    except Exception as e:
        raise SystemExit(f"Cannot connect to Redis ({url.split('@')[-1]}): {e}") from e
    log.info("config store: Redis at %s", url.split("@")[-1])
    return RedisConfigStore(client)


# ----------------------------------------------------------------- shared state


@dataclass
class AlertChannelState:
    last_fired: float = 0.0
    breached: bool = False


@dataclass
class ChatRuntime:
    cfg: ChatConfig
    in_state: AlertChannelState = field(default_factory=AlertChannelState)
    out_state: AlertChannelState = field(default_factory=AlertChannelState)
    out_max_state: AlertChannelState = field(default_factory=AlertChannelState)
    oi_state: AlertChannelState = field(default_factory=AlertChannelState)


class BotState:
    def __init__(self) -> None:
        self.chats: dict[int, ChatRuntime] = {}
        self.lock = asyncio.Lock()

    async def get_or_create(self, store: ConfigStore, chat_id: int) -> ChatRuntime:
        is_new = False
        async with self.lock:
            rt = self.chats.get(chat_id)
            if rt is None:
                cfg = ChatConfig(chat_id=chat_id)
                rt = ChatRuntime(cfg=cfg)
                self.chats[chat_id] = rt
                is_new = True
        if is_new:
            await store.upsert(rt.cfg)
        return rt

    async def hydrate(self, store: ConfigStore) -> None:
        loaded = await store.load()
        async with self.lock:
            for cid, cfg in loaded.items():
                if cid not in self.chats:
                    self.chats[cid] = ChatRuntime(cfg=cfg)
        if loaded:
            log.info("hydrated %d chat(s): %s", len(loaded), list(loaded.keys()))
        else:
            log.info("no chats in Redis yet")


# ----------------------------------------------------------------- formatting


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "--"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.3f}%"


def _fmt_usd(v: Optional[float], decimals: int = 4) -> str:
    if v is None:
        return "--"
    if v >= 0:
        return f"+${v:.{decimals}f}"
    return f"-${abs(v):.{decimals}f}"


def _fmt_price(v: Optional[float]) -> str:
    if v is None:
        return "--"
    return f"${v:.4f}"


def _fmt_funding(v: Optional[float]) -> str:
    if v is None:
        return "--"
    return f"{v * 100:+.4f}%"


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_in_alert(snap: dict) -> str:
    g = snap["gate"]
    h = snap["hl"]
    return (
        "🟢 ARB ENTRY SIGNAL — ANTHROPIC\n"
        f"📈 IN Spread: {_fmt_pct(snap['in_spread_pct'])} ({_fmt_usd(snap['in_spread_usd'])})\n"
        f"Gate bid: {_fmt_price(g['bid'])} | HL ask: {_fmt_price(h['ask'])}\n"
        f"Fund diff: Gate {_fmt_funding(g['funding'])} / HL {_fmt_funding(h['funding'])}\n"
        f"⏰ {_now_utc_str()}"
    )


def fmt_out_alert(snap: dict, hl_depth: float, gate_depth: float) -> str:
    g = snap["gate"]
    h = snap["hl"]
    return (
        "🔴 ARB EXIT SIGNAL — ANTHROPIC\n"
        f"📉 OUT Spread: {_fmt_pct(snap['out_spread_pct'])} ({_fmt_usd(snap['out_spread_usd'])})\n"
        f"HL bid: {_fmt_price(h['bid'])} | Gate ask: {_fmt_price(g['ask'])}\n"
        f"Depth (-{OUT_DEPTH_RANGE_PCT}%): HL bids {hl_depth:.3f} | Gate asks {gate_depth:.3f} coins\n"
        f"Fund diff: Gate {_fmt_funding(g['funding'])} / HL {_fmt_funding(h['funding'])}\n"
        f"⏰ {_now_utc_str()}"
    )


def _fmt_oi(v: Optional[float]) -> str:
    if v is None:
        return "--"
    return f"${v / 1_000_000:.3f}M"


def fmt_out_max_alert(snap: dict, hl_depth: float, gate_depth: float) -> str:
    g = snap["gate"]
    h = snap["hl"]
    return (
        "🔴🔴 OUT SPREAD EXTREME — ANTHROPIC\n"
        f"📉 OUT Spread: {_fmt_pct(snap['out_spread_pct'])} ({_fmt_usd(snap['out_spread_usd'])})\n"
        f"HL bid: {_fmt_price(h['bid'])} | Gate ask: {_fmt_price(g['ask'])}\n"
        f"Depth (-{OUT_DEPTH_RANGE_PCT}%): HL bids {hl_depth:.3f} | Gate asks {gate_depth:.3f} coins\n"
        f"Fund diff: Gate {_fmt_funding(g['funding'])} / HL {_fmt_funding(h['funding'])}\n"
        f"⏰ {_now_utc_str()}"
    )


def fmt_oi_alert(snap: dict, threshold: float) -> str:
    h = snap["hl"]
    oi_usd = snap["hl_oi_usd"]
    return (
        "📊 OI LIMIT ALERT — ANTHROPIC\n"
        f"HL OI: {_fmt_oi(oi_usd)} ≥ threshold {_fmt_oi(threshold)}\n"
        f"HL mark: {_fmt_price(h.get('mark_px'))} | OI coins: {h.get('oi', '--')}\n"
        f"⏰ {_now_utc_str()}"
    )


def fmt_status(snap: dict, cfg: ChatConfig) -> str:
    g = snap["gate"]
    h = snap["hl"]
    return (
        "📊 ANTHROPIC status\n"
        f"Gate.io  bid={_fmt_price(g['bid'])} ask={_fmt_price(g['ask'])} "
        f"fund={_fmt_funding(g['funding'])} {'STALE' if snap['gate_stale'] else 'LIVE'}\n"
        f"HyperLiq bid={_fmt_price(h['bid'])} ask={_fmt_price(h['ask'])} "
        f"fund={_fmt_funding(h['funding'])} OI={_fmt_oi(snap.get('hl_oi_usd'))} "
        f"{'STALE' if snap['hl_stale'] else 'LIVE'}\n"
        f"Out zone depth (-{OUT_DEPTH_RANGE_PCT}%): "
        f"HL {PriceFeed.hl_out_depth(snap.get('hl_bids',[]), snap['gate']['ask'] or 0, OUT_DEPTH_RANGE_PCT):.3f} | "
        f"Gate {PriceFeed.gate_out_depth(snap.get('gate_asks',[]), snap['hl']['bid'] or 0, OUT_DEPTH_RANGE_PCT):.3f} "
        f"coins (min {cfg.out_min_depth})\n"
        f"\nIN  spread: {_fmt_pct(snap['in_spread_pct'])} ({_fmt_usd(snap['in_spread_usd'])})  "
        f"thr={cfg.in_threshold:.3f}%\n"
        f"OUT spread: {_fmt_pct(snap['out_spread_pct'])} ({_fmt_usd(snap['out_spread_usd'])})  "
        f"thr={cfg.out_threshold:.3f}% / max={cfg.out_max_threshold:.3f}%\n"
        f"OI threshold: {_fmt_oi(cfg.oi_threshold)}\n"
        f"alerts: {'ON' if cfg.alerts_on else 'OFF'}"
    )


# ----------------------------------------------------------------- handlers


def _bot_data(ctx: ContextTypes.DEFAULT_TYPE) -> dict:
    return ctx.application.bot_data


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    bd = _bot_data(ctx)
    state: BotState = bd["state"]
    store: ConfigStore = bd["store"]
    rt = await state.get_or_create(store, update.effective_chat.id)
    await update.effective_chat.send_message(
        "👋 ANTHROPIC spread alert bot connected.\n\n"
        f"IN  threshold: {rt.cfg.in_threshold:.3f}%\n"
        f"OUT threshold: {rt.cfg.out_threshold:.3f}%\n"
        f"OUT max threshold: {rt.cfg.out_max_threshold:.3f}%\n"
        f"OI threshold: {_fmt_oi(rt.cfg.oi_threshold)}\n"
        f"OUT min depth: {rt.cfg.out_min_depth} coins (range -{OUT_DEPTH_RANGE_PCT}%)\n"
        f"Alerts: {'ON' if rt.cfg.alerts_on else 'OFF'}\n\n"
        "Commands: /status /set_in /set_out /set_outmax /set_oi /set_out_depth /thresholds /alerts_on /alerts_off /help"
    )


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    await update.effective_chat.send_message(
        "Commands:\n"
        "/start — register and show thresholds\n"
        "/status — live spread + OI snapshot\n"
        "/set_in <pct> — set IN spread alert threshold (e.g. /set_in 0.5)\n"
        "/set_out <pct> — set OUT spread lower threshold\n"
        "/set_outmax <pct> — set OUT spread upper threshold (e.g. /set_outmax 20)\n"
        "/set_oi <millions> — set OI alert threshold in $M (e.g. /set_oi 6.9)\n"
        f"/set_out_depth <coins> — min HL bid depth within -{OUT_DEPTH_RANGE_PCT}% for OUT alert (e.g. /set_out_depth 0.5)\n"
        "/thresholds — show all thresholds\n"
        "/alerts_on — enable alerts in this chat\n"
        "/alerts_off — disable alerts in this chat\n"
        "/help — this message"
    )


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    bd = _bot_data(ctx)
    state: BotState = bd["state"]
    store: ConfigStore = bd["store"]
    feed: PriceFeed = bd["feed"]
    rt = await state.get_or_create(store, update.effective_chat.id)
    snap = await feed.snapshot()
    await update.effective_chat.send_message(fmt_status(snap, rt.cfg))


async def _set_pct_threshold(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, kind: str
) -> None:
    if update.effective_chat is None:
        return
    bd = _bot_data(ctx)
    state: BotState = bd["state"]
    store: ConfigStore = bd["store"]
    cmd = {"IN": "set_in", "OUT": "set_out", "OUT_MAX": "set_outmax"}[kind]
    if not ctx.args:
        await update.effective_chat.send_message(f"Usage: /{cmd} <pct>")
        return
    try:
        val = float(ctx.args[0])
    except ValueError:
        await update.effective_chat.send_message("Could not parse number.")
        return
    rt = await state.get_or_create(store, update.effective_chat.id)
    if kind == "IN":
        rt.cfg.in_threshold = val
    elif kind == "OUT":
        rt.cfg.out_threshold = val
    else:
        rt.cfg.out_max_threshold = val
    await store.upsert(rt.cfg)
    label = {"IN": "IN", "OUT": "OUT", "OUT_MAX": "OUT max"}[kind]
    await update.effective_chat.send_message(f"{label} threshold set to {val:.3f}%")


async def cmd_set_in(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_pct_threshold(update, ctx, "IN")


async def cmd_set_out(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_pct_threshold(update, ctx, "OUT")


async def cmd_set_outmax(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_pct_threshold(update, ctx, "OUT_MAX")


async def cmd_set_out_depth(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    bd = _bot_data(ctx)
    state: BotState = bd["state"]
    store: ConfigStore = bd["store"]
    if not ctx.args:
        await update.effective_chat.send_message(
            f"Usage: /set_out_depth <coins>  (e.g. /set_out_depth 0.5)\n"
            f"OUT alerts only fire when HL bid depth within -{OUT_DEPTH_RANGE_PCT}% ≥ this value."
        )
        return
    try:
        val = float(ctx.args[0])
    except ValueError:
        await update.effective_chat.send_message("Could not parse number.")
        return
    rt = await state.get_or_create(store, update.effective_chat.id)
    rt.cfg.out_min_depth = val
    await store.upsert(rt.cfg)
    await update.effective_chat.send_message(
        f"OUT min depth set to {val} coins (range -{OUT_DEPTH_RANGE_PCT}%)"
    )


async def cmd_set_oi(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    bd = _bot_data(ctx)
    state: BotState = bd["state"]
    store: ConfigStore = bd["store"]
    if not ctx.args:
        await update.effective_chat.send_message("Usage: /set_oi <millions>  (e.g. /set_oi 6.9)")
        return
    try:
        val = float(ctx.args[0]) * 1_000_000
    except ValueError:
        await update.effective_chat.send_message("Could not parse number.")
        return
    rt = await state.get_or_create(store, update.effective_chat.id)
    rt.cfg.oi_threshold = val
    await store.upsert(rt.cfg)
    await update.effective_chat.send_message(f"OI threshold set to {_fmt_oi(val)}")


async def cmd_thresholds(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    bd = _bot_data(ctx)
    state: BotState = bd["state"]
    store: ConfigStore = bd["store"]
    rt = await state.get_or_create(store, update.effective_chat.id)
    await update.effective_chat.send_message(
        f"IN  threshold: {rt.cfg.in_threshold:.3f}%\n"
        f"OUT threshold: {rt.cfg.out_threshold:.3f}%\n"
        f"OUT max threshold: {rt.cfg.out_max_threshold:.3f}%\n"
        f"OI threshold: {_fmt_oi(rt.cfg.oi_threshold)}\n"
        f"OUT min depth: {rt.cfg.out_min_depth} coins (range -{OUT_DEPTH_RANGE_PCT}%)"
    )


async def _set_alerts(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, on: bool
) -> None:
    if update.effective_chat is None:
        return
    bd = _bot_data(ctx)
    state: BotState = bd["state"]
    store: ConfigStore = bd["store"]
    rt = await state.get_or_create(store, update.effective_chat.id)
    rt.cfg.alerts_on = on
    await store.upsert(rt.cfg)
    await update.effective_chat.send_message(f"Alerts {'ON' if on else 'OFF'}.")


async def cmd_alerts_on(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_alerts(update, ctx, True)


async def cmd_alerts_off(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_alerts(update, ctx, False)


# ----------------------------------------------------------------- dispatcher


async def _safe_send(bot, chat_id: int, text: str) -> None:
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except RetryAfter as e:
        log.warning("rate limited; sleeping %s", e.retry_after)
        await asyncio.sleep(float(e.retry_after) + 0.5)
        try:
            await bot.send_message(chat_id=chat_id, text=text)
        except TelegramError as e2:
            log.warning("retry send failed: %s", e2)
    except TelegramError as e:
        log.warning("telegram send failed for chat %s: %s", chat_id, e)


async def alert_dispatcher(app: Application, feed: PriceFeed, state: BotState) -> None:
    """Single global loop polling the feed once per second and fanning out per chat.

    User explicitly asked for spammy alerts (wake them up):
    - re-fires every ALERT_REFIRE_INTERVAL_S while breached
    - sends the main alert + a short follow-up so the phone double-buzzes
    - no hysteresis dead-zone: stays "breached" until pct drops below threshold
    """
    bot = app.bot
    while True:
        try:
            snap = await feed.snapshot()
        except Exception as e:
            log.warning("snapshot error: %s", e)
            await asyncio.sleep(ALERT_POLL_INTERVAL_S)
            continue

        in_pct = snap["in_spread_pct"]
        out_pct = snap["out_spread_pct"]
        oi_usd = snap.get("hl_oi_usd")
        now = time.monotonic()

        async with state.lock:
            chats = list(state.chats.values())

        for rt in chats:
            if not rt.cfg.alerts_on:
                for s in (rt.in_state, rt.out_state, rt.out_max_state, rt.oi_state):
                    s.breached = False
                continue

            # IN spread
            if in_pct is not None and in_pct >= rt.cfg.in_threshold:
                rt.in_state.breached = True
                if now - rt.in_state.last_fired >= ALERT_REFIRE_INTERVAL_S:
                    rt.in_state.last_fired = now
                    await _safe_send(bot, rt.cfg.chat_id, fmt_in_alert(snap))
                    await _safe_send(bot, rt.cfg.chat_id,
                        f"🚨 STILL ACTIVE — IN {_fmt_pct(in_pct)} ≥ thr {rt.cfg.in_threshold:.3f}%")
            else:
                rt.in_state.breached = False

            # depth: coins available within the -OUT_DEPTH_RANGE_PCT% exit spread zone
            gate_ask = snap["gate"]["ask"] or 0.0
            hl_bid   = snap["hl"]["bid"]   or 0.0
            hl_depth   = PriceFeed.hl_out_depth(snap.get("hl_bids", []),   gate_ask, OUT_DEPTH_RANGE_PCT)
            gate_depth = PriceFeed.gate_out_depth(snap.get("gate_asks", []), hl_bid,  OUT_DEPTH_RANGE_PCT)
            depth_ok = hl_depth >= rt.cfg.out_min_depth and gate_depth >= rt.cfg.out_min_depth

            # OUT spread (lower threshold) — only fires when both depth conditions met
            if out_pct is not None and out_pct >= rt.cfg.out_threshold and depth_ok:
                rt.out_state.breached = True
                if now - rt.out_state.last_fired >= ALERT_REFIRE_INTERVAL_S:
                    rt.out_state.last_fired = now
                    await _safe_send(bot, rt.cfg.chat_id, fmt_out_alert(snap, hl_depth, gate_depth))
                    await _safe_send(bot, rt.cfg.chat_id,
                        f"🚨 STILL ACTIVE — OUT {_fmt_pct(out_pct)} ≥ thr {rt.cfg.out_threshold:.3f}% | HL {hl_depth:.2f} / Gate {gate_depth:.2f}")
            else:
                rt.out_state.breached = False

            # OUT spread (upper / extreme threshold) — also gated on depth
            if out_pct is not None and out_pct >= rt.cfg.out_max_threshold and depth_ok:
                rt.out_max_state.breached = True
                if now - rt.out_max_state.last_fired >= ALERT_REFIRE_INTERVAL_S:
                    rt.out_max_state.last_fired = now
                    await _safe_send(bot, rt.cfg.chat_id, fmt_out_max_alert(snap, hl_depth, gate_depth))
                    await _safe_send(bot, rt.cfg.chat_id,
                        f"🚨🚨 EXTREME OUT — {_fmt_pct(out_pct)} ≥ max {rt.cfg.out_max_threshold:.3f}% | HL {hl_depth:.2f} / Gate {gate_depth:.2f}")
            else:
                rt.out_max_state.breached = False

            # OI threshold
            if oi_usd is not None and oi_usd >= rt.cfg.oi_threshold:
                rt.oi_state.breached = True
                if now - rt.oi_state.last_fired >= ALERT_REFIRE_INTERVAL_S:
                    rt.oi_state.last_fired = now
                    await _safe_send(bot, rt.cfg.chat_id, fmt_oi_alert(snap, rt.cfg.oi_threshold))
                    await _safe_send(bot, rt.cfg.chat_id,
                        f"🚨 OI STILL HIGH — {_fmt_oi(oi_usd)} ≥ {_fmt_oi(rt.cfg.oi_threshold)}")
            else:
                rt.oi_state.breached = False

        await asyncio.sleep(ALERT_POLL_INTERVAL_S)


# ----------------------------------------------------------------- lifecycle


async def _post_init(app: Application) -> None:
    feed: PriceFeed = app.bot_data["feed"]
    state: BotState = app.bot_data["state"]
    store = await build_config_store()
    app.bot_data["store"] = store
    await feed.start()
    await state.hydrate(store)
    app.bot_data["dispatcher_task"] = asyncio.create_task(
        alert_dispatcher(app, feed, state)
    )
    log.info("bot ready; %d chat(s) loaded from Redis", len(state.chats))
    # notify all registered chats that the bot is live again
    if state.chats:
        async with state.lock:
            chat_ids = [rt.cfg.chat_id for rt in state.chats.values()]
        for cid in chat_ids:
            try:
                await app.bot.send_message(
                    chat_id=cid,
                    text=f"✅ Bot restarted — monitoring ANTHROPIC spreads.\n{len(chat_ids)} chat(s) active.",
                )
            except Exception as e:
                log.warning("restart notify failed for %s: %s", cid, e)


async def _post_shutdown(app: Application) -> None:
    task: asyncio.Task = app.bot_data.get("dispatcher_task")
    if task is not None:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    feed: PriceFeed = app.bot_data.get("feed")
    if feed is not None:
        await feed.stop()
    store: ConfigStore = app.bot_data.get("store")
    if store is not None:
        await store.close()


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("TELEGRAM_BOT_TOKEN missing — set it in .env or env vars")

    app: Application = (
        ApplicationBuilder()
        .token(token)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )

    feed = PriceFeed()
    state = BotState()
    app.bot_data["feed"] = feed
    app.bot_data["state"] = state

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("set_in", cmd_set_in))
    app.add_handler(CommandHandler("set_out", cmd_set_out))
    app.add_handler(CommandHandler("set_outmax", cmd_set_outmax))
    app.add_handler(CommandHandler("set_oi", cmd_set_oi))
    app.add_handler(CommandHandler("set_out_depth", cmd_set_out_depth))
    app.add_handler(CommandHandler("thresholds", cmd_thresholds))
    app.add_handler(CommandHandler("alerts_on", cmd_alerts_on))
    app.add_handler(CommandHandler("alerts_off", cmd_alerts_off))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
