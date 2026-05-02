"""Multi-exchange spread alert bot.

Commands:
  /start              -- register chat
  /newspread          -- wizard: paste two trade URLs -> creates spread
  /spreads            -- list spreads with inline keyboard
  /status             -- current spread prices & spreads
  /set_in <pct>       -- IN alert threshold for selected spread
  /set_out <pct>      -- OUT alert threshold
  /set_outmax <pct>   -- extreme OUT threshold
  /set_oi <usd>       -- OI alert threshold
  /set_out_depth <n>  -- min depth for OUT alert
  /thresholds         -- show current settings
  /alerts_on/off      -- toggle alerts
  /help
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from redis.asyncio import from_url as redis_from_url
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TelegramError
from telegram.ext import (
    Application, ApplicationHandlerStop, CallbackQueryHandler, CommandHandler,
    ContextTypes, ConversationHandler, MessageHandler, TypeHandler, filters,
)

from exchanges import ExchangeInfo
from feed_manager import FeedManager, SpreadSnapshot

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("alert_bot")

OUT_DEPTH_RANGE_PCT = 4.5
ALERT_INTERVAL_S = 3.0
DISPATCH_TICK_S = 1.0

REQUIRED_CHANNEL = "@l1xosha"   # users must be subscribed to this channel
# Comma-separated Telegram user IDs that bypass the subscription gate
_admin_ids_raw = os.getenv("ADMIN_IDS", "")
ADMIN_IDS: set[int] = {int(x.strip()) for x in _admin_ids_raw.split(",") if x.strip().isdigit()}


async def _is_subscribed(bot, user_id: int) -> bool:
    """Return True if user is a subscriber of REQUIRED_CHANNEL."""
    from telegram.error import BadRequest as TGBadRequest, NetworkError
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status not in ("left", "kicked", "banned")
    except TGBadRequest:
        # Never was a member → Telegram 400
        return False
    except (NetworkError, Exception):
        return True  # fail-open for network / unknown errors


_GATE_KB = InlineKeyboardMarkup([[
    InlineKeyboardButton("✅ Перевірити підписку", callback_data="check_sub"),
    InlineKeyboardButton(f"➡️ {REQUIRED_CHANNEL}", url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}"),
]])


async def _gate_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Runs before every handler (group -1). Blocks non-subscribers."""
    user = update.effective_user
    if user is None:
        raise ApplicationHandlerStop

    # Admins always bypass the gate
    if user.id in ADMIN_IDS:
        return

    # Let the "check_sub" callback through so we can handle it ourselves
    cq = update.callback_query
    if cq and cq.data == "check_sub":
        return  # handled separately below

    if not await _is_subscribed(context.bot, user.id):
        if update.message:
            await update.message.reply_text(
                f"🔒 Бот доступний лише для підписників каналу {REQUIRED_CHANNEL}\n\n"
                f"Підпишись на канал і натисни «✅ Перевірити підписку».\n\n"
                f"<i>Твій ID: <code>{user.id}</code></i>",
                reply_markup=_GATE_KB,
                parse_mode="HTML",
            )
        elif cq:
            await cq.answer(f"🔒 Підпишись на {REQUIRED_CHANNEL}", show_alert=True)
        raise ApplicationHandlerStop


async def _cb_check_sub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback: re-check subscription after user subscribes."""
    cq = update.callback_query
    await cq.answer()
    user = update.effective_user
    if user is None:
        return
    if await _is_subscribed(context.bot, user.id):
        await cq.edit_message_text(
            "✅ Підписку підтверджено! Тепер можеш користуватись ботом.\n\nНапиши /start"
        )
    else:
        await cq.edit_message_text(
            f"❌ Підписку не знайдено.\n\nПідпишись → {REQUIRED_CHANNEL} і натисни знову.",
            reply_markup=_GATE_KB,
        )

ASK_LEG_A_EX, ASK_LEG_A_SYM, ASK_LEG_B_EX, ASK_LEG_B_SYM = range(4)

# ── exchange menu ──────────────────────────────────────────────────────────────
_EXCHANGES = [
    # ── Futures first (most common use case) ──────────────────
    ("Aster Futures",    "aster",       "futures"),
    ("Hyperliquid",      "hyperliquid", "futures"),
    ("Binance Futures",  "binance",     "futures"),
    ("Bybit Futures",    "bybit",       "futures"),
    ("Gate Futures",     "gate",        "futures"),
    ("OKX Futures",      "okx",         "futures"),
    ("MEXC Futures",     "mexc",        "futures"),
    ("KuCoin Futures",   "kucoin",      "futures"),
    ("Bitget Futures",   "bitget",      "futures"),
    # ── Spot ──────────────────────────────────────────────────
    ("Binance Spot",     "binance",     "spot"),
    ("Bybit Spot",       "bybit",       "spot"),
    ("Gate Spot",        "gate",        "spot"),
    ("OKX Spot",         "okx",         "spot"),
    ("MEXC Spot",        "mexc",        "spot"),
    ("KuCoin Spot",      "kucoin",      "spot"),
    ("Bitget Spot",      "bitget",      "spot"),
]

# ticker format hints shown to the user after exchange selection
_SYM_HINT = {
    ("gate",        "futures"): "напр. `ANTHROPIC_USDT`",
    ("gate",        "spot"):    "напр. `ANTHROPIC_USDT`",
    ("binance",     "futures"): "напр. `ANTHROPICUSDT`",
    ("binance",     "spot"):    "напр. `ANTHROPICUSDT`",
    ("bybit",       "futures"): "напр. `ANTHROPICUSDT`",
    ("bybit",       "spot"):    "напр. `ANTHROPICUSDT`",
    ("okx",         "futures"): "напр. `ANTHROPIC-USDT-SWAP`",
    ("okx",         "spot"):    "напр. `ANTHROPIC-USDT`",
    ("mexc",        "futures"): "напр. `ANTHROPIC_USDT`",
    ("mexc",        "spot"):    "напр. `ANTHROPIC_USDT`",
    ("kucoin",      "futures"): "напр. `XBTUSDTM` (з M на кінці)",
    ("kucoin",      "spot"):    "напр. `ANTHROPIC-USDT`",
    ("bitget",      "futures"): "напр. `ANTHROPICUSDT`",
    ("bitget",      "spot"):    "напр. `ANTHROPICUSDT`",
    ("aster",       "futures"): "напр. `UBUSDT`",
    ("hyperliquid", "futures"): "напр. `ANTHROPIC`",
}

def _ex_keyboard(cb_prefix: str) -> InlineKeyboardMarkup:
    """Two-column exchange selection keyboard."""
    rows = []
    for i in range(0, len(_EXCHANGES), 2):
        row = []
        for label, ex, mt in _EXCHANGES[i:i+2]:
            row.append(InlineKeyboardButton(label, callback_data=f"{cb_prefix}:{ex}:{mt}"))
        rows.append(row)
    return InlineKeyboardMarkup(rows)

def _normalize_symbol(exchange: str, market_type: str, symbol: str) -> str:
    """Convert user-typed symbol to the API format required by each exchange."""
    sym = symbol.upper()
    if exchange == "hyperliquid":
        # HL uses coin names only: "BTCUSDT" → "BTC", "ETHUSDT" → "ETH"
        for suffix in ("USDT", "USDC", "USD", "PERP"):
            if sym.endswith(suffix) and len(sym) > len(suffix):
                return sym[:-len(suffix)]
    elif exchange == "okx":
        # OKX uses dash-separated: "LABUSDT" → "LAB-USDT-SWAP" or "LAB-USDT"
        if "-" not in sym:
            for quote in ("USDT", "USDC", "BTC", "ETH", "USD"):
                if sym.endswith(quote) and len(sym) > len(quote):
                    base = sym[:-len(quote)]
                    return f"{base}-{quote}-SWAP" if market_type == "futures" else f"{base}-{quote}"
    elif exchange == "kucoin" and market_type == "futures":
        # KuCoin futures: needs M suffix — BTCUSDT → BTCUSDTM
        if not sym.endswith("M"):
            sym = sym + "M"
    return sym


def _make_info(exchange: str, market_type: str, symbol: str) -> ExchangeInfo:
    label = next((lbl for lbl, ex, mt in _EXCHANGES if ex == exchange and mt == market_type), exchange)
    normalized = _normalize_symbol(exchange, market_type, symbol)
    return ExchangeInfo(exchange=exchange, symbol=normalized,
                        market_type=market_type, display=f"{label} · {normalized}")


# ─────────────────────────────────────────── data models

@dataclass
class SpreadConfig:
    spread_id: str
    leg_a: ExchangeInfo
    leg_b: ExchangeInfo
    in_threshold: float = 0.3
    out_threshold: float = -0.2
    out_max_threshold: float = 20.0
    oi_threshold: float = 6_900_000.0
    out_min_depth: float = 0.5
    alerts_on: bool = True
    pushover_on: bool = False   # per-spread Pushover toggle

    @property
    def name(self) -> str:
        return f"{self.leg_a.display} / {self.leg_b.display}"

    def to_redis_hash(self) -> dict:
        return {
            "leg_a_ex":   self.leg_a.exchange,
            "leg_a_sym":  self.leg_a.symbol,
            "leg_a_mt":   self.leg_a.market_type,
            "leg_a_disp": self.leg_a.display,
            "leg_b_ex":   self.leg_b.exchange,
            "leg_b_sym":  self.leg_b.symbol,
            "leg_b_mt":   self.leg_b.market_type,
            "leg_b_disp": self.leg_b.display,
            "in_threshold":      str(self.in_threshold),
            "out_threshold":     str(self.out_threshold),
            "out_max_threshold": str(self.out_max_threshold),
            "oi_threshold":      str(self.oi_threshold),
            "out_min_depth":     str(self.out_min_depth),
            "alerts_on":         "1" if self.alerts_on else "0",
            "pushover_on":       "1" if self.pushover_on else "0",
        }

    @classmethod
    def from_redis_hash(cls, spread_id: str, h: dict) -> "SpreadConfig":
        leg_a = ExchangeInfo(
            exchange=h["leg_a_ex"], symbol=h["leg_a_sym"],
            market_type=h["leg_a_mt"], display=h["leg_a_disp"],
        )
        leg_b = ExchangeInfo(
            exchange=h["leg_b_ex"], symbol=h["leg_b_sym"],
            market_type=h["leg_b_mt"], display=h["leg_b_disp"],
        )
        return cls(
            spread_id=spread_id, leg_a=leg_a, leg_b=leg_b,
            in_threshold=float(h.get("in_threshold", 0.3)),
            out_threshold=float(h.get("out_threshold", -0.2)),
            out_max_threshold=float(h.get("out_max_threshold", 20.0)),
            oi_threshold=float(h.get("oi_threshold", 6_900_000.0)),
            out_min_depth=float(h.get("out_min_depth", 0.5)),
            alerts_on=h.get("alerts_on", "1") == "1",
            pushover_on=h.get("pushover_on", "0") == "1",
        )


ALERT_CONFIRM_S = 2.0   # spread must hold above threshold this long before first alert


@dataclass
class AlertChannelState:
    active: bool = False
    last_fired: float = 0.0
    triggered_at: float = 0.0   # monotonic time when threshold was first crossed


@dataclass
class ChatRuntime:
    chat_id: int
    selected_spread_id: Optional[str] = None
    alert_states: dict = field(default_factory=dict)   # sid -> {ch: AlertChannelState}

    def channel(self, sid: str, name: str) -> AlertChannelState:
        self.alert_states.setdefault(sid, {})
        self.alert_states[sid].setdefault(name, AlertChannelState())
        return self.alert_states[sid][name]


# ─────────────────────────────────────────── Redis store

class RedisStore:
    def __init__(self, client) -> None:
        self._r = client

    async def register_chat(self, chat_id: int) -> None:
        await self._r.sadd("chats", str(chat_id))

    async def all_chat_ids(self) -> list[int]:
        ids = await self._r.smembers("chats")
        return [int(i) for i in ids]

    async def get_selected_spread(self, chat_id: int) -> Optional[str]:
        return await self._r.get(f"chat:{chat_id}:selected")

    async def set_selected_spread(self, chat_id: int, sid: str) -> None:
        await self._r.set(f"chat:{chat_id}:selected", sid)

    async def save_spread(self, chat_id: int, cfg: SpreadConfig) -> None:
        await self._r.hset(f"spread:{cfg.spread_id}", mapping=cfg.to_redis_hash())
        await self._r.sadd(f"spreads:{chat_id}", cfg.spread_id)

    async def delete_spread(self, chat_id: int, sid: str) -> None:
        await self._r.delete(f"spread:{sid}")
        await self._r.srem(f"spreads:{chat_id}", sid)

    async def load_spreads(self, chat_id: int) -> list[SpreadConfig]:
        ids = await self._r.smembers(f"spreads:{chat_id}")
        out = []
        for sid in ids:
            h = await self._r.hgetall(f"spread:{sid}")
            if h:
                try:
                    out.append(SpreadConfig.from_redis_hash(sid, h))
                except Exception as e:
                    log.warning("skip bad spread %s: %s", sid, e)
        return out

    async def update_field(self, sid: str, key: str, value: str) -> None:
        await self._r.hset(f"spread:{sid}", key, value)

    async def set_monitor_spread(self, sid: str) -> None:
        await self._r.set("monitor:active_spread", sid)
        # bump a counter so the watcher detects the change even for the same spread
        await self._r.incr("monitor:active_spread_ts")

    async def get_pushover_key(self, chat_id: int) -> Optional[str]:
        return await self._r.get(f"chat:{chat_id}:pushover_key")

    async def set_pushover_key(self, chat_id: int, key: str) -> None:
        await self._r.set(f"chat:{chat_id}:pushover_key", key)


# ─────────────────────────────────────────── bot

class SpreadBot:
    def __init__(self, token: str) -> None:
        self._token = token
        self._store: Optional[RedisStore] = None
        self._feed:  Optional[FeedManager] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._app: Optional[Application] = None
        self._runtimes:    dict[int, ChatRuntime]   = {}
        self._spreads:     dict[str, SpreadConfig]  = {}
        self._chat_spreads:dict[int, list[str]]     = {}

    async def _post_init(self, app: Application) -> None:
        self._app = app

        redis_url = os.getenv("REDIS_URL", "").strip()
        if not redis_url:
            raise SystemExit("REDIS_URL not set")
        try:
            rc = redis_from_url(redis_url, decode_responses=True)
            await rc.ping()
        except Exception as e:
            raise SystemExit(f"Redis error: {e}") from e

        self._store = RedisStore(rc)
        self._session = aiohttp.ClientSession()
        self._feed = FeedManager()
        await self._feed.start(self._session)

        await self._hydrate()
        asyncio.create_task(self._dispatch_loop())

    async def _hydrate(self) -> None:
        assert self._store is not None and self._feed is not None
        chat_ids = await self.store.all_chat_ids()
        for cid in chat_ids:
            rt = ChatRuntime(chat_id=cid)
            rt.selected_spread_id = await self.store.get_selected_spread(cid)
            self._runtimes[cid] = rt
            spreads = await self.store.load_spreads(cid)
            self._chat_spreads[cid] = [s.spread_id for s in spreads]
            for cfg in spreads:
                self._spreads[cfg.spread_id] = cfg
                await self.feed.subscribe(cfg.leg_a)
                await self.feed.subscribe(cfg.leg_b)
        log.info("hydrated %d chats, %d spreads", len(chat_ids), len(self._spreads))
        # restart notification intentionally removed — too noisy

    @property
    def store(self) -> RedisStore:
        assert self._store is not None, "store not initialised"
        return self._store

    @property
    def feed(self) -> FeedManager:
        assert self._feed is not None, "feed not initialised"
        return self._feed

    def _rt(self, cid: int) -> ChatRuntime:
        if cid not in self._runtimes:
            self._runtimes[cid] = ChatRuntime(chat_id=cid)
        return self._runtimes[cid]

    def _selected(self, cid: int) -> Optional[SpreadConfig]:
        rt = self._rt(cid)
        sid = rt.selected_spread_id
        if sid and sid in self._spreads:
            return self._spreads[sid]
        ids = self._chat_spreads.get(cid, [])
        if ids:
            rt.selected_spread_id = ids[0]
            return self._spreads.get(ids[0])
        return None

    # ─── commands

    async def cmd_start(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        cid = u.effective_chat.id
        await self.store.register_chat(cid)
        self._runtimes.setdefault(cid, ChatRuntime(chat_id=cid))
        self._chat_spreads.setdefault(cid, [])
        await u.message.reply_text(
            "👋 Spread monitor bot.\n\n"
            "Use /newspread to add a spread.\n"
            "/help for all commands."
        )

    async def cmd_help(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        await u.message.reply_text(
            "/newspread — додати спред\n"
            "/spreads — список спредів\n"
            "/status — поточні ціни\n"
            "/set_in /set_out — пороги\n"
            "/alerts_on | /alerts_off — toggle\n"
            "/set_pushover <key> — Pushover User Key\n"
            "/test_push — тест пуш\n"
            "/debug — статус з'єднань\n"
            "/cancel — скасувати"
        )

    # ─── Pushover

    async def _send_pushover(self, user_key: str, title: str, message: str,
                              priority: int = 1) -> None:
        """Send a Pushover notification. priority=1 bypasses quiet hours."""
        app_token = os.getenv("PUSHOVER_APP_TOKEN", "").strip()
        if not app_token:
            return
        data = {
            "token":    app_token,
            "user":     user_key,
            "title":    title,
            "message":  message,
            "sound":    "siren",
            "priority": str(priority),
        }
        try:
            assert self._session is not None
            async with self._session.post(
                "https://api.pushover.net/1/messages.json",
                data=data,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                body = await resp.json(content_type=None)
                if body.get("status") != 1:
                    log.warning("pushover error: %s", body)
        except Exception as e:
            log.debug("pushover send failed: %s", e)

    async def cmd_set_pushover(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        cid = u.effective_chat.id
        if not c.args:
            key = await self.store.get_pushover_key(cid)
            status = f"Встановлено ({'*****' + key[-4:] if key else 'немає'})"
            await u.message.reply_text(
                f"Pushover: {status}\n\nВикористання: /set_pushover YOUR_USER_KEY\n"
                "Ключ знайдеш на pushover.net (головна сторінка, 30 символів)."
            )
            return
        key = c.args[0].strip()
        if len(key) < 20:
            await u.message.reply_text("❌ Ключ занадто короткий.")
            return
        await self.store.set_pushover_key(cid, key)
        await u.message.reply_text("✅ Pushover ключ збережено! Тест: /test_push")

    async def cmd_test_push(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        cid = u.effective_chat.id
        key = await self.store.get_pushover_key(cid)
        if not key:
            await u.message.reply_text("Спочатку /set_pushover YOUR_KEY")
            return
        await u.message.reply_text("Надсилаю тестовий пуш...")
        await self._send_pushover(key,
            "🧪 Тест Spread Bot",
            "Пуш-нотифікації працюють! Звук: сирена 🚨"
        )
        await u.message.reply_text("✅ Відправлено! Перевіряй телефон.")

    # ─── /newspread conversation

    async def cmd_newspread(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> int:
        await u.message.reply_text(
            "📊 *Новий спред — Leg A (SHORT)*\nВибери біржу:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_ex_keyboard("leg_a"),
        )
        return ASK_LEG_A_EX

    async def conv_leg_a_ex(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> int:
        q = u.callback_query
        await q.answer()
        _, exchange, market_type = q.data.split(":", 2)
        c.user_data["leg_a_ex"] = exchange
        c.user_data["leg_a_mt"] = market_type
        hint = _SYM_HINT.get((exchange, market_type), "введи тікер")
        label = next((lbl for lbl, ex, mt in _EXCHANGES if ex == exchange and mt == market_type), exchange)
        await q.edit_message_text(
            f"✅ Leg A: *{label}*\n\nВведи тікер ({hint}):",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ASK_LEG_A_SYM

    async def conv_leg_a_sym(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> int:
        sym = u.message.text.strip().upper()
        c.user_data["leg_a_sym"] = sym
        exchange = c.user_data["leg_a_ex"]
        market_type = c.user_data["leg_a_mt"]
        label = next((lbl for lbl, ex, mt in _EXCHANGES if ex == exchange and mt == market_type), exchange)
        await u.message.reply_text(
            f"✅ Leg A: *{label} · {sym}*\n\n📊 *Leg B (LONG)* — вибери біржу:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_ex_keyboard("leg_b"),
        )
        return ASK_LEG_B_EX

    async def conv_leg_b_ex(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> int:
        q = u.callback_query
        await q.answer()
        _, exchange, market_type = q.data.split(":", 2)
        c.user_data["leg_b_ex"] = exchange
        c.user_data["leg_b_mt"] = market_type
        hint = _SYM_HINT.get((exchange, market_type), "введи тікер")
        label = next((lbl for lbl, ex, mt in _EXCHANGES if ex == exchange and mt == market_type), exchange)
        await q.edit_message_text(
            f"✅ Leg B: *{label}*\n\nВведи тікер ({hint}):",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ASK_LEG_B_SYM

    async def conv_leg_b_sym(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> int:
        cid = u.effective_chat.id
        sym_b = u.message.text.strip().upper()
        ud = c.user_data
        info_a = _make_info(ud["leg_a_ex"], ud["leg_a_mt"], ud["leg_a_sym"])
        info_b = _make_info(ud["leg_b_ex"], ud["leg_b_mt"], sym_b)
        c.user_data.clear()

        sid = str(uuid.uuid4())[:8]
        cfg = SpreadConfig(spread_id=sid, leg_a=info_a, leg_b=info_b)
        await self.store.register_chat(cid)
        await self.store.save_spread(cid, cfg)
        self._spreads[sid] = cfg
        self._chat_spreads.setdefault(cid, []).append(sid)
        await self.feed.subscribe(info_a)
        await self.feed.subscribe(info_b)
        rt = self._rt(cid)
        rt.selected_spread_id = sid
        await self.store.set_selected_spread(cid, sid)
        await u.message.reply_text(
            f"✅ Спред додано!\n*{cfg.name}*\n\n"
            f"Пороги: IN {cfg.in_threshold}% | OUT {cfg.out_threshold}%\n"
            "Дивись /status",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    async def conv_cancel(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> int:
        c.user_data.clear()
        await u.message.reply_text("Скасовано.")
        return ConversationHandler.END

    # ─── /spreads

    async def cmd_spreads(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        cid = u.effective_chat.id
        ids = self._chat_spreads.get(cid, [])
        if not ids:
            await u.message.reply_text("No spreads yet. Use /newspread.")
            return
        rt = self._rt(cid)
        kb = []
        for sid in ids:
            cfg = self._spreads.get(sid)
            if not cfg:
                continue
            tick = "✅ " if sid == rt.selected_spread_id else ""
            kb.append([InlineKeyboardButton(f"{tick}{cfg.name}", callback_data=f"sel:{sid}")])
        kb.append([InlineKeyboardButton("➕ New spread", callback_data="newspread")])
        await u.message.reply_text("Your spreads:", reply_markup=InlineKeyboardMarkup(kb))

    async def cb_query(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        q = u.callback_query
        await q.answer()
        cid = u.effective_chat.id
        data: str = q.data

        if data == "newspread":
            await q.message.reply_text(
                "📊 *Новий спред — Leg A (SHORT)*\nВибери біржу:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_ex_keyboard("leg_a"),
            )
        elif data.startswith("sel:"):
            sid = data[4:]
            cfg = self._spreads.get(sid)
            if not cfg:
                await q.edit_message_text("Spread not found.")
                return
            rt = self._rt(cid)
            rt.selected_spread_id = sid
            await self.store.set_selected_spread(cid, sid)
            await self._show_detail(q, cid, cfg)
        elif data.startswith("del:"):
            sid = data[4:]
            await self.store.delete_spread(cid, sid)
            self._spreads.pop(sid, None)
            ids = self._chat_spreads.get(cid, [])
            if sid in ids:
                ids.remove(sid)
            rt = self._rt(cid)
            if rt.selected_spread_id == sid:
                rt.selected_spread_id = ids[0] if ids else None
            await q.edit_message_text("🗑 Spread deleted.")
        elif data == "noop":
            pass  # label-only button
        elif data.startswith("adj_in:") or data.startswith("adj_out:"):
            kind, rest = data.split(":", 1)
            sid, delta_s = rest.rsplit(":", 1)
            cfg = self._spreads.get(sid)
            if cfg:
                try:
                    delta = float(delta_s)
                    if kind == "adj_in":
                        # IN is always >= 0 (entry only makes sense on positive spread)
                        cfg.in_threshold = max(0.0, round(cfg.in_threshold + delta, 3))
                        await self.store.update_field(sid, "in_threshold", str(cfg.in_threshold))
                    else:
                        # OUT can go negative (exit at a loss is normal, e.g. -0.2 to -1.0)
                        cfg.out_threshold = round(cfg.out_threshold + delta, 3)
                        await self.store.update_field(sid, "out_threshold", str(cfg.out_threshold))
                    await self._show_detail(q, cid, cfg)
                except Exception:
                    pass
        elif data.startswith("mute:"):
            sid = data[5:]
            cfg = self._spreads.get(sid)
            if cfg:
                cfg.alerts_on = False
                await self.store.update_field(sid, "alerts_on", "0")
                await q.answer("🔕 Алерти вимкнено", show_alert=False)
                try:
                    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔔 Увімкнути алерти", callback_data=f"unmute:{sid}"),
                    ]]))
                except Exception:
                    pass
        elif data.startswith("unmute:"):
            sid = data[7:]
            cfg = self._spreads.get(sid)
            if cfg:
                cfg.alerts_on = True
                await self.store.update_field(sid, "alerts_on", "1")
                await q.answer("🔔 Алерти увімкнено", show_alert=False)
                try:
                    await q.edit_message_reply_markup(reply_markup=None)
                except Exception:
                    pass
        elif data.startswith("toggle:"):
            sid = data[7:]
            cfg = self._spreads.get(sid)
            if cfg:
                cfg.alerts_on = not cfg.alerts_on
                await self.store.update_field(sid, "alerts_on", "1" if cfg.alerts_on else "0")
                await self._show_detail(q, cid, cfg)
        elif data.startswith("push_toggle:"):
            sid = data[12:]
            cfg = self._spreads.get(sid)
            if cfg:
                # Check if pushover key is configured for this chat
                pk = await self.store.get_pushover_key(cid)
                if not pk:
                    await q.answer("⚠️ Спочатку встанови Pushover ключ: /set_pushover YOUR_KEY",
                                   show_alert=True)
                    return
                cfg.pushover_on = not cfg.pushover_on
                await self.store.update_field(sid, "pushover_on", "1" if cfg.pushover_on else "0")
                await self._show_detail(q, cid, cfg)
        elif data.startswith("terminal:"):
            sid = data[9:]
            await self.store.set_monitor_spread(sid)
            await q.answer("📺 Terminal synced!", show_alert=False)

    def _detail_kb(self, cfg: SpreadConfig) -> InlineKeyboardMarkup:
        sid = cfg.spread_id
        it = cfg.in_threshold
        ot = cfg.out_threshold
        al = "🔔 ON" if cfg.alerts_on else "🔕 OFF"
        pu = "📳 Push ON" if cfg.pushover_on else "📴 Push OFF"
        return InlineKeyboardMarkup([
            # IN label row
            [InlineKeyboardButton(f"📉 IN threshold: {it:.2f}%", callback_data="noop")],
            # IN adjust row
            [
                InlineKeyboardButton("−0.5", callback_data=f"adj_in:{sid}:-0.5"),
                InlineKeyboardButton("−0.1", callback_data=f"adj_in:{sid}:-0.1"),
                InlineKeyboardButton("+0.1", callback_data=f"adj_in:{sid}:+0.1"),
                InlineKeyboardButton("+0.5", callback_data=f"adj_in:{sid}:+0.5"),
            ],
            # OUT label row
            [InlineKeyboardButton(f"📈 OUT threshold: {ot:.2f}%", callback_data="noop")],
            # OUT adjust row
            [
                InlineKeyboardButton("−0.5", callback_data=f"adj_out:{sid}:-0.5"),
                InlineKeyboardButton("−0.1", callback_data=f"adj_out:{sid}:-0.1"),
                InlineKeyboardButton("+0.1", callback_data=f"adj_out:{sid}:+0.1"),
                InlineKeyboardButton("+0.5", callback_data=f"adj_out:{sid}:+0.5"),
            ],
            # Alerts + Pushover toggle
            [
                InlineKeyboardButton(al, callback_data=f"toggle:{sid}"),
                InlineKeyboardButton(pu, callback_data=f"push_toggle:{sid}"),
            ],
            # Terminal + Delete
            [
                InlineKeyboardButton("📺 Terminal", callback_data=f"terminal:{sid}"),
                InlineKeyboardButton("🗑 Delete",   callback_data=f"del:{sid}"),
            ],
        ])

    async def _show_detail(self, q, cid: int, cfg: SpreadConfig) -> None:
        ss = self.feed.spread_snapshot(cfg.leg_a, cfg.leg_b)
        in_s  = f"{ss.in_spread_pct:+.3f}%"  if ss.in_spread_pct  is not None else "N/A"
        out_s = f"{ss.out_spread_pct:+.3f}%" if ss.out_spread_pct is not None else "N/A"
        alerts = "ON ✅" if cfg.alerts_on else "OFF 🔇"
        text = (
            f"*{cfg.name}*\n\n"
            f"IN: `{in_s}` | OUT: `{out_s}`\n"
            f"Thresholds: IN {cfg.in_threshold}% | OUT {cfg.out_threshold}%\n"
            f"Alerts: {alerts}"
        )
        try:
            await q.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                       reply_markup=self._detail_kb(cfg))
        except Exception:
            pass

    # ─── /status

    async def cmd_status(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        cid = u.effective_chat.id
        cfg = self._selected(cid)
        if cfg is None:
            await u.message.reply_text("No spread selected. Use /newspread first.")
            return
        ss = self.feed.spread_snapshot(cfg.leg_a, cfg.leg_b)
        await u.message.reply_text(_fmt_status(cfg, ss), parse_mode=ParseMode.MARKDOWN)

    # ─── /debug — show exchange connection state

    async def cmd_debug(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        mgr = self.feed
        lines = ["*🔌 Exchange connections*\n"]
        for (ex_name, mt), ex in mgr._exchanges.items():
            ws_ok = ex._ws is not None
            tasks_ok = bool(ex._tasks)
            # Build the WS URL that would be used
            try:
                ws_url = ex._ws_url()
                ws_url_short = ws_url[len("wss://"):60] + "…"
            except Exception:
                ws_url_short = "?"
            lines.append(f"*{ex_name}/{mt}*")
            lines.append(f"  tasks={tasks_ok} ws_open={ws_ok}")
            lines.append(f"  url: `{ws_url_short}`")
            for sym, state in ex._state.items():
                ts = state.get("ts", 0.0)
                age = time.monotonic() - ts if ts else None
                if age is None or age > 3.0:
                    status = f"⚠️ NO DATA ({age:.0f}s)" if age else "⚠️ NO DATA"
                else:
                    bid = state.get("bid")
                    status = f"✅ {age:.1f}s bid={bid}"
                lines.append(f"  sym `{sym}`: {status}")
        await u.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    # ─── threshold setters

    async def _set_field(self, u: Update, c: ContextTypes.DEFAULT_TYPE,
                          attr: str, label: str) -> None:
        cid = u.effective_chat.id
        cfg = self._selected(cid)
        if cfg is None:
            await u.message.reply_text("No spread selected.")
            return
        if not c.args:
            await u.message.reply_text(f"Current {label}: {getattr(cfg, attr)}\nUsage: /{label} <value>")
            return
        try:
            val = float(c.args[0])
        except ValueError:
            await u.message.reply_text(f"Invalid: {c.args[0]}")
            return
        setattr(cfg, attr, val)
        await self.store.update_field(cfg.spread_id, attr, str(val))
        await u.message.reply_text(f"✅ {label} = {val}")

    async def cmd_set_in(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        await self._set_field(u, c, "in_threshold", "set_in")

    async def cmd_set_out(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        await self._set_field(u, c, "out_threshold", "set_out")

    async def cmd_set_outmax(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        await self._set_field(u, c, "out_max_threshold", "set_outmax")

    async def cmd_set_oi(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        await self._set_field(u, c, "oi_threshold", "set_oi")

    async def cmd_set_out_depth(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        await self._set_field(u, c, "out_min_depth", "set_out_depth")

    async def cmd_thresholds(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        cid = u.effective_chat.id
        cfg = self._selected(cid)
        if cfg is None:
            await u.message.reply_text("No spread selected.")
            return
        await u.message.reply_text(
            f"*{cfg.name}*\n\n"
            f"IN: {cfg.in_threshold}%  OUT: {cfg.out_threshold}%\n"
            f"OUT-max: {cfg.out_max_threshold}%\n"
            f"OI: ${cfg.oi_threshold:,.0f}\n"
            f"Min depth: {cfg.out_min_depth} coins\n"
            f"Alerts: {'ON' if cfg.alerts_on else 'OFF'}",
            parse_mode=ParseMode.MARKDOWN,
        )

    async def cmd_alerts_on(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        cid = u.effective_chat.id
        cfg = self._selected(cid)
        if not cfg:
            await u.message.reply_text("No spread selected.")
            return
        cfg.alerts_on = True
        await self.store.update_field(cfg.spread_id, "alerts_on", "1")
        await u.message.reply_text("✅ Alerts ON")

    async def cmd_alerts_off(self, u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
        cid = u.effective_chat.id
        cfg = self._selected(cid)
        if not cfg:
            await u.message.reply_text("No spread selected.")
            return
        cfg.alerts_on = False
        await self.store.update_field(cfg.spread_id, "alerts_on", "0")
        await u.message.reply_text("🔇 Alerts OFF")

    # ─── dispatcher

    async def _dispatch_loop(self) -> None:
        bot: Bot = self._app.bot
        while True:
            await asyncio.sleep(DISPATCH_TICK_S)
            now = time.monotonic()
            for cid, rt in list(self._runtimes.items()):
                for sid in list(self._chat_spreads.get(cid, [])):
                    cfg = self._spreads.get(sid)
                    if not cfg or not cfg.alerts_on:
                        continue
                    try:
                        ss = self.feed.spread_snapshot(cfg.leg_a, cfg.leg_b)
                        await self._check(bot, cid, cfg, ss, rt, now)
                    except Exception as e:
                        log.debug("dispatch err cid=%s sid=%s: %s", cid, sid, e)

    async def _check(self, bot, cid, cfg, ss: SpreadSnapshot, rt, now) -> None:
        sid = cfg.spread_id

        async def _fire(kind: str, ch: AlertChannelState, msg_text: str, push_title: str,
                        above: bool) -> None:
            if above:
                # Start / keep the confirmation timer
                if ch.triggered_at == 0.0:
                    ch.triggered_at = now          # first tick above threshold
                held = now - ch.triggered_at
                if held < ALERT_CONFIRM_S:
                    return                          # not held long enough yet
                # Confirmed — fire if cooldown elapsed
                if now - ch.last_fired < ALERT_INTERVAL_S:
                    return
                ch.last_fired = now
                await _send_alert(bot, cid, cfg, ss, kind)
                if cfg.pushover_on:
                    pk = await self.store.get_pushover_key(cid)
                    if pk:
                        await self._send_pushover(pk, push_title, msg_text)
            else:
                # Below threshold — reset confirmation timer and active flag
                ch.triggered_at = 0.0
                ch.active = False

        # IN
        if ss.in_spread_pct is not None:
            ch = rt.channel(sid, "in")
            above = ss.in_spread_pct >= cfg.in_threshold
            pct_s = f"{ss.in_spread_pct:+.3f}%"
            await _fire("IN", ch,
                f"IN Spread: {pct_s}\n{cfg.leg_a.display} / {cfg.leg_b.display}",
                f"🟢 ARB ENTRY {pct_s}", above)

        # OUT
        if ss.out_spread_pct is not None:
            ch = rt.channel(sid, "out")
            depth_ok = True
            if cfg.out_min_depth > 0:
                da = ss.out_depth_a(OUT_DEPTH_RANGE_PCT)
                db = ss.out_depth_b(OUT_DEPTH_RANGE_PCT)
                depth_ok = da >= cfg.out_min_depth and db >= cfg.out_min_depth
            above = ss.out_spread_pct >= cfg.out_threshold and depth_ok
            pct_s = f"{ss.out_spread_pct:+.3f}%"
            await _fire("OUT", ch,
                f"OUT Spread: {pct_s}\n{cfg.leg_a.display} / {cfg.leg_b.display}",
                f"🔴 ARB EXIT {pct_s}", above)

        # OUT-MAX
        if ss.out_spread_pct is not None:
            ch = rt.channel(sid, "out_max")
            above = ss.out_spread_pct >= cfg.out_max_threshold
            pct_s = f"{ss.out_spread_pct:+.3f}%"
            await _fire("OUT_MAX", ch,
                f"OUT MAX: {pct_s}\n{cfg.leg_a.display} / {cfg.leg_b.display}",
                f"🔥 OUT MAX {pct_s}", above)

        # OI
        oi_vals = [v for v in (ss.leg_a.oi_usd, ss.leg_b.oi_usd) if v is not None]
        if oi_vals:
            ch = rt.channel(sid, "oi")
            max_oi = max(oi_vals)
            above = max_oi >= cfg.oi_threshold
            await _fire("OI", ch,
                f"OI: ${max_oi:,.0f}\n{cfg.leg_a.display} / {cfg.leg_b.display}",
                f"📊 OI Alert ${max_oi/1e6:.1f}M", above)


# ─────────────────────────────────────────── formatters

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_status(cfg: SpreadConfig, ss: SpreadSnapshot) -> str:
    a, b = ss.leg_a, ss.leg_b
    in_p  = f"{ss.in_spread_pct:+.3f}%"  if ss.in_spread_pct  is not None else "N/A"
    out_p = f"{ss.out_spread_pct:+.3f}%" if ss.out_spread_pct is not None else "N/A"
    in_u  = f"${ss.in_spread_usd:+.4f}"  if ss.in_spread_usd  is not None else ""
    out_u = f"${ss.out_spread_usd:+.4f}" if ss.out_spread_usd is not None else ""
    af = f"{a.funding*100:.4f}%" if a.funding is not None else "N/A"
    bf = f"{b.funding*100:.4f}%" if b.funding is not None else "N/A"
    sa = " ⚠️STALE" if a.stale else ""
    sb = " ⚠️STALE" if b.stale else ""
    return (
        f"*{cfg.name}*\n\n"
        f"*A* {cfg.leg_a.display}{sa}\n"
        f"  bid `{a.bid}` ask `{a.ask}` fund {af}\n\n"
        f"*B* {cfg.leg_b.display}{sb}\n"
        f"  bid `{b.bid}` ask `{b.ask}` fund {bf}\n\n"
        f"IN  `{in_p}` {in_u}\n"
        f"OUT `{out_p}` {out_u}\n\n"
        f"⏰ {_ts()}"
    )


async def _send_alert(bot, cid, cfg: SpreadConfig, ss: SpreadSnapshot, ch: str) -> None:
    a, b = ss.leg_a, ss.leg_b
    af = f"{a.funding*100:.4f}%" if a.funding is not None else "N/A"
    bf = f"{b.funding*100:.4f}%" if b.funding is not None else "N/A"
    sid = cfg.spread_id
    mute_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔕 Вимкнути алерти", callback_data=f"mute:{sid}"),
    ]])
    if ch == "IN":
        p = f"{ss.in_spread_pct:+.3f}%" if ss.in_spread_pct is not None else "N/A"
        u = f"${ss.in_spread_usd:+.4f}" if ss.in_spread_usd is not None else ""
        main = (
            f"🟢 ARB ENTRY SIGNAL\n"
            f"📈 IN Spread: {p} ({u})\n"
            f"{cfg.leg_a.display} bid: {a.bid} | {cfg.leg_b.display} ask: {b.ask}\n"
            f"Fund: A {af} / B {bf}\n"
            f"⏰ {_ts()}"
        )
        follow = f"↗️ STILL OPEN IN {p}"
    else:
        p = f"{ss.out_spread_pct:+.3f}%" if ss.out_spread_pct is not None else "N/A"
        u = f"${ss.out_spread_usd:+.4f}" if ss.out_spread_usd is not None else ""
        emoji = "🚨" if ch == "OUT_MAX" else "🔴"
        label = "EXTREME EXIT" if ch == "OUT_MAX" else "ARB EXIT SIGNAL"
        main = (
            f"{emoji} {label}\n"
            f"📉 OUT Spread: {p} ({u})\n"
            f"{cfg.leg_b.display} bid: {b.bid} | {cfg.leg_a.display} ask: {a.ask}\n"
            f"Fund: A {af} / B {bf}\n"
            f"⏰ {_ts()}"
        )
        follow = f"↘️ STILL OPEN OUT {p}"
    await _safe_send(bot, cid, main, reply_markup=mute_kb)
    await asyncio.sleep(0.1)
    await _safe_send(bot, cid, follow)


async def _send_oi_alert(bot, cid, cfg: SpreadConfig, oi: float) -> None:
    await _safe_send(bot, cid, f"⚡ HIGH OI  ${oi/1e6:.2f}M\n{cfg.name}\n⏰ {_ts()}")
    await asyncio.sleep(0.1)
    await _safe_send(bot, cid, f"⚡ OI STILL ${oi/1e6:.2f}M")


async def _safe_send(bot, cid, text, reply_markup=None) -> None:
    try:
        await bot.send_message(cid, text, reply_markup=reply_markup)
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        await bot.send_message(cid, text, reply_markup=reply_markup)
    except TelegramError as e:
        log.warning("send error %s: %s", cid, e)


# ─────────────────────────────────────────── entry point

def main() -> None:
    """Synchronous entry point — PTB manages its own event loop via run_polling()."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("TELEGRAM_BOT_TOKEN not set")

    bot_core = SpreadBot(token)

    app = (
        Application.builder()
        .token(token)
        .post_init(bot_core._post_init)
        .build()
    )

    conv = ConversationHandler(
        entry_points=[CommandHandler("newspread", bot_core.cmd_newspread)],
        states={
            ASK_LEG_A_EX:  [CallbackQueryHandler(bot_core.conv_leg_a_ex,  pattern=r"^leg_a:")],
            ASK_LEG_A_SYM: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot_core.conv_leg_a_sym)],
            ASK_LEG_B_EX:  [CallbackQueryHandler(bot_core.conv_leg_b_ex,  pattern=r"^leg_b:")],
            ASK_LEG_B_SYM: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot_core.conv_leg_b_sym)],
        },
        fallbacks=[CommandHandler("cancel", bot_core.conv_cancel)],
        per_message=False,
    )

    # Gate: runs before every handler — blocks non-subscribers
    app.add_handler(TypeHandler(Update, _gate_check), group=-1)
    # "Check subscription" button — runs AFTER gate (gate lets check_sub pass)
    app.add_handler(CallbackQueryHandler(_cb_check_sub, pattern=r"^check_sub$"), group=0)

    app.add_handler(conv)
    app.add_handler(CommandHandler("start",          bot_core.cmd_start))
    app.add_handler(CommandHandler("help",           bot_core.cmd_help))
    app.add_handler(CommandHandler("spreads",        bot_core.cmd_spreads))
    app.add_handler(CommandHandler("status",         bot_core.cmd_status))
    app.add_handler(CommandHandler("set_in",         bot_core.cmd_set_in))
    app.add_handler(CommandHandler("set_out",        bot_core.cmd_set_out))
    app.add_handler(CommandHandler("set_outmax",     bot_core.cmd_set_outmax))
    app.add_handler(CommandHandler("set_oi",         bot_core.cmd_set_oi))
    app.add_handler(CommandHandler("set_out_depth",  bot_core.cmd_set_out_depth))
    app.add_handler(CommandHandler("thresholds",     bot_core.cmd_thresholds))
    app.add_handler(CommandHandler("alerts_on",      bot_core.cmd_alerts_on))
    app.add_handler(CommandHandler("alerts_off",     bot_core.cmd_alerts_off))
    app.add_handler(CommandHandler("set_pushover",   bot_core.cmd_set_pushover))
    app.add_handler(CommandHandler("test_push",      bot_core.cmd_test_push))
    app.add_handler(CommandHandler("debug",          bot_core.cmd_debug))
    app.add_handler(CallbackQueryHandler(bot_core.cb_query))

    log.info("Bot starting…")
    app.run_polling(drop_pending_updates=True)   # synchronous — owns its own event loop


if __name__ == "__main__":
    main()
