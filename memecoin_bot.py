"""
Memecoin Auto-Scanner Bot v2
- Scans DexScreener every 60 seconds (latest + boosted + trending feeds)
- Age limit: 60 minutes
- Liquidity: > $10K
- Rug score: LOW only (80+)
- Top holder: < 20%
- Group scanner: monitors every group the bot is in for contract addresses / coin mentions
- Sends private alert to CHAT_ID when anything passes filters
"""

import asyncio
import logging
import os
import re
import time
import json

import aiohttp
from telegram import Bot, Update
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    MessageHandler,
    filters,
    ContextTypes,
)

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHAT_ID     = os.getenv("CHAT_ID",   "YOUR_CHAT_ID_HERE")

SCAN_INTERVAL   = 60        # seconds between auto-scans
MIN_LIQUIDITY   = 10_000    # USD
MAX_TOP_HOLDER  = 20.0      # percent
MAX_AGE_MINUTES = 60        # extended to 60 min
SEEN_FILE       = "seen_tokens.json"

# DexScreener endpoints
DEXSCREENER_LATEST  = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_BOOSTED = "https://api.dexscreener.com/token-boosts/latest/v1"
DEXSCREENER_TOP     = "https://api.dexscreener.com/token-boosts/top/v1"
DEXSCREENER_PAIRS   = "https://api.dexscreener.com/latest/dex/tokens/{address}"
DEXSCREENER_SEARCH  = "https://api.dexscreener.com/latest/dex/search?q={query}"

RUGCHECK_SUMMARY = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
RUGCHECK_FULL    = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"

# Solana address pattern (base58, 32-44 chars)
SOLANA_ADDR_RE = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')
# EVM address pattern
EVM_ADDR_RE    = re.compile(r'\b0x[a-fA-F0-9]{40}\b')
# Coin symbol pattern e.g. $PEPE $DOGE
SYMBOL_RE      = re.compile(r'\$([A-Z]{2,10})\b')

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ── Seen-token persistence ────────────────────────────────────────────────────

def load_seen() -> set:
    try:
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_seen(seen: set):
    try:
        with open(SEEN_FILE, "w") as f:
            json.dump(list(seen), f)
    except Exception as e:
        log.warning("Could not save seen file: %s", e)


# ── Formatters ────────────────────────────────────────────────────────────────

def fmt_usd(n) -> str:
    try:
        n = float(n)
        if n >= 1_000_000:
            return f"${n/1_000_000:.2f}M"
        if n >= 1_000:
            return f"${n/1_000:.1f}K"
        return f"${n:.2f}"
    except Exception:
        return "N/A"


def minutes_ago(unix_ms: int) -> str:
    diff = (int(time.time() * 1000) - unix_ms) / 60_000
    if diff < 60:
        return f"{int(diff)}m ago"
    if diff < 1440:
        return f"{diff/60:.1f}h ago"
    return f"{diff/1440:.1f}d ago"


def pct_arrow(v) -> str:
    try:
        v = float(v)
        return f"{'up' if v >= 0 else 'down'} {v:+.2f}%"
    except Exception:
        return "N/A"


def rug_label(score: int) -> str:
    if score >= 80:
        return f"LOW ({score}/100)"
    if score >= 50:
        return f"MEDIUM ({score}/100)"
    if score >= 30:
        return f"HIGH ({score}/100)"
    return f"VERY HIGH ({score}/100)"


# ── API helpers ───────────────────────────────────────────────────────────────

async def fetch_json(session, url: str) -> list | dict | None:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        log.warning("fetch error %s: %s", url[:60], e)
    return None


async def get_all_latest_tokens(session) -> list[str]:
    """Pull token addresses from latest + boosted + top feeds."""
    addresses = []
    for url in [DEXSCREENER_LATEST, DEXSCREENER_BOOSTED, DEXSCREENER_TOP]:
        data = await fetch_json(session, url)
        if isinstance(data, list):
            for item in data:
                addr = item.get("tokenAddress") or item.get("address") or ""
                if addr:
                    addresses.append(addr)
    log.info("Collected %d addresses from all feeds.", len(addresses))
    return list(set(addresses))


async def get_pair_data(session, address: str) -> dict | None:
    data = await fetch_json(session, DEXSCREENER_PAIRS.format(address=address))
    if data:
        pairs = data.get("pairs") or []
        if pairs:
            return max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
    return None


async def search_pair(session, query: str) -> dict | None:
    """Search DexScreener by symbol or name."""
    data = await fetch_json(session, DEXSCREENER_SEARCH.format(query=query))
    if data:
        pairs = data.get("pairs") or []
        if pairs:
            return max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
    return None


async def get_rugcheck(session, mint: str) -> tuple:
    score   = -1
    risks   = []
    holders = []
    try:
        data = await fetch_json(session, RUGCHECK_SUMMARY.format(mint=mint))
        if data:
            score = int(data.get("score", -1))
            risks = data.get("risks", [])
    except Exception as e:
        log.warning("rugcheck summary: %s", e)
    try:
        data = await fetch_json(session, RUGCHECK_FULL.format(mint=mint))
        if data:
            holders = data.get("topHolders", [])
            if score == -1:
                score = int(data.get("score", -1))
    except Exception as e:
        log.warning("rugcheck full: %s", e)
    return score, risks, holders


# ── Filter logic ──────────────────────────────────────────────────────────────

def passes_filters(pair: dict, rug_score: int, holders: list) -> tuple:
    created_at = pair.get("pairCreatedAt", 0) or 0
    if not created_at:
        return False, "no creation time"
    age_min = (int(time.time() * 1000) - created_at) / 60_000
    if age_min > MAX_AGE_MINUTES:
        return False, f"too old ({int(age_min)}m)"

    liq = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    if liq < MIN_LIQUIDITY:
        return False, f"low liquidity (${liq:,.0f})"

    if rug_score == -1:
        return False, "rug score unavailable"
    if rug_score < 80:
        return False, f"rug not LOW (score {rug_score})"

    if holders:
        top1_raw = float(holders[0].get("pct", 0))
        # pct can be 0-1 or 0-100 depending on API version
        top1 = top1_raw * 100 if top1_raw <= 1 else top1_raw
        if top1 >= MAX_TOP_HOLDER:
            return False, f"top holder {top1:.1f}%"

    return True, ""


# ── Alert builder ─────────────────────────────────────────────────────────────

def build_alert(pair: dict, rug_score: int, risks: list, holders: list, source: str = "auto-scan") -> str:
    base      = pair.get("baseToken") or {}
    name      = base.get("name", "Unknown")
    symbol    = base.get("symbol", "???")
    mint      = base.get("address", "")
    chain     = pair.get("chainId", "").upper()
    dex       = pair.get("dexId", "").upper()
    liq       = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    vol24     = float((pair.get("volume") or {}).get("h24", 0) or 0)
    mcap      = float(pair.get("fdv", 0) or 0)
    price_usd = pair.get("priceUsd", "N/A")
    chg       = pair.get("priceChange") or {}
    created   = pair.get("pairCreatedAt", 0) or 0

    txns  = (pair.get("txns") or {}).get("h24") or {}
    buys  = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    total = buys + sells
    bs    = f"{buys/total*100:.0f}% buys ({buys}B/{sells}S)" if total else "no txns yet"

    top1_pct     = "N/A"
    top10_pct    = "N/A"
    holder_lines = "  N/A"
    if holders:
        raw0 = float(holders[0].get("pct", 0))
        mult = 100 if raw0 <= 1 else 1
        pcts = [float(h.get("pct", 0)) * mult for h in holders[:10]]
        top1_pct  = f"{pcts[0]:.2f}%"
        top10_pct = f"{sum(pcts):.2f}%"
        lines = []
        for i, h in enumerate(holders[:10], 1):
            addr  = h.get("address", "???")
            short = f"{addr[:4]}...{addr[-4:]}"
            p     = float(h.get("pct", 0)) * mult
            tag   = "[insider]" if h.get("insider") else ("[owner]" if h.get("owner") else "")
            lines.append(f"  {i:>2}. {short} {tag} - {p:.2f}%")
        holder_lines = "\n".join(lines)

    risk_text = ""
    if risks:
        lines = [f"  - {r.get('name','')}: {r.get('description','')}" for r in risks[:4]]
        risk_text = "\n\nRisk flags:\n" + "\n".join(lines)

    source_line = f"Source: {source}"
    dex_link = f"https://dexscreener.com/{pair.get('chainId','')}/{mint}"
    rug_link = f"https://rugcheck.xyz/tokens/{mint}"

    return (
        f"NEW GEM FOUND\n"
        f"{source_line}\n\n"
        f"Name: {name} (${symbol})\n"
        f"Chain: {chain} | DEX: {dex}\n"
        f"Address: {mint}\n"
        f"Created: {minutes_ago(created) if created else 'Unknown'}\n\n"
        f"Price: ${price_usd}\n"
        f"5m: {pct_arrow(chg.get('m5',0))} | 1h: {pct_arrow(chg.get('h1',0))} | 6h: {pct_arrow(chg.get('h6',0))}\n\n"
        f"Liquidity: {fmt_usd(liq)}\n"
        f"Market Cap: {fmt_usd(mcap)}\n"
        f"24h Volume: {fmt_usd(vol24)}\n"
        f"Buy/Sell: {bs}\n\n"
        f"Top Holder: {top1_pct}\n"
        f"Top 10 Combined: {top10_pct}\n\n"
        f"Top 10 Holders:\n{holder_lines}\n\n"
        f"Rug Score: {rug_label(rug_score)}"
        f"{risk_text}\n\n"
        f"PASSED ALL FILTERS\n\n"
        f"DexScreener: {dex_link}\n"
        f"Rugcheck: {rug_link}"
    )


# ── Send alert helper ─────────────────────────────────────────────────────────

async def send_alert(bot: Bot, pair: dict, rug_score: int, risks: list, holders: list, source: str):
    sym = (pair.get("baseToken") or {}).get("symbol", "???")
    log.info("GEM FOUND: %s — sending alert! (source: %s)", sym, source)
    alert = build_alert(pair, rug_score, risks, holders, source)
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=alert,
            disable_web_page_preview=True,
        )
    except TelegramError as te:
        log.error("Telegram send error: %s", te)


# ── Auto scanner loop ─────────────────────────────────────────────────────────

async def scanner_loop(bot: Bot, seen: set, session: aiohttp.ClientSession):
    while True:
        try:
            log.info("Auto-scanning all feeds...")
            addresses = await get_all_latest_tokens(session)

            for addr in addresses:
                if addr in seen:
                    continue
                seen.add(addr)

                pair = await get_pair_data(session, addr)
                if not pair:
                    continue

                created = pair.get("pairCreatedAt", 0) or 0
                if created:
                    age_min = (int(time.time() * 1000) - created) / 60_000
                    if age_min > MAX_AGE_MINUTES:
                        continue

                rug_score, risks, holders = await get_rugcheck(session, addr)
                passed, reason = passes_filters(pair, rug_score, holders)
                sym = (pair.get("baseToken") or {}).get("symbol", addr[:8])

                if not passed:
                    log.info("Filtered: %s — %s", sym, reason)
                    continue

                await send_alert(bot, pair, rug_score, risks, holders, "Auto-scan")
                await asyncio.sleep(1)

            save_seen(seen)

        except Exception as e:
            log.error("Scanner loop error: %s", e)

        log.info("Sleeping %ds...", SCAN_INTERVAL)
        await asyncio.sleep(SCAN_INTERVAL)


# ── Group message handler ─────────────────────────────────────────────────────

def make_group_handler(seen: set, session: aiohttp.ClientSession, bot: Bot):
    async def handle_group_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        msg = update.message
        if not msg or not msg.text:
            return

        text     = msg.text
        chat     = msg.chat
        sender   = msg.from_user
        group_name = chat.title or chat.username or str(chat.id)
        user_name  = sender.username or sender.first_name if sender else "unknown"

        found_addresses = []

        # 1. Look for Solana addresses
        for addr in SOLANA_ADDR_RE.findall(text):
            if addr not in seen:
                found_addresses.append(("address", addr))

        # 2. Look for EVM addresses
        for addr in EVM_ADDR_RE.findall(text):
            if addr not in seen:
                found_addresses.append(("address", addr))

        # 3. Look for $SYMBOL mentions
        for symbol in SYMBOL_RE.findall(text):
            found_addresses.append(("symbol", symbol))

        if not found_addresses:
            return

        log.info("Group '%s' | @%s mentioned: %s", group_name, user_name, found_addresses)

        for kind, query in found_addresses:
            key = f"group:{query}"
            if key in seen:
                continue
            seen.add(key)

            try:
                if kind == "address":
                    pair = await get_pair_data(session, query)
                else:
                    pair = await search_pair(session, query)

                if not pair:
                    log.info("No pair found for group mention: %s", query)
                    continue

                rug_score, risks, holders = await get_rugcheck(
                    session, (pair.get("baseToken") or {}).get("address", query)
                )
                passed, reason = passes_filters(pair, rug_score, holders)
                sym = (pair.get("baseToken") or {}).get("symbol", query)

                if not passed:
                    log.info("Group mention filtered: %s — %s", sym, reason)
                    # Still notify you it was mentioned but didn't pass
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(
                                f"GROUP MENTION (did not pass filters)\n\n"
                                f"Group: {group_name}\n"
                                f"User: @{user_name}\n"
                                f"Coin: {sym} ({query})\n"
                                f"Reason failed: {reason}\n\n"
                                f"Message: {text[:200]}"
                            ),
                            disable_web_page_preview=True,
                        )
                    except TelegramError:
                        pass
                    continue

                source = f"Group mention in '{group_name}' by @{user_name}"
                await send_alert(bot, pair, rug_score, risks, holders, source)

            except Exception as e:
                log.error("Group handler error for %s: %s", query, e)

            await asyncio.sleep(0.5)

    return handle_group_message


# ── Entry point ───────────────────────────────────────────────────────────────

async def post_init(app):
    """Called after the app is initialized — start the background scanner."""
    seen    = load_seen()
    session = aiohttp.ClientSession()
    bot     = app.bot

    # Store session for cleanup
    app.bot_data["session"] = session
    app.bot_data["seen"]    = seen

    # Register group handler
    handler = make_group_handler(seen, session, bot)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handler))

    # Send startup message
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                "Memecoin Scanner v2 is LIVE\n\n"
                "Auto-scanning ALL chains every 60 seconds\n"
                "(latest + boosted + trending feeds)\n\n"
                "Active filters:\n"
                "- Created less than 60 minutes ago\n"
                "- Liquidity over $10,000\n"
                "- Rug score LOW only (80+/100)\n"
                "- Top holder under 20%\n\n"
                "Group scanner: ACTIVE\n"
                "Add me to any Telegram group and I will\n"
                "scan every coin address or $SYMBOL mentioned!\n\n"
                "I will ping you when a gem passes everything!"
            ),
        )
    except TelegramError as e:
        log.error("Startup message failed: %s", e)

    # Launch background scanner
    asyncio.create_task(scanner_loop(bot, seen, session))


async def post_shutdown(app):
    session = app.bot_data.get("session")
    if session:
        await session.close()
    seen = app.bot_data.get("seen")
    if seen:
        save_seen(seen)


def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    log.info("Starting bot...")
    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
