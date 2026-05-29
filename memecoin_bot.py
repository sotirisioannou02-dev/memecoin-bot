"""
Memecoin Auto-Scanner Bot
- Scans DexScreener every 60 seconds across ALL chains
- Filters: liquidity > $10K, top holder < 20%, rug LOW, created < 30 min ago
- Sends Telegram alert only when a coin passes ALL filters
- Never sends the same coin twice
"""

import asyncio
import logging
import os
import time
import json

import aiohttp
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHAT_ID     = os.getenv("CHAT_ID",   "YOUR_CHAT_ID_HERE")   # your Telegram user ID

SCAN_INTERVAL   = 60       # seconds between scans
MIN_LIQUIDITY   = 10_000   # USD
MAX_TOP_HOLDER  = 20.0     # percent
MAX_AGE_MINUTES = 30       # coin must be newer than this
SEEN_FILE       = "seen_tokens.json"

DEXSCREENER_LATEST = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_PAIRS  = "https://api.dexscreener.com/latest/dex/tokens/{address}"
RUGCHECK_SUMMARY   = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
RUGCHECK_FULL      = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"

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

async def get_latest_tokens(session) -> list:
    try:
        async with session.get(DEXSCREENER_LATEST, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                data = await r.json()
                return data if isinstance(data, list) else []
    except Exception as e:
        log.warning("latest tokens fetch error: %s", e)
    return []


async def get_pair_data(session, address: str):
    try:
        url = DEXSCREENER_PAIRS.format(address=address)
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                data = await r.json()
                pairs = data.get("pairs") or []
                if pairs:
                    return max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
    except Exception as e:
        log.warning("pair fetch error for %s: %s", address, e)
    return None


async def get_rugcheck(session, mint: str):
    score   = -1
    risks   = []
    holders = []
    try:
        url = RUGCHECK_SUMMARY.format(mint=mint)
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                d     = await r.json()
                score = int(d.get("score", -1))
                risks = d.get("risks", [])
    except Exception as e:
        log.warning("rugcheck summary error: %s", e)
    try:
        url = RUGCHECK_FULL.format(mint=mint)
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                d       = await r.json()
                holders = d.get("topHolders", [])
                if score == -1:
                    score = int(d.get("score", -1))
    except Exception as e:
        log.warning("rugcheck full error: %s", e)
    return score, risks, holders


# ── Filter logic ──────────────────────────────────────────────────────────────

def passes_filters(pair: dict, rug_score: int, holders: list):
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
        top1 = float(holders[0].get("pct", 0)) * 100
        if top1 >= MAX_TOP_HOLDER:
            return False, f"top holder {top1:.1f}%"

    return True, ""


# ── Alert builder ─────────────────────────────────────────────────────────────

def build_alert(pair: dict, rug_score: int, risks: list, holders: list) -> str:
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
        pcts         = [float(h.get("pct", 0)) * 100 for h in holders[:10]]
        top1_pct     = f"{pcts[0]:.2f}%"
        top10_pct    = f"{sum(pcts):.2f}%"
        lines = []
        for i, h in enumerate(holders[:10], 1):
            addr  = h.get("address", "???")
            short = f"{addr[:4]}...{addr[-4:]}"
            p     = float(h.get("pct", 0)) * 100
            tag   = "[insider]" if h.get("insider") else ("[owner]" if h.get("owner") else "")
            lines.append(f"  {i:>2}. {short} {tag} - {p:.2f}%")
        holder_lines = "\n".join(lines)

    risk_text = ""
    if risks:
        lines = []
        for r in risks[:4]:
            lines.append(f"  - {r.get('name','')}: {r.get('description','')}")
        risk_text = "\n\nRisk flags:\n" + "\n".join(lines)

    dex_link = f"https://dexscreener.com/{pair.get('chainId','')}/{mint}"
    rug_link = f"https://rugcheck.xyz/tokens/{mint}"

    msg = (
        "NEW GEM FOUND\n\n"
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
    return msg


# ── Scanner loop ──────────────────────────────────────────────────────────────

async def scanner_loop(bot: Bot):
    seen = load_seen()
    log.info("Scanner started. %d tokens already seen.", len(seen))

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                log.info("Scanning DexScreener...")
                profiles = await get_latest_tokens(session)
                log.info("Got %d token profiles.", len(profiles))

                for profile in profiles:
                    mint = profile.get("tokenAddress") or profile.get("address") or ""
                    if not mint or mint in seen:
                        continue

                    seen.add(mint)

                    pair = await get_pair_data(session, mint)
                    if not pair:
                        continue

                    # Quick age check before hitting rugcheck
                    created = pair.get("pairCreatedAt", 0) or 0
                    if created:
                        age_min = (int(time.time() * 1000) - created) / 60_000
                        if age_min > MAX_AGE_MINUTES:
                            continue

                    rug_score, risks, holders = await get_rugcheck(session, mint)
                    passed, reason = passes_filters(pair, rug_score, holders)
                    sym = (pair.get("baseToken") or {}).get("symbol", mint[:8])

                    if not passed:
                        log.info("Filtered: %s - %s", sym, reason)
                        continue

                    log.info("GEM FOUND: %s (%s) - sending alert!", sym, mint[:10])
                    alert = build_alert(pair, rug_score, risks, holders)

                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=alert,
                            disable_web_page_preview=True,
                        )
                    except TelegramError as te:
                        log.error("Telegram send error: %s", te)

                    await asyncio.sleep(1)

                save_seen(seen)

            except Exception as e:
                log.error("Scanner loop error: %s", e)

            log.info("Sleeping %ds...", SCAN_INTERVAL)
            await asyncio.sleep(SCAN_INTERVAL)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    bot = Bot(token=BOT_TOKEN)
    me  = await bot.get_me()
    log.info("Bot connected: @%s", me.username)

    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                "Memecoin Scanner is LIVE\n\n"
                "Scanning ALL chains every 60 seconds.\n\n"
                "Active filters:\n"
                "- Created less than 30 minutes ago\n"
                "- Liquidity over $10,000\n"
                "- Rug score LOW only (80+ out of 100)\n"
                "- Top holder under 20%\n\n"
                "I will ping you when a gem passes everything!"
            ),
        )
    except TelegramError as e:
        log.error("Startup message failed - check CHAT_ID. Error: %s", e)

    await scanner_loop(bot)


if __name__ == "__main__":
    asyncio.run(main())
