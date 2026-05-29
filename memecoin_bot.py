"""
Memecoin Auto-Scanner Bot v3
- Scans DexScreener every 60 seconds (latest + boosted + trending feeds)
- Age limit: 60 minutes | Liquidity > $10K | Rug LOW | Top holder < 20%
- Group scanner: monitors every group for contract addresses / $SYMBOL mentions
- GEM RATING: Perfect / Good / Risky with reasons
- Fixed rug score display (handles scores > 100)
"""

import asyncio
import logging
import os
import re
import time
import json

import aiohttp
from telegram import Bot, Update
from telegram.error import TelegramError
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHAT_ID     = os.getenv("CHAT_ID",   "YOUR_CHAT_ID_HERE")

SCAN_INTERVAL   = 60
MIN_LIQUIDITY   = 10_000
MAX_TOP_HOLDER  = 20.0
MAX_AGE_MINUTES = 60
SEEN_FILE       = "seen_tokens.json"

DEXSCREENER_LATEST  = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_BOOSTED = "https://api.dexscreener.com/token-boosts/latest/v1"
DEXSCREENER_TOP     = "https://api.dexscreener.com/token-boosts/top/v1"
DEXSCREENER_PAIRS   = "https://api.dexscreener.com/latest/dex/tokens/{address}"
DEXSCREENER_SEARCH  = "https://api.dexscreener.com/latest/dex/search?q={query}"
RUGCHECK_SUMMARY    = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
RUGCHECK_FULL       = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"

SOLANA_ADDR_RE = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')
EVM_ADDR_RE    = re.compile(r'\b0x[a-fA-F0-9]{40}\b')
SYMBOL_RE      = re.compile(r'\$([A-Z]{2,10})\b')

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


# ── Persistence ───────────────────────────────────────────────────────────────

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
        log.warning("Could not save seen: %s", e)


# ── Formatters ────────────────────────────────────────────────────────────────

def fmt_usd(n) -> str:
    try:
        n = float(n)
        if n >= 1_000_000: return f"${n/1_000_000:.2f}M"
        if n >= 1_000:     return f"${n/1_000:.1f}K"
        return f"${n:.2f}"
    except Exception:
        return "N/A"

def minutes_ago(unix_ms: int) -> str:
    diff = (int(time.time() * 1000) - unix_ms) / 60_000
    if diff < 60:   return f"{int(diff)}m ago"
    if diff < 1440: return f"{diff/60:.1f}h ago"
    return f"{diff/1440:.1f}d ago"

def pct_arrow(v) -> str:
    try:
        v = float(v)
        return f"{'up' if v >= 0 else 'down'} {v:+.2f}%"
    except Exception:
        return "N/A"

def rug_label(score: int) -> str:
    # Clamp display — Rugcheck sometimes returns > 100
    display = min(score, 100)
    if score >= 80: return f"LOW (score {display}/100)"
    if score >= 50: return f"MEDIUM (score {display}/100)"
    if score >= 30: return f"HIGH (score {display}/100)"
    return f"VERY HIGH (score {display}/100)"


# ── GEM RATING ────────────────────────────────────────────────────────────────

def rate_gem(pair: dict, rug_score: int, holders: list, risks: list) -> tuple[str, list]:
    """
    Returns (rating_line, reasons_list)
    Rating: PERFECT / GOOD / RISKY
    Scores points based on multiple signals.
    """
    score  = 0
    plus   = []   # positive reasons
    minus  = []   # negative reasons

    liq   = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    vol24 = float((pair.get("volume") or {}).get("h24", 0) or 0)
    mcap  = float(pair.get("fdv", 0) or 0)
    chg   = pair.get("priceChange") or {}
    txns  = (pair.get("txns") or {}).get("h24") or {}
    buys  = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    total = buys + sells
    created = pair.get("pairCreatedAt", 0) or 0
    age_min = (int(time.time() * 1000) - created) / 60_000 if created else 999

    # Holder analysis
    top1_pct  = 0
    top10_pct = 0
    if holders:
        raw0 = float(holders[0].get("pct", 0))
        mult = 100 if raw0 <= 1 else 1
        pcts = [float(h.get("pct", 0)) * mult for h in holders[:10]]
        top1_pct  = pcts[0]
        top10_pct = sum(pcts)

    # ── Liquidity scoring ──
    if liq >= 100_000:
        score += 3; plus.append(f"Very high liquidity ({fmt_usd(liq)})")
    elif liq >= 50_000:
        score += 2; plus.append(f"Strong liquidity ({fmt_usd(liq)})")
    elif liq >= 20_000:
        score += 1; plus.append(f"Good liquidity ({fmt_usd(liq)})")
    else:
        minus.append(f"Low liquidity ({fmt_usd(liq)}) — harder to exit")

    # ── Rug score ──
    clamped = min(rug_score, 100)
    if clamped >= 95:
        score += 3; plus.append(f"Excellent rug score ({clamped}/100)")
    elif clamped >= 85:
        score += 2; plus.append(f"Good rug score ({clamped}/100)")
    else:
        score += 1; plus.append(f"Acceptable rug score ({clamped}/100)")

    # ── Top holder ──
    if top1_pct > 0:
        if top1_pct < 5:
            score += 3; plus.append(f"Top holder very low ({top1_pct:.1f}%) — well distributed")
        elif top1_pct < 10:
            score += 2; plus.append(f"Top holder healthy ({top1_pct:.1f}%)")
        elif top1_pct < 15:
            score += 1; plus.append(f"Top holder acceptable ({top1_pct:.1f}%)")
        else:
            minus.append(f"Top holder borderline ({top1_pct:.1f}%) — watch for dumps")

    # ── Top 10 concentration ──
    if top10_pct > 0:
        if top10_pct < 20:
            score += 2; plus.append(f"Top 10 very distributed ({top10_pct:.1f}% combined)")
        elif top10_pct < 35:
            score += 1; plus.append(f"Top 10 reasonably distributed ({top10_pct:.1f}% combined)")
        else:
            minus.append(f"Top 10 hold a lot ({top10_pct:.1f}% combined)")

    # ── Buy/sell pressure ──
    if total > 0:
        buy_pct = buys / total * 100
        if buy_pct >= 70:
            score += 3; plus.append(f"Very strong buy pressure ({buy_pct:.0f}% buys)")
        elif buy_pct >= 60:
            score += 2; plus.append(f"Strong buy pressure ({buy_pct:.0f}% buys)")
        elif buy_pct >= 50:
            score += 1; plus.append(f"Slight buy pressure ({buy_pct:.0f}% buys)")
        else:
            minus.append(f"Sell pressure dominant ({100-buy_pct:.0f}% sells)")

    # ── Volume vs liquidity ratio ──
    if liq > 0 and vol24 > 0:
        ratio = vol24 / liq
        if ratio >= 5:
            score += 2; plus.append(f"Very high volume/liquidity ratio ({ratio:.1f}x) — hot coin")
        elif ratio >= 2:
            score += 1; plus.append(f"Good volume activity ({ratio:.1f}x liquidity)")
        else:
            minus.append(f"Low volume relative to liquidity ({ratio:.1f}x)")

    # ── Age ──
    if age_min < 10:
        score += 2; plus.append(f"Very fresh launch ({int(age_min)}m old) — early entry")
    elif age_min < 30:
        score += 1; plus.append(f"Fresh launch ({int(age_min)}m old)")
    else:
        minus.append(f"Not very new ({int(age_min)}m old) — may have missed early move")

    # ── Price momentum ──
    try:
        m5 = float(chg.get("m5", 0) or 0)
        h1 = float(chg.get("h1", 0) or 0)
        if m5 > 20 and h1 > 50:
            score += 2; plus.append(f"Strong momentum (5m: +{m5:.0f}%, 1h: +{h1:.0f}%)")
        elif m5 > 5 or h1 > 20:
            score += 1; plus.append(f"Positive momentum (5m: {m5:+.1f}%, 1h: {h1:+.1f}%)")
        elif m5 < -10:
            minus.append(f"Dropping fast (5m: {m5:.1f}%) — be careful")
    except Exception:
        pass

    # ── Risk flags from rugcheck ──
    danger_flags = [r for r in risks if (r.get("level") or "").upper() == "DANGER"]
    warn_flags   = [r for r in risks if (r.get("level") or "").upper() == "WARN"]
    if danger_flags:
        score -= 3
        for r in danger_flags[:2]:
            minus.append(f"DANGER: {r.get('name','')}")
    if warn_flags:
        score -= 1
        for r in warn_flags[:2]:
            minus.append(f"Warning: {r.get('name','')}")

    # ── Final rating ──
    if score >= 12:
        rating = "PERFECT"
        emoji  = "💎"
        summary = "This coin checks nearly every box. Strong entry opportunity."
    elif score >= 7:
        rating = "GOOD"
        emoji  = "✅"
        summary = "Solid coin with good fundamentals. Worth watching closely."
    else:
        rating = "RISKY"
        emoji  = "⚠️"
        summary = "Passed filters but has some red flags. Trade with caution."

    rating_line = f"{emoji} {rating} (score: {score})\n{summary}"
    return rating_line, plus, minus


# ── API helpers ───────────────────────────────────────────────────────────────

async def fetch_json(session, url: str):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        log.warning("fetch error %s: %s", url[:60], e)
    return None

async def get_all_latest_tokens(session) -> list:
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

async def get_pair_data(session, address: str):
    data = await fetch_json(session, DEXSCREENER_PAIRS.format(address=address))
    if data:
        pairs = data.get("pairs") or []
        if pairs:
            return max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
    return None

async def search_pair(session, query: str):
    data = await fetch_json(session, DEXSCREENER_SEARCH.format(query=query))
    if data:
        pairs = data.get("pairs") or []
        if pairs:
            return max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
    return None

async def get_rugcheck(session, mint: str):
    score   = -1
    risks   = []
    holders = []
    data = await fetch_json(session, RUGCHECK_SUMMARY.format(mint=mint))
    if data:
        score = int(data.get("score", -1))
        risks = data.get("risks", [])
    data = await fetch_json(session, RUGCHECK_FULL.format(mint=mint))
    if data:
        holders = data.get("topHolders", [])
        if score == -1:
            score = int(data.get("score", -1))
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
        return False, f"rug not LOW (score {min(rug_score,100)})"
    if holders:
        raw0 = float(holders[0].get("pct", 0))
        mult = 100 if raw0 <= 1 else 1
        top1 = raw0 * mult
        if top1 >= MAX_TOP_HOLDER:
            return False, f"top holder {top1:.1f}%"
    return True, ""


# ── Alert builder ─────────────────────────────────────────────────────────────

def build_alert(pair: dict, rug_score: int, risks: list, holders: list, source: str = "Auto-scan") -> str:
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
    txns      = (pair.get("txns") or {}).get("h24") or {}
    buys      = txns.get("buys", 0)
    sells     = txns.get("sells", 0)
    total     = buys + sells
    bs        = f"{buys/total*100:.0f}% buys ({buys}B/{sells}S)" if total else "no txns yet"

    # Holders
    top1_pct     = "N/A"
    top10_pct    = "N/A"
    holder_lines = "  N/A"
    if holders:
        raw0  = float(holders[0].get("pct", 0))
        mult  = 100 if raw0 <= 1 else 1
        pcts  = [float(h.get("pct", 0)) * mult for h in holders[:10]]
        top1_pct  = f"{pcts[0]:.2f}%"
        top10_pct = f"{sum(pcts):.2f}%"
        lines = []
        for i, h in enumerate(holders[:10], 1):
            addr  = h.get("address", "???")
            short = f"{addr[:4]}...{addr[-4:]}"
            p     = float(h.get("pct", 0)) * mult
            tag   = " [insider]" if h.get("insider") else (" [owner]" if h.get("owner") else "")
            lines.append(f"  {i:>2}. {short}{tag} - {p:.2f}%")
        holder_lines = "\n".join(lines)

    # Rug risks
    risk_text = ""
    if risks:
        lines = [f"  - {r.get('name','')}: {r.get('description','')}" for r in risks[:4]]
        risk_text = "\nRisk flags:\n" + "\n".join(lines)

    # Gem rating
    rating_line, plus_reasons, minus_reasons = rate_gem(pair, rug_score, holders, risks)
    plus_text  = "\n".join(f"  + {r}" for r in plus_reasons)  if plus_reasons  else "  None"
    minus_text = "\n".join(f"  - {r}" for r in minus_reasons) if minus_reasons else "  None"

    dex_link = f"https://dexscreener.com/{pair.get('chainId','')}/{mint}"
    rug_link = f"https://rugcheck.xyz/tokens/{mint}"

    return (
        f"NEW GEM FOUND\n"
        f"Source: {source}\n\n"
        f"---- GEM RATING ----\n"
        f"{rating_line}\n\n"
        f"Positives:\n{plus_text}\n\n"
        f"Negatives:\n{minus_text}\n"
        f"--------------------\n\n"
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


# ── Send alert ────────────────────────────────────────────────────────────────

async def send_alert(bot: Bot, pair: dict, rug_score: int, risks: list, holders: list, source: str):
    sym = (pair.get("baseToken") or {}).get("symbol", "???")
    log.info("GEM: %s — sending alert (source: %s)", sym, source)
    text = build_alert(pair, rug_score, risks, holders, source)
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text, disable_web_page_preview=True)
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
        text       = msg.text
        chat       = msg.chat
        sender     = msg.from_user
        group_name = chat.title or chat.username or str(chat.id)
        user_name  = (sender.username or sender.first_name) if sender else "unknown"

        found = []
        for addr in SOLANA_ADDR_RE.findall(text):
            found.append(("address", addr))
        for addr in EVM_ADDR_RE.findall(text):
            found.append(("address", addr))
        for sym in SYMBOL_RE.findall(text):
            found.append(("symbol", sym))

        if not found:
            return

        log.info("Group '%s' | @%s | found: %s", group_name, user_name, found)

        for kind, query in found:
            key = f"group:{query}"
            if key in seen:
                continue
            seen.add(key)
            try:
                pair = await get_pair_data(session, query) if kind == "address" else await search_pair(session, query)
                if not pair:
                    continue
                mint = (pair.get("baseToken") or {}).get("address", query)
                rug_score, risks, holders = await get_rugcheck(session, mint)
                passed, reason = passes_filters(pair, rug_score, holders)
                sym = (pair.get("baseToken") or {}).get("symbol", query)

                if not passed:
                    log.info("Group mention filtered: %s — %s", sym, reason)
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(
                                f"GROUP MENTION (did not pass filters)\n\n"
                                f"Group: {group_name}\n"
                                f"User: @{user_name}\n"
                                f"Coin: {sym} ({query})\n"
                                f"Failed: {reason}\n\n"
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
    seen    = load_seen()
    session = aiohttp.ClientSession()
    bot     = app.bot
    app.bot_data["session"] = session
    app.bot_data["seen"]    = seen

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, make_group_handler(seen, session, bot)))

    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                "Memecoin Scanner v3 is LIVE\n\n"
                "Auto-scanning ALL chains every 60 seconds\n"
                "(latest + boosted + trending feeds)\n\n"
                "Filters:\n"
                "- Created less than 60 minutes ago\n"
                "- Liquidity over $10,000\n"
                "- Rug score LOW only (80+/100)\n"
                "- Top holder under 20%\n\n"
                "Each alert now includes a GEM RATING:\n"
                "PERFECT / GOOD / RISKY with full reasons\n\n"
                "Group scanner: ACTIVE\n"
                "Add me to any group to monitor coin mentions!"
            ),
        )
    except TelegramError as e:
        log.error("Startup message failed: %s", e)

    asyncio.create_task(scanner_loop(bot, seen, session))


async def post_shutdown(app):
    s = app.bot_data.get("session")
    if s: await s.close()
    seen = app.bot_data.get("seen")
    if seen: save_seen(seen)


def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    log.info("Starting bot v3...")
    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
