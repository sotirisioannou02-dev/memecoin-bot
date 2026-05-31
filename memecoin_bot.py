"""
Memecoin Auto-Scanner Bot v5.3 — Permanent Cloud Memory
- All memory saved to Supabase cloud (survives Railway restarts)
- Auto-backup every 5 minutes
- All v5.2 filters intact
"""

import asyncio
import logging
import os
import re
import time
import json
from datetime import datetime, timezone

import aiohttp
from telegram import Bot, Update
from telegram.error import TelegramError
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN",    "YOUR_BOT_TOKEN_HERE")
CHAT_ID      = os.getenv("CHAT_ID",     "YOUR_CHAT_ID_HERE")
SUPABASE_URL = os.getenv("SUPABASE_URL","https://xzxplgjpfknoeezawmvr.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY","eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inh6eHBsZ2pwZmtub2VlemF3bXZyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAxMzg3NTcsImV4cCI6MjA5NTcxNDc1N30.eM8snBlDjWuqPoPQzWjTspKg_M8cUFSDW6_bfyPceFg")

SUPABASE_TABLE   = "bot_memory"
BACKUP_INTERVAL  = 300   # save to cloud every 5 min

SCAN_INTERVAL    = 60
MIN_LIQUIDITY    = 10_000
MAX_LIQUIDITY    = 2_000_000
MIN_AGE_MINUTES  = 5
MAX_AGE_MINUTES  = 20
MAX_TOP_HOLDER   = 15.0
MAX_TOP10        = 40.0
MIN_BUY_PRESSURE = 65.0
MIN_MCAP         = 10_000
MAX_MCAP         = 500_000
FOLLOWUP_INTERVAL  = 60
FOLLOWUP_DURATION  = 3600 * 4
PUMP_THRESHOLD     = 50.0
DUMP_THRESHOLD     = -30.0
DAILY_SUMMARY_HOUR = 8

SEEN_FILE      = "seen_tokens.json"
WHALE_FILE     = "whale_wallets.json"
PORTFOLIO_FILE = "portfolio.json"

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

_holder_snapshots: dict = {}


# ── Supabase cloud persistence ────────────────────────────────────────────────

def _sb_headers() -> dict:
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",
    }

async def cloud_save(session: aiohttp.ClientSession, key: str, value):
    """Save a value to Supabase."""
    try:
        url  = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
        body = {"key": key, "value": value, "updated_at": datetime.now(timezone.utc).isoformat()}
        async with session.post(url, headers=_sb_headers(), json=body,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (200, 201):
                log.info("Cloud save OK: %s", key)
            else:
                txt = await r.text()
                log.warning("Cloud save failed %s: %s %s", key, r.status, txt)
    except Exception as e:
        log.warning("Cloud save error %s: %s", key, e)

async def cloud_load(session: aiohttp.ClientSession, key: str):
    """Load a value from Supabase. Returns None if not found."""
    try:
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?key=eq.{key}&select=value"
        async with session.get(url, headers=_sb_headers(),
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                rows = await r.json()
                if rows:
                    return rows[0]["value"]
    except Exception as e:
        log.warning("Cloud load error %s: %s", key, e)
    return None

async def load_all_from_cloud(session: aiohttp.ClientSession):
    """Load seen, whales, portfolio from Supabase. Falls back to local files."""
    log.info("Loading memory from Supabase...")

    seen_data      = await cloud_load(session, "seen_tokens")
    whale_data     = await cloud_load(session, "whale_wallets")
    portfolio_data = await cloud_load(session, "portfolio")

    # Fallback to local files if cloud empty
    def local_fallback(path, default):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return default

    seen      = set(seen_data      if seen_data      is not None else local_fallback(SEEN_FILE, []))
    whales    = dict(whale_data    if whale_data     is not None else local_fallback(WHALE_FILE, {}))
    portfolio = list(portfolio_data if portfolio_data is not None else local_fallback(PORTFOLIO_FILE, []))

    log.info("Loaded: %d seen, %d whales, %d portfolio items", len(seen), len(whales), len(portfolio))
    return seen, whales, portfolio

async def save_all_to_cloud(session: aiohttp.ClientSession, seen: set, whales: dict, portfolio: list):
    """Save all memory to Supabase + local files as backup."""
    await asyncio.gather(
        cloud_save(session, "seen_tokens",   list(seen)),
        cloud_save(session, "whale_wallets", whales),
        cloud_save(session, "portfolio",     portfolio),
    )
    # Also save locally as backup
    for path, data in [(SEEN_FILE, list(seen)), (WHALE_FILE, whales), (PORTFOLIO_FILE, portfolio)]:
        try:
            with open(path, "w") as f:
                json.dump(data, f)
        except Exception as e:
            log.warning("Local save error %s: %s", path, e)

async def backup_loop(session: aiohttp.ClientSession, seen: set, whales: dict, portfolio: list):
    """Auto-backup to cloud every 5 minutes. Caps seen list at 1000 to prevent bloat."""
    while True:
        await asyncio.sleep(BACKUP_INTERVAL)
        log.info("Auto-backup to Supabase...")
        # Cap seen list at 1000 most recent — prevents it growing forever
        # and ensures Supabase clear actually takes effect on restart
        if len(seen) > 1000:
            seen_list = list(seen)
            seen.clear()
            seen.update(seen_list[-1000:])
            log.info("Capped seen list to 1000 items.")
        await save_all_to_cloud(session, seen, whales, portfolio)


# ── Formatters ────────────────────────────────────────────────────────────────

def fmt_usd(n) -> str:
    try:
        n = float(n)
        if n >= 1_000_000: return f"${n/1_000_000:.2f}M"
        if n >= 1_000:     return f"${n/1_000:.1f}K"
        return f"${n:.4f}" if n < 1 else f"${n:.2f}"
    except Exception:
        return "N/A"

def fmt_pct(n) -> str:
    try:
        n = float(n)
        return f"{'+' if n >= 0 else ''}{n:.1f}%"
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
    display = min(score, 100)
    if score >= 80: return f"LOW (score {display}/100)"
    if score >= 50: return f"MEDIUM (score {display}/100)"
    if score >= 30: return f"HIGH (score {display}/100)"
    return f"VERY HIGH (score {display}/100)"

def normalize_pct(raw: float) -> float:
    return raw * 100 if raw <= 1.0 else raw


# ── Social links ──────────────────────────────────────────────────────────────

def extract_socials(pair: dict) -> tuple:
    website = ""
    twitter = ""
    info = pair.get("info") or {}
    for w in (info.get("websites") or []):
        url = w.get("url", "")
        if url and not website:
            website = url
    for s in (info.get("socials") or []):
        stype = (s.get("type") or "").lower()
        url   = s.get("url", "")
        if stype in ("twitter", "x") and url and not twitter:
            twitter = url
        if stype == "website" and url and not website:
            website = url
    for link in (pair.get("links") or []):
        ltype = (link.get("type") or link.get("label") or "").lower()
        url   = link.get("url", "")
        if ("twitter" in ltype or "x.com" in url or "twitter.com" in url) and not twitter:
            twitter = url
        elif not website and url:
            website = url
    return website, twitter


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
    log.info("Collected %d addresses.", len(addresses))
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
    score    = -1
    risks    = []
    holders  = []
    deployer = ""
    data = await fetch_json(session, RUGCHECK_SUMMARY.format(mint=mint))
    if data:
        score = int(data.get("score", -1))
        risks = data.get("risks", [])
    data = await fetch_json(session, RUGCHECK_FULL.format(mint=mint))
    if data:
        holders  = data.get("topHolders", [])
        deployer = data.get("creator", "") or data.get("deployer", "") or ""
        if score == -1:
            score = int(data.get("score", -1))
    return score, risks, holders, deployer

async def is_pumpfun_graduate(session, mint: str) -> bool:
    try:
        data = await fetch_json(session, DEXSCREENER_PAIRS.format(address=mint))
        if not data:
            return False
        for pair in (data.get("pairs") or []):
            dex = (pair.get("dexId") or "").lower()
            if "raydium" in dex:
                url      = (pair.get("url") or "").lower()
                labels   = [str(l).lower() for l in (pair.get("labels") or [])]
                info     = pair.get("info") or {}
                websites = [str(w.get("url","")).lower() for w in (info.get("websites") or [])]
                all_text = url + " ".join(labels) + " ".join(websites)
                if "pump" in all_text:
                    return True
                created = pair.get("pairCreatedAt", 0) or 0
                if created and (int(time.time() * 1000) - created) / 3_600_000 < 1:
                    return True
        return False
    except Exception as e:
        log.warning("pumpfun check error: %s", e)
        return False


# ── Dev wallet check ──────────────────────────────────────────────────────────

def dev_has_sold(holders: list, deployer: str) -> tuple:
    if not deployer:
        return False, ""
    deployer = deployer.lower()
    for h in holders:
        addr = (h.get("address") or "").lower()
        if addr == deployer:
            pct = normalize_pct(float(h.get("pct", 0)))
            if pct < 0.5:
                return True, f"Dev wallet almost empty ({pct:.2f}% left)"
            return False, ""
    return True, "Dev wallet not found in holders (likely sold)"


# ── Holder velocity ───────────────────────────────────────────────────────────

def check_holder_velocity(mint: str, current_count: int) -> tuple:
    now = time.time()
    if mint not in _holder_snapshots:
        _holder_snapshots[mint] = (current_count, now)
        return 0, "First snapshot"
    prev_count, prev_time = _holder_snapshots[mint]
    _holder_snapshots[mint] = (current_count, now)
    elapsed_min = (now - prev_time) / 60
    if elapsed_min < 0.5:
        return 0, "Too soon"
    growth = current_count - prev_count
    rate   = growth / elapsed_min
    if rate >= 20:  return 3, f"Explosive holder growth: +{growth} in {elapsed_min:.0f}m ({rate:.0f}/min)"
    if rate >= 5:   return 2, f"Strong holder growth: +{growth} in {elapsed_min:.0f}m"
    if rate >= 1:   return 1, f"Steady holder growth: +{growth} in {elapsed_min:.0f}m"
    if growth < 0:  return -1, f"Holders DROPPING: {growth} in {elapsed_min:.0f}m"
    return 0, f"Low growth: +{growth} in {elapsed_min:.0f}m"


# ── Whale tracker ─────────────────────────────────────────────────────────────

def check_known_whales(holders: list, whales: dict) -> tuple:
    found = []
    for h in holders[:10]:
        addr = h.get("address", "")
        if addr in whales:
            w    = whales[addr]
            wins = w.get("wins", 0)
            total= w.get("total", 0)
            pct  = normalize_pct(float(h.get("pct", 0)))
            wr   = (wins/total*100) if total > 0 else 0
            found.append(f"Whale {addr[:6]}...{addr[-4:]} ({pct:.1f}% holding, {wins}/{total} wins, {wr:.0f}% WR)")
    if found:
        return True, "\n".join(found)
    return False, ""

def record_whale_result(holders: list, whales: dict, won: bool):
    for h in holders[:5]:
        addr = h.get("address", "")
        if not addr:
            continue
        if addr not in whales:
            whales[addr] = {"wins": 0, "total": 0, "last_seen": int(time.time())}
        whales[addr]["total"] += 1
        if won:
            whales[addr]["wins"] += 1
        whales[addr]["last_seen"] = int(time.time())
    cutoff    = int(time.time()) - 30 * 86400
    to_delete = [a for a, w in whales.items() if w.get("last_seen", 0) < cutoff]
    for a in to_delete:
        del whales[a]


# ── Filter logic ──────────────────────────────────────────────────────────────

def passes_filters(pair: dict, rug_score: int, holders: list, deployer: str,
                   is_graduated: bool, risks: list = None) -> tuple:

    # 1. Golden age window
    created_at = pair.get("pairCreatedAt", 0) or 0
    if not created_at:
        return False, "no creation time"
    age_min = (int(time.time() * 1000) - created_at) / 60_000
    if age_min < MIN_AGE_MINUTES:
        return False, f"too fresh ({int(age_min)}m)"
    if age_min > MAX_AGE_MINUTES:
        return False, f"too old ({int(age_min)}m)"

    # 2. Liquidity
    liq = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    if liq < MIN_LIQUIDITY:
        return False, f"low liquidity ({fmt_usd(liq)})"
    if liq > MAX_LIQUIDITY:
        return False, f"liquidity too high ({fmt_usd(liq)})"

    # 3. Market cap
    mcap = float(pair.get("fdv", 0) or 0)
    if mcap > 0:
        if mcap < MIN_MCAP:
            return False, f"mcap too low ({fmt_usd(mcap)})"
        if mcap > MAX_MCAP:
            return False, f"mcap too high ({fmt_usd(mcap)})"

    # 4. Rug score
    if rug_score == -1:
        return False, "rug score unavailable"
    if rug_score < 80:
        return False, f"rug not LOW ({min(rug_score,100)})"

    # 5. Buy pressure
    txns  = (pair.get("txns") or {}).get("h24") or {}
    buys  = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    total = buys + sells
    if total > 0:
        buy_pct = buys / total * 100
        if buy_pct < MIN_BUY_PRESSURE:
            return False, f"weak buy pressure ({buy_pct:.0f}%)"

    # 6. Top holder + top 10
    if holders:
        top1 = normalize_pct(float(holders[0].get("pct", 0)))
        if top1 >= MAX_TOP_HOLDER:
            return False, f"top holder {top1:.1f}%"
        pcts  = [normalize_pct(float(h.get("pct", 0))) for h in holders[:10]]
        top10 = sum(pcts)
        if top10 >= MAX_TOP10:
            return False, f"top 10 too concentrated ({top10:.1f}%)"

    # 7. Dev wallet
    dev_sold, dev_reason = dev_has_sold(holders, deployer)
    if dev_sold:
        return False, f"dev sold: {dev_reason}"

    # 8. Social links
    website, twitter = extract_socials(pair)
    if not website:
        return False, "no website"
    if not twitter:
        return False, "no Twitter/X"

    # 9. Clone wallet pattern (mathematical)
    if len(holders) >= 5:
        pcts = [normalize_pct(float(h.get("pct", 0))) for h in holders[:10]]
        small_pcts = pcts[1:]
        if small_pcts:
            spread = max(small_pcts) - min(small_pcts)
            if spread <= 0.20 and len(small_pcts) >= 6:
                return False, f"clone wallet pattern (spread {spread:.3f}%) — rug"
        owner_count = sum(
            1 for h in holders[:10]
            if h.get("owner") or h.get("insider") or
               (h.get("ownerAddress") == h.get("address"))
        )
        if owner_count >= 4:
            return False, f"too many owner wallets ({owner_count}/10)"

    # 10. PumpSwap rejection
    dex_id = (pair.get("dexId") or "").lower()
    if "pumpswap" in dex_id or ("pump" in dex_id and "raydium" not in dex_id):
        return False, "on PumpSwap — not graduated to Raydium"

    # 11. Low LP Providers
    for risk in (risks or []):
        name = (risk.get("name") or "").lower()
        desc = (risk.get("description") or "").lower()
        if "lp provider" in name or "lp provider" in desc or "few users" in desc:
            return False, "low LP providers — easy to rug"

    # 12. Address ends in 'pump'
    base_addr = (pair.get("baseToken") or {}).get("address", "")
    if base_addr.endswith("pump"):
        return False, "address ends in 'pump' — not graduated"

    return True, ""


# ── GEM RATING ────────────────────────────────────────────────────────────────

def rate_gem(pair, rug_score, holders, risks, website, twitter,
             is_graduated, holder_velocity, velocity_desc,
             whale_found, whale_desc) -> tuple:
    score = 0
    plus  = []
    minus = []

    liq   = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    vol24 = float((pair.get("volume") or {}).get("h24", 0) or 0)
    vol5m = float((pair.get("volume") or {}).get("m5", 0) or 0)
    mcap  = float(pair.get("fdv", 0) or 0)
    chg   = pair.get("priceChange") or {}
    txns  = (pair.get("txns") or {}).get("h24") or {}
    buys  = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    total = buys + sells
    created = pair.get("pairCreatedAt", 0) or 0
    age_min = (int(time.time() * 1000) - created) / 60_000 if created else 999
    top1_pct  = 0
    top10_pct = 0
    if holders:
        pcts = [normalize_pct(float(h.get("pct", 0))) for h in holders[:10]]
        top1_pct  = pcts[0]
        top10_pct = sum(pcts)

    if is_graduated:
        score += 4; plus.append("Graduated from Pump.fun to Raydium — proven demand")
    if whale_found:
        score += 4; plus.append(f"Known winning whale!\n    {whale_desc}")
    if holder_velocity == 3:
        score += 3; plus.append(velocity_desc)
    elif holder_velocity == 2:
        score += 2; plus.append(velocity_desc)
    elif holder_velocity == 1:
        score += 1; plus.append(velocity_desc)
    elif holder_velocity == -1:
        score -= 2; minus.append(velocity_desc)

    score += 2; plus.append("Has website + Twitter/X")

    if liq >= 100_000:   score += 3; plus.append(f"Very high liquidity ({fmt_usd(liq)})")
    elif liq >= 50_000:  score += 2; plus.append(f"Strong liquidity ({fmt_usd(liq)})")
    else:                score += 1; plus.append(f"Adequate liquidity ({fmt_usd(liq)})")

    clamped = min(rug_score, 100)
    if clamped >= 95:    score += 3; plus.append(f"Excellent rug score ({clamped}/100)")
    elif clamped >= 85:  score += 2; plus.append(f"Good rug score ({clamped}/100)")
    else:                score += 1; plus.append(f"Acceptable rug score ({clamped}/100)")

    if top1_pct > 0:
        if top1_pct < 5:    score += 3; plus.append(f"Top holder very low ({top1_pct:.1f}%)")
        elif top1_pct < 10: score += 2; plus.append(f"Top holder healthy ({top1_pct:.1f}%)")
        else:               score += 1; plus.append(f"Top holder acceptable ({top1_pct:.1f}%)")

    if top10_pct > 0:
        if top10_pct < 20:  score += 3; plus.append(f"Top 10 very distributed ({top10_pct:.1f}%)")
        elif top10_pct < 30:score += 2; plus.append(f"Top 10 well distributed ({top10_pct:.1f}%)")
        else:               score += 1; plus.append(f"Top 10 acceptable ({top10_pct:.1f}%)")

    if total > 0:
        buy_pct = buys / total * 100
        if buy_pct >= 80:   score += 3; plus.append(f"Explosive buy pressure ({buy_pct:.0f}%)")
        elif buy_pct >= 70: score += 2; plus.append(f"Strong buy pressure ({buy_pct:.0f}%)")
        else:               score += 1; plus.append(f"Decent buy pressure ({buy_pct:.0f}%)")

    if vol5m > 0 and liq > 0:
        spike = vol5m / liq
        if spike >= 0.5:    score += 3; plus.append(f"Massive 5m spike ({spike:.1f}x liquidity)")
        elif spike >= 0.2:  score += 2; plus.append(f"Strong 5m spike ({spike:.1f}x)")
        elif spike >= 0.05: score += 1; plus.append(f"Some 5m volume ({spike:.2f}x)")
        else:               minus.append("Low 5m volume")

    if liq > 0 and vol24 > 0:
        ratio = vol24 / liq
        if ratio >= 10:     score += 2; plus.append(f"Very high 24h vol/liq ratio ({ratio:.1f}x)")
        elif ratio >= 3:    score += 1; plus.append(f"Good 24h volume ({ratio:.1f}x)")
        else:               minus.append(f"Moderate 24h volume ({ratio:.1f}x)")

    if 7 <= age_min <= 15:  score += 3; plus.append(f"Perfect age ({int(age_min)}m) — prime entry")
    elif 5 <= age_min <= 20:score += 2; plus.append(f"Good age ({int(age_min)}m) — still early")

    if 0 < mcap <= 100_000:   score += 3; plus.append(f"Very low mcap ({fmt_usd(mcap)}) — massive upside")
    elif mcap <= 250_000:     score += 2; plus.append(f"Low mcap ({fmt_usd(mcap)}) — good upside")
    elif mcap <= 500_000:     score += 1; plus.append(f"Moderate mcap ({fmt_usd(mcap)})")

    try:
        m5 = float(chg.get("m5", 0) or 0)
        h1 = float(chg.get("h1", 0) or 0)
        if m5 > 30 and h1 > 50:    score += 3; plus.append(f"Explosive momentum (5m: +{m5:.0f}%, 1h: +{h1:.0f}%)")
        elif m5 > 10 or h1 > 20:   score += 2; plus.append(f"Strong momentum (5m: {m5:+.1f}%)")
        elif m5 > 0:                score += 1; plus.append(f"Positive momentum (5m: {m5:+.1f}%)")
        elif m5 < -15:              score -= 2; minus.append(f"Dropping fast (5m: {m5:.1f}%)")
    except Exception:
        pass

    for r in (risks or []):
        lvl = (r.get("level") or "").upper()
        if lvl == "DANGER":   score -= 4; minus.append(f"DANGER: {r.get('name','')}")
        elif lvl == "WARN":   score -= 1; minus.append(f"Warning: {r.get('name','')}")

    if score >= 22:
        return f"PERFECT (score: {score})\nHigh confidence entry.", plus, minus
    elif score >= 14:
        return f"GOOD (score: {score})\nSolid. Worth a calculated entry.", plus, minus
    else:
        return f"RISKY (score: {score})\nPassed filters but signals are mixed. Be careful.", plus, minus


# ── Alert builder ─────────────────────────────────────────────────────────────

def build_alert(pair, rug_score, risks, holders, source,
                is_graduated, holder_velocity, velocity_desc,
                whale_found, whale_desc) -> str:
    base      = pair.get("baseToken") or {}
    name      = base.get("name", "Unknown")
    symbol    = base.get("symbol", "???")
    mint      = base.get("address", "")
    chain     = pair.get("chainId", "").upper()
    dex       = pair.get("dexId", "").upper()
    liq       = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    vol24     = float((pair.get("volume") or {}).get("h24", 0) or 0)
    vol5m     = float((pair.get("volume") or {}).get("m5", 0) or 0)
    mcap      = float(pair.get("fdv", 0) or 0)
    price_usd = pair.get("priceUsd", "N/A")
    chg       = pair.get("priceChange") or {}
    created   = pair.get("pairCreatedAt", 0) or 0
    txns      = (pair.get("txns") or {}).get("h24") or {}
    buys      = txns.get("buys", 0)
    sells     = txns.get("sells", 0)
    total     = buys + sells
    bs        = f"{buys/total*100:.0f}% buys ({buys}B/{sells}S)" if total else "no txns"
    website, twitter = extract_socials(pair)

    top1_pct  = "N/A"
    top10_pct = "N/A"
    h_lines   = "  N/A"
    if holders:
        pcts      = [normalize_pct(float(h.get("pct", 0))) for h in holders[:10]]
        top1_pct  = f"{pcts[0]:.2f}%"
        top10_pct = f"{sum(pcts):.2f}%"
        lines = []
        for i, h in enumerate(holders[:10], 1):
            addr  = h.get("address", "???")
            short = f"{addr[:4]}...{addr[-4:]}"
            p     = normalize_pct(float(h.get("pct", 0)))
            tag   = " [insider]" if h.get("insider") else (" [owner]" if h.get("owner") else "")
            lines.append(f"  {i:>2}. {short}{tag} - {p:.2f}%")
        h_lines = "\n".join(lines)

    risk_text = ""
    if risks:
        risk_text = "\nRisk flags:\n" + "\n".join(
            f"  - {r.get('name','')}: {r.get('description','')}" for r in risks[:4])

    rating_line, plus_r, minus_r = rate_gem(
        pair, rug_score, holders, risks, website, twitter,
        is_graduated, holder_velocity, velocity_desc, whale_found, whale_desc)

    emoji = "💎" if "PERFECT" in rating_line else ("✅" if "GOOD" in rating_line else "⚠️")
    plus_text  = "\n".join(f"  + {r}" for r in plus_r)  or "  None"
    minus_text = "\n".join(f"  - {r}" for r in minus_r) or "  None"
    grad_line  = "Yes (Pump.fun graduate)" if is_graduated else "No"
    dex_link   = f"https://dexscreener.com/{pair.get('chainId','')}/{mint}"
    rug_link   = f"https://rugcheck.xyz/tokens/{mint}"

    return (
        f"NEW GEM FOUND\n"
        f"Source: {source}\n\n"
        f"---- GEM RATING ----\n"
        f"{emoji} {rating_line}\n\n"
        f"Positives:\n{plus_text}\n\n"
        f"Negatives:\n{minus_text}\n"
        f"--------------------\n\n"
        f"Name: {name} (${symbol})\n"
        f"Chain: {chain} | DEX: {dex}\n"
        f"Pump.fun Graduate: {grad_line}\n"
        f"Address: {mint}\n"
        f"Created: {minutes_ago(created) if created else 'Unknown'}\n\n"
        f"Price: ${price_usd}\n"
        f"5m: {pct_arrow(chg.get('m5',0))} | 1h: {pct_arrow(chg.get('h1',0))} | 6h: {pct_arrow(chg.get('h6',0))}\n\n"
        f"Liquidity: {fmt_usd(liq)}\n"
        f"Market Cap: {fmt_usd(mcap)}\n"
        f"24h Volume: {fmt_usd(vol24)}\n"
        f"5m Volume: {fmt_usd(vol5m)}\n"
        f"Buy/Sell: {bs}\n\n"
        f"Top Holder: {top1_pct}\n"
        f"Top 10 Combined: {top10_pct}\n\n"
        f"Top 10 Holders:\n{h_lines}\n\n"
        f"Rug Score: {rug_label(rug_score)}"
        f"{risk_text}\n\n"
        f"Website: {website}\n"
        f"Twitter: {twitter}\n\n"
        f"PASSED ALL FILTERS\n\n"
        f"DexScreener: {dex_link}\n"
        f"Rugcheck: {rug_link}"
    )


# ── Send alert ────────────────────────────────────────────────────────────────

async def send_alert(bot, pair, rug_score, risks, holders, source,
                     is_graduated, holder_velocity, velocity_desc,
                     whale_found, whale_desc, portfolio, whales):
    sym  = (pair.get("baseToken") or {}).get("symbol", "???")
    name = (pair.get("baseToken") or {}).get("name", "Unknown")
    mint = (pair.get("baseToken") or {}).get("address", "")
    log.info("GEM: %s — sending alert", sym)
    text = build_alert(pair, rug_score, risks, holders, source, is_graduated,
                       holder_velocity, velocity_desc, whale_found, whale_desc)
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text, disable_web_page_preview=True)
    except TelegramError as te:
        log.error("Telegram send error: %s", te)
        return
    try:
        entry_price = float(pair.get("priceUsd", 0) or 0)
    except Exception:
        entry_price = 0
    if mint and entry_price > 0:
        portfolio.append({
            "mint": mint, "symbol": sym, "name": name,
            "chain": pair.get("chainId", ""),
            "entry_price": entry_price,
            "alert_time": int(time.time()),
            "holders_snap": holders[:5],
            "pumped_alerted": False,
            "dumped_alerted": False,
        })
    record_whale_result(holders, whales, won=False)


# ── Follow-up price monitor ───────────────────────────────────────────────────

async def followup_loop(bot, portfolio, whales, session):
    while True:
        await asyncio.sleep(FOLLOWUP_INTERVAL)
        now     = int(time.time())
        to_keep = []
        for item in portfolio:
            age = now - item.get("alert_time", now)
            if age > FOLLOWUP_DURATION:
                pair = await get_pair_data(session, item["mint"])
                if pair:
                    try:
                        cur   = float(pair.get("priceUsd", 0) or 0)
                        entry = item["entry_price"]
                        chg   = ((cur - entry) / entry * 100) if entry > 0 else 0
                        record_whale_result(item.get("holders_snap", []), whales, won=chg >= 30)
                    except Exception:
                        pass
                continue
            try:
                pair = await get_pair_data(session, item["mint"])
                if not pair:
                    to_keep.append(item); continue
                cur   = float(pair.get("priceUsd", 0) or 0)
                entry = item["entry_price"]
                if entry <= 0:
                    to_keep.append(item); continue
                chg_pct = (cur - entry) / entry * 100
                dex_link = f"https://dexscreener.com/{item['chain']}/{item['mint']}"
                if chg_pct >= PUMP_THRESHOLD and not item["pumped_alerted"]:
                    item["pumped_alerted"] = True
                    await bot.send_message(chat_id=CHAT_ID, text=(
                        f"PUMP ALERT\n\n"
                        f"{item['name']} (${item['symbol']}) is up {chg_pct:+.1f}%\n\n"
                        f"Entry: ${entry:.8f}\nNow: ${cur:.8f}\n\n"
                        f"Consider taking some profit!\n\n"
                        f"DexScreener: {dex_link}"
                    ), disable_web_page_preview=True)
                    record_whale_result(item.get("holders_snap", []), whales, won=True)
                if chg_pct <= DUMP_THRESHOLD and not item["dumped_alerted"]:
                    item["dumped_alerted"] = True
                    await bot.send_message(chat_id=CHAT_ID, text=(
                        f"DUMP ALERT\n\n"
                        f"{item['name']} (${item['symbol']}) is down {chg_pct:.1f}%\n\n"
                        f"Entry: ${entry:.8f}\nNow: ${cur:.8f}\n\n"
                        f"Consider cutting losses!\n\n"
                        f"DexScreener: {dex_link}"
                    ), disable_web_page_preview=True)
                to_keep.append(item)
            except Exception as e:
                log.warning("Follow-up error: %s", e)
                to_keep.append(item)
        portfolio.clear()
        portfolio.extend(to_keep)


# ── Daily summary ─────────────────────────────────────────────────────────────

async def daily_summary_loop(bot, portfolio, whales, session):
    while True:
        now_utc  = datetime.now(timezone.utc)
        next_8am = now_utc.replace(hour=DAILY_SUMMARY_HOUR, minute=0, second=0, microsecond=0)
        if now_utc >= next_8am:
            from datetime import timedelta; next_8am = next_8am + timedelta(days=1)
        await asyncio.sleep((next_8am - now_utc).total_seconds())
        try:
            today_start = int(time.time()) - 86400
            todays      = [p for p in portfolio if p.get("alert_time", 0) >= today_start]
            pumped = dumped = live = 0
            lines  = []
            for item in todays:
                pair = await get_pair_data(session, item["mint"])
                if pair:
                    try:
                        cur  = float(pair.get("priceUsd", 0) or 0)
                        chg  = ((cur - item["entry_price"]) / item["entry_price"] * 100) if item["entry_price"] > 0 else 0
                        e    = "💎" if chg >= 50 else ("✅" if chg >= 0 else "❌")
                        lines.append(f"  {e} ${item['symbol']}: {fmt_pct(chg)}")
                        if chg >= 30:   pumped += 1
                        elif chg <= -30:dumped += 1
                        else:           live   += 1
                    except Exception:
                        lines.append(f"  ? ${item['symbol']}: N/A")
            good_whales = sum(1 for w in whales.values() if w.get("wins", 0) >= 2)
            summary = (
                f"DAILY SUMMARY\n"
                f"{now_utc.strftime('%B %d, %Y')}\n\n"
                f"Gems sent today: {len(todays)}\n"
                f"Pumped 30%+: {pumped}\n"
                f"Dumped 30%+: {dumped}\n"
                f"Still live: {live}\n\n"
            )
            if lines:
                summary += "Performance:\n" + "\n".join(lines) + "\n\n"
            summary += (
                f"Whale tracker:\n"
                f"  Wallets tracked: {len(whales)}\n"
                f"  Proven winners (2+ wins): {good_whales}\n\n"
                f"Cloud memory: ACTIVE (Supabase)\n"
                f"Scanner running 24/7!"
            )
            await bot.send_message(chat_id=CHAT_ID, text=summary, disable_web_page_preview=True)
            log.info("Daily summary sent.")
        except Exception as e:
            log.error("Daily summary error: %s", e)


# ── Auto scanner loop ─────────────────────────────────────────────────────────

async def scanner_loop(bot, seen, session, portfolio, whales):
    while True:
        try:
            log.info("Scanning...")
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
                    if age_min < MIN_AGE_MINUTES or age_min > MAX_AGE_MINUTES:
                        continue
                rug_score, risks, holders, deployer = await get_rugcheck(session, addr)
                is_graduated     = await is_pumpfun_graduate(session, addr)
                vel_score, vel_d = check_holder_velocity(addr, len(holders))
                whale_f, whale_d = check_known_whales(holders, whales)
                passed, reason   = passes_filters(pair, rug_score, holders, deployer, is_graduated, risks)
                sym = (pair.get("baseToken") or {}).get("symbol", addr[:8])
                if not passed:
                    log.info("Filtered: %s — %s", sym, reason)
                    continue
                await send_alert(bot, pair, rug_score, risks, holders, "Auto-scan",
                                 is_graduated, vel_score, vel_d, whale_f, whale_d, portfolio, whales)
                await asyncio.sleep(1)
        except Exception as e:
            log.error("Scanner error: %s", e)
        log.info("Sleeping %ds...", SCAN_INTERVAL)
        await asyncio.sleep(SCAN_INTERVAL)


# ── Group handler ─────────────────────────────────────────────────────────────

def make_group_handler(seen, session, bot, portfolio, whales):
    async def handle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        msg = update.message
        if not msg or not msg.text:
            return
        text       = msg.text
        chat       = msg.chat
        sender     = msg.from_user
        group_name = chat.title or str(chat.id)
        user_name  = (sender.username or sender.first_name) if sender else "unknown"
        found = []
        for a in SOLANA_ADDR_RE.findall(text): found.append(("address", a))
        for a in EVM_ADDR_RE.findall(text):    found.append(("address", a))
        for s in SYMBOL_RE.findall(text):      found.append(("symbol", s))
        if not found:
            return
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
                rug_score, risks, holders, deployer = await get_rugcheck(session, mint)
                is_grad          = await is_pumpfun_graduate(session, mint)
                vel_score, vel_d = check_holder_velocity(mint, len(holders))
                whale_f, whale_d = check_known_whales(holders, whales)
                passed, reason   = passes_filters(pair, rug_score, holders, deployer, is_grad, risks)
                sym = (pair.get("baseToken") or {}).get("symbol", query)
                if not passed:
                    try:
                        await bot.send_message(chat_id=CHAT_ID, text=(
                            f"GROUP MENTION (failed filters)\n\n"
                            f"Group: {group_name}\nUser: @{user_name}\n"
                            f"Coin: {sym} ({query})\nFailed: {reason}\n\n"
                            f"Message: {text[:200]}"
                        ), disable_web_page_preview=True)
                    except TelegramError:
                        pass
                    continue
                source = f"Group: '{group_name}' by @{user_name}"
                await send_alert(bot, pair, rug_score, risks, holders, source,
                                 is_grad, vel_score, vel_d, whale_f, whale_d, portfolio, whales)
            except Exception as e:
                log.error("Group handler error: %s", e)
            await asyncio.sleep(0.5)
    return handle


# ── Entry point ───────────────────────────────────────────────────────────────

async def post_init(app):
    session = aiohttp.ClientSession()
    seen, whales, portfolio = await load_all_from_cloud(session)
    bot = app.bot
    app.bot_data.update({"session": session, "seen": seen,
                          "whales": whales, "portfolio": portfolio})
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        make_group_handler(seen, session, bot, portfolio, whales)
    ))
    try:
        await bot.send_message(chat_id=CHAT_ID, text=(
            "Memecoin Scanner v5.3 is LIVE\n"
            "PERMANENT CLOUD MEMORY EDITION\n\n"
            "Memory saved to Supabase cloud:\n"
            "- Seen tokens: never alerts same coin twice\n"
            "- Whale wallets: learning your winners\n"
            "- Portfolio: tracking price follow-ups\n"
            "- Auto-backup every 5 minutes\n"
            "- Survives Railway restarts\n\n"
            "All filters active + bulletproof rug detection.\n"
            "Group scanner: ACTIVE"
        ))
    except TelegramError as e:
        log.error("Startup message failed: %s", e)
    asyncio.create_task(scanner_loop(bot, seen, session, portfolio, whales))
    asyncio.create_task(followup_loop(bot, portfolio, whales, session))
    asyncio.create_task(daily_summary_loop(bot, portfolio, whales, session))
    asyncio.create_task(backup_loop(session, seen, whales, portfolio))


async def post_shutdown(app):
    session   = app.bot_data.get("session")
    seen      = app.bot_data.get("seen", set())
    whales    = app.bot_data.get("whales", {})
    portfolio = app.bot_data.get("portfolio", [])
    if session:
        await save_all_to_cloud(session, seen, whales, portfolio)
        await session.close()


def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    log.info("Starting bot v5.3...")
    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
