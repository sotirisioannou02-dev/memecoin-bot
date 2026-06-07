"""
Memecoin Quality-Coin Screener v6.0
- BIG CHANGE: repurposed from a 3-30min fresh-launch sniper into a
  "quality coin screener". It now finds LIQUID, ACTIVE, non-rug coins
  that are 2-24h old and sends them to you to CHART by hand. The bot
  screens for quality; YOU apply your own technical analysis (support/
  resistance, Fibonacci, multi-timeframe) and decide the entry.
  Long-only — you cannot short these coins on Solana DEXs.
- Filter changes: age 2-24h, liquidity $15k-$5M, mcap $50k-$5M,
  new 24h-volume floor ($50k), buy-pressure filter REMOVED.
- Everything else from v5.9.3 intact: 'show all failed filters' logging,
  watchdog/self-monitoring, /status, ancient-coin blacklist, cloud memory.
"""
import asyncio
import logging
import os
import re
import time
import json
from datetime import datetime, timezone, timedelta
import aiohttp
from telegram import Bot, Update
from telegram.error import TelegramError
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes

# Config
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHAT_ID = os.getenv("CHAT_ID", "YOUR_CHAT_ID_HERE")
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://xzxplgjpfknoeezawmvr.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "8fdbbc24-7fc5-4d65-a454-90f015afa71e")
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_API = "https://api.helius.xyz/v0"
HELIUS_WS = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

RAYDIUM_AMM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp"
RAYDIUM_CPMM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

SKIP_MINTS = {
    "So11111111111111111111111111111111111111112",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
}

SUPABASE_TABLE = "bot_memory"
BACKUP_INTERVAL = 300
SCAN_INTERVAL = 60
# ── v6.0: "24h chartable quality coin" defaults ────────────────
# Goal changed: instead of 3-30min fresh launches (mostly junk, unchartable),
# find coins 2-24h old that are LIQUID, ACTIVE, and not rugs — so YOU can
# pull up the chart and apply your own TA (support/resistance, Fibonacci,
# multi-timeframe) and decide the entry by hand. The bot is a candidate
# finder; you are the trader.
MIN_LIQUIDITY = 15_000          # was 7k — quality coins have real liquidity
MAX_LIQUIDITY = 5_000_000       # was 2M — allow bigger, more established names
MIN_AGE_MINUTES = 120           # was 3 — min 2h so there's a chartable history
MAX_AGE_MINUTES = 1440          # was 30 — max 24h
MAX_TOP_HOLDER = 15.0
MAX_TOP10 = 40.0
MIN_BUY_PRESSURE = 55.0         # v6.0: NO LONGER a hard filter (see passes_filters).
                                # Kept only so the value still prints in alerts.
MIN_MCAP = 50_000               # was 5k — real coins, not dust
MAX_MCAP = 5_000_000            # was 500k — allow coins that already showed strength
MIN_RUG_SCORE = 60
MIN_VOLUME_24H = 50_000         # v6.0 NEW: proves the coin is ACTIVE, not dead.
                                # (We can't get total holder count from our data
                                #  sources, so volume+liquidity is the "alive" proxy.)
FOLLOWUP_INTERVAL = 60
FOLLOWUP_DURATION = 3600 * 4
PUMP_THRESHOLD = 80.0
DUMP_THRESHOLD = -25.0
DAILY_SUMMARY_HOUR = 8

# ── v5.9.2: Self-monitoring / watchdog config ──────────────────
# How long a single source can keep returning 0 before the bot warns you.
SOURCE_DEAD_MINUTES = 30
# How long the WHOLE scanner can find nothing across all sources before warning.
ALL_QUIET_MINUTES = 30
# Min gap between two identical health warnings, so you aren't spammed.
HEALTH_ALERT_COOLDOWN = 1800   # 30 min
# Consecutive scanner-loop crashes before an escalation alert.
SCANNER_ERROR_THRESHOLD = 3

# Paper trading
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"
PAPER_TRADE_SIZE = float(os.getenv("PAPER_TRADE_SIZE", "20"))

SEEN_FILE = "seen_tokens.json"
WHALE_FILE = "whale_wallets.json"
PORTFOLIO_FILE = "portfolio.json"
PAPER_FILE = "paper_trades.json"

DEXSCREENER_PAIRS = "https://api.dexscreener.com/latest/dex/tokens/{address}"
DEXSCREENER_SEARCH = "https://api.dexscreener.com/latest/dex/search?q={query}"
RUGCHECK_SUMMARY = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
RUGCHECK_FULL = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"

SOLANA_ADDR_RE = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')
EVM_ADDR_RE = re.compile(r'\b0x[a-fA-F0-9]{40}\b')
SYMBOL_RE = re.compile(r'\$([A-Z]{2,10})\b')

_holder_snapshots: dict = {}

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# v5.9.2: Self-monitoring / watchdog state
# ─────────────────────────────────────────────
# Tracks the last time each source returned at least one token, the last
# time the scanner did anything useful, error streaks, and a per-message
# cooldown so the same warning doesn't fire every 60 seconds.
HEALTH = {
    "start_time": int(time.time()),
    "source_last_ok": {            # source name -> unix ts of last non-empty result
        "Helius Enhanced": int(time.time()),
        "Helius RPC": int(time.time()),
        "DexScreener": int(time.time()),
        "GeckoTerminal": int(time.time()),
    },
    "source_warned": {},           # source name -> True if we've already warned it's dead
    "last_collected_time": int(time.time()),  # last scan that collected >0 tokens
    "all_quiet_warned": False,
    "scanner_errors": 0,           # consecutive scanner-loop exceptions
    "last_alert_at": {},           # alert-key -> unix ts (cooldown tracking)
    "scans_completed": 0,
    "alerts_sent_today": 0,
    "supabase_fails": 0,
    "rate_limit_hits": 0,
}


def _cooldown_ok(key):
    """True if enough time has passed since we last sent this alert key."""
    now = int(time.time())
    last = HEALTH["last_alert_at"].get(key, 0)
    if now - last >= HEALTH_ALERT_COOLDOWN:
        HEALTH["last_alert_at"][key] = now
        return True
    return False


async def health_alert(bot, key, text):
    """Send a watchdog alert to Telegram, respecting the per-key cooldown."""
    if not _cooldown_ok(key):
        return
    try:
        await bot.send_message(chat_id=CHAT_ID, text=f"WATCHDOG ALERT\n\n{text}",
                               disable_web_page_preview=True)
        log.warning("Health alert sent [%s]: %s", key, text.replace("\n", " ")[:120])
    except TelegramError as e:
        log.error("Health alert send failed: %s", e)



# ─────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────
def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, data):
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.warning("Save error %s: %s", path, e)

def load_seen(): return set(load_json(SEEN_FILE, []))
def save_seen(s): save_json(SEEN_FILE, list(s))
def load_whales(): return load_json(WHALE_FILE, {})
def save_whales(w): save_json(WHALE_FILE, w)
def load_portfolio(): return load_json(PORTFOLIO_FILE, [])
def save_portfolio(p): save_json(PORTFOLIO_FILE, p)
def load_paper(): return load_json(PAPER_FILE, [])
def save_paper(p): save_json(PAPER_FILE, p)


# ─────────────────────────────────────────────
# Supabase
# ─────────────────────────────────────────────
def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

async def cloud_save(session, key, value):
    try:
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
        body = {"key": key, "value": value, "updated_at": datetime.now(timezone.utc).isoformat()}
        async with session.post(url, headers=sb_headers(), json=body,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status not in (200, 201):
                log.warning("Cloud save failed %s: %s", key, r.status)
                HEALTH["supabase_fails"] += 1
            else:
                HEALTH["supabase_fails"] = 0  # a success clears the streak
    except Exception as e:
        log.warning("Cloud save error %s: %s", key, e)
        HEALTH["supabase_fails"] += 1

async def cloud_load(session, key):
    try:
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?key=eq.{key}&select=value"
        async with session.get(url, headers=sb_headers(),
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                rows = await r.json()
                if rows:
                    return rows[0]["value"]
    except Exception as e:
        log.warning("Cloud load error %s: %s", key, e)
    return None

async def load_all_from_cloud(session):
    log.info("Loading memory from Supabase...")
    seen_d = await cloud_load(session, "seen_tokens")
    whale_d = await cloud_load(session, "whale_wallets")
    port_d = await cloud_load(session, "portfolio")
    seen = set(seen_d if seen_d is not None else load_json(SEEN_FILE, []))
    whales = dict(whale_d if whale_d is not None else load_json(WHALE_FILE, {}))
    portfolio = list(port_d if port_d is not None else load_json(PORTFOLIO_FILE, []))
    log.info("Loaded: %d seen, %d whales, %d portfolio", len(seen), len(whales), len(portfolio))
    return seen, whales, portfolio

async def save_all_to_cloud(session, seen, whales, portfolio):
    await asyncio.gather(
        cloud_save(session, "seen_tokens", list(seen)),
        cloud_save(session, "whale_wallets", whales),
        cloud_save(session, "portfolio", portfolio),
    )
    for path, data in [(SEEN_FILE, list(seen)), (WHALE_FILE, whales), (PORTFOLIO_FILE, portfolio)]:
        try:
            with open(path, "w") as f:
                json.dump(data, f)
        except Exception:
            pass

async def backup_loop(session, seen, whales, portfolio, bot=None):
    while True:
        await asyncio.sleep(BACKUP_INTERVAL)
        log.info("Auto-backup to Supabase...")
        # v5.9.1: cap raised from 1000 -> 50000. The blacklist of ancient coins
        # lives in 'seen', and a 1000 cap got flushed within minutes, letting
        # old coins re-enter the pending pool. 50k addrs is only ~2MB.
        if len(seen) > 50_000:
            sl = list(seen)
            seen.clear()
            seen.update(sl[-50_000:])
        await save_all_to_cloud(session, seen, whales, portfolio)
        # v5.9.2: warn if cloud memory has been failing repeatedly. Without it,
        # a Railway restart would wipe the blacklist and whale stats.
        if bot and HEALTH["supabase_fails"] >= 3:
            await health_alert(bot, "supabase_down",
                               f"Supabase cloud save has failed {HEALTH['supabase_fails']} "
                               f"times in a row.\nMemory still works locally, but a Railway "
                               f"restart could wipe the blacklist and whale stats. "
                               f"Check the SUPABASE_KEY and project status.")


# ─────────────────────────────────────────────
# Formatters
# ─────────────────────────────────────────────
def fmt_usd(n):
    try:
        n = float(n)
        if n >= 1_000_000: return f"${n/1_000_000:.2f}M"
        if n >= 1_000: return f"${n/1_000:.1f}K"
        return f"${n:.4f}" if n < 1 else f"${n:.2f}"
    except Exception:
        return "N/A"

def fmt_pct(n):
    try:
        n = float(n)
        return f"{'+' if n >= 0 else ''}{n:.1f}%"
    except Exception:
        return "N/A"

def minutes_ago(unix_ms):
    diff = (int(time.time() * 1000) - unix_ms) / 60_000
    if diff < 60: return f"{int(diff)}m ago"
    if diff < 1440: return f"{diff/60:.1f}h ago"
    return f"{diff/1440:.1f}d ago"

def pct_arrow(v):
    try:
        v = float(v)
        return f"{'up' if v >= 0 else 'down'} {v:+.2f}%"
    except Exception:
        return "N/A"

def rug_label(score):
    d = min(score, 100)
    if score >= 80: return f"LOW ({d}/100)"
    if score >= 60: return f"MEDIUM ({d}/100)"
    if score >= 30: return f"HIGH ({d}/100)"
    return f"VERY HIGH ({d}/100)"

def normalize_pct(raw):
    return raw * 100 if raw <= 1.0 else raw


# ─────────────────────────────────────────────
# Social links
# ─────────────────────────────────────────────
def extract_socials(pair):
    website = ""
    twitter = ""
    info = pair.get("info") or {}
    for w in (info.get("websites") or []):
        url = w.get("url", "")
        if url and not website:
            website = url
    for s in (info.get("socials") or []):
        stype = (s.get("type") or "").lower()
        url = s.get("url", "")
        if stype in ("twitter", "x") and url and not twitter:
            twitter = url
        if stype == "website" and url and not website:
            website = url
    for link in (pair.get("links") or []):
        ltype = (link.get("type") or link.get("label") or "").lower()
        url = link.get("url", "")
        if ("twitter" in ltype or "x.com" in url or "twitter.com" in url) and not twitter:
            twitter = url
        elif not website and url:
            website = url
    return website, twitter


# ─────────────────────────────────────────────
# API helpers
# ─────────────────────────────────────────────
async def fetch_json(session, url):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        log.warning("fetch error %s: %s", url[:60], e)
    return None

async def get_raydium_liquidity_helius(session, mint):
    try:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenLargestAccounts",
            "params": [mint, {"commitment": "confirmed"}]
        }
        async with session.post(HELIUS_RPC, json=payload,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                accts = (data.get("result") or {}).get("value") or []
                if accts:
                    largest = float(accts[0].get("uiAmount", 0) or 0)
                    if largest > 0:
                        return max(largest * 0.001, 1000)
    except Exception as e:
        log.warning("Helius liquidity error: %s", e)
    return 0

async def get_pair_data(session, address):
    data = await fetch_json(session, DEXSCREENER_PAIRS.format(address=address))
    if data:
        pairs = data.get("pairs") or []
        if pairs:
            best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
            liq = float((best.get("liquidity") or {}).get("usd", 0) or 0)
            if liq == 0:
                hl = await get_raydium_liquidity_helius(session, address)
                if hl > 0:
                    log.info("Helius liquidity fallback: %s $%.0f", address[:8], hl)
                    if not best.get("liquidity"):
                        best["liquidity"] = {}
                    best["liquidity"]["usd"] = hl
            return best
    return None

async def search_pair(session, query):
    data = await fetch_json(session, DEXSCREENER_SEARCH.format(query=query))
    if data:
        pairs = data.get("pairs") or []
        if pairs:
            return max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
    return None

async def get_rugcheck(session, mint):
    score = -1
    risks = []
    holders = []
    deployer = ""
    data = await fetch_json(session, RUGCHECK_SUMMARY.format(mint=mint))
    if data:
        score = int(data.get("score", -1))
        risks = data.get("risks", [])
    data = await fetch_json(session, RUGCHECK_FULL.format(mint=mint))
    if data:
        holders = data.get("topHolders", [])
        deployer = data.get("creator", "") or data.get("deployer", "") or ""
        if score == -1:
            score = int(data.get("score", -1))
    return score, risks, holders, deployer

async def is_pumpfun_graduate(session, mint):
    try:
        data = await fetch_json(session, DEXSCREENER_PAIRS.format(address=mint))
        if not data:
            return False
        for pair in (data.get("pairs") or []):
            dex = (pair.get("dexId") or "").lower()
            if "raydium" in dex:
                url = (pair.get("url") or "").lower()
                labels = [str(l).lower() for l in (pair.get("labels") or [])]
                info = pair.get("info") or {}
                websites = [str(w.get("url", "")).lower() for w in (info.get("websites") or [])]
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


# ─────────────────────────────────────────────
# v5.8 KEY CHANGE: Pre-filter helpers
# Instead of: get address → fetch pair → check age → skip (wasteful)
# Now:        get address WITH timestamp → check age → only fetch if fresh ✅
# ─────────────────────────────────────────────
def is_age_in_window(timestamp_ms):
    """Full window check (min AND max) — used at scanner stage right before alerting."""
    if not timestamp_ms:
        return False
    age_min = (int(time.time() * 1000) - timestamp_ms) / 60_000
    return MIN_AGE_MINUTES <= age_min <= MAX_AGE_MINUTES

def is_not_too_old(timestamp_ms):
    """
    v5.9: Source-level check — MAX age only.
    Brand-new coins (seconds old) MUST be collected now so they can
    mature into the 3-30m window. The min-age check happens later at
    the scanner stage. Filtering min-age here is what made v5.8 return 0.
    """
    if not timestamp_ms:
        return False
    age_min = (int(time.time() * 1000) - timestamp_ms) / 60_000
    return age_min <= MAX_AGE_MINUTES


# ─────────────────────────────────────────────
# Helius sources — now return (mint, timestamp_ms) tuples
# ─────────────────────────────────────────────
async def get_fresh_from_helius(session):
    """Returns list of (mint, timestamp_ms) — only within age window."""
    results = []
    now_s = int(time.time())
    max_age_s = MAX_AGE_MINUTES * 60

    programs = [
        (RAYDIUM_AMM, "Raydium AMM"),
        (RAYDIUM_CPMM, "Raydium CPMM"),
        (PUMPFUN_PROGRAM, "Pump.fun"),
    ]
    for program, name in programs:
        try:
            url = f"{HELIUS_API}/addresses/{program}/transactions?api-key={HELIUS_API_KEY}&limit=100"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    continue
                txs = await r.json()
                if not isinstance(txs, list):
                    continue
                fresh = 0
                for tx in txs:
                    ts = tx.get("timestamp", 0) or 0
                    if ts <= 0:
                        continue
                    age_s = now_s - ts
                    # v5.9: MAX age only — let brand-new coins through to mature
                    if age_s > max_age_s:
                        continue
                    ts_ms = ts * 1000
                    for transfer in (tx.get("tokenTransfers") or []):
                        mint = transfer.get("mint", "")
                        if mint and mint not in SKIP_MINTS:
                            if not any(m == mint for m, _ in results):
                                results.append((mint, ts_ms))
                                fresh += 1
                log.info("Helius %s: %d fresh tokens (pre-filtered)", name, fresh)
        except Exception as e:
            log.warning("Helius %s error: %s", name, e)

    log.info("Helius Enhanced total fresh: %d", len(results))
    return results


async def get_fresh_from_helius_rpc(session):
    """Returns list of (mint, timestamp_ms) — only within age window."""
    results = []
    now_s = int(time.time())
    max_age_s = MAX_AGE_MINUTES * 60

    async def fetch_sigs(program):
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getSignaturesForAddress",
            "params": [program, {"limit": 100, "commitment": "confirmed"}]
        }
        try:
            async with session.post(HELIUS_RPC, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    # v5.9: MAX age only on signatures (let new coins mature)
                    return [
                        (s["signature"], s.get("blockTime", 0))
                        for s in (data.get("result") or [])
                        if not s.get("err")
                        and (s.get("blockTime") or 0) > 0
                        and abs(now_s - (s.get("blockTime") or 0)) <= max_age_s
                    ]
        except Exception:
            pass
        return []

    sig_results = await asyncio.gather(
        fetch_sigs(RAYDIUM_AMM),
        fetch_sigs(RAYDIUM_CPMM),
        fetch_sigs(PUMPFUN_PROGRAM),
        return_exceptions=True
    )

    all_sigs = []
    for r in sig_results:
        if isinstance(r, list):
            all_sigs.extend(r)

    log.info("Helius RPC: %d fresh sigs (pre-filtered by age)", len(all_sigs))

    # Only fetch transaction details for fresh signatures
    for sig, block_time in all_sigs[:20]:
        try:
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getTransaction",
                "params": [sig, {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }]
            }
            async with session.post(HELIUS_RPC, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    continue
                tx = (await r.json()).get("result")
                if not tx:
                    continue
                ts_ms = block_time * 1000
                meta = tx.get("meta") or {}
                for bal in (meta.get("postTokenBalances") or []):
                    mint = bal.get("mint", "")
                    if mint and mint not in SKIP_MINTS:
                        if not any(m == mint for m, _ in results):
                            results.append((mint, ts_ms))
        except Exception:
            continue

    log.info("Helius RPC fresh mints: %d", len(results))
    return results


async def get_fresh_from_dexscreener(session):
    """Returns list of (mint, timestamp_ms) — only within age window."""
    results = []
    now_ms = int(time.time() * 1000)
    max_age_ms = MAX_AGE_MINUTES * 60 * 1000

    for url in [
        "https://api.dexscreener.com/latest/dex/pairs/solana/raydium",
        "https://api.dexscreener.com/latest/dex/pairs/solana/orca",
        "https://api.dexscreener.com/latest/dex/pairs/solana/meteora",
    ]:
        try:
            data = await fetch_json(session, url)
            if not data:
                continue
            fresh_count = 0
            for pair in (data.get("pairs") or []):
                created = pair.get("pairCreatedAt", 0) or 0
                if not created:
                    continue
                age_ms = now_ms - created
                # v5.9: MAX age only — keep brand-new pairs so they can mature
                if age_ms > max_age_ms or age_ms < 0:
                    continue
                addr = (pair.get("baseToken") or {}).get("address", "")
                sym = (pair.get("baseToken") or {}).get("symbol", "???")
                if addr and addr not in SKIP_MINTS:
                    if not any(m == addr for m, _ in results):
                        results.append((addr, created))
                        fresh_count += 1
                        log.info("DexScreener fresh: %s (%.1fm)", sym, age_ms / 60_000)
            log.info("DexScreener %s: %d fresh pairs", url.split("/")[-1], fresh_count)
        except Exception as e:
            log.warning("DexScreener error: %s", e)

    return results


async def get_fresh_from_geckoterminal(session):
    """Returns list of (mint, timestamp_ms) — only within age window."""
    results = []
    now_utc = datetime.now(timezone.utc)

    for url in [
        "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1",
        "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=2",
    ]:
        try:
            async with session.get(url, headers={"Accept": "application/json"},
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    continue
                data = await r.json()
                fresh_count = 0
                for pool in (data.get("data") or []):
                    attrs = pool.get("attributes") or {}
                    created_str = attrs.get("pool_created_at") or ""
                    addr = attrs.get("base_token_address") or ""
                    if not addr or not created_str:
                        continue
                    try:
                        created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                        age_min = (now_utc - created_dt).total_seconds() / 60
                        # v5.9: MAX age only — keep new pools so they can mature
                        if age_min > MAX_AGE_MINUTES or age_min < 0:
                            continue
                        ts_ms = int(created_dt.timestamp() * 1000)
                        if addr not in SKIP_MINTS:
                            if not any(m == addr for m, _ in results):
                                results.append((addr, ts_ms))
                                fresh_count += 1
                    except Exception:
                        pass
                log.info("GeckoTerminal page: %d fresh pools", fresh_count)
        except Exception as e:
            log.warning("GeckoTerminal error: %s", e)

    return results


async def get_all_latest_tokens(session, bot=None):
    """
    v5.8: Returns deduplicated list of (mint, timestamp_ms) tuples.
    All sources pre-filter by age — no old coins ever reach the pair fetch step.
    v5.9.2: Also records per-source health and warns if a source goes dark.
    """
    source_names = ["Helius Enhanced", "Helius RPC", "DexScreener", "GeckoTerminal"]
    results = await asyncio.gather(
        get_fresh_from_helius(session),
        get_fresh_from_helius_rpc(session),
        get_fresh_from_dexscreener(session),
        get_fresh_from_geckoterminal(session),
        return_exceptions=True
    )

    now = int(time.time())
    seen_mints = set()
    unique = []
    for name, r in zip(source_names, results):
        if isinstance(r, Exception):
            # Source threw — treat as not-ok, will trip the dead-source check below.
            log.warning("Source %s raised: %s", name, r)
            continue
        if isinstance(r, list):
            if len(r) > 0:
                # Source returned data — it's alive. Reset its dead-state.
                HEALTH["source_last_ok"][name] = now
                if HEALTH["source_warned"].get(name):
                    HEALTH["source_warned"][name] = False
                    if bot:
                        await health_alert(bot, f"recover:{name}",
                                           f"{name} is back online (returned {len(r)} tokens).")
            for mint, ts_ms in r:
                if mint not in seen_mints:
                    seen_mints.add(mint)
                    unique.append((mint, ts_ms))

    # Dead-source detection: warn once per source if it's been empty too long.
    if bot:
        for name in source_names:
            dead_for = (now - HEALTH["source_last_ok"][name]) / 60
            if dead_for >= SOURCE_DEAD_MINUTES and not HEALTH["source_warned"].get(name):
                HEALTH["source_warned"][name] = True
                await health_alert(bot, f"dead:{name}",
                                   f"{name} has returned 0 tokens for {int(dead_for)} min.\n"
                                   f"Other sources may still be working. Check if its API "
                                   f"changed or is rate-limited.")

    log.info("Total unique FRESH addresses (pre-filtered): %d", len(unique))
    return unique


# ─────────────────────────────────────────────
# Dev wallet check
# ─────────────────────────────────────────────
def dev_has_sold(holders, deployer):
    if not deployer:
        return False, ""
    deployer = deployer.lower()
    for h in holders:
        addr = (h.get("address") or "").lower()
        if addr == deployer:
            pct = normalize_pct(float(h.get("pct", 0)))
            if pct < 0.5:
                return True, f"Dev wallet almost empty ({pct:.2f}%)"
            return False, ""
    return True, "Dev wallet not found (likely sold)"


# ─────────────────────────────────────────────
# Holder velocity
# ─────────────────────────────────────────────
def check_holder_velocity(mint, current_count):
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
    rate = growth / elapsed_min
    if rate >= 20: return 3, f"Explosive growth: +{growth} in {elapsed_min:.0f}m"
    if rate >= 5:  return 2, f"Strong growth: +{growth} in {elapsed_min:.0f}m"
    if rate >= 1:  return 1, f"Steady growth: +{growth} in {elapsed_min:.0f}m"
    if growth < 0: return -1, f"Holders dropping: {growth} in {elapsed_min:.0f}m"
    return 0, f"Low growth: +{growth} in {elapsed_min:.0f}m"


# ─────────────────────────────────────────────
# Whale tracker
# ─────────────────────────────────────────────
def check_known_whales(holders, whales):
    found = []
    for h in holders[:10]:
        addr = h.get("address", "")
        if addr in whales:
            w = whales[addr]
            wins = w.get("wins", 0)
            total = w.get("total", 0)
            pct = normalize_pct(float(h.get("pct", 0)))
            wr = (wins / total * 100) if total > 0 else 0
            src = w.get("source", "unknown")
            found.append(f"{src} ({pct:.1f}% holding, {wins}/{total} wins, {wr:.0f}% WR)")
    if found:
        return True, "\n".join(found)
    return False, ""

def record_whale_result(holders, whales, won):
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
    cutoff = int(time.time()) - 30 * 86400
    to_delete = [a for a, w in whales.items() if w.get("last_seen", 0) < cutoff]
    for a in to_delete:
        del whales[a]


# ─────────────────────────────────────────────
# Filter logic
# ─────────────────────────────────────────────
def passes_filters(pair, rug_score, holders, deployer, is_graduated, risks=None):
    # v5.9.3: instead of stopping at the FIRST failed filter, collect EVERY
    # failure so the log shows the full picture (e.g. "FAILED 3/12: ...").
    # The (passed, reason) return shape is unchanged, and every original
    # reason-keyword (no Twitter, dev sold, too old, clone wallet) still
    # appears in the joined reason string, so the scanner's blacklist logic
    # keeps working exactly as before.
    fails = []

    created_at = pair.get("pairCreatedAt", 0) or 0
    if not created_at:
        # Age math depends on this, so we still have to bail early here.
        return False, "no creation time"

    # 1-2. Age window
    age_min = (int(time.time() * 1000) - created_at) / 60_000
    if age_min < MIN_AGE_MINUTES:
        fails.append(f"too fresh ({int(age_min)}m)")
    if age_min > MAX_AGE_MINUTES:
        fails.append(f"too old ({int(age_min)}m)")

    # 3-4. Liquidity
    liq = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    if liq < MIN_LIQUIDITY:
        fails.append(f"low liquidity ({fmt_usd(liq)})")
    if liq > MAX_LIQUIDITY:
        fails.append(f"liquidity too high ({fmt_usd(liq)})")

    # 5. Market cap
    mcap = float(pair.get("fdv", 0) or 0)
    if mcap > 0:
        if mcap < MIN_MCAP:
            fails.append(f"mcap too low ({fmt_usd(mcap)})")
        if mcap > MAX_MCAP:
            fails.append(f"mcap too high ({fmt_usd(mcap)})")

    # 6. Rug score
    if rug_score == -1:
        fails.append("rug score unavailable")
    elif rug_score < MIN_RUG_SCORE:
        fails.append(f"rug score too low ({min(rug_score, 100)})")

    # 7. v6.0: 24h VOLUME floor — the coin must be ACTIVE / not dead.
    # (Replaces the old buy-pressure hard filter. Buy pressure is no longer a
    #  gate because a healthy 24h coin naturally has both buys and sells, and
    #  because you may be looking to enter on either side.)
    vol24 = float((pair.get("volume") or {}).get("h24", 0) or 0)
    if vol24 < MIN_VOLUME_24H:
        fails.append(f"low 24h volume ({fmt_usd(vol24)}) - likely dead")

    # 8-10. Holder distribution / clone / owner wallets
    if holders:
        top1 = normalize_pct(float(holders[0].get("pct", 0)))
        if top1 >= MAX_TOP_HOLDER:
            fails.append(f"top holder {top1:.1f}%")
        pcts = [normalize_pct(float(h.get("pct", 0))) for h in holders[:10]]
        top10 = sum(pcts)
        if top10 >= MAX_TOP10:
            fails.append(f"top 10 too concentrated ({top10:.1f}%)")
        if len(pcts) >= 7:
            small = pcts[1:]
            if small and (max(small) - min(small)) <= 0.20:
                fails.append("clone wallet pattern detected")
        owner_count = sum(1 for h in holders[:10] if h.get("owner") or h.get("insider"))
        if owner_count >= 4:
            fails.append(f"too many owner wallets ({owner_count}/10)")

    # 11. Dev wallet sold
    dev_sold, dev_reason = dev_has_sold(holders, deployer)
    if dev_sold:
        fails.append(f"dev sold: {dev_reason}")

    # 12. Twitter/X required
    _, twitter = extract_socials(pair)
    if not twitter:
        fails.append("no Twitter/X")

    # Extra: LP providers (non-PumpSwap), PumpSwap top holder, pump-address
    dex_id = (pair.get("dexId") or "").lower()
    is_pumpswap = "pumpswap" in dex_id or "pump-swap" in dex_id
    if not is_pumpswap:
        for risk in (risks or []):
            name = (risk.get("name") or "").lower()
            desc = (risk.get("description") or "").lower()
            if "lp provider" in name or "few users" in desc:
                fails.append("low LP providers")
                break
    if is_pumpswap and holders:
        pcts = [normalize_pct(float(h.get("pct", 0))) for h in holders[:10]]
        if pcts and pcts[0] >= 10.0:
            fails.append(f"PumpSwap top holder {pcts[0]:.1f}%")

    base_addr = (pair.get("baseToken") or {}).get("address", "")
    if base_addr.endswith("pump") and not is_pumpswap:
        fails.append("address ends in pump but not on PumpSwap")

    if fails:
        # One combined reason string: "FAILED N: reason1 | reason2 | reason3"
        return False, f"FAILED {len(fails)}: " + " | ".join(fails)
    return True, ""


# ─────────────────────────────────────────────
# GEM RATING
# ─────────────────────────────────────────────
def rate_gem(pair, rug_score, holders, risks, website, twitter,
             is_graduated, holder_velocity, velocity_desc, whale_found, whale_desc):
    score = 0
    plus = []
    minus = []

    liq = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    vol24 = float((pair.get("volume") or {}).get("h24", 0) or 0)
    vol5m = float((pair.get("volume") or {}).get("m5", 0) or 0)
    mcap = float(pair.get("fdv", 0) or 0)
    chg = pair.get("priceChange") or {}
    txns = (pair.get("txns") or {}).get("h24") or {}
    buys = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    total = buys + sells
    created = pair.get("pairCreatedAt", 0) or 0
    age_min = (int(time.time() * 1000) - created) / 60_000 if created else 999

    top1_pct = top10_pct = 0
    if holders:
        pcts = [normalize_pct(float(h.get("pct", 0))) for h in holders[:10]]
        top1_pct = pcts[0]
        top10_pct = sum(pcts)

    if is_graduated:
        score += 4; plus.append("Graduated from Pump.fun - proven demand")
    if whale_found:
        score += 4; plus.append(f"Known winning whale: {whale_desc}")
    if holder_velocity == 3:
        score += 3; plus.append(velocity_desc)
    elif holder_velocity == 2:
        score += 2; plus.append(velocity_desc)
    elif holder_velocity == 1:
        score += 1; plus.append(velocity_desc)
    elif holder_velocity == -1:
        score -= 2; minus.append(velocity_desc)

    score += 1; plus.append("Has Twitter/X")

    if liq >= 100_000: score += 3; plus.append(f"Very high liquidity ({fmt_usd(liq)})")
    elif liq >= 50_000: score += 2; plus.append(f"Strong liquidity ({fmt_usd(liq)})")
    else: score += 1; plus.append(f"Adequate liquidity ({fmt_usd(liq)})")

    clamped = min(rug_score, 100)
    if clamped >= 95: score += 3; plus.append(f"Excellent rug score ({clamped}/100)")
    elif clamped >= 80: score += 2; plus.append(f"Good rug score ({clamped}/100)")
    elif clamped >= 60: score += 1; plus.append(f"Acceptable rug score ({clamped}/100)")

    if top1_pct > 0:
        if top1_pct < 5: score += 3; plus.append(f"Top holder very low ({top1_pct:.1f}%)")
        elif top1_pct < 10: score += 2; plus.append(f"Top holder healthy ({top1_pct:.1f}%)")
        else: score += 1; plus.append(f"Top holder acceptable ({top1_pct:.1f}%)")

    if top10_pct > 0:
        if top10_pct < 20: score += 3; plus.append(f"Top 10 very distributed ({top10_pct:.1f}%)")
        elif top10_pct < 30: score += 2; plus.append(f"Top 10 well distributed ({top10_pct:.1f}%)")
        else: score += 1; plus.append(f"Top 10 acceptable ({top10_pct:.1f}%)")

    if total > 0:
        buy_pct = buys / total * 100
        if buy_pct >= 80: score += 3; plus.append(f"Explosive buy pressure ({buy_pct:.0f}%)")
        elif buy_pct >= 70: score += 2; plus.append(f"Strong buy pressure ({buy_pct:.0f}%)")
        else: score += 1; plus.append(f"Decent buy pressure ({buy_pct:.0f}%)")

    if vol5m > 0 and liq > 0:
        spike = vol5m / liq
        if spike >= 0.5: score += 3; plus.append(f"Massive 5m spike ({spike:.1f}x)")
        elif spike >= 0.2: score += 2; plus.append(f"Strong 5m spike ({spike:.1f}x)")
        elif spike >= 0.05: score += 1; plus.append(f"Some 5m volume")
        else: minus.append("Low 5m volume")

    if 7 <= age_min <= 15: score += 3; plus.append(f"Perfect age ({int(age_min)}m)")
    elif 5 <= age_min <= 20: score += 2; plus.append(f"Good age ({int(age_min)}m)")

    if 0 < mcap <= 100_000: score += 3; plus.append(f"Very low mcap ({fmt_usd(mcap)}) - huge upside")
    elif mcap <= 250_000: score += 2; plus.append(f"Low mcap ({fmt_usd(mcap)})")
    elif mcap <= 500_000: score += 1; plus.append(f"Moderate mcap ({fmt_usd(mcap)})")

    try:
        m5 = float(chg.get("m5", 0) or 0)
        h1 = float(chg.get("h1", 0) or 0)
        if m5 > 30 and h1 > 50: score += 3; plus.append(f"Explosive momentum (5m: +{m5:.0f}%)")
        elif m5 > 10 or h1 > 20: score += 2; plus.append(f"Strong momentum (5m: {m5:+.1f}%)")
        elif m5 > 0: score += 1; plus.append(f"Positive momentum (5m: {m5:+.1f}%)")
        elif m5 < -15: score -= 2; minus.append(f"Dropping fast (5m: {m5:.1f}%)")
    except Exception:
        pass

    for r in (risks or []):
        lvl = (r.get("level") or "").upper()
        if lvl == "DANGER": score -= 4; minus.append(f"DANGER: {r.get('name', '')}")
        elif lvl == "WARN": score -= 1; minus.append(f"Warning: {r.get('name', '')}")

    if score >= 22:
        return f"PERFECT (score: {score}) - High confidence entry.", plus, minus
    elif score >= 14:
        return f"GOOD (score: {score}) - Worth a calculated entry.", plus, minus
    else:
        return f"RISKY (score: {score}) - Mixed signals. Be careful.", plus, minus


# ─────────────────────────────────────────────
# Alert builder
# ─────────────────────────────────────────────
def build_alert(pair, rug_score, risks, holders, source, is_graduated,
                holder_velocity, velocity_desc, whale_found, whale_desc):
    base = pair.get("baseToken") or {}
    name = base.get("name", "Unknown")
    symbol = base.get("symbol", "???")
    mint = base.get("address", "")
    chain = pair.get("chainId", "").upper()
    dex = pair.get("dexId", "").upper()
    liq = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    vol24 = float((pair.get("volume") or {}).get("h24", 0) or 0)
    vol5m = float((pair.get("volume") or {}).get("m5", 0) or 0)
    mcap = float(pair.get("fdv", 0) or 0)
    price_usd = pair.get("priceUsd", "N/A")
    chg = pair.get("priceChange") or {}
    created = pair.get("pairCreatedAt", 0) or 0
    txns = (pair.get("txns") or {}).get("h24") or {}
    buys = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    total = buys + sells
    bs = f"{buys/total*100:.0f}% buys ({buys}B/{sells}S)" if total else "no txns"
    website, twitter = extract_socials(pair)

    top1_pct = top10_pct = "N/A"
    h_lines = " N/A"
    if holders:
        pcts = [normalize_pct(float(h.get("pct", 0))) for h in holders[:10]]
        top1_pct = f"{pcts[0]:.2f}%"
        top10_pct = f"{sum(pcts):.2f}%"
        lines = []
        for i, h in enumerate(holders[:10], 1):
            addr = h.get("address", "???")
            short = f"{addr[:4]}...{addr[-4:]}"
            p = normalize_pct(float(h.get("pct", 0)))
            tag = " [insider]" if h.get("insider") else (" [owner]" if h.get("owner") else "")
            lines.append(f" {i:>2}. {short}{tag} - {p:.2f}%")
        h_lines = "\n".join(lines)

    risk_text = ""
    if risks:
        risk_text = "\nRisk flags:\n" + "\n".join(
            f" - {r.get('name', '')}: {r.get('description', '')}" for r in risks[:4])

    rating_line, plus_r, minus_r = rate_gem(
        pair, rug_score, holders, risks, website, twitter,
        is_graduated, holder_velocity, velocity_desc, whale_found, whale_desc)

    emoji = "PERFECT" if "PERFECT" in rating_line else ("GOOD" if "GOOD" in rating_line else "RISKY")
    plus_text = "\n".join(f" + {r}" for r in plus_r) or " None"
    minus_text = "\n".join(f" - {r}" for r in minus_r) or " None"
    grad_line = "Yes (Pump.fun graduate)" if is_graduated else "No"
    dex_link = f"https://dexscreener.com/{pair.get('chainId', '')}/{mint}"
    rug_link = f"https://rugcheck.xyz/tokens/{mint}"

    return (
        f"NEW GEM FOUND\n"
        f"Source: {source}\n\n"
        f"---- GEM RATING: {emoji} ----\n"
        f"{rating_line}\n\n"
        f"Positives:\n{plus_text}\n\n"
        f"Negatives:\n{minus_text}\n"
        f"--------------------\n\n"
        f"Name: {name} (${symbol})\n"
        f"Chain: {chain} | DEX: {dex}\n"
        f"Pump.fun Graduate: {grad_line}\n"
        f"Address: {mint}\n"
        f"Created: {minutes_ago(created) if created else 'Unknown'}\n\n"
        f"Price: ${price_usd}\n"
        f"5m: {pct_arrow(chg.get('m5', 0))} | 1h: {pct_arrow(chg.get('h1', 0))} | "
        f"6h: {pct_arrow(chg.get('h6', 0))}\n"
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
        f"Twitter: {twitter}\n\n"
        f"PASSED ALL FILTERS\n\n"
        f"DexScreener: {dex_link}\n"
        f"Rugcheck: {rug_link}"
    )


# ─────────────────────────────────────────────
# Send alert (paper or real)
# ─────────────────────────────────────────────
async def send_alert(bot, pair, rug_score, risks, holders, source,
                     is_graduated, holder_velocity, velocity_desc,
                     whale_found, whale_desc, portfolio, whales):
    sym = (pair.get("baseToken") or {}).get("symbol", "???")
    name = (pair.get("baseToken") or {}).get("name", "Unknown")
    mint = (pair.get("baseToken") or {}).get("address", "")
    chain = pair.get("chainId", "")
    log.info("GEM: %s (paper=%s)", sym, PAPER_MODE)

    try:
        entry_price = float(pair.get("priceUsd", 0) or 0)
    except Exception:
        entry_price = 0

    if PAPER_MODE:
        fake_eur = PAPER_TRADE_SIZE
        liq = fmt_usd(float((pair.get("liquidity") or {}).get("usd", 0) or 0))
        mcap = fmt_usd(float(pair.get("fdv", 0) or 0))
        vol24 = fmt_usd(float((pair.get("volume") or {}).get("h24", 0) or 0))
        dex_link = f"https://dexscreener.com/{chain}/{mint}"
        _, twitter = extract_socials(pair)
        rating_line, plus_r, minus_r = rate_gem(
            pair, rug_score, holders, risks, "", twitter,
            is_graduated, holder_velocity, velocity_desc, whale_found, whale_desc)
        emoji = "PERFECT" if "PERFECT" in rating_line else ("GOOD" if "GOOD" in rating_line else "RISKY")
        # v6.0: buy/sell counts shown for context (NOT a filter anymore)
        txns = (pair.get("txns") or {}).get("h24") or {}
        b = txns.get("buys", 0); s = txns.get("sells", 0); tot = b + s
        bs_line = f"{b/tot*100:.0f}% buys ({b}B/{s}S)" if tot else "no txn data"
        # age for context
        created = pair.get("pairCreatedAt", 0) or 0
        age_h = ((int(time.time() * 1000) - created) / 3_600_000) if created else 0
        msg = (
            "CHART THIS CANDIDATE (passed quality filters)\n\n"
            f"Quality rating: {emoji}\n"
            f"{rating_line}\n\n"
            f"Coin: {name} (${sym})\n"
            f"Source: {source}\n"
            f"Address: {mint}\n\n"
            f"Age: {age_h:.1f}h\n"
            f"Price: ${entry_price:.8f}\n"
            f"Liquidity: {liq}\n"
            f"Market Cap: {mcap}\n"
            f"24h Volume: {vol24}\n"
            f"Buy/Sell (24h): {bs_line}\n\n"
            "--- YOUR JOB NOW ---\n"
            "Open the chart. Check support/resistance, Fibonacci,\n"
            "multiple timeframes. The bot only confirms it's liquid,\n"
            "active, and not an obvious rug. The entry decision is yours.\n\n"
            f"Paper-tracking from here at ${entry_price:.8f} so you can\n"
            "see how flagged candidates perform.\n\n"
            f"DexScreener: {dex_link}"
        )
        try:
            await bot.send_message(chat_id=CHAT_ID, text=msg, disable_web_page_preview=True)
        except TelegramError as te:
            log.error("Paper send error: %s", te)
            return
        if mint and entry_price > 0:
            paper = load_paper()
            paper.append({
                "mint": mint, "symbol": sym, "name": name, "chain": chain,
                "entry_price": entry_price, "fake_eur": fake_eur,
                "alert_time": int(time.time()),
                "holders_snap": holders[:5],
                "pumped_alerted": False, "dumped_alerted": False,
                "closed": False, "final_pnl": None,
            })
            save_paper(paper)
    else:
        text = build_alert(pair, rug_score, risks, holders, source, is_graduated,
                           holder_velocity, velocity_desc, whale_found, whale_desc)
        try:
            await bot.send_message(chat_id=CHAT_ID, text=text, disable_web_page_preview=True)
        except TelegramError as te:
            log.error("Alert send error: %s", te)
            return
        if mint and entry_price > 0:
            portfolio.append({
                "mint": mint, "symbol": sym, "name": name, "chain": chain,
                "entry_price": entry_price, "alert_time": int(time.time()),
                "holders_snap": holders[:5],
                "pumped_alerted": False, "dumped_alerted": False,
            })
            record_whale_result(holders, whales, won=False)


# ─────────────────────────────────────────────
# Paper followup loop
# ─────────────────────────────────────────────
async def paper_followup_loop(bot, session):
    while True:
        await asyncio.sleep(FOLLOWUP_INTERVAL)
        if not PAPER_MODE:
            continue
        try:
            paper = load_paper()
            now = int(time.time())
            changed = False
            for trade in paper:
                if trade.get("closed"):
                    continue
                age = now - trade.get("alert_time", now)
                pair = await get_pair_data(session, trade["mint"])
                if not pair:
                    if age > FOLLOWUP_DURATION:
                        trade["closed"] = True
                        trade["final_pnl"] = 0
                        changed = True
                    continue

                cur = float(pair.get("priceUsd", 0) or 0)
                entry = trade["entry_price"]
                if entry <= 0:
                    continue
                pct = (cur - entry) / entry * 100
                pnl = trade["fake_eur"] * (pct / 100)
                dex_link = f"https://dexscreener.com/{trade['chain']}/{trade['mint']}"

                if pct >= PUMP_THRESHOLD and not trade.get("pumped_alerted"):
                    trade["pumped_alerted"] = True
                    trade["closed"] = True
                    trade["final_pnl"] = pnl
                    changed = True
                    record_whale_result(trade.get("holders_snap", []), {}, True)
                    msg = (
                        "PAPER TRADE - TAKE PROFIT\n\n"
                        f"Coin: {trade['name']} (${trade['symbol']})\n\n"
                        f"Entry: EUR{trade['fake_eur']:.0f} at ${entry:.8f}\n"
                        f"Exit: ${cur:.8f}\n"
                        f"Change: +{pct:.1f}%\n\n"
                        f"Fake profit: +EUR{pnl:.2f}\n"
                        f"EUR{trade['fake_eur']:.0f} -> EUR{trade['fake_eur']+pnl:.2f}\n\n"
                        f"DexScreener: {dex_link}"
                    )
                    try:
                        await bot.send_message(chat_id=CHAT_ID, text=msg, disable_web_page_preview=True)
                    except TelegramError:
                        pass

                elif pct <= DUMP_THRESHOLD and not trade.get("dumped_alerted"):
                    trade["dumped_alerted"] = True
                    trade["closed"] = True
                    trade["final_pnl"] = pnl
                    changed = True
                    msg = (
                        "PAPER TRADE - STOP LOSS\n\n"
                        f"Coin: {trade['name']} (${trade['symbol']})\n\n"
                        f"Entry: EUR{trade['fake_eur']:.0f} at ${entry:.8f}\n"
                        f"Exit: ${cur:.8f}\n"
                        f"Change: {pct:.1f}%\n\n"
                        f"Fake loss: -EUR{abs(pnl):.2f}\n"
                        f"EUR{trade['fake_eur']:.0f} -> EUR{trade['fake_eur']+pnl:.2f}\n\n"
                        f"Good thing this was paper trading!\n\n"
                        f"DexScreener: {dex_link}"
                    )
                    try:
                        await bot.send_message(chat_id=CHAT_ID, text=msg, disable_web_page_preview=True)
                    except TelegramError:
                        pass

                elif age > FOLLOWUP_DURATION and not trade.get("closed"):
                    trade["closed"] = True
                    trade["final_pnl"] = pnl
                    changed = True
                    msg = (
                        "PAPER TRADE - EXPIRED (4h)\n\n"
                        f"Coin: {trade['name']} (${trade['symbol']})\n"
                        f"Final change: {pct:+.1f}%\n"
                        f"Final P&L: {'+'if pnl>=0 else ''}EUR{pnl:.2f}\n\n"
                        f"DexScreener: {dex_link}"
                    )
                    try:
                        await bot.send_message(chat_id=CHAT_ID, text=msg, disable_web_page_preview=True)
                    except TelegramError:
                        pass

            if changed:
                save_paper(paper)
        except Exception as e:
            log.error("Paper followup error: %s", e)


# ─────────────────────────────────────────────
# Real followup loop
# ─────────────────────────────────────────────
async def followup_loop(bot, portfolio, whales, session):
    while True:
        await asyncio.sleep(FOLLOWUP_INTERVAL)
        if PAPER_MODE:
            continue
        now = int(time.time())
        to_keep = []
        for item in portfolio:
            age = now - item.get("alert_time", now)
            if age > FOLLOWUP_DURATION:
                pair = await get_pair_data(session, item["mint"])
                if pair:
                    try:
                        cur = float(pair.get("priceUsd", 0) or 0)
                        entry = item["entry_price"]
                        chg = ((cur - entry) / entry * 100) if entry > 0 else 0
                        record_whale_result(item.get("holders_snap", []), whales, won=chg >= 50)
                        save_whales(whales)
                    except Exception:
                        pass
                continue
            try:
                pair = await get_pair_data(session, item["mint"])
                if not pair:
                    to_keep.append(item)
                    continue
                cur = float(pair.get("priceUsd", 0) or 0)
                entry = item["entry_price"]
                if entry <= 0:
                    to_keep.append(item)
                    continue
                pct = (cur - entry) / entry * 100
                dex_link = f"https://dexscreener.com/{item['chain']}/{item['mint']}"
                if pct >= PUMP_THRESHOLD and not item["pumped_alerted"]:
                    item["pumped_alerted"] = True
                    record_whale_result(item.get("holders_snap", []), whales, won=True)
                    save_whales(whales)
                    await bot.send_message(chat_id=CHAT_ID, text=(
                        f"PUMP ALERT\n\n{item['name']} (${item['symbol']}) is up {pct:+.1f}%\n"
                        f"Entry: ${entry:.8f}\nNow: ${cur:.8f}\n\nConsider taking profit!\n\n"
                        f"DexScreener: {dex_link}"
                    ), disable_web_page_preview=True)
                if pct <= DUMP_THRESHOLD and not item["dumped_alerted"]:
                    item["dumped_alerted"] = True
                    await bot.send_message(chat_id=CHAT_ID, text=(
                        f"DUMP ALERT\n\n{item['name']} (${item['symbol']}) is down {pct:.1f}%\n"
                        f"Entry: ${entry:.8f}\nNow: ${cur:.8f}\n\nConsider cutting losses!\n\n"
                        f"DexScreener: {dex_link}"
                    ), disable_web_page_preview=True)
                to_keep.append(item)
            except Exception as e:
                log.warning("Followup error: %s", e)
                to_keep.append(item)
        portfolio.clear()
        portfolio.extend(to_keep)
        save_portfolio(portfolio)


# ─────────────────────────────────────────────
# Daily summary
# ─────────────────────────────────────────────
async def daily_summary_loop(bot, portfolio, whales, session):
    while True:
        now_utc = datetime.now(timezone.utc)
        next_8am = now_utc.replace(hour=DAILY_SUMMARY_HOUR, minute=0, second=0, microsecond=0)
        if now_utc >= next_8am:
            next_8am = next_8am + timedelta(days=1)
        await asyncio.sleep((next_8am - now_utc).total_seconds())
        try:
            good_whales = sum(1 for w in whales.values() if w.get("wins", 0) >= 2)
            if PAPER_MODE:
                paper = load_paper()
                today_start = int(time.time()) - 86400
                todays = [t for t in paper if t.get("alert_time", 0) >= today_start]
                closed = [t for t in todays if t.get("closed")]
                wins = sum(1 for t in closed if (t.get("final_pnl") or 0) > 0)
                losses = sum(1 for t in closed if (t.get("final_pnl") or 0) <= 0)
                today_pnl = sum(t.get("final_pnl") or 0 for t in closed)
                all_closed = [t for t in paper if t.get("closed")]
                all_pnl = sum(t.get("final_pnl") or 0 for t in all_closed)
                all_wins = sum(1 for t in all_closed if (t.get("final_pnl") or 0) > 0)
                all_wr = (all_wins / len(all_closed) * 100) if all_closed else 0
                summary = (
                    f"DAILY SUMMARY - {now_utc.strftime('%B %d, %Y')}\n"
                    f"PAPER TRADING MODE\n\n"
                    f"TODAY:\n"
                    f"Trades: {len(todays)}\n"
                    f"Won: {wins} | Lost: {losses}\n"
                    f"Today P&L: {'+'if today_pnl>=0 else ''}EUR{today_pnl:.2f}\n"
                    f"Open positions: {len(todays)-len(closed)}\n\n"
                    f"ALL TIME:\n"
                    f"Total closed trades: {len(all_closed)}\n"
                    f"Win rate: {all_wr:.0f}%\n"
                    f"Total fake P&L: {'+'if all_pnl>=0 else ''}EUR{all_pnl:.2f}\n\n"
                    f"Whale tracker:\n"
                    f" Wallets tracked: {len(whales)}\n"
                    f" Proven winners: {good_whales}\n\n"
                    f"v6.0 - 24h quality screener ACTIVE\n"
                    f"Cloud memory: ACTIVE\n"
                    f"Scanner running 24/7!"
                )
            else:
                today_start = int(time.time()) - 86400
                todays = [p for p in portfolio if p.get("alert_time", 0) >= today_start]
                pumped = dumped = live = 0
                lines = []
                for item in todays:
                    pair = await get_pair_data(session, item["mint"])
                    if pair:
                        try:
                            cur = float(pair.get("priceUsd", 0) or 0)
                            chg = ((cur - item["entry_price"]) / item["entry_price"] * 100) if item["entry_price"] > 0 else 0
                            e = "WIN" if chg >= 50 else ("OK" if chg >= 0 else "LOSS")
                            lines.append(f" {e} ${item['symbol']}: {fmt_pct(chg)}")
                            if chg >= 30: pumped += 1
                            elif chg <= -30: dumped += 1
                            else: live += 1
                        except Exception:
                            lines.append(f" ? ${item['symbol']}: N/A")
                summary = (
                    f"DAILY SUMMARY - {now_utc.strftime('%B %d, %Y')}\n\n"
                    f"Gems sent: {len(todays)}\n"
                    f"Pumped 30%+: {pumped}\n"
                    f"Dumped 30%+: {dumped}\n"
                    f"Still live: {live}\n\n"
                )
                if lines:
                    summary += "Performance:\n" + "\n".join(lines) + "\n\n"
                summary += (
                    f"Whale tracker:\n"
                    f" Wallets tracked: {len(whales)}\n"
                    f" Proven winners: {good_whales}\n\n"
                    f"v6.0 - 24h quality screener ACTIVE\n"
                    f"Cloud memory: ACTIVE\n"
                    f"Scanner running 24/7!"
                )
            await bot.send_message(chat_id=CHAT_ID, text=summary, disable_web_page_preview=True)
            log.info("Daily summary sent.")
            HEALTH["alerts_sent_today"] = 0  # v5.9.2: reset daily counter
        except Exception as e:
            log.error("Daily summary error: %s", e)


# ─────────────────────────────────────────────
# v5.9.2: Status builder + heartbeat watchdog
# ─────────────────────────────────────────────
def build_status(seen, whales, portfolio):
    """Human-readable health snapshot — used by /status and the heartbeat."""
    now = int(time.time())
    uptime_h = (now - HEALTH["start_time"]) / 3600

    src_lines = []
    for name, last_ok in HEALTH["source_last_ok"].items():
        mins = (now - last_ok) / 60
        state = "OK" if mins < SOURCE_DEAD_MINUTES else "DOWN"
        src_lines.append(f"  {state} {name} (last data {int(mins)}m ago)")

    quiet_for = (now - HEALTH["last_collected_time"]) / 60

    return (
        f"BOT STATUS\n\n"
        f"Mode: {'PAPER' if PAPER_MODE else 'LIVE'}\n"
        f"Uptime: {uptime_h:.1f}h\n"
        f"Scans completed: {HEALTH['scans_completed']}\n"
        f"Alerts sent today: {HEALTH['alerts_sent_today']}\n"
        f"Last token collected: {int(quiet_for)}m ago\n\n"
        f"Sources:\n" + "\n".join(src_lines) + "\n\n"
        f"Memory: {len(seen)} seen | {len(whales)} whales | {len(portfolio)} portfolio\n"
        f"Supabase fail streak: {HEALTH['supabase_fails']}\n"
        f"Scanner crash streak: {HEALTH['scanner_errors']}"
    )


async def heartbeat_loop(bot, seen, session, portfolio, whales):
    """
    Sends a status snapshot every 6 hours. Its real value is being a
    'dead-man's switch': if these stop arriving, the whole worker is down
    (Railway crash, network gone) — and silence itself tells you something.
    """
    HEARTBEAT_INTERVAL = 6 * 3600
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            # reset the daily alert counter roughly once a day via heartbeat math
            status = build_status(seen, whales, portfolio)
            await bot.send_message(chat_id=CHAT_ID,
                                   text="HEARTBEAT (every 6h)\n\n" + status,
                                   disable_web_page_preview=True)
            log.info("Heartbeat sent.")
        except Exception as e:
            log.error("Heartbeat error: %s", e)



async def scanner_loop(bot, seen, session, portfolio, whales):
    alerted: set = set()
    pending: dict = {}  # v5.9: mint -> first-seen timestamp_ms (coins waiting to mature)
    while True:
        try:
            log.info("Scanning... (v6.0 24h-quality)")
            # Sources now return MAX-age-filtered (mint, ts_ms) — includes brand-new coins
            token_list = await get_all_latest_tokens(session, bot)

            # v5.9.2: track whether the scanner is finding anything at all
            now = int(time.time())
            if len(token_list) > 0:
                HEALTH["last_collected_time"] = now
                if HEALTH["all_quiet_warned"]:
                    HEALTH["all_quiet_warned"] = False
                    await health_alert(bot, "all_quiet_recover",
                                       "Scanner is collecting tokens again.")
            else:
                quiet_for = (now - HEALTH["last_collected_time"]) / 60
                if quiet_for >= ALL_QUIET_MINUTES and not HEALTH["all_quiet_warned"]:
                    HEALTH["all_quiet_warned"] = True
                    await health_alert(bot, "all_quiet",
                                       f"ALL sources have returned 0 tokens for "
                                       f"{int(quiet_for)} min straight.\n"
                                       f"This usually means an API format change or a "
                                       f"network/DNS issue on Railway — not a quiet market.")

            # Merge newly-found tokens into the pending pool
            for addr, ts_ms in token_list:
                if addr in alerted or addr in seen:
                    continue
                if addr not in pending:
                    pending[addr] = ts_ms

            log.info("Collected %d new | %d pending in window-wait", len(token_list), len(pending))

            # Clean out anything that aged past MAX while waiting
            stale = [a for a, ts in pending.items()
                     if (int(time.time() * 1000) - ts) / 60_000 > MAX_AGE_MINUTES]
            for a in stale:
                del pending[a]

            # Check every pending coin that has now matured into the 3-30m window
            ready = [a for a, ts in pending.items() if is_age_in_window(ts)]
            log.info("%d coins matured into 3-%dm window", len(ready), MAX_AGE_MINUTES)

            for addr in ready:
                if addr in alerted or addr in seen:
                    pending.pop(addr, None)
                    continue

                pair = await get_pair_data(session, addr)
                if not pair:
                    continue

                created = pair.get("pairCreatedAt", 0) or 0
                if not created:
                    continue

                age_min = (int(time.time() * 1000) - created) / 60_000
                sym = (pair.get("baseToken") or {}).get("symbol", addr[:8])
                log.info("Checking %s - age: %.1fm", sym, age_min)

                if age_min > MAX_AGE_MINUTES:
                    log.info("Skip %s - too old (%.1fm) [blacklisted]", sym, age_min)
                    pending.pop(addr, None)
                    seen.add(addr)  # v5.9.1: permanently ignore — stops re-entry from source
                    continue
                if age_min < MIN_AGE_MINUTES:
                    log.info("Skip %s - too fresh (%.1fm), keeping for next scan", sym, age_min)
                    continue

                rug_score, risks, holders, deployer = await get_rugcheck(session, addr)
                is_graduated = await is_pumpfun_graduate(session, addr)
                vel_score, vel_d = check_holder_velocity(addr, len(holders))
                whale_f, whale_d = check_known_whales(holders, whales)

                passed, reason = passes_filters(pair, rug_score, holders, deployer,
                                                is_graduated, risks)
                if not passed:
                    log.info("Filtered: %s - %s", sym, reason)
                    pending.pop(addr, None)
                    # v5.9.1: blacklist permanent fails so they don't re-enter from source.
                    # Temporary fails (buy pressure, holder counts) get another chance
                    # if the coin is still inside the window next scan.
                    permanent = ("no Twitter" in reason or "dev sold" in reason
                                 or "ends in pump" in reason or "too old" in reason
                                 or "clone wallet" in reason)
                    if permanent:
                        seen.add(addr)
                    continue

                alerted.add(addr)
                seen.add(addr)
                pending.pop(addr, None)
                HEALTH["alerts_sent_today"] += 1
                await send_alert(bot, pair, rug_score, risks, holders, "Auto-scan",
                                 is_graduated, vel_score, vel_d, whale_f, whale_d,
                                 portfolio, whales)
                await asyncio.sleep(1)

            # v5.9.2: a full clean loop — reset the crash streak and count the scan.
            HEALTH["scans_completed"] += 1
            HEALTH["scanner_errors"] = 0

        except Exception as e:
            log.error("Scanner error: %s", e)
            # v5.9.2: escalate only if the scanner keeps crashing back-to-back.
            HEALTH["scanner_errors"] += 1
            if HEALTH["scanner_errors"] >= SCANNER_ERROR_THRESHOLD:
                await health_alert(bot, "scanner_crash",
                                   f"Scanner has crashed {HEALTH['scanner_errors']} times "
                                   f"in a row.\nLast error: {str(e)[:200]}\n\n"
                                   f"The loop keeps retrying, but something is wrong. "
                                   f"Send me this message and the recent logs.")

        log.info("Sleeping %ds...", SCAN_INTERVAL)
        await asyncio.sleep(SCAN_INTERVAL)


# ─────────────────────────────────────────────
# Group handler
# ─────────────────────────────────────────────
def make_group_handler(seen, session, bot, portfolio, whales):
    async def handle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        msg = update.message
        if not msg or not msg.text:
            return
        text = msg.text
        chat = msg.chat
        sender = msg.from_user
        group_name = chat.title or str(chat.id)
        user_name = (sender.username or sender.first_name) if sender else "unknown"

        found = []
        for a in SOLANA_ADDR_RE.findall(text): found.append(("address", a))
        for a in EVM_ADDR_RE.findall(text): found.append(("address", a))
        for s in SYMBOL_RE.findall(text): found.append(("symbol", s))
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
                is_grad = await is_pumpfun_graduate(session, mint)
                vel_score, vel_d = check_holder_velocity(mint, len(holders))
                whale_f, whale_d = check_known_whales(holders, whales)
                passed, reason = passes_filters(pair, rug_score, holders, deployer,
                                                is_grad, risks)
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
                                 is_grad, vel_score, vel_d, whale_f, whale_d,
                                 portfolio, whales)
            except Exception as e:
                log.error("Group handler error: %s", e)
            await asyncio.sleep(0.5)
    return handle


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
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

    # v5.9.2: /status command — ask the bot how it's doing any time.
    async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        try:
            await update.message.reply_text(build_status(seen, whales, portfolio),
                                            disable_web_page_preview=True)
        except Exception as e:
            log.error("Status cmd error: %s", e)
    app.add_handler(CommandHandler("status", status_cmd))

    mode_str = "PAPER TRADING (no real money)" if PAPER_MODE else "LIVE TRADING"
    try:
        await bot.send_message(chat_id=CHAT_ID, text=(
            f"Memecoin Quality-Coin Screener v6.0 is LIVE\n"
            f"Mode: {mode_str}\n\n"
            f"NEW PURPOSE (v6.0):\n"
            f"This is no longer a fresh-launch sniper. It now finds\n"
            f"LIQUID, ACTIVE, non-rug coins 2-24h old and sends them to\n"
            f"you to CHART. The bot screens for quality; YOU apply your\n"
            f"TA (support/resistance, Fibonacci, timeframes) and decide\n"
            f"the entry. Long-only (you can't short these coins).\n\n"
            f"Quality filters (v6.0):\n"
            f"- Age: 2-24 hours (chartable history)\n"
            f"- Liquidity: $15k - $5M\n"
            f"- Market cap: $50k - $5M\n"
            f"- 24h volume: $50k+ (must be active, not dead)\n"
            f"- Rug score: 60+ | Top holder <15% | Top 10 <40%\n"
            f"- Twitter required | dev-sold & clone checks on\n"
            f"- Buy-pressure filter REMOVED (healthy coins have both\n"
            f"  buys and sells; shown in alert as info only)\n\n"
            f"Still active: watchdog, /status, blacklist, cloud memory,\n"
            f"6h heartbeat, group scanner, self-monitoring.\n\n"
            f"Note: first scans may show extra 'too old' rejections as\n"
            f"the blacklist fills — that's expected and self-corrects.\n\n"
            f"Watchdog: ACTIVE | Helius: ACTIVE | Cloud memory: ACTIVE"
        ))
    except TelegramError as e:
        log.error("Startup message failed: %s", e)

    asyncio.create_task(scanner_loop(bot, seen, session, portfolio, whales))
    asyncio.create_task(followup_loop(bot, portfolio, whales, session))
    asyncio.create_task(paper_followup_loop(bot, session))
    asyncio.create_task(daily_summary_loop(bot, portfolio, whales, session))
    asyncio.create_task(backup_loop(session, seen, whales, portfolio, bot))
    asyncio.create_task(heartbeat_loop(bot, seen, session, portfolio, whales))


async def post_shutdown(app):
    session = app.bot_data.get("session")
    seen = app.bot_data.get("seen", set())
    whales = app.bot_data.get("whales", {})
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
    log.info("Starting bot v6.0...")
    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
