# scanner.py
# ShopGoodwill Deal Scanner (stable + Windows-safe + resilient to SGW 500s + improved time-left)
#
# Requirements (install in venv):
#   pip install requests PyYAML python-dateutil
#
# This version:
# ✅ Uses your WORKING Search endpoint + payload (NO /Scroll)
# ✅ Separate windows:
#    - scan.ending_soon_minutes = ALERT window (instant alerts only)
#    - scan.digest_window_minutes = DIGEST window (what gets browsed/queued)
# ✅ Two Discord channels via webhooks
#    - notifications.discord.best_webhook_url  (notified)
#    - notifications.discord.browse_webhook_url (silent, lots of hits)
# ✅ Silent browse uses Discord "SUPPRESS_NOTIFICATIONS" flag (4096)
# ✅ SHIPPING REMOVED from scoring + filtering (won't block anything)
# ✅ Prevent duplicate BROWSE posts across scans (browse_ts)
# ✅ NEW: Adds eBay SOLD comps link to BEST + BROWSE + DIGEST
# ✅ NEW (optional): Cleans titles for better eBay comps searches

import os
import re
import time
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple, Dict, Any
from urllib.parse import quote_plus

import requests
import yaml
from dateutil import parser as dateparser


# -----------------------------
# DB
# -----------------------------
DB_PATH = os.path.join("data", "scanner.sqlite3")


def ensure_dirs():
    os.makedirs("data", exist_ok=True)


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def db_connect():
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            listing_id TEXT PRIMARY KEY,
            first_seen_ts INTEGER NOT NULL,
            last_seen_ts INTEGER NOT NULL,
            last_score INTEGER NOT NULL,
            alerted_ts INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS digest_queue (
            listing_id TEXT PRIMARY KEY,
            score INTEGER NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            feed_name TEXT NOT NULL,
            queued_ts INTEGER NOT NULL
        )
    """)

    # --- Non-breaking migration: add browse_ts if missing ---
    try:
        conn.execute("ALTER TABLE seen ADD COLUMN browse_ts INTEGER")
        conn.commit()
    except sqlite3.OperationalError:
        # Column already exists
        pass

    conn.commit()
    return conn


def meta_get(conn, key: str, default: str = "0") -> str:
    cur = conn.execute("SELECT v FROM meta WHERE k=?", (key,))
    row = cur.fetchone()
    return row[0] if row else default


def meta_set(conn, key: str, value: str):
    conn.execute("""
        INSERT INTO meta (k, v) VALUES (?, ?)
        ON CONFLICT(k) DO UPDATE SET v=excluded.v
    """, (key, value))
    conn.commit()


def mark_seen(conn, listing_id: str, score: int):
    now = int(time.time())
    conn.execute("""
        INSERT INTO seen (listing_id, first_seen_ts, last_seen_ts, last_score, alerted_ts)
        VALUES (?, ?, ?, ?, NULL)
        ON CONFLICT(listing_id) DO UPDATE SET
          last_seen_ts=excluded.last_seen_ts,
          last_score=excluded.last_score
    """, (listing_id, now, now, int(score)))
    conn.commit()


def was_alerted(conn, listing_id: str) -> bool:
    cur = conn.execute("SELECT alerted_ts FROM seen WHERE listing_id=?", (listing_id,))
    row = cur.fetchone()
    return bool(row and row[0] is not None)


def mark_alerted(conn, listing_id: str):
    now = int(time.time())
    conn.execute("UPDATE seen SET alerted_ts=? WHERE listing_id=?", (now, listing_id))
    conn.commit()


# --- Browse duplicate prevention ---
def was_browsed(conn, listing_id: str) -> bool:
    cur = conn.execute("SELECT browse_ts FROM seen WHERE listing_id=?", (listing_id,))
    row = cur.fetchone()
    return bool(row and row[0] is not None)


def mark_browsed(conn, listing_id: str):
    now = int(time.time())
    conn.execute("UPDATE seen SET browse_ts=? WHERE listing_id=?", (now, listing_id))
    conn.commit()

# --- User actions (from moderation bot) ---
def ensure_user_actions_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_actions (
            listing_id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

def get_user_action(conn: sqlite3.Connection, listing_id: str) -> Optional[str]:
    cur = conn.execute("SELECT action FROM user_actions WHERE listing_id=? LIMIT 1", (str(listing_id),))
    row = cur.fetchone()
    return str(row[0]) if row else None

def alerts_in_last_hour(conn) -> int:
    now = int(time.time())
    one_hour_ago = now - 3600
    cur = conn.execute("""
        SELECT COUNT(*) FROM seen
        WHERE alerted_ts IS NOT NULL AND alerted_ts >= ?
    """, (one_hour_ago,))
    return int(cur.fetchone()[0])


def digest_enqueue(conn, listing_id: str, score: int, title: str, url: str, feed_name: str):
    now = int(time.time())
    conn.execute("""
        INSERT INTO digest_queue (listing_id, score, title, url, feed_name, queued_ts)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(listing_id) DO UPDATE SET
          score=excluded.score,
          title=excluded.title,
          url=excluded.url,
          feed_name=excluded.feed_name,
          queued_ts=excluded.queued_ts
    """, (listing_id, int(score), title, url, feed_name, now))
    conn.commit()


def digest_pop_all(conn, limit: int = 20) -> List[Tuple[int, str, str, str]]:
    cur = conn.execute("""
        SELECT score, title, url, feed_name
        FROM digest_queue
        ORDER BY score DESC, queued_ts DESC
        LIMIT ?
    """, (int(limit),))
    rows = cur.fetchall()
    conn.execute("DELETE FROM digest_queue")
    conn.commit()
    return [(int(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in rows]


# -----------------------------
# Config helper (NO KeyError)
# -----------------------------
def cfg_get(cfg: dict, path: str, default):
    cur: Any = cfg
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


# -----------------------------
# Listing
# -----------------------------
@dataclass
class Listing:
    listing_id: str
    title: str
    url: str
    current_bid: float
    bid_count: int
    time_left_minutes: Optional[int]
    shipping: Optional[float]
    feed_name: str


# -----------------------------
# Parsing helpers
# -----------------------------
def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def _to_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(v)
    except Exception:
        return default


def _extract_first_key(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


# -----------------------------
# eBay comps helpers (NEW)
# -----------------------------
_NOISE_PHRASES = [
    # common SGW / reseller fluff
    "as is", "as-is", "as is!", "as-is!", "read", "please read", "see photos", "see pictures",
    "untested", "for parts", "parts", "repair", "broken", "damaged",
    "lot", "bundle", "mixed", "random", "assorted",
    "no returns", "returns not accepted",
    "new listing", "great condition", "good condition", "excellent condition",
    "fast shipping", "ships fast",
]

_NOISE_REGEX = re.compile(r"\b(" + "|".join(re.escape(p) for p in _NOISE_PHRASES) + r")\b", re.IGNORECASE)

# keep these because they help comps for games/media
_KEEP_TOKENS = {"cib", "sealed", "complete", "steelbook", "criterion", "4k", "blu-ray", "bluray"}


def clean_comps_query(title: str) -> str:
    """
    Optional cleaner to improve eBay SOLD search accuracy.
    Goal: remove fluff, keep important tokens, avoid huge noisy queries.
    Safe: if it over-cleans, it falls back to original title.
    """
    if not title:
        return ""

    t = title.strip()

    # remove bracketed fluff like [READ] (TESTED) etc.
    t = re.sub(r"[\[\(\{].*?[\]\)\}]", " ", t)

    # remove known noise phrases
    t = _NOISE_REGEX.sub(" ", t)

    # remove excessive punctuation but keep hyphen for model names
    t = re.sub(r"[^\w\s\-\/]", " ", t)

    # normalize spaces
    t = re.sub(r"\s+", " ", t).strip()

    if not t:
        return title.strip()

    # Token-level cleanup (drop tiny tokens unless special)
    tokens = t.split()
    kept: List[str] = []
    for tok in tokens:
        low = tok.lower()
        if len(low) <= 2 and low not in _KEEP_TOKENS:
            continue
        kept.append(tok)

    # If we nuked too much, use original
    if len(kept) < 2:
        return title.strip()

    # Cap length so eBay query isn't insane
    cleaned = " ".join(kept)
    if len(cleaned) > 80:
        cleaned = cleaned[:80].rsplit(" ", 1)[0].strip()

    return cleaned or title.strip()


def ebay_sold_url(query: str) -> str:
    q = quote_plus(query.strip())
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


# --- Pacific DST without ZoneInfo ---
def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> datetime:
    first = datetime(year, month, 1)
    shift = (weekday - first.weekday()) % 7
    day = 1 + shift + (n - 1) * 7
    return datetime(year, month, day)


def _first_weekday_of_month(year: int, month: int, weekday: int) -> datetime:
    return _nth_weekday_of_month(year, month, weekday, 1)


def _is_us_pacific_dst(dt_naive: datetime) -> bool:
    y = dt_naive.year
    dst_start_day = _nth_weekday_of_month(y, 3, 6, 2)  # 2nd Sunday March
    dst_end_day = _first_weekday_of_month(y, 11, 6)    # 1st Sunday Nov
    dst_start = dst_start_day.replace(hour=2, minute=0, second=0, microsecond=0)
    dst_end = dst_end_day.replace(hour=2, minute=0, second=0, microsecond=0)
    return dst_start <= dt_naive < dst_end


def _pacific_tzinfo_for(dt_naive: datetime):
    offset_hours = -7 if _is_us_pacific_dst(dt_naive) else -8
    return timezone(timedelta(hours=offset_hours))


def _parse_dt_any(v: Any) -> Optional[datetime]:
    """
    Returns tz-aware datetime in UTC.
    Handles:
      - .NET "/Date(1700000000000)/" (milliseconds)
      - epoch seconds/millis
      - ISO strings (tz-aware or naive)
    If tz-naive, assumes US Pacific (DST-aware).
    """
    if v is None or v == "":
        return None

    if isinstance(v, str):
        m = re.search(r"/Date\((\d+)\)/", v)
        if m:
            try:
                ms = int(m.group(1))
                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            except Exception:
                return None

    if isinstance(v, (int, float)):
        try:
            n = float(v)
            if n > 1e12:
                return datetime.fromtimestamp(n / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(n, tz=timezone.utc)
        except Exception:
            return None

    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try:
                n = float(s)
                if n > 1e12:
                    return datetime.fromtimestamp(n / 1000.0, tz=timezone.utc)
                return datetime.fromtimestamp(n, tz=timezone.utc)
            except Exception:
                pass

    try:
        dtp = dateparser.parse(str(v))
        if not dtp:
            return None
        if dtp.tzinfo is None:
            dtp = dtp.replace(tzinfo=_pacific_tzinfo_for(dtp))
        return dtp.astimezone(timezone.utc)
    except Exception:
        return None


def _minutes_left(end_utc: Optional[datetime]) -> Optional[int]:
    if not end_utc:
        return None
    now = datetime.now(timezone.utc)
    mins = int((end_utc - now).total_seconds() // 60)
    return mins if mins >= 0 else 0


def extract_time_left_minutes(obj: Dict[str, Any]) -> Optional[int]:
    if not isinstance(obj, dict):
        return None

    candidate_keys = [
        "minutesLeft", "minutes_left", "remainingMinutes", "remaining_minutes",
        "timeLeftInMinutes", "timeLeftMinutes",
        "secondsLeft", "seconds_left", "remainingSeconds", "remaining_seconds",
        "timeLeftInSeconds", "timeLeftSeconds",
        "timeLeft", "timeRemaining", "remainingTime",
    ]

    for k in candidate_keys:
        if k not in obj or obj[k] in (None, ""):
            continue
        v = obj[k]

        if isinstance(v, (int, float)):
            n = float(v)
            if n > 10000:
                return int(n // 60)
            return int(n)

        if isinstance(v, str):
            s = v.strip().lower()
            if s.isdigit():
                n = float(s)
                if n > 10000:
                    return int(n // 60)
                return int(n)

            days = hours = minutes = 0
            md = re.search(r"(\d+)\s*d", s)
            mh = re.search(r"(\d+)\s*h", s)
            mm = re.search(r"(\d+)\s*m", s)
            if md:
                days = int(md.group(1))
            if mh:
                hours = int(mh.group(1))
            if mm:
                minutes = int(mm.group(1))
            if days or hours or minutes:
                return days * 1440 + hours * 60 + minutes

            if re.fullmatch(r"\d{1,2}:\d{2}(:\d{2})?", s):
                parts = [int(x) for x in s.split(":")]
                if len(parts) == 3:
                    h, m, sec = parts
                    return h * 60 + m + (1 if sec >= 30 else 0)
                if len(parts) == 2:
                    m, sec = parts
                    return m + (1 if sec >= 30 else 0)

    return None


# -----------------------------
# Notifications
# -----------------------------
def notify_pushover(user_key: str, api_token: str, title: str, message: str, url: Optional[str] = None):
    payload = {"token": api_token, "user": user_key, "title": title, "message": message, "priority": 0}
    if url:
        payload["url"] = url
        payload["url_title"] = "Open listing"
    r = requests.post("https://api.pushover.net/1/messages.json", data=payload, timeout=20)
    r.raise_for_status()


def notify_discord(webhook_url: str, content: Optional[str] = None, *, embed: Optional[dict] = None, silent: bool = False):
    """
    Sends a webhook message.
    - content: optional plain text (used for BEST mention ping)
    - embed: optional single embed dict
    """
    payload = {}
    if content:
        payload["content"] = str(content)
    if embed:
        payload["embeds"] = [embed]
    if silent:
        payload["flags"] = 4096  # SUPPRESS_NOTIFICATIONS

    r = requests.post(webhook_url, json=payload, timeout=20)
    r.raise_for_status()

def notify(cfg: dict, title: str, body, url: Optional[str] = None, channel: str = "best"):
    """
    body can be:
      - str  (plain message)
      - dict (Discord embed)
    """
    mode = str(cfg_get(cfg, "notifications.mode", "print")).lower().strip()

    if mode == "pushover":
        pk = cfg_get(cfg, "notifications.pushover.user_key", "")
        tok = cfg_get(cfg, "notifications.pushover.api_token", "")
        if not pk or not tok:
            print(title)
            print(body)
            print()
            return
        msg = body if isinstance(body, str) else str(body)
        notify_pushover(pk, tok, title=title, message=msg, url=url)
        return

    if mode == "discord":
        best_wh = cfg_get(cfg, "notifications.discord.best_webhook_url", "")
        browse_wh = cfg_get(cfg, "notifications.discord.browse_webhook_url", "")
        mention = str(cfg_get(cfg, "notifications.discord.best_mention", "") or "").strip()
        legacy_wh = cfg_get(cfg, "notifications.discord.webhook_url", "")

        if channel == "browse":
            wh = browse_wh or legacy_wh or best_wh
            silent = True
        else:
            wh = best_wh or legacy_wh
            silent = False

        if not wh:
            print(title); print(body); print()
            return

        # For BEST, put mention in content so you still get pinged
        content = mention if (channel == "best" and mention) else None

        try:
            if isinstance(body, dict):
                notify_discord(wh, content=content, embed=body, silent=silent)
            else:
                msg = f"{title}\n{body}"
                if content:
                    msg = f"{content}\n{msg}"
                notify_discord(wh, content=msg, embed=None, silent=silent)
        except Exception as e:
            print(f"[WARN] Discord notify failed ({channel}): {e}")
        return

    print(title)
    print(body)
    print()


# -----------------------------
# Scoring (v2: platform + lot-size + anomaly + better time weighting)
# -----------------------------

# Platform/brand detection patterns (normalized title is lowercase)
_PLATFORM_RULES: List[Tuple[re.Pattern, int]] = [
    # Ultra high-signal
    (re.compile(r"\bpokemon\b|\bpok[eé]mon\b|\bpikachu\b"), 18),
    (re.compile(r"\bgame\s*boy\b|\bgameboy\b|\bgba\b|\badvance\b"), 12),
    (re.compile(r"\bds\b|\b3ds\b|\bnintendo\s*ds\b|\bnintendo\s*3ds\b"), 10),

    # Nintendo family
    (re.compile(r"\bnintendo\b|\bswitch\b|\bwii\b|\bwii\s*u\b|\bgamecube\b|\bn64\b|\bsnes\b|\bsuper\s*nintendo\b|\bnes\b"), 12),

    # Sony / PlayStation family
    (re.compile(r"\bplaystation\b|\bps\s?1\b|\bps1\b|\bps\s?2\b|\bps2\b|\bps\s?3\b|\bps3\b|\bps\s?4\b|\bps4\b|\bps\s?5\b|\bps5\b|\bpsp\b|\bps\s?vita\b|\bvita\b"), 10),

    # Xbox family
    (re.compile(r"\bxbox\b|\bxbox\s*360\b|\bxbox\s*one\b|\bseries\s*x\b|\bseries\s*s\b"), 8),

    # General gaming hardware/accessories
    (re.compile(r"\bconsole\b|\bhandheld\b|\bcontroller\b|\bjoystick\b|\bgamepad\b"), 5),

    # Media collectors
    (re.compile(r"\bsteelbook\b|\bcriterion\b|\b4k\b|\bblu[\s\-]?ray\b|\bbluray\b"), 4),
]

# Lot size detection (tries to infer quantity: "lot of 25", "25 games", "x20", "20 pcs", etc.)
_LOT_PATTERNS: List[re.Pattern] = [
    re.compile(r"\blot\s+of\s+(\d{1,3})\b"),
    re.compile(r"\bbundle\s+of\s+(\d{1,3})\b"),
    re.compile(r"\bset\s+of\s+(\d{1,3})\b"),
    re.compile(r"\bcollection\s+of\s+(\d{1,3})\b"),
    re.compile(r"\b(\d{1,3})\s*(?:games?|discs?|dvds?|blu[\s\-]?rays?|items?|controllers?|consoles?|books?|manga|volumes?|pcs|pieces)\b"),
    re.compile(r"\bx\s*(\d{1,3})\b"),  # "x20"
]

def _detect_platform_bonus(title_norm: str) -> int:
    bonus = 0
    for pat, pts in _PLATFORM_RULES:
        if pat.search(title_norm):
            bonus += pts
    # Cap so a title that matches many patterns doesn't blow up the score
    return min(30, bonus)

def _detect_lot_count(title_norm: str) -> int:
    counts: List[int] = []
    for pat in _LOT_PATTERNS:
        m = pat.search(title_norm)
        if m:
            try:
                counts.append(int(m.group(1)))
            except Exception:
                pass
    if not counts:
        return 0
    # Use the largest inferred count, cap it
    return max(0, min(120, max(counts)))

def _lot_bonus_from_count(n: int) -> int:
    # Conservative tiers (prevents junk "1000 pcs screws" from dominating too much)
    if n >= 50:
        return 18
    if n >= 30:
        return 14
    if n >= 20:
        return 10
    if n >= 10:
        return 6
    if n >= 6:
        return 3
    return 0

def score_listing(cfg: dict, listing: Listing) -> int:
    scoring = cfg_get(cfg, "scoring", {}) or {}
    title_norm = normalize(listing.title)

    score = 0

    # 1) YAML keyword boosts/penalties (your existing config)
    for k, v in (scoring.get("keyword_boosts") or {}).items():
        if str(k).lower() in title_norm:
            score += int(v)
    for k, v in (scoring.get("keyword_penalties") or {}).items():
        if str(k).lower() in title_norm:
            score -= int(v)

    # 2) Platform/brand bonus (new)
    platform_bonus = _detect_platform_bonus(title_norm)
    score += platform_bonus

    # 3) Lot size inference (new)
    lot_count = _detect_lot_count(title_norm)
    lot_bonus = _lot_bonus_from_count(lot_count)
    score += lot_bonus

    # 4) Bid count (keep your logic, slightly refined)
    if listing.bid_count == 0:
        score += 12
    elif listing.bid_count == 1:
        score += 10
    elif listing.bid_count <= 2:
        score += 8
    elif listing.bid_count <= 5:
        score += 4
    elif listing.bid_count <= 10:
        score += 1
    elif listing.bid_count >= 25:
        score -= 4

    # 5) Current bid (slightly refined)
    if listing.current_bid <= 10:
        score += 12
    elif listing.current_bid <= 20:
        score += 10
    elif listing.current_bid <= 50:
        score += 6
    elif listing.current_bid <= 80:
        score += 2
    elif listing.current_bid >= 150:
        score -= 4

    # 6) Ending soon (stronger weighting close to end)
    tl = listing.time_left_minutes
    if tl is not None:
        if tl <= 15:
            score += 18
        elif tl <= 30:
            score += 12
        elif tl <= 60:
            score += 8
        elif tl <= 120:
            score += 4
        elif tl <= 240:
            score += 2

    # 7) Price anomaly bonus (new): low bid + strong signals
    # Helps surface sleepers with bad titles but clear value signals.
    strong_signal = (platform_bonus >= 10) or (lot_bonus >= 6)
    if strong_signal:
        if listing.current_bid <= 15:
            score += 12
        elif listing.current_bid <= 30:
            score += 6

    # SHIPPING REMOVED from scoring entirely (keep as-is)

    return max(0, min(120, int(score)))

def score_listing_explain(cfg: dict, listing: Listing) -> Tuple[int, str]:
    """
    Returns (score, explanation_text) where explanation_text is a compact human-readable breakdown.
    Safe: uses the same logic as score_listing, just logs the contributions.
    """
    scoring = cfg_get(cfg, "scoring", {}) or {}
    title_norm = normalize(listing.title)

    parts: List[str] = []
    score = 0

    def add(pts: int, label: str):
        nonlocal score
        if pts == 0:
            return
        score += int(pts)
        sign = "+" if pts > 0 else ""
        parts.append(f"{sign}{int(pts)} {label}")

    # 1) YAML keyword boosts/penalties
    for k, v in (scoring.get("keyword_boosts") or {}).items():
        if str(k).lower() in title_norm:
            add(int(v), f"kw:{k}")
    for k, v in (scoring.get("keyword_penalties") or {}).items():
        if str(k).lower() in title_norm:
            add(-int(v), f"pen:{k}")

    # 2) Platform/brand bonus
    platform_bonus = _detect_platform_bonus(title_norm)
    add(platform_bonus, "platform")

    # 3) Lot size inference
    lot_count = _detect_lot_count(title_norm)
    lot_bonus = _lot_bonus_from_count(lot_count)
    if lot_bonus:
        add(lot_bonus, f"lot({lot_count})")

    # 4) Bid count
    if listing.bid_count == 0:
        add(12, "0 bids")
    elif listing.bid_count == 1:
        add(10, "1 bid")
    elif listing.bid_count <= 2:
        add(8, "2 bids")
    elif listing.bid_count <= 5:
        add(4, "3-5 bids")
    elif listing.bid_count <= 10:
        add(1, "6-10 bids")
    elif listing.bid_count >= 25:
        add(-4, "25+ bids")

    # 5) Current bid
    if listing.current_bid <= 10:
        add(12, "bid<=10")
    elif listing.current_bid <= 20:
        add(10, "bid<=20")
    elif listing.current_bid <= 50:
        add(6, "bid<=50")
    elif listing.current_bid <= 80:
        add(2, "bid<=80")
    elif listing.current_bid >= 150:
        add(-4, "bid>=150")

    # 6) Ending soon
    tl = listing.time_left_minutes
    if tl is not None:
        if tl <= 15:
            add(18, "ends<=15m")
        elif tl <= 30:
            add(12, "ends<=30m")
        elif tl <= 60:
            add(8, "ends<=60m")
        elif tl <= 120:
            add(4, "ends<=120m")
        elif tl <= 240:
            add(2, "ends<=240m")

    # 7) Price anomaly bonus
    strong_signal = (platform_bonus >= 10) or (lot_bonus >= 6)
    if strong_signal:
        if listing.current_bid <= 15:
            add(12, "anomaly(low bid + strong signal)")
        elif listing.current_bid <= 30:
            add(6, "anomaly(mid bid + strong signal)")

    # Clamp like score_listing
    score = max(0, min(120, int(score)))

    # Make the explanation compact
    expl = " | ".join(parts) if parts else "(no signals)"
    return score, expl

# -----------------------------
# SGW API (YOUR WORKING ENDPOINTS)
# -----------------------------
SEARCH_API_URL = "https://buyerapi.shopgoodwill.com/api/Search/ItemListing"
DETAIL_API_URL = "https://buyerapi.shopgoodwill.com/api/ItemDetail/GetItemDetailModelByItemId"

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://shopgoodwill.com",
    "Referer": "https://shopgoodwill.com/",
})


def post_with_retries(url: str, payload: dict, tries: int = 6, base_sleep: float = 1.5) -> Optional[requests.Response]:
    for attempt in range(tries):
        try:
            resp = _SESSION.post(url, json=payload, timeout=30)

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = min(60.0, base_sleep * (2 ** attempt)) + random.uniform(0.0, 1.5)
                print(f"[WARN] {resp.status_code} from SGW. Backing off {sleep_s:.1f}s (attempt {attempt+1}/{tries})")
                time.sleep(sleep_s)
                continue

            print(f"[WARN] Non-retryable HTTP {resp.status_code}: {resp.text[:200]}")
            return None

        except requests.RequestException as e:
            sleep_s = min(60.0, base_sleep * (2 ** attempt)) + random.uniform(0.0, 1.5)
            print(f"[WARN] Request error: {e}. Backing off {sleep_s:.1f}s (attempt {attempt+1}/{tries})")
            time.sleep(sleep_s)

    return None


def fetch_search_page(cfg: dict, query: str, page: int = 1, page_size: int = 40) -> Optional[Dict[str, Any]]:
    payload = {
        "isSize": False,
        "isWeddingCatagory": False,
        "isMultipleCategoryIds": False,
        "isFromHeaderMenuTab": False,
        "layout": "grid",
        "isFromHomePage": False,

        "searchText": query,
        "selectedGroup": "Keyword",

        "selectedCategoryIds": "",
        "selectedSellerIds": "",

        "lowPrice": "0",
        "highPrice": "999999",

        "searchBuyNowOnly": "",

        "searchPickupOnly": False,
        "searchNoPickupOnly": False,
        "searchOneCentShippingOnly": False,
        "searchDescriptions": False,
        "searchClosedAuctions": False,

        "closedAuctionEndingDate": "01/01/1970",
        "closedAuctionDaysBack": "7",

        "searchCanadaShipping": False,
        "searchInternationalShippingOnly": False,
        "searchUSOnlyShipping": False,

        "sortColumn": "1",
        "sortDescending": False,
        "page": str(page),
        "pageSize": str(page_size),

        "savedSearchId": 0,
        "useBuyerPrefs": bool(cfg_get(cfg, "scan.use_buyer_prefs", False)),

        "categoryLevelNo": "1",
        "categoryLevel": 1,
        "categoryId": 0,
        "partNumber": "",
        "catIds": "",
    }

    resp = post_with_retries(SEARCH_API_URL, payload, tries=6, base_sleep=1.5)
    if resp is None:
        return None

    try:
        return resp.json()
    except Exception:
        print("[WARN] Could not parse JSON response.")
        return None


def fetch_item_detail(item_id: str) -> Optional[Dict[str, Any]]:
    try:
        r = _SESSION.get(f"{DETAIL_API_URL}/{item_id}", timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            return None
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _get_items_from_search_response(api_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    sr = api_json.get("searchResults") if isinstance(api_json, dict) else None
    if isinstance(sr, dict):
        items = sr.get("items")
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
    return []


def parse_listings_from_api(api_json: Dict[str, Any], feed_name: str, cfg: dict) -> List[Listing]:
    debug_time = bool(cfg_get(cfg, "scan.debug_time", False))
    out: List[Listing] = []

    for it in _get_items_from_search_response(api_json):
        item_id = _extract_first_key(it, ["itemId", "ItemId", "id"])
        if item_id is None:
            continue
        item_id = str(item_id)

        title = str(_extract_first_key(it, ["title", "Title"]) or f"Item {item_id}")
        bid = _to_float(_extract_first_key(it, ["currentBid", "currentPrice"]), 0.0) or 0.0
        bids = _to_int(_extract_first_key(it, ["bidCount", "numberOfBids"]), 0)

        tl = extract_time_left_minutes(it)

        if tl is None:
            end_raw = _extract_first_key(it, ["endTime", "endDate", "auctionEndDate", "endTimeUtc", "endDateUtc"])
            end_dt = _parse_dt_any(end_raw)
            tl = _minutes_left(end_dt)
            if debug_time and end_raw:
                print(f"[TIME-DEBUG] search {item_id} end_raw={end_raw} -> end_dt_utc={end_dt} -> tl={tl}")

        ship = _to_float(_extract_first_key(it, ["shippingCost"]), None)
        if ship == 0.0:
            ship = None

        url = f"https://shopgoodwill.com/item/{item_id}"

        out.append(Listing(
            listing_id=item_id,
            title=title,
            url=url,
            current_bid=bid,
            bid_count=bids,
            time_left_minutes=tl,
            shipping=ship,
            feed_name=feed_name,
        ))

    return out


def enrich_listing(listing: Listing, cfg: dict) -> Listing:
    debug_time = bool(cfg_get(cfg, "scan.debug_time", False))
    detail = fetch_item_detail(listing.listing_id)
    if not detail:
        return listing

    model = detail.get("itemDetailModel") or detail
    if not isinstance(model, dict):
        return listing

    tl = extract_time_left_minutes(model)

    if tl is None:
        end_raw = _extract_first_key(model, ["endTime", "endDate", "auctionEndDate", "endTimeUtc", "endDateUtc"])
        end_dt = _parse_dt_any(end_raw)
        tl = _minutes_left(end_dt)
        if debug_time and end_raw:
            print(f"[TIME-DEBUG] detail {listing.listing_id} end_raw={end_raw} -> end_dt_utc={end_dt} -> tl={tl}")

    if tl is not None:
        listing.time_left_minutes = tl

    if listing.shipping is None:
        ship = _to_float(_extract_first_key(model, ["shippingCost", "shippingTotal", "shipping"]), None)
        if ship == 0.0:
            ship = None
        listing.shipping = ship

    listing.current_bid = _to_float(_extract_first_key(model, ["currentBid", "currentPrice"]), listing.current_bid) or listing.current_bid
    listing.bid_count = _to_int(_extract_first_key(model, ["bidCount", "numberOfBids"]), listing.bid_count)

    return listing


# -----------------------------
# Filters + formatting (2-window logic) - SHIPPING REMOVED
# -----------------------------
def passes_filters(cfg: dict, listing: Listing, for_alert: bool) -> bool:
    alert_window = int(cfg_get(cfg, "scan.ending_soon_minutes", 180))
    digest_window = int(cfg_get(cfg, "scan.digest_window_minutes", 360))

    max_bids = int(cfg_get(cfg, "scan.max_bids", 8))

    if listing.bid_count > max_bids:
        return False

    if listing.time_left_minutes is None:
        return not for_alert

    if for_alert:
        return listing.time_left_minutes <= alert_window
    return listing.time_left_minutes <= digest_window




def _money(v: Optional[float]) -> str:
    if v is None:
        return "?"
    return f"${v:,.2f}"

def build_listing_embed(listing: Listing, score: int, explain: Optional[str], kind: str) -> dict:
    """
    kind: "best" | "browse" | "digest"
    """
    # Colors (hex -> int)
    # best: green, browse: blue, digest: gray
    color = 0x2ecc71 if kind == "best" else (0x3498db if kind == "browse" else 0x95a5a6)

    tl = f"{listing.time_left_minutes}m" if listing.time_left_minutes is not None else "?"
    comps_q = clean_comps_query(listing.title)
    comps_url = ebay_sold_url(comps_q)

    # Keep description readable + within embed limits
    why_line = f"**Why:** {explain}\n" if explain else ""

    desc = (
        f"**{listing.title}**\n"
        f"{why_line}"
        f"**SGW:** {listing.url}\n"
        f"**eBay SOLD:** {comps_url}"
    )

    embed = {
        "title": f"{'🔥 BEST' if kind=='best' else ('🧭 BROWSE' if kind=='browse' else '🧾 DIGEST')} • Score {score}",
        "url": listing.url,
        "description": desc,
        "color": color,
        "fields": [
            {"name": "Ends In", "value": tl, "inline": True},
            {"name": "Bids", "value": str(listing.bid_count), "inline": True},
            {"name": "Current Bid", "value": _money(listing.current_bid), "inline": True},
            {"name": "Shipping", "value": _money(listing.shipping), "inline": True},
            {"name": "Feed", "value": listing.feed_name or "?", "inline": False},
        ],
        # IMPORTANT: bot will read listing_id from footer
        "footer": {"text": f"listing_id={listing.listing_id}"},
    }
    return embed

def format_alert(listing: Listing, score: int, explain: Optional[str] = None) -> Tuple[str, dict, str]:
    title_line = f"🔥 SLEEPER (Score {score})"
    embed = build_listing_embed(listing, score, explain, kind="best")
    return title_line, embed, listing.url


def format_browse_line(listing: Listing, score: int, explain: Optional[str] = None) -> dict:
    return build_listing_embed(listing, score, explain, kind="browse")



def chunk_lines_for_discord(lines, max_chars=1800):
    """
    Splits lines into chunks under Discord's 2000 char limit.
    Keeps lines intact.
    """
    buf = ""
    for line in lines:
        if len(buf) + len(line) + 1 > max_chars:
            if buf:
                yield buf.rstrip()
            buf = line + "\n"
        else:
            buf += line + "\n"
    if buf:
        yield buf.rstrip()


def maybe_send_digest(conn, cfg: dict):
    digest_every = int(cfg_get(cfg, "scan.tiers.digest_every_minutes", 60))
    digest_max_items = int(cfg_get(cfg, "scan.digest_max_items", 20))

    now = int(time.time())
    last_ts = int(meta_get(conn, "last_digest_ts", "0") or "0")
    if (now - last_ts) < (digest_every * 60):
        return

    items = digest_pop_all(conn, limit=digest_max_items)
    meta_set(conn, "last_digest_ts", str(now))

    if not items:
        return

    # Include eBay comps links in digest too
    lines = []
    for score, title, url, feed in items:
        comps_q = clean_comps_query(title)
        comps_url = ebay_sold_url(comps_q)
        lines.append(f"• ({score}) {title} [{feed}] — SGW: {url} | eBay SOLD: {comps_url}")

    title = f"🧾 Browse Digest ({len(items)} items)"

    chunks = list(chunk_lines_for_discord(lines, max_chars=1800))

    for i, chunk in enumerate(chunks, start=1):
        part_title = title if len(chunks) == 1 else f"{title} (Part {i}/{len(chunks)})"
        notify(cfg, part_title, chunk, channel="browse")
        time.sleep(0.25)

    print(f"[DIGEST] sent {len(items)} items in {len(chunks)} message(s) (browse)")

# -----------------------------
# Runner
# -----------------------------
def run_once(cfg: dict):
    conn = db_connect()
    ensure_user_actions_table(conn)

    max_alerts_per_hour = int(cfg_get(cfg, "scan.max_alerts_per_hour", 999999))
    max_alerts_per_scan = int(cfg_get(cfg, "scan.max_alerts_per_scan", 10))
    enrich_top_n = int(cfg_get(cfg, "scan.enrich_top_n", 12))
    tier_b_min = int(cfg_get(cfg, "scan.tiers.tier_b_min_score", 55))

    best_min_score = int(cfg_get(cfg, "scan.best_min_score", tier_b_min + 25))
    browse_max_stream = int(cfg_get(cfg, "scan.browse_max_posts_per_scan", 25))

    if alerts_in_last_hour(conn) >= max_alerts_per_hour:
        print("[INFO] Throttle: max alerts/hour reached.")
        maybe_send_digest(conn, cfg)
        conn.close()
        return

    candidates: List[Tuple[int, Listing]] = []
    candidate_ids: set = set()

    feeds = cfg_get(cfg, "feeds", [])
    for feed in feeds:
        feed_name = str(feed.get("name", "Feed"))
        query = str(feed.get("query", "")).strip()
        if not query:
            continue

        for page in (1, 2):
            api_json = fetch_search_page(cfg, query=query, page=page)
            if not api_json:
                print(f"[WARN] Feed '{feed_name}' page {page} returned no data (SGW 500/429). Skipping.")
                continue

            listings = parse_listings_from_api(api_json, feed_name=feed_name, cfg=cfg)
            print(f"[DEBUG] {feed_name} page {page}: parsed {len(listings)} listings")

            time.sleep(float(cfg_get(cfg, "scan.request_delay_seconds", 0.6)))

            for it in listings:
                score = score_listing(cfg, it)
                mark_seen(conn, it.listing_id, score)
                action = get_user_action(conn, it.listing_id)
                if action in ("dismiss", "save"):
                    continue

                # Skip if already BEST-alerted in the past
                if was_alerted(conn, it.listing_id):
                    continue

                # Must meet base threshold to be considered
                if score < tier_b_min:
                    continue

                # Candidate window uses digest window (wider)
                if not passes_filters(cfg, it, for_alert=False):
                    continue

                # De-dupe across overlapping feeds/pages
                if it.listing_id in candidate_ids:
                    continue
                candidate_ids.add(it.listing_id)

                candidates.append((score, it))

    if not candidates:
        maybe_send_digest(conn, cfg)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] No candidates.")
        conn.close()
        return

    candidates.sort(key=lambda x: -x[0])

    enriched: List[Tuple[int, Listing, str]] = []
    for score, it in candidates[:max(enrich_top_n, 1)]:
        it = enrich_listing(it, cfg=cfg)
        score, expl = score_listing_explain(cfg, it)
        enriched.append((score, it, expl))

    enriched.sort(key=lambda x: -x[0])

    sent_best = 0
    sent_browse_stream = 0

    # Track within-scan postings so we don't cross-post or duplicate
    alerted_this_scan: set = set()
    browsed_this_scan: set = set()

    for score, it, expl in enriched:
        lid = it.listing_id
        sent_to_best = False

        # --- BEST FIRST (exclusive) ---
        if sent_best >= max_alerts_per_scan:
            # no more BEST alerts this scan
            pass
        elif alerts_in_last_hour(conn) >= max_alerts_per_hour:
            print("[INFO] Throttle reached mid-scan.")
            break
        else:
            # If already alerted (previous scan or earlier this scan), don't alert again
            if lid in alerted_this_scan or was_alerted(conn, lid):
                # Still let it be digested (optional)
                digest_enqueue(conn, lid, score, it.title, it.url, it.feed_name)
            else:
                if it.time_left_minutes is None:
                    digest_enqueue(conn, lid, score, it.title, it.url, it.feed_name)
                elif not passes_filters(cfg, it, for_alert=True):
                    digest_enqueue(conn, lid, score, it.title, it.url, it.feed_name)
                elif score < best_min_score:
                    digest_enqueue(conn, lid, score, it.title, it.url, it.feed_name)
                else:
                    title, body, url = format_alert(it, score, expl)
                    notify(cfg, title, body, url=url, channel="best")
                    mark_alerted(conn, lid)
                    alerted_this_scan.add(lid)
                    sent_best += 1
                    sent_to_best = True
                    print(f"[ALERTED-BEST] {title}")

        # --- BROWSE ONLY IF NOT SENT TO BEST (prevents overlap) ---
        if (
            not sent_to_best
            and sent_browse_stream < browse_max_stream
            and lid not in browsed_this_scan
            and not was_browsed(conn, lid)
            and lid not in alerted_this_scan
            and not was_alerted(conn, lid)
        ):
            notify(cfg, "🧭 Browse Hit", format_browse_line(it, score, expl), channel="browse")
            mark_browsed(conn, lid)
            browsed_this_scan.add(lid)
            sent_browse_stream += 1

        
    # Queue everything else (beyond enrichment depth) for digest
    for score, it in candidates[enrich_top_n:]:
        digest_enqueue(conn, it.listing_id, score, it.title, it.url, it.feed_name)

    maybe_send_digest(conn, cfg)
    print(f"[DEBUG] Alerts (BEST) sent this scan: {sent_best} | Browse stream sent: {sent_browse_stream}")
    conn.close()


def main():
    cfg = load_config("config.yaml")
    interval = int(cfg_get(cfg, "scan.interval_minutes", 10))
    print(f"Deal Scanner running. Interval: {interval} minutes. DB: {DB_PATH}")

    while True:
        run_once(cfg)
        print(f"[INFO] Sleeping {interval} minutes until next scan...\n")
        time.sleep(interval * 60)


if __name__ == "__main__":
    main()