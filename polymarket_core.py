"""
polymarket_core.py — Shared plumbing for all Polymarket scripts.
Import from here instead of duplicating across files.
"""

import os, re, json, time, hmac, hashlib, base64, requests
from datetime import datetime, timezone
from dotenv import dotenv_values

# ─── ENV ──────────────────────────────────────────────────
def _find_env():
    d = os.path.dirname(os.path.abspath(__file__))
    for p in [os.path.join(d, "polymarket.env"),
              os.path.expanduser("~/polymarket/polymarket.env"),
              os.path.expanduser("~/polymarket.env")]:
        if os.path.exists(p): return p
    return os.path.join(d, "polymarket.env")

ENV_PATH = _find_env()
ENV = dotenv_values(ENV_PATH)

# ─── PATHS ────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Use /root/polymarket/ on VPS, script dir locally.
# All runtime artifacts live here so every script shares one canonical layout.
DATA_DIR = os.environ.get("POLYMARKET_DATA_DIR") or (
    "/root/polymarket" if os.path.isdir("/root/polymarket") else _SCRIPT_DIR
)


def data_path(*parts):
    return os.path.join(DATA_DIR, *parts)


TRADES_JSONL = data_path("trades.jsonl")
ARB_STATE_JSON = data_path("arb_state.json")
DELTA_SIGNALS_JSONL = data_path("delta_signals.jsonl")
BRIER_LOG_JSONL = data_path("brier_log.jsonl")
TRUTH_CACHE_JSON = data_path("truth_cache.json")
CALIBRATION_STATS_JSON = data_path("calibration_stats.json")
STARTING_BANKROLL_JSON = data_path("starting_bankroll.json")
EMOS_COEFFICIENTS_JSON = data_path("emos_coefficients.json")
HYPOTHESES_JSON = data_path("hypotheses.json")
CITY_CALIBRATION_JSON = data_path("city_calibration.json")
EMOS_RETRAIN_QUEUE_JSON = data_path("emos_retrain_queue.json")

# ─── API ENDPOINTS ────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"

# ─── CITY COORDS (resolution station coordinates) ────────
# Base from polymarket_model.py (has resolution station comments).
# All entries from polymarket_delta_arb.py are present here.
CITY_COORDS = {
    "london":        (51.5074,  -0.1278),
    "paris":         (48.8566,   2.3522),
    "new york city": (40.7769, -73.8740),  # LaGuardia KLGA (resolution station)
    "new york":      (40.7769, -73.8740),
    "nyc":           (40.7769, -73.8740),
    "seoul":         (37.5665, 126.9780),
    "toronto":       (43.6532, -79.3832),
    "seattle":       (47.6062,-122.3321),
    "dallas":        (32.8481, -96.8517),  # Love Field KDAL (resolution station)
    "atlanta":       (33.7490, -84.3880),
    "miami":         (25.7617, -80.1918),
    "chicago":       (41.8781, -87.6298),
    "houston":       (29.7604, -95.3698),
    "austin":        (30.2672, -97.7431),
    "denver":        (39.7392,-104.9903),
    "los angeles":   (34.0522,-118.2437),
    "san francisco": (37.7749,-122.4194),
    "tokyo":         (35.6762, 139.6503),
    "hong kong":     (22.3020, 114.1739),  # HKO HQ Tsim Sha Tsui (resolution station)
    "shanghai":      (31.2304, 121.4737),
    "singapore":     (1.3644,  103.9915),  # Changi Airport WSSS (resolution station)
    "beijing":       (39.9042, 116.4074),
    "shenzhen":      (22.5431, 114.0579),
    "guangzhou":     (23.1291, 113.2644),
    "taipei":        (25.0330, 121.5654),
    "wuhan":         (30.5928, 114.3055),
    "chengdu":       (30.5728, 104.0668),
    "chongqing":     (29.4316, 106.9123),
    "madrid":        (40.4168,  -3.7038),
    "milan":         (45.4642,   9.1900),
    "amsterdam":     (52.3676,   4.9041),
    "warsaw":        (52.2297,  21.0122),
    "helsinki":      (60.1699,  24.9384),
    "moscow":        (55.7558,  37.6173),
    "istanbul":      (41.0082,  28.9784),
    "ankara":        (39.9334,  32.8597),
    "tel aviv":      (32.0853,  34.7818),
    "buenos aires":  (-34.6037, -58.3816),
    "sao paulo":     (-23.5505, -46.6333),
    "wellington":    (-41.2866, 174.7756),
    "cape town":     (-33.9249,  18.4241),
    "mexico city":   (19.4326, -99.1332),
    "panama city":   (8.9936,  -79.5197),
    "kuala lumpur":  (2.7456,  101.7072),  # KLIA airport (resolution station)
    "jakarta":       (-6.1256, 106.6559),  # Soekarno-Hatta Airport (resolution station)
    "jeddah":        (21.4858,  39.1925),
    "lagos":         (6.5244,    3.3792),
    "karachi":       (24.8607,  67.0011),
    "manila":        (14.5995, 120.9842),
    "lucknow":       (26.8467,  80.9462),
    "busan":         (35.1796, 129.0756),
    "munich":        (48.1351,  11.5820),
}

# ─── CITY / QUESTION PARSING ─────────────────────────────
def detect_city(text):
    t = text.lower()
    for city in sorted(CITY_COORDS, key=len, reverse=True):
        if city in t:
            return city, CITY_COORDS[city]
    return None, None

def parse_temp_range(question):
    """Parse temperature bucket from Polymarket question string.
    Returns (low, high, unit) or None."""
    q = question
    m = re.search(r'between\s+(\d+\.?\d*)[–\-](\d+\.?\d*)\s*°?([CF])', q, re.I)
    if m:
        return float(m.group(1)), float(m.group(2)), m.group(3).upper()
    m = re.search(r'be\s+(-?\d+\.?\d*)\s*°?([CF])\s+or\s+below', q, re.I)
    if m:
        return -999.0, float(m.group(1)), m.group(2).upper()
    m = re.search(r'be\s+(\d+\.?\d*)\s*°?([CF])\s+or\s+(higher|above)', q, re.I)
    if m:
        return float(m.group(1)), 999.0, m.group(2).upper()
    m = re.search(r'be\s+(-?\d+\.?\d*)\s*°([CF])\s+on', q, re.I)
    if m:
        v = float(m.group(1))
        return v, v, m.group(2).upper()
    return None

# ─── TEMPERATURE UTILS ────────────────────────────────────
def c_to_f(c): return c * 9/5 + 32
def f_to_c(f): return (f - 32) * 5/9


def parse_iso_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def hk_resolution_datetime(end_date_str):
    try:
        end_date = str(end_date_str)[:10]
        return datetime.fromisoformat(f"{end_date}T16:00:00+00:00")
    except Exception:
        return None


def hours_to_resolution(end_date_str, now=None):
    now = now or datetime.now(timezone.utc)
    resolution_dt = hk_resolution_datetime(end_date_str)
    if resolution_dt is None:
        return None
    return (resolution_dt - now).total_seconds() / 3600.0


def market_timing_metrics(created_ts, end_date_str, now=None):
    """
    Return lifecycle metrics for a Polymarket weather contract.

    Score interpretation:
      1.0 -> first quartile / fresh market, highest directional mispricing
      0.8 -> late lifecycle / near resolution, still actionable but more cautious
      0.5 -> middle lifecycle, require stronger edge
    """
    now = now or datetime.now(timezone.utc)
    hours_left = hours_to_resolution(end_date_str, now)
    created_dt = parse_iso_dt(created_ts)

    age_hours = None
    life_progress = None
    score = 0.7

    if created_dt is not None:
        age_hours = max((now - created_dt).total_seconds() / 3600.0, 0.0)
        resolution_dt = hk_resolution_datetime(end_date_str)
        if resolution_dt is not None and resolution_dt > created_dt:
            total_life = (resolution_dt - created_dt).total_seconds() / 3600.0
            if total_life > 0:
                life_progress = min(max(age_hours / total_life, 0.0), 1.0)

    if life_progress is not None:
        if life_progress <= 0.25:
            score = 1.0
        elif life_progress >= 0.75:
            score = 0.8
        else:
            score = 0.5
    elif age_hours is not None:
        if age_hours <= 12:
            score = 1.0
        elif hours_left is not None and hours_left <= 6:
            score = 0.8
        else:
            score = 0.5
    elif hours_left is not None and hours_left <= 6:
        score = 0.8

    return {
        "age_score": score,
        "age_hours": round(age_hours, 2) if age_hours is not None else None,
        "hours_to_resolution": round(hours_left, 2) if hours_left is not None else None,
        "life_progress": round(life_progress, 4) if life_progress is not None else None,
    }

# ─── DISCORD ──────────────────────────────────────────────
def discord_post(content, embeds=None):
    token   = ENV.get("DISCORD_BOT_TOKEN", "")
    channel = ENV.get("DISCORD_CHANNEL_ID", "")
    if not token or not channel:
        return
    try:
        payload = {"content": content}
        if embeds:
            payload["embeds"] = embeds
        requests.post(
            f"https://discord.com/api/v10/channels/{channel}/messages",
            headers={"Authorization": f"Bot {token}", "Content-Type": "application/json"},
            json=payload, timeout=8,
        )
    except Exception:
        pass

# ─── CLOB CLIENT ──────────────────────────────────────────
def get_clob_client():
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from py_clob_client.constants import POLYGON
    creds = ApiCreds(
        api_key=ENV["POLY_API_KEY"],
        api_secret=ENV["POLY_SECRET"],
        api_passphrase=ENV["POLY_PASSPHRASE"],
    )
    return ClobClient(
        host=CLOB_API, chain_id=POLYGON,
        key=ENV["POLY_PRIVATE_KEY"], creds=creds,
        signature_type=1, funder=ENV["POLY_ADDRESS"],
    )

# ─── L2 AUTH HEADERS ─────────────────────────────────────
def l2_headers(method, path, body=""):
    ts  = str(int(time.time()))
    msg = ts + method.upper() + path + (body or "")
    sec = base64.urlsafe_b64decode(ENV["POLY_SECRET"])
    sig = base64.urlsafe_b64encode(
        hmac.new(sec, msg.encode(), hashlib.sha256).digest()
    ).decode()
    from eth_account import Account
    signer_addr = Account.from_key(ENV["POLY_PRIVATE_KEY"]).address
    return {
        "POLY_ADDRESS":     signer_addr,
        "POLY_API_KEY":     ENV["POLY_API_KEY"],
        "POLY_PASSPHRASE":  ENV["POLY_PASSPHRASE"],
        "POLY_TIMESTAMP":   ts,
        "POLY_SIGNATURE":   sig,
        "Content-Type":     "application/json",
    }
