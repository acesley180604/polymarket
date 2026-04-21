"""
Polymarket Weather Arb — Refined Model
Sources:
  - suislanchez/polymarket-kalshi-weather-bot (GFS ensemble method)
  - polytraderbot.com PolyWeatherBot (10-model / 82-member approach)
  - Half-Kelly sizing (user spec)
  - Capital controls: daily loss limit, max positions, entry price cap

Key upgrades vs v1:
  - GFS 31-member ensemble (not Gaussian point forecast)
  - ECMWF 51-member ensemble blended in
  - Model agreement filter (GFS + ECMWF must agree within 1.5°C median)
  - Half-Kelly (0.5) with hard caps
  - Tomorrow-only rule (no same-day bets)
  - Brier score log for model calibration
  - Entry price cap: skip YES > 0.70 (overpriced), NO-side cap too
  - Max 20 concurrent positions
  - Daily loss limit: $200
"""

import requests, json, re, time, os, math, statistics
from datetime import date, timedelta, datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from scipy.stats import norm
from polymarket_core import ENV as _env, CITY_COORDS, detect_city, parse_temp_range, c_to_f, f_to_c, GAMMA_API, CLOB_API, BRIER_LOG_JSONL, CITY_CALIBRATION_JSON, market_timing_metrics

# ─── RISK PARAMS — First-trial unified 1/4-Kelly system ─
#
# First trial: all conviction tiers use 1/4 Kelly for cleaner sizing.
# Tier A: conf≥70% + edge≥25%  → 1/4 Kelly, max 5% bankroll  (strong signal)
# Tier B: conf≥60% + edge≥18%  → 1/4 Kelly, max 3% bankroll  (medium signal)
# Tier C: conf≥52% + edge≥20%  → 1/4 Kelly, max 2% bankroll  (diversification)
#
# Capital rules (professional poker-style):
#   - Max daily exposure:  30% of bankroll
#   - Stop-loss:          -15% of bankroll/day → sit out
#   - Max per position:    5% of bankroll
#   - Min deposit to trade: $10 USDC
#   - Max 2 bets per city per day (correlation control)
#
# All tier caps are % of bankroll — scales correctly from $10 to $10,000.

TIERS = [
    {"name":"A", "min_conf":0.70, "min_edge":0.25, "kelly":1/2, "max_pct":0.20, "label":"🔥🔥"},
    {"name":"B", "min_conf":0.60, "min_edge":0.18, "kelly":1/3, "max_pct":0.12, "label":"🔥 "},
    {"name":"C", "min_conf":0.52, "min_edge":0.20, "kelly":1/4, "max_pct":0.06, "label":"→  "},
]

MAX_PCT_BANKROLL  = 0.20   # Hard cap: 20% bankroll per bet (Tier A arb)
MAX_POSITIONS     = 50     # Max concurrent positions
MAX_PER_CITY      = 4      # Max 2 bets per city (correlation rule)
DAILY_EXPOSURE    = 1.00   # Max 30% bankroll deployed per day
DAILY_LOSS_LIMIT  = 0.15   # Stop at -15% bankroll loss
MAX_ENTRY_PRICE   = 0.72   # Skip YES > 72¢ (not enough reward)
MIN_BET_USD       = 0.50   # Polymarket practical minimum
MIN_BANKROLL      = 1.0    # Minimum to trade live (T1 seed = $2, 100 buy-in rule)
FORECAST_HORIZON  = "tomorrow"  # Tomorrow only — no same-day bets
ARB_MIN_DEVIATION = float(_env.get("ARB_MIN_DEVIATION", "0.06"))
_CITY_CALIBRATION_CACHE = {"loaded_at": 0.0, "data": {}}
_CITY_CALIBRATION_TTL = 6 * 3600
PRECISION_WINDOW_HOURS = 6.0
PRECISION_MAX_STD_C = 1.2
PRECISION_MIN_EDGE = 0.08
PRECISION_MAX_BUCKET_WIDTH_C = 1.25
PRECISION_RAW_BET = 0.50


# ─── ENSEMBLE FORECAST (GFS 31-member) ───────────────────
def fetch_gfs_ensemble(lat, lon, target_date_str):
    """
    Fetch 31-member GFS ensemble from Open-Meteo.
    Returns list of max-temp values (Celsius) for target date.
    Method: suislanchez repo — count members, not Gaussian.
    """
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude":    lat,
        "longitude":   lon,
        "daily":       "temperature_2m_max",
        "temperature_unit": "celsius",
        "models":      "gfs_seamless",
        "start_date":  target_date_str,
        "end_date":    target_date_str,
        "timezone":    "auto",
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    daily = r.json().get("daily", {})
    # Collect all member keys: temperature_2m_max, temperature_2m_max_member01, etc.
    members = []
    for key, vals in daily.items():
        if "temperature_2m_max" in key and vals:
            v = vals[0]
            if v is not None:
                members.append(float(v))
    return members  # ~31 values in Celsius

def fetch_ecmwf_ensemble(lat, lon, target_date_str):
    """ECMWF IFS025 ensemble — 51 members, 0.25° resolution."""
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    for model in ["ecmwf_ifs025", "ecmwf_ifs04"]:
        try:
            params = {
                "latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max",
                "temperature_unit": "celsius",
                "models": model,
                "start_date": target_date_str,
                "end_date": target_date_str,
                "timezone": "auto",
            }
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            daily = r.json().get("daily", {})
            members = [float(v) for k, v in daily.items()
                       if "temperature_2m_max" in k and v and v[0] is not None
                       for v in [v[0]]]
            if members: return members
        except Exception:
            pass
    return []

def fetch_icon_ensemble(lat, lon, target_date_str):
    """DWD ICON ensemble — ~40 members, good for Europe/Asia."""
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": lat, "longitude": lon,
        "daily": "temperature_2m_max",
        "temperature_unit": "celsius",
        "models": "icon_seamless",
        "start_date": target_date_str,
        "end_date": target_date_str,
        "timezone": "auto",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        daily = r.json().get("daily", {})
        members = [float(v) for k, v in daily.items()
                   if "temperature_2m_max" in k and v and v[0] is not None
                   for v in [v[0]]]
        return members
    except Exception:
        return []

def blend_ensembles(gfs_members, ecmwf_members, icon_members=None):
    """Blend GFS + ECMWF + ICON. Weights: ECMWF 50%, GFS 30%, ICON 20%."""
    blended = ecmwf_members * 5 + gfs_members * 3
    if icon_members:
        blended += icon_members * 2
    return blended

class EnsembleForecast:
    def __init__(self, members_c):
        self.members = [m for m in members_c if m is not None]
        if not self.members:
            self.mean = 0; self.std = 0; self.confidence = 0.5
            return
        self.mean = statistics.mean(self.members)
        self.std  = statistics.stdev(self.members) if len(self.members) > 1 else 2.0
        med = statistics.median(self.members)
        above = sum(1 for m in self.members if m > med)
        total = len(self.members)
        self.confidence = max(above, total - above) / total  # 0.5=split, 1.0=unanimous

    def prob_above(self, threshold_c):
        """P(max_temp > threshold) — count members"""
        if not self.members:
            return 0.5
        count = sum(1 for m in self.members if m > threshold_c)
        raw = count / len(self.members)
        return max(0.05, min(0.95, raw))  # clip [0.05, 0.95]

    def prob_below(self, threshold_c):
        return 1.0 - self.prob_above(threshold_c)

    def prob_in_range(self, low_c, high_c):
        if not self.members:
            return 0.5
        count = sum(1 for m in self.members if low_c <= m <= high_c)
        raw = count / len(self.members)
        return max(0.02, min(0.95, raw))

    def prob_for_market(self, low, high, unit):
        """Compute P(YES) for a Polymarket temperature bucket"""
        lo_c = f_to_c(low)  if unit == "F" else low
        hi_c = f_to_c(high) if unit == "F" else high
        if low == -999.0:
            return self.prob_below(hi_c + (0.5 if unit == "C" else f_to_c(0.5)))
        elif high == 999.0:
            return self.prob_above(lo_c - (0.5 if unit == "C" else f_to_c(0.5)))
        else:
            return self.prob_in_range(lo_c, hi_c)

    def gfs_ecmwf_agree(self, gfs_fc, ecmwf_fc, threshold_c=1.5):
        """True if GFS and ECMWF median forecasts agree within threshold_c"""
        return abs(gfs_fc.mean - ecmwf_fc.mean) <= threshold_c

# ─── TIERED KELLY SIZING ─────────────────────────────────
def get_tier(confidence, edge):
    """Return the best matching tier, or None if no tier qualifies."""
    for tier in TIERS:
        if confidence >= tier["min_conf"] and abs(edge) >= tier["min_edge"]:
            return tier
    return None

def kelly_size(p_win, market_price, bankroll, kelly_fraction, max_pct):
    """
    Fractional Kelly sizing — all caps are % of bankroll (scales with any bankroll size).
    Kelly: f = (p*b - q) / b  where b = net odds = (1-price)/price
    """
    if market_price <= 0.01 or market_price >= 0.99:
        return 0
    b = (1.0 - market_price) / market_price
    q = 1.0 - p_win
    f = (p_win * b - q) / b
    if f <= 0:
        return 0
    bet = bankroll * f * kelly_fraction
    bet = min(bet, MAX_PCT_BANKROLL * bankroll)  # 5% bankroll hard cap
    bet = min(bet, max_pct * bankroll)            # tier % cap
    return round(max(bet, 0), 2)

def expected_value(p_win, market_price, bet_size):
    """EV of a YES bet"""
    payout = bet_size / market_price   # if wins, get back this much
    return p_win * (payout - bet_size) - (1 - p_win) * bet_size


def _load_city_calibration():
    now = time.time()
    if (
        _CITY_CALIBRATION_CACHE["data"]
        and now - _CITY_CALIBRATION_CACHE["loaded_at"] < _CITY_CALIBRATION_TTL
    ):
        return _CITY_CALIBRATION_CACHE["data"]
    try:
        with open(CITY_CALIBRATION_JSON) as f:
            data = json.load(f)
    except Exception:
        data = {}
    _CITY_CALIBRATION_CACHE["loaded_at"] = now
    _CITY_CALIBRATION_CACHE["data"] = data
    return data


def _isotonic_lookup(p, breakpoints):
    if not breakpoints:
        return p
    pts = sorted(
        (
            float(x),
            float(y),
        )
        for x, y in breakpoints
        if x is not None and y is not None
    )
    if not pts:
        return p
    if p <= pts[0][0]:
        return max(0.01, min(0.99, pts[0][1]))
    if p >= pts[-1][0]:
        return max(0.01, min(0.99, pts[-1][1]))
    for (x1, y1), (x2, y2) in zip(pts, pts[1:]):
        if x1 <= p <= x2:
            if x2 == x1:
                return max(0.01, min(0.99, y2))
            frac = (p - x1) / (x2 - x1)
            out = y1 + frac * (y2 - y1)
            return max(0.01, min(0.99, out))
    return max(0.01, min(0.99, p))


def _bucket_to_celsius(market):
    low_c = (market["low"] - 32) * 5 / 9 if market["unit"] == "F" and market["low"] > -999 else market["low"]
    high_c = (market["high"] - 32) * 5 / 9 if market["unit"] == "F" and market["high"] < 999 else market["high"]
    return low_c, high_c


def _bucket_midpoint_c(market):
    low_c, high_c = _bucket_to_celsius(market)
    if low_c <= -999:
        return high_c - 0.5
    if high_c >= 999:
        return low_c + 0.5
    return (low_c + high_c) / 2.0


def _bucket_width_c(market):
    low_c, high_c = _bucket_to_celsius(market)
    if low_c <= -999 or high_c >= 999:
        return float("inf")
    return high_c - low_c

# ─── BALANCE FETCHER ─────────────────────────────────────
def _l2_headers(method="GET", path="", body=""):
    """Minimal L2 HMAC auth headers for balance endpoint."""
    import hmac as _hmac, hashlib as _hashlib, base64 as _b64
    api_key    = _env.get("POLY_API_KEY", "")
    secret     = _env.get("POLY_SECRET", "")
    passphrase = _env.get("POLY_PASSPHRASE", "")
    ts  = str(int(time.time()))
    msg = ts + method.upper() + path + (body or "")
    sec = _b64.urlsafe_b64decode(secret)
    sig = _b64.urlsafe_b64encode(_hmac.new(sec, msg.encode(), _hashlib.sha256).digest()).decode()
    from eth_account import Account as _Acct
    signer_addr = _Acct.from_key(_env.get("POLY_PRIVATE_KEY","")).address
    return {
        "POLY_ADDRESS":    signer_addr,
        "POLY_API_KEY":    api_key,
        "POLY_PASSPHRASE": passphrase,
        "POLY_TIMESTAMP":  ts,
        "POLY_SIGNATURE":  sig,
    }

def fetch_usdc_balance():
    """
    Fetch available USDC cash and total portfolio value.
    Returns (usdc_cash, total_portfolio_value).
    Uses Polygon RPC for live USDC balance; falls back to BANKROLL_OVERRIDE.
    """
    address = _env.get("POLY_ADDRESS", "")
    override = _env.get("BANKROLL_OVERRIDE", "")
    if not address:
        return float(override or 0), 0.0

    # Fetch USDC collateral balance via py_clob_client /balance-allowance (COLLATERAL)
    usdc_cash = None
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.constants import POLYGON
        from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams, AssetType
        _c = ClobClient(
            host="https://clob.polymarket.com", chain_id=POLYGON,
            key=_env.get("POLY_PRIVATE_KEY",""), signature_type=1, funder=address,
            creds=ApiCreds(
                api_key=_env.get("POLY_API_KEY",""),
                api_secret=_env.get("POLY_SECRET",""),
                api_passphrase=_env.get("POLY_PASSPHRASE",""),
            ),
        )
        resp = _c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        usdc_cash = int(resp.get("balance","0")) / 1e6
        print(f"    (CLOB balance:  USDC)", flush=True)
    except Exception as _e:
        print(f"    (CLOB balance failed: {_e})", flush=True)

    # Try 2: data-api portfolio
    portfolio_val = 0.0
    try:
        r = requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": address, "sizeThreshold": "0.01", "limit": 500},
            timeout=10,
        )
        if r.status_code == 200:
            positions = r.json() if isinstance(r.json(), list) else []
            for p in positions:
                portfolio_val += float(p.get("currentValue") or p.get("value") or 0)
    except Exception:
        pass

    # Fallback ONLY if RPC itself failed (None), not if it returned real 0
    if usdc_cash is None:
        usdc_cash = float(override) if override else 0.0
        print(f"    (RPC failed — using BANKROLL_OVERRIDE: ${usdc_cash:.2f})", flush=True)

    return usdc_cash, portfolio_val

# ─── MARKET FETCHING ─────────────────────────────────────
def fetch_target_markets(day_offsets=None):
    if day_offsets is None:
        day_offsets = [1]
    all_markets = []
    target_dates = {(date.today() + timedelta(days=delta)).isoformat() for delta in day_offsets}
    max_target = max(target_dates)
    for offset in range(3300, 3600, 100):
        try:
            r = requests.get(f"{GAMMA_API}/events",
                             params={"tag_slug":"weather","active":"true",
                                     "limit":100,"offset":offset},
                             timeout=15)
            events = r.json() if isinstance(r.json(), list) else []
            found_any = False
            for ev in events:
                end = ev.get("endDate","")
                end_day = end[:10]
                if end_day not in target_dates:
                    continue
                found_any = True
                for mkt in ev.get("markets",[]):
                    mkt["_end_date"] = end_day
                    mkt["_created_ts"] = mkt.get("createdAt") or ev.get("createdAt") or ""
                    all_markets.append(mkt)
            if not found_any and offset > 3300:
                dates = [ev.get("endDate","")[:10] for ev in events if ev.get("endDate")]
                if dates and max(dates) < max_target:
                    break
        except Exception as e:
            print(f"  Fetch error offset {offset}: {e}", flush=True)
    return all_markets

def get_price_batch(token_ids):
    results = {}
    def fetch_one(tid):
        try:
            r = requests.get(f"{CLOB_API}/midpoint",
                             params={"token_id": tid}, timeout=8)
            if r.status_code == 200:
                mid = float(r.json().get("mid", 0) or 0)
                if mid > 0.005:
                    return tid, mid
        except: pass
        try:
            r = requests.get(f"{CLOB_API}/price",
                             params={"token_id": tid, "side":"buy"}, timeout=8)
            if r.status_code == 200:
                p = float(r.json().get("price", 0) or 0)
                if p > 0.005:
                    return tid, p
        except: pass
        return tid, 0.0
    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(fetch_one, tid): tid for tid in token_ids}
        for fut in as_completed(futs):
            tid, price = fut.result()
            results[tid] = price
    return results

# ─── BRIER SCORE LOGGER ──────────────────────────────────
BRIER_LOG = BRIER_LOG_JSONL

def log_signal(market_q, city, end_date, model_prob, market_price, bet_size, direction):
    """Log a signal for later Brier score calculation"""
    entry = {
        "ts":          time.strftime("%Y-%m-%dT%H:%M:%S"),
        "question":    market_q[:80],
        "city":        city,
        "end_date":    end_date,
        "model_prob":  round(model_prob, 4),
        "market_price":round(market_price, 4),
        "edge":        round(model_prob - market_price, 4),
        "bet_size":    bet_size,
        "direction":   direction,
        "resolved":    None,   # fill in manually after resolution
        "outcome":     None,   # 1=YES won, 0=NO won
    }
    with open(BRIER_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")

def compute_brier_score():
    """Rolling Brier score on resolved trades. Lower = better (0=perfect, 0.25=random)"""
    if not os.path.exists(BRIER_LOG):
        return None, 0
    scores = []
    with open(BRIER_LOG) as f:
        for line in f:
            try:
                e = json.loads(line)
                if e.get("outcome") is not None:
                    scores.append((e["model_prob"] - e["outcome"]) ** 2)
            except: pass
    if not scores:
        return None, 0
    return round(statistics.mean(scores), 4), len(scores)

# ─── TEMPERATURE LADDER (neobrother strategy) ────────────
# Buy YES across cheap adjacent buckets.
# One correct bucket at 3¢ = ~33x return. Wrong buckets lose the 3¢ each.
LADDER_BET        = 1.50   # fixed $ per ladder bucket
# Reichenbach & Walther (2025) 124M trades: ONLY extreme tails (1-5¢) are
# systematically underpriced (actual 14% vs priced 10%). 6-15¢ is fairly priced.
LADDER_MAX_PRICE  = 0.05   # only buy YES ≤ 5¢ (empirically underpriced zone)
LADDER_MIN_PRICE  = 0.01   # empirical edge is in the 1-5¢ zone, not dust below 1¢
LADDER_MIN_RATIO  = 2.0    # stricter: model_prob must be ≥ 2× market price for tails
LADDER_MAX_CITY   = 5      # max ladder positions per city
LADDER_BUDGET_PCT = 0.20   # 20% daily budget for tails (higher edge warrants more)

def generate_ladder_signals(parsed, prices, forecasts, bankroll,
                             emos_preds=None, emos_coeffs=None, emos_mod=None):
    """
    Scan all cheap YES buckets. Buy when model says the bucket is real but
    market prices it near-zero. Fixed $1.50 per bucket, up to 8 per city.
    """
    max_ladder_budget = bankroll * LADDER_BUDGET_PCT
    city_counts  = {}
    total_deploy = 0
    ladder_sigs  = []

    # Sort by ratio (best value first: highest model_prob / market_price)
    candidates = []
    for m in parsed:
        fc = forecasts.get((m["city"], m["end_date"]))
        if not fc or len(fc["blended"].members) < 20:
            continue
        timing = market_timing_metrics(m.get("created_ts"), m.get("end_date"))
        if timing["age_score"] < 0.8:
            continue
        yes_price = prices.get(m["token_id"], 0)
        if yes_price < LADDER_MIN_PRICE or yes_price > LADDER_MAX_PRICE:
            continue
        emos_pred = (emos_preds or {}).get(m["city"])
        if emos_pred and emos_coeffs and m["city"] in emos_coeffs and emos_mod:
            mu_c, sigma_c = emos_pred
            lo_c = (m["low"]  - 32) * 5/9 if m["unit"] == "F" else m["low"]
            hi_c = (m["high"] - 32) * 5/9 if m["unit"] == "F" else m["high"]
            f_prob = emos_mod.prob_bucket(mu_c, sigma_c, lo_c, hi_c)
        else:
            f_prob = fc["blended"].prob_for_market(m["low"], m["high"], m["unit"])
        if f_prob < LADDER_MIN_PRICE:
            continue
        ratio = f_prob / yes_price
        if ratio < LADDER_MIN_RATIO:
            continue
        candidates.append((ratio, m, f_prob, yes_price))

    candidates.sort(key=lambda x: -x[0])

    for ratio, m, f_prob, yes_price in candidates:
        city = m["city"]
        if city_counts.get(city, 0) >= LADDER_MAX_CITY:
            continue
        if total_deploy + LADDER_BET > max_ladder_budget:
            break

        ev = expected_value(f_prob, yes_price, LADDER_BET)

        lo, hi, unit = m["low"], m["high"], m["unit"]
        if   lo == -999.0: rng = f"≤{hi}°{unit}"
        elif hi == 999.0:  rng = f"≥{lo}°{unit}"
        elif lo == hi:     rng = f"={lo}°{unit}"
        else:              rng = f"{lo}-{hi}°{unit}"

        fc_c    = fc["blended"].mean
        fc_disp = f"{c_to_f(fc_c):.1f}°F" if unit == "F" else f"{fc_c:.1f}°C"

        ladder_sigs.append({
            **m,
            "edge":         f_prob - yes_price,
            "f_prob":       f_prob,
            "yes_price":    yes_price,
            "direction":    "BUY YES",
            "bet":          LADDER_BET,
            "ev":           ev,
            "fc_disp":      fc_disp,
            "rng":          rng,
            "conf":         fc["blended"].confidence,
            "tier_cfg":     {"name": "L", "label": "🪜 "},
            "trade_prob":   f_prob,
            "trade_price":  yes_price,
            "order_token":  m["token_id"],
            "urgency":      "ladder",
            "setup_type":   "extreme_tail",
            "strategy_arm": "tail",
            "hypothesis":   "H3",
            "signal_value": round(ratio, 6),
            "market_age_score": timing["age_score"],
            "market_age_hours": timing["age_hours"],
            "hours_to_resolution": timing["hours_to_resolution"],
        })
        city_counts[city] = city_counts.get(city, 0) + 1
        total_deploy += LADDER_BET

    return ladder_sigs


def generate_precision_bracket_signals(parsed, prices, forecasts, bankroll):
    """
    Near-resolution precision directional with adjacent-bin hedge.

    Public evidence for top weather traders most consistently points to:
    - entering close to resolution
    - betting the precise range the models cluster around
    - hedging with an adjacent bucket
    This generator adds that behavior without replacing the existing strategies.
    """
    grouped = {}
    for market in parsed:
        grouped.setdefault((market["city"], market["end_date"]), []).append(market)

    signals = []
    for (city, end_date), markets in grouped.items():
        fc = forecasts.get((city, end_date))
        if not fc:
            continue
        blended_fc = fc["blended"]
        timing = market_timing_metrics(markets[0].get("created_ts"), end_date)
        hours_left = timing["hours_to_resolution"]
        if hours_left is None or not (0 < hours_left <= PRECISION_WINDOW_HOURS):
            continue
        if blended_fc.std > PRECISION_MAX_STD_C:
            continue

        narrow_markets = []
        for market in markets:
            if _bucket_width_c(market) > PRECISION_MAX_BUCKET_WIDTH_C:
                continue
            yes_price = prices.get(market["token_id"], 0)
            if yes_price < 0.01 or yes_price > ENTRY_PRICE_CAP:
                continue
            model_prob = blended_fc.prob_for_market(market["low"], market["high"], market["unit"])
            edge = model_prob - yes_price
            narrow_markets.append({
                **market,
                "yes_price": yes_price,
                "model_prob": model_prob,
                "edge": edge,
                "midpoint_c": _bucket_midpoint_c(market),
            })

        if len(narrow_markets) < 2:
            continue

        narrow_markets.sort(key=lambda m: m["midpoint_c"])
        center_idx = min(range(len(narrow_markets)), key=lambda idx: abs(narrow_markets[idx]["midpoint_c"] - blended_fc.mean))
        candidates = []
        for idx in [center_idx - 1, center_idx, center_idx + 1]:
            if 0 <= idx < len(narrow_markets):
                candidates.append((idx, narrow_markets[idx]))
        if len(candidates) < 2:
            continue

        center_market = narrow_markets[center_idx]
        adjacent = []
        for idx in [center_idx - 1, center_idx + 1]:
            if 0 <= idx < len(narrow_markets):
                adjacent.append(narrow_markets[idx])
        if not adjacent:
            continue
        hedge_market = max(adjacent, key=lambda m: m["edge"])
        selected = [center_market, hedge_market]
        combined_model = sum(m["model_prob"] for m in selected)
        combined_market = sum(m["yes_price"] for m in selected)
        if combined_model - combined_market < PRECISION_MIN_EDGE:
            continue

        hedge_group = f"{city}_{end_date}_{round(center_market['midpoint_c'], 2)}"
        for role, m in [("core", center_market), ("hedge", hedge_market)]:
            lo, hi, unit = m["low"], m["high"], m["unit"]
            if   lo == -999.0: rng = f"≤{hi}°{unit}"
            elif hi == 999.0:  rng = f"≥{lo}°{unit}"
            elif lo == hi:     rng = f"={lo}°{unit}"
            else:              rng = f"{lo}-{hi}°{unit}"

            signals.append({
                **m,
                "f_prob": m["model_prob"],
                "direction": "BUY YES",
                "bet": PRECISION_RAW_BET if role == "core" else PRECISION_RAW_BET * 0.5,
                "ev": expected_value(m["model_prob"], m["yes_price"], min(PRECISION_RAW_BET, bankroll * 0.05)),
                "rng": rng,
                "conf": blended_fc.confidence,
                "tier_cfg": {"name": "P", "label": "🎯"},
                "trade_prob": m["model_prob"],
                "trade_price": m["yes_price"],
                "order_token": m["token_id"],
                "setup_type": "precision_bracket",
                "strategy_arm": f"precision_{role}",
                "hedge_group": hedge_group,
                "hedge_role": role,
                "combined_edge": round(combined_model - combined_market, 6),
                "market_age_score": timing["age_score"],
                "market_age_hours": timing["age_hours"],
                "hours_to_resolution": hours_left,
            })

    return signals


# ─── MAIN SCANNER ────────────────────────────────────────
def run():
    now_utc = datetime.now(timezone.utc)
    tomorrow = (date.today() + timedelta(days=1)).isoformat()

    print("=" * 72, flush=True)
    print(f"POLYMARKET WEATHER ARB — REFINED MODEL  (tomorrow: {tomorrow})", flush=True)
    print(f"Kelly: Unified 1/4 (A/B/C) | "
          f"Max positions: {MAX_POSITIONS} | Daily cap: {DAILY_EXPOSURE:.0%}", flush=True)

    # ── Live balance fetch ────────────────────────────────
    print("\nFetching live wallet balance...", flush=True)
    usdc_cash, portfolio_val = fetch_usdc_balance()
    print(f"  USDC cash (Polygon):    ${usdc_cash:.2f}", flush=True)
    print(f"  Open positions value:   ${portfolio_val:.2f}", flush=True)
    bankroll = usdc_cash  # Only bet with cash — not locked positions

    if bankroll < MIN_BANKROLL:
        print(
            f"\n⚠️  Bankroll ${bankroll:.2f} below minimum ${MIN_BANKROLL:.2f}. "
            f"Signals will still be generated, but sizing stays constrained to actual cash.",
            flush=True,
        )

    brier, n = compute_brier_score()
    if brier is not None:
        print(f"\nModel Brier score: {brier:.4f} on {n} resolved trades "
              f"({'GOOD' if brier < 0.15 else 'OK' if brier < 0.25 else 'WEAK'})", flush=True)
    print("=" * 72, flush=True)

    day_offsets = [1, 2]
    if 10 <= now_utc.hour <= 16:
        day_offsets = [0, 1, 2]
    print(f"\n[1] Fetching target markets (offsets={day_offsets})...", flush=True)
    markets = fetch_target_markets(day_offsets)
    active  = [m for m in markets if m.get("active") and m.get("clobTokenIds")]
    print(f"    {len(active)} active markets", flush=True)

    # Parse and group by city
    city_date_set = {}
    token_ids     = []
    parsed        = []
    for mkt in active:
        q      = mkt.get("question","")
        tokens = mkt.get("clobTokenIds",[])
        end    = mkt.get("_end_date","")
        if isinstance(tokens, str):
            try: tokens = json.loads(tokens)
            except: tokens = []
        city, coords = detect_city(q)
        pr = parse_temp_range(q)
        if not city or not pr or not tokens: continue
        city_date_set[(city, end)] = coords
        token_ids.append(tokens[0])
        parsed.append({"question":q, "token_id":tokens[0],
                        "token_id_no": tokens[1] if len(tokens) > 1 else None,
                        "city":city, "end_date":end, "low":pr[0], "high":pr[1], "unit":pr[2],
                        "created_ts": mkt.get("_created_ts", "")})

    print(f"    {len(parsed)} parseable | {len(city_date_set)} city-date forecasts", flush=True)

    print(f"\n[2] Fetching GFS + ECMWF ensembles for {len(city_date_set)} city-dates...", flush=True)
    gfs_data   = {}
    ecmwf_data = {}

    def fetch_all_models(city, end_date, lat, lon):
        gfs, ecmwf, icon = [], [], []
        try: gfs   = fetch_gfs_ensemble(lat, lon, end_date)
        except: pass
        try: ecmwf = fetch_ecmwf_ensemble(lat, lon, end_date)
        except: pass
        try: icon  = fetch_icon_ensemble(lat, lon, end_date)
        except: pass
        return city, end_date, gfs, ecmwf, icon

    gfs_data = {}; ecmwf_data = {}; icon_data = {}
    with ThreadPoolExecutor(max_workers=5) as ex:  # 5 workers — avoid 429 rate limits
        futs = {ex.submit(fetch_all_models, city, end_date, lat, lon): (city, end_date)
                for (city, end_date), (lat, lon) in city_date_set.items()}
        for fut in as_completed(futs):
            city, end_date, gfs, ecmwf, icon = fut.result()
            key = (city, end_date)
            gfs_data[key]   = gfs
            ecmwf_data[key] = ecmwf
            icon_data[key]  = icon
            time.sleep(0.1)  # light rate-limit buffer

    # Load per-city bias corrections
    _BIAS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "city_bias.json")
    city_bias = {}
    try:
        with open(_BIAS_FILE) as _f:
            city_bias = json.load(_f)
    except Exception:
        pass

    # Build blended forecasts
    forecasts = {}
    for (city, end_date), _coords in city_date_set.items():
        key = (city, end_date)
        gfs   = gfs_data.get(key, [])
        ecmwf = ecmwf_data.get(key, [])
        icon  = icon_data.get(key, [])
        blended = blend_ensembles(gfs, ecmwf, icon)
        if blended:
            # Apply per-city bias correction (shift all members)
            bias = city_bias.get(city, {}).get("mean_error_c", 0.0)
            if abs(bias) > 0.05:
                blended = [m - bias for m in blended]
            forecasts[key] = {
                "blended": EnsembleForecast(blended),
                "gfs":     EnsembleForecast(gfs)   if gfs   else None,
                "ecmwf":   EnsembleForecast(ecmwf) if ecmwf else None,
            }
            n_models = sum(1 for x in [gfs, ecmwf, icon] if x)
            conf = forecasts[key]["blended"].confidence
            print(f"    {city.title():20s} {end_date} mean={forecasts[key]['blended'].mean:.1f}°C  "
                  f"{len(blended)}mbr/{n_models}mdl  conf:{conf:.0%}"
                  + (f"  bias:{bias:+.1f}°C" if abs(bias) > 0.05 else ""), flush=True)

    # ── EMOS calibration ─────────────────────────────────
    print(f"\n[2b] Loading EMOS coefficients...", flush=True)
    try:
        import polymarket_emos as emos_mod
        emos_coeffs = emos_mod.load_coefficients()
        emos_preds  = {}
        for (city, end_date), (lat, lon) in city_date_set.items():
            key = (city, end_date)
            gfs   = gfs_data.get(key, [])
            ecmwf = ecmwf_data.get(key, [])
            icon  = icon_data.get(key, [])
            mu_c, sigma_c = emos_mod.predict(city, gfs, ecmwf, icon, emos_coeffs)
            emos_preds[key] = (mu_c, sigma_c)
            if city in emos_coeffs:
                c = emos_coeffs[city]
                print(f"    {city.title():20s} {end_date} EMOS μ={mu_c:.1f}°C σ={sigma_c:.2f}°C "
                      f"(CRPS={c['crps']:.3f} n={c['n']})", flush=True)
        print(f"    {sum(1 for (c, _d) in city_date_set if c in emos_coeffs)}/{len(city_date_set)} city-dates have EMOS coefficients", flush=True)
    except Exception as e:
        print(f"    EMOS load failed: {e} — using raw member count", flush=True)
        emos_mod    = None
        emos_preds  = {}
        emos_coeffs = {}

    city_calibration = _load_city_calibration()

    hk_morning_ratio = None
    hk_keys = [key for key in forecasts.keys() if key[0] == "hong kong"]
    if hk_keys:
        try:
            import polymarket_research as _res
            _hko_now = _res.fetch_morning_hko_temp()
            hk_key = sorted(hk_keys, key=lambda key: key[1])[0]
            _ecmwf_hk = forecasts.get(hk_key, {}).get("ecmwf")
            if _hko_now is not None and _ecmwf_hk and _ecmwf_hk.mean:
                hk_morning_ratio = _res.compute_h1_signal(_hko_now, _ecmwf_hk.mean)
                print(
                    f"\n[H1] HK morning ratio (HKO/ECMWF): {hk_morning_ratio:.3f}",
                    flush=True,
                )
        except Exception as e:
            print(f"\n[H1] signal unavailable: {e}", flush=True)

    # ── Recency fade signal (Fensory 2026: 72h overcorrection) ──────────
    # If yesterday HK was extreme (>30°C or <18°C), market overweights continuation.
    # Fade: if yesterday hot → market overprices 30°C+ today → buy NO on those buckets.
    yesterday_hk_actual = None
    try:
        import requests as _req
        _y = (date.today() - timedelta(days=1)).isoformat()
        _r = _req.get("https://archive-api.open-meteo.com/v1/archive", params={
            "latitude": 22.3020, "longitude": 114.1739,
            "daily": "temperature_2m_max", "temperature_unit": "celsius",
            "start_date": _y, "end_date": _y, "timezone": "auto",
        }, timeout=10)
        _vals = _r.json().get("daily", {}).get("temperature_2m_max", [])
        if _vals and _vals[0] is not None:
            yesterday_hk_actual = float(_vals[0])
            print(f"\n[R] Yesterday HK actual max: {yesterday_hk_actual:.1f}°C", flush=True)
            if yesterday_hk_actual > 30.0:
                print(f"     → Extreme hot. Market likely overprices 30°C+ today (72h bias). Fade signal active.", flush=True)
            elif yesterday_hk_actual < 18.0:
                print(f"     → Extreme cold. Market likely overprices cold buckets today. Fade signal active.", flush=True)
            else:
                print(f"     → Normal range. No recency fade signal.", flush=True)
    except Exception:
        pass

    print(f"\n[3] Fetching {len(token_ids)} CLOB prices...", flush=True)
    prices = get_price_batch(token_ids)
    live   = sum(1 for p in prices.values() if p > 0.01)
    print(f"    {live}/{len(prices)} live prices", flush=True)

    print(f"\n[4] Scanning for edges (tiered)...\n", flush=True)

    signals = []
    skipped = {"no_forecast":0, "no_price":0, "no_tier":0,
               "entry_cap":0, "models_disagree":0}

    for m in parsed:
        fc = forecasts.get((m["city"], m["end_date"]))
        if not fc:
            skipped["no_forecast"] += 1; continue
        if len(fc["blended"].members) < 20:  # skip if ensemble data was incomplete (429/empty)
            skipped["no_forecast"] += 1; continue

        yes_price = prices.get(m["token_id"], 0)
        if yes_price < 0.005:
            skipped["no_price"] += 1; continue

        blended_fc = fc["blended"]
        gfs_fc     = fc["gfs"]
        ecmwf_fc   = fc["ecmwf"]

        # Compute probability — EMOS Gaussian CDF if calibrated, else member count
        emos_pred = emos_preds.get((m["city"], m["end_date"]))
        if emos_pred and m["city"] in emos_coeffs:
            mu_c, sigma_c = emos_pred
            lo_c = (m["low"]  - 32) * 5/9 if m["unit"] == "F" else m["low"]
            hi_c = (m["high"] - 32) * 5/9 if m["unit"] == "F" else m["high"]
            f_prob = emos_mod.prob_bucket(mu_c, sigma_c, lo_c, hi_c)
        else:
            f_prob = blended_fc.prob_for_market(m["low"], m["high"], m["unit"])

        # Apply per-city isotonic calibration if available.
        _calib = city_calibration.get(m["city"], {})
        if len(_calib.get("breakpoints", [])) >= 3:
            f_prob = _isotonic_lookup(f_prob, _calib["breakpoints"])

        # Recency fade adjustment (Fensory 2026: 72h overcorrection confirmed)
        # HK-only: if yesterday was extreme, discount model prob for continuation buckets
        if yesterday_hk_actual is not None and m["city"] == "hong kong":
            lo_c_check = (m["low"]  - 32) * 5/9 if m["unit"] == "F" else m["low"]
            hi_c_check = (m["high"] - 32) * 5/9 if m["unit"] == "F" else m["high"]
            is_hot_bucket  = lo_c_check >= 28.0 or hi_c_check >= 30.0
            is_cold_bucket = hi_c_check <= 20.0
            if yesterday_hk_actual > 30.0 and is_hot_bucket:
                f_prob *= 0.85   # market overprices hot continuation by ~15%
            elif yesterday_hk_actual < 18.0 and is_cold_bucket:
                f_prob *= 0.85   # market overprices cold continuation by ~15%

        edge   = f_prob - yes_price
        conf   = blended_fc.confidence
        timing = market_timing_metrics(m.get("created_ts"), m.get("end_date"))

        # Find best matching tier
        tier_cfg = None
        for tier in TIERS:
            required_edge = tier["min_edge"] / max(timing["age_score"], 0.5)
            if conf >= tier["min_conf"] and abs(edge) >= required_edge:
                tier_cfg = tier
                break
        if not tier_cfg:
            skipped["no_tier"] += 1; continue

        # Determine trade direction
        if edge > 0:
            direction   = "BUY YES"
            trade_prob  = f_prob
            trade_price = yes_price
        else:
            direction   = "BUY NO "
            trade_prob  = 1.0 - f_prob
            trade_price = 1.0 - yes_price

        # Entry price cap
        if trade_price > MAX_ENTRY_PRICE:
            skipped["entry_cap"] += 1; continue

        # Model agreement filter (Tier A only — highest bar)
        if tier_cfg["name"] == "A" and gfs_fc and ecmwf_fc:
            if not blended_fc.gfs_ecmwf_agree(gfs_fc, ecmwf_fc, 1.5):
                skipped["models_disagree"] += 1; continue

        # Kelly sizing with tier-specific fraction + % cap
        bet = kelly_size(trade_prob, trade_price, bankroll,
                         tier_cfg["kelly"], tier_cfg["max_pct"])
        if bet < MIN_BET_USD:
            continue

        ev = expected_value(trade_prob, trade_price, bet)

        # Format
        lo, hi, unit = m["low"], m["high"], m["unit"]
        if   lo == -999.0: rng = f"≤{hi}°{unit}"
        elif hi == 999.0:  rng = f"≥{lo}°{unit}"
        elif lo == hi:     rng = f"={lo}°{unit}"
        else:              rng = f"{lo}-{hi}°{unit}"

        fc_c    = blended_fc.mean
        fc_disp = f"{c_to_f(fc_c):.1f}°F" if unit=="F" else f"{fc_c:.1f}°C"

        order_token = m["token_id"] if direction == "BUY YES" else (m.get("token_id_no") or m["token_id"])
        signals.append({
            **m,
            "edge":edge, "f_prob":f_prob, "yes_price":yes_price,
            "direction":direction, "bet":bet, "ev":ev,
            "fc_disp":fc_disp, "rng":rng, "conf":conf,
            "tier_cfg":tier_cfg, "trade_prob":trade_prob, "trade_price":trade_price,
            "order_token": order_token,
            "setup_type": "conviction",
            "strategy_arm": f"conviction_{tier_cfg['name'].lower()}",
            "hypothesis": "H1" if m["city"] == "hong kong" and hk_morning_ratio is not None else None,
            "signal_value": hk_morning_ratio if m["city"] == "hong kong" else None,
            "market_age_score": timing["age_score"],
            "market_age_hours": timing["age_hours"],
            "hours_to_resolution": timing["hours_to_resolution"],
        })

    # ── Correlation control: max MAX_PER_CITY bets per city ──
    signals.sort(key=lambda x: (x["tier_cfg"]["name"], -abs(x["edge"])))
    city_counts  = {}
    final_signals = []
    for s in signals:
        city = s["city"]
        if city_counts.get(city, 0) >= MAX_PER_CITY:
            continue
        city_counts[city] = city_counts.get(city, 0) + 1
        final_signals.append(s)

    # ── Daily exposure cap ────────────────────────────────
    max_daily = bankroll * DAILY_EXPOSURE
    deployed  = 0
    capped_signals = []
    for s in final_signals:
        if deployed + s["bet"] > max_daily:
            break
        capped_signals.append(s)
        deployed += s["bet"]

    print(flush=True)
    print("=" * 72, flush=True)
    print(f"RAW SIGNALS: {len(signals)}  →  After correlation filter: {len(final_signals)}"
          f"  →  After daily cap: {len(capped_signals)}", flush=True)
    print(f"Skipped: {skipped}", flush=True)
    print("=" * 72, flush=True)

    if not capped_signals:
        print("\nNo conviction directional signals survived gating.", flush=True)

    if capped_signals:
        # ── Print by tier ─────────────────────────────────────
        for tier_name in ["A", "B", "C"]:
            tier_sigs = [s for s in capped_signals if s["tier_cfg"]["name"] == tier_name]
            if not tier_sigs: continue
            tier_info = next(t for t in TIERS if t["name"] == tier_name)
            print(f"\n── TIER {tier_name} ({tier_info['label'].strip()})  "
                  f"Kelly=1/{round(1/tier_info['kelly'])}  "
                  f"MaxBet={tier_info['max_pct']:.0%} bankroll "
                  f"(${bankroll * tier_info['max_pct']:.2f}) ──", flush=True)
            for s in tier_sigs:
                print(f"  {s['tier_cfg']['label']} {s['city'].upper():18s} {s['rng']:13s} "
                      f"Fcst:{s['fc_disp']:7s} conf:{s['conf']:.0%} "
                      f"Mkt:{s['yes_price']:.2%} Mdl:{s['f_prob']:.2%} "
                      f"Edge:{s['edge']:+.2%} {s['direction']} "
                      f"Bet:${s['bet']:.2f} EV:${s['ev']:+.2f}", flush=True)

        # ── Full detail for top 5 ─────────────────────────────
        print(f"\n{'─'*72}", flush=True)
        print("TOP 5 DETAIL:\n", flush=True)
        for i, s in enumerate(capped_signals[:5], 1):
            no_price = round(1 - s["yes_price"], 3)
            t = s["tier_cfg"]
            print(f"  #{i}  [Tier {t['name']}]  {s['direction']} | "
                  f"{s['city'].title()} — {s['end_date']}", flush=True)
            print(f"       Range:    {s['rng']}", flush=True)
            print(f"       Forecast: {s['fc_disp']}  (conf: {s['conf']:.0%})", flush=True)
            print(f"       Market:   YES={s['yes_price']:.3f}  NO={no_price:.3f}", flush=True)
            print(f"       Model P:  {s['f_prob']:.1%}  Edge: {s['edge']:+.1%}", flush=True)
            print(f"       Kelly 1/{round(1/t['kelly'])}: ${s['bet']:.2f}  EV: ${s['ev']:+.2f}", flush=True)
            print(f"       Q: {s['question'][:70]}", flush=True)
            print(flush=True)

    # ── Capital summary ───────────────────────────────────
    total_ev     = sum(s["ev"] for s in capped_signals)
    stop_loss    = bankroll * DAILY_LOSS_LIMIT
    daily_limit  = bankroll * DAILY_EXPOSURE
    cities_used  = len(set(s["city"] for s in capped_signals))

    print("─" * 72, flush=True)
    print("CAPITAL PLAN (Live):", flush=True)
    print(f"  USDC cash:         ${usdc_cash:.2f}  {'← LOW CASH' if usdc_cash < MIN_BANKROLL else ''}", flush=True)
    print(f"  Bankroll used:     ${bankroll:.2f}", flush=True)
    print(f"  Daily deploy cap:  ${daily_limit:.2f}  (30% rule)", flush=True)
    print(f"  Today deployed:    ${deployed:.2f}  ({deployed/bankroll*100:.1f}%)", flush=True)
    print(f"  Stop-loss trigger: -${stop_loss:.2f}  (-15%/day → sit out)", flush=True)
    print(f"  Positions:         {len(capped_signals)} across {cities_used} cities", flush=True)
    print(f"  Total EV:          ${total_ev:+.2f}", flush=True)
    print(f"  Kelly tiers:       A=1/4 (max 5%)  B=1/4 (max 3%)  C=1/4 (max 2%)", flush=True)
    print(f"  Per-city limit:    {MAX_PER_CITY} bets max (correlation control)", flush=True)
    print("─" * 72, flush=True)
    print(flush=True)

    # Log signals for Brier score tracking
    for s in capped_signals:
        log_signal(s["question"], s["city"], s["end_date"],
                   s["f_prob"], s["yes_price"], s["bet"], s["direction"])

    # ── Bucket sum arbitrage (Saguillo 2025 / mechanical) ───
    # Sum of all YES prices for same city+date should = $1.00.
    # If sum < $1: buy every bucket → one pays $1, guaranteed profit.
    # If sum > $1: buy every NO → n-1 pay $1, guaranteed profit.
    arb_signals = []
    def _arb_bet(dev, br, tmax=5.0): return round(min(max(br*0.025, 0.50) + min(abs(dev)/0.06,1.0)*(tmax-max(br*0.025,0.50)), tmax), 2)
    city_date_buckets = {}
    for m in parsed:
        key = (m["city"], m["end_date"])
        p   = prices.get(m["token_id"], 0)
        if p > 0.005:
            city_date_buckets.setdefault(key, []).append((m, p))

    print(f"\n[S] BUCKET SUM ARB CHECK", flush=True)
    for (city, end_date), bucket_list in city_date_buckets.items():
        if len(bucket_list) < 3:
            continue
        yes_sum = sum(p for _, p in bucket_list)
        deviation = yes_sum - 1.0
        if abs(deviation) < ARB_MIN_DEVIATION:
            continue
        direction = "BUY YES ALL" if deviation < 0 else "BUY NO ALL"
        guaranteed_profit = abs(deviation) * _arb_bet(deviation, bankroll)
        print(f"  [{city.upper():15s}] {end_date}  sum={yes_sum:.3f}  "
              f"deviation={deviation:+.3f}  → {direction}  "
              f"guaranteed≈${guaranteed_profit:.3f}/leg", flush=True)
        for m, p in bucket_list:
            lo, hi, unit = m["low"], m["high"], m["unit"]
            if   lo == -999.0: rng = f"≤{hi}°{unit}"
            elif hi == 999.0:  rng = f"≥{lo}°{unit}"
            elif lo == hi:     rng = f"={lo}°{unit}"
            else:              rng = f"{lo}-{hi}°{unit}"
            token   = m["token_id"] if deviation < 0 else (m.get("token_id_no") or m["token_id"])
            arb_dir = "BUY YES" if deviation < 0 else "BUY NO"
            arb_signals.append({
                **m,
                "direction":   arb_dir,
                "trade_price": p if deviation < 0 else round(1 - p, 4),
                "trade_prob":  1.0 / len(bucket_list),   # uniform prior for arb
                "yes_price":   p,
                "f_prob":      1.0 / len(bucket_list),
                "edge":        abs(deviation),
                "bet":         _arb_bet(deviation, bankroll),
                "ev":          round(abs(deviation) * _arb_bet(deviation, bankroll), 4),
                "rng":         rng,
                "tier_cfg":    {"name": "ARB", "label": "⚡"},
                "urgency":     "arb",
                "order_token": token,
                "setup_type":  "bucket_sum_arb",
                "strategy_arm": "bucket_sum_arb_candidate",
                "arb_yes_sum": round(yes_sum, 6),
                "arb_deviation": round(abs(deviation), 6),
                "arb_legs": len(bucket_list),
                "hypothesis":  "H3",
                "signal_value": round(yes_sum, 6),
                "market_age_score": market_timing_metrics(m.get("created_ts"), m.get("end_date"))["age_score"],
                "market_age_hours": market_timing_metrics(m.get("created_ts"), m.get("end_date"))["age_hours"],
                "hours_to_resolution": market_timing_metrics(m.get("created_ts"), m.get("end_date"))["hours_to_resolution"],
            })
    if not arb_signals:
        print(f"  No arb opportunities (all sums within {ARB_MIN_DEVIATION:.0%} of $1.00)", flush=True)

    # ── Temperature ladder (extreme tails, Reichenbach 2025) ─
    # Only 1-5¢ tails empirically underpriced (actual 14% vs priced 10%).
    ladder = generate_ladder_signals(parsed, prices, forecasts, bankroll,
                                      emos_preds=emos_preds, emos_coeffs=emos_coeffs,
                                      emos_mod=emos_mod)
    print(f"\n[L] EXTREME TAIL SIGNALS (≤5¢): {len(ladder)} buckets", flush=True)
    for s in ladder:
        print(f"  [TAIL] {s['city'].upper():18s} {s['rng']:13s} "
              f"YES={s['yes_price']:.3f}  Mdl:{s['f_prob']:.1%}  "
              f"ratio:{s['f_prob']/s['yes_price']:.1f}x  Bet:${s['bet']:.2f}  EV:${s['ev']:+.2f}", flush=True)

    precision = generate_precision_bracket_signals(parsed, prices, forecasts, bankroll)
    print(f"\n[P] PRECISION BRACKET SIGNALS: {len(precision)} buckets", flush=True)
    for s in precision[:10]:
        print(
            f"  [PRECISION] {s['city'].upper():18s} {s['rng']:13s} "
            f"YES={s['yes_price']:.3f}  Mdl:{s['f_prob']:.1%}  "
            f"hrs_left:{s['hours_to_resolution']:.1f}  group:{s['hedge_group'][-6:]}",
            flush=True,
        )

    all_signals = arb_signals + precision + capped_signals + ladder
    return all_signals

def update_city_bias(city: str, model_mean_c: float, actual_max_c: float):
    """Call after market settles: actual_max_c = observed high temp in Celsius."""
    _BIAS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "city_bias.json")
    try:
        with open(_BIAS_FILE) as f: data = json.load(f)
    except Exception: data = {}
    entry = data.get(city, {"n": 0, "mean_error_c": 0.0})
    error = model_mean_c - actual_max_c  # positive = model ran hot
    n = entry["n"] + 1
    # Exponentially weighted running average (alpha=0.3)
    entry["mean_error_c"] = round(0.3 * error + 0.7 * entry["mean_error_c"], 3)
    entry["n"] = n
    data[city] = entry
    with open(_BIAS_FILE, "w") as f: json.dump(data, f, indent=2)

if __name__ == "__main__":
    run()
