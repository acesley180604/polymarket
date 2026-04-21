"""
Historical Backtest — HK Weather Markets
Uses 6 months of ERA5 actuals + hindcasts to simulate signal generation.
Reports: PnL, Brier score, calibration, Kelly sizing performance.

Run: python3 polymarket_backtest.py
"""

import argparse, os, json, time, math, random, statistics
import requests
from datetime import date, timedelta
from polymarket_core import CITY_COORDS, f_to_c, c_to_f
from polymarket_model import (
    get_tier as live_get_tier,
    kelly_size as live_kelly_size,
    expected_value as live_expected_value,
    MAX_PER_CITY as LIVE_MAX_PER_CITY,
    PRECISION_MAX_STD_C,
    PRECISION_MIN_EDGE,
    PRECISION_RAW_BET,
)
from polymarket_capital import TIERS as ACCOUNT_TIERS

# ─── CONFIG ───────────────────────────────────────────────

_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
COEFF_FILE    = os.path.join(_SCRIPT_DIR, "emos_coefficients.json")

# HK canonical temperature buckets in Fahrenheit (as Polymarket lists them).
# [low_f, high_f] — use -999 / 999 for open-ended buckets.
HK_BUCKETS_F = [
    (-999.0, 78.8),    # <26°C  (≤78.8°F)
    (78.8,   82.4),    # 26–28°C
    (82.4,   86.0),    # 28–30°C
    (86.0,   89.6),    # 30–32°C
    (89.6,  999.0),    # >32°C  (≥89.6°F)
]


# ─── CITY BUCKET FETCHING ─────────────────────────────────

def fetch_city_buckets_f(city: str) -> list:
    """
    Fetch real bucket structure for a city from Polymarket Gamma API.
    Returns list of (low_f, high_f) tuples representing the most recent
    complete daily market set for that city.
    Falls back to HK_BUCKETS_F only if API fails.
    """
    from polymarket_core import GAMMA_API, detect_city, parse_temp_range, c_to_f

    city_l = city.strip().lower()
    date_buckets = {}

    try:
        for offset in range(3300, 3600, 100):
            r = requests.get(
                f"{GAMMA_API}/events",
                params={"tag_slug": "weather", "active": "true", "limit": 100, "offset": offset},
                timeout=15,
            )
            events = r.json() if isinstance(r.json(), list) else []
            if not events:
                break
            for ev in events:
                end_day = ev.get("endDate", "")[:10]
                if not end_day:
                    continue
                for mkt in ev.get("markets", []):
                    q = mkt.get("question", "")
                    detected, _ = detect_city(q)
                    if detected != city_l:
                        continue
                    parsed = parse_temp_range(q)
                    if not parsed:
                        continue
                    low, high, unit = parsed
                    if unit == "C":
                        low_f  = c_to_f(low)  if low  > -999.0 else -999.0
                        high_f = c_to_f(high) if high < 999.0  else  999.0
                    else:
                        low_f, high_f = float(low), float(high)
                    low_f  = round(low_f, 1)
                    high_f = round(high_f, 1)
                    date_buckets.setdefault(end_day, set()).add((low_f, high_f))
    except Exception as e:
        print(f"  [buckets] Gamma fetch failed for {city}: {e}", flush=True)

    if not date_buckets:
        print(f"  [buckets] No markets found for {city}, using HK_BUCKETS_F fallback", flush=True)
        return list(HK_BUCKETS_F)

    # Pick the most recent date with the largest complete market set (>= 3 buckets)
    best_day, best_buckets = None, []
    for day in sorted(date_buckets.keys(), reverse=True):
        bkts = sorted(date_buckets[day])
        if len(bkts) >= 3:
            best_day, best_buckets = day, bkts
            break

    if not best_buckets:
        print(f"  [buckets] No complete set for {city}, using HK_BUCKETS_F fallback", flush=True)
        return list(HK_BUCKETS_F)

    print(f"  [buckets] {city.title()}: {len(best_buckets)} buckets from {best_day}", flush=True)
    return best_buckets


TAIL_MIN_PRICE       = 0.01    # match live extreme-tail zone
TAIL_MAX_PRICE       = 0.05
TAIL_RATIO_THRESH    = 2.0
ARB_MIN_DEVIATION    = 0.04    # match live arb candidate generation
ARB_LOOSE_MIN_DEVIATION = 0.08 # match live loose-arb promotion threshold
ARB_BET_PER_LEG      = 0.50
ENTRY_PRICE_CAP      = 0.72    # match live directional cap
STANDARD_MIN_EDGE    = 0.12    # live-ish proxy since historical market quotes are unavailable
RAW_SIGNAL_MIN_BET   = 0.50    # live model gate before autotrader cap

# ─── LIVE CONFIG FROM ENV ───────────────────────────────────
def _load_live_capital():
    """Read BANKROLL_OVERRIDE and TIER from polymarket.env to scale backtest realistically."""
    from polymarket_core import ENV as _env
    from polymarket_capital import TIERS as _CTIERS
    try:
        bankroll = float(_env.get("BANKROLL_OVERRIDE") or 0)
    except (TypeError, ValueError):
        bankroll = 0
    try:
        tier_num = int(_env.get("TIER") or 1)
    except (TypeError, ValueError):
        tier_num = 1
    if bankroll <= 0:
        bankroll = 2.0
        tier_num = 1
    tier_cfg = _CTIERS.get(tier_num, _CTIERS[1])
    return bankroll, tier_cfg["max_bet"], _CTIERS[tier_num]["max_daily_pct"]

_LIVE_BANKROLL, _LIVE_MAX_BET, _LIVE_MAX_DAILY_PCT = _load_live_capital()
INITIAL_BANKROLL  = _LIVE_BANKROLL
MAX_BET           = _LIVE_MAX_BET
MAX_DAILY_EXPOSURE = _LIVE_MAX_DAILY_PCT
RISK_FREE_RATE       = 0.0     # Sharpe calculation
DEFAULT_MONTE_CARLO_ITERATIONS = 5000
DEFAULT_MONTE_CARLO_SEED       = 42
DEFAULT_MONTE_CARLO_MODE       = "block"
DEFAULT_MONTE_CARLO_BLOCK_DAYS = 5
DEFAULT_EXECUTION_SLIPPAGE_BPS = 15.0
DEFAULT_EXECUTION_FEE_BPS      = 0.0
DEFAULT_FILL_RATE              = 0.97
DEFAULT_LOOSE_ARB_FILL_RATE    = 0.82
DEFAULT_STALE_FILL_RATE        = 0.0
DEFAULT_ABLATION_BOTTOM_N      = 5


# ─── 1. FETCH BACKTEST DATA ───────────────────────────────

def fetch_backtest_data(city: str, lat: float, lon: float,
                        start_date: str, end_date: str) -> list:
    """
    Fetch ERA5 actuals + GFS/ECMWF/ICON hindcasts for [start_date, end_date].
    Returns list of day_records sorted by date:
      {date, gfs_max, ecmwf_max, icon_max, actual_max, day_of_year}
    """
    # ── ERA5 actuals ─────────────────────────────────────
    actuals_by_date = {}
    try:
        r = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude":         lat,
                "longitude":        lon,
                "daily":            "temperature_2m_max",
                "temperature_unit": "celsius",
                "start_date":       start_date,
                "end_date":         end_date,
                "timezone":         "auto",
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json().get("daily", {})
        dates = data.get("time", [])
        vals  = data.get("temperature_2m_max", [])
        for d, v in zip(dates, vals):
            if v is not None:
                actuals_by_date[d] = float(v)
        print(f"  [data] ERA5 actuals: {len(actuals_by_date)} days", flush=True)
    except Exception as e:
        print(f"  [data] ERA5 fetch failed: {e}", flush=True)

    if not actuals_by_date:
        return []

    # ── Hindcasts (GFS / ECMWF / ICON) ──────────────────
    hindcasts = {}
    for model_key, api_model in [
        ("gfs_max",   "gfs_seamless"),
        ("ecmwf_max", "ecmwf_ifs025"),
        ("icon_max",  "icon_seamless"),
    ]:
        hindcasts[model_key] = {}
        try:
            r = requests.get(
                "https://historical-forecast-api.open-meteo.com/v1/forecast",
                params={
                    "latitude":         lat,
                    "longitude":        lon,
                    "daily":            "temperature_2m_max",
                    "temperature_unit": "celsius",
                    "models":           api_model,
                    "start_date":       start_date,
                    "end_date":         end_date,
                    "timezone":         "auto",
                },
                timeout=30,
            )
            r.raise_for_status()
            data  = r.json().get("daily", {})
            dates = data.get("time", [])
            vals  = data.get("temperature_2m_max", [])
            for d, v in zip(dates, vals):
                if v is not None:
                    hindcasts[model_key][d] = float(v)
            print(f"  [data] {api_model}: {len(hindcasts[model_key])} days", flush=True)
        except Exception as e:
            print(f"  [data] {api_model} hindcast failed: {e}", flush=True)
        time.sleep(0.4)

    # ── Assemble records ──────────────────────────────────
    records = []
    for day_str in sorted(actuals_by_date.keys()):
        actual = actuals_by_date[day_str]
        try:
            d = date.fromisoformat(day_str)
        except ValueError:
            continue
        records.append({
            "date":        day_str,
            "gfs_max":     hindcasts["gfs_max"].get(day_str),
            "ecmwf_max":   hindcasts["ecmwf_max"].get(day_str),
            "icon_max":    hindcasts["icon_max"].get(day_str),
            "actual_max":  actual,
            "day_of_year": d.timetuple().tm_yday,
        })

    return records


# ─── 2. SIMULATE A SINGLE MARKET ─────────────────────────

def _bucket_outcome(actual_c: float, low_f: float, high_f: float) -> bool:
    low_c  = f_to_c(low_f)  if low_f  > -999.0 else -999.0
    high_c = f_to_c(high_f) if high_f <  999.0  else  999.0
    # Exact-degree market: round actual to nearest 0.5°C
    if abs(low_c - high_c) < 0.05 and low_c > -999.0 and high_c < 999.0:
        low_c  = low_c  - 0.5
        high_c = high_c + 0.5
    if low_c <= -999.0:
        return actual_c <= high_c
    if high_c >= 999.0:
        return actual_c >= low_c
    return low_c <= actual_c <= high_c


def simulate_market(actual_c: float, low_f: float, high_f: float,
                    model_prob: float, trade_price: float, bet_size: float,
                    direction: str) -> dict:
    """
    Simulate a BUY YES / BUY NO trade for the given F bucket.
    Returns {outcome, pnl, brier, price_paid, bet_size}.
    """
    yes_outcome = _bucket_outcome(actual_c, low_f, high_f)
    is_buy_no = "NO" in direction.upper()

    # PnL uses the actual proxy fill price.
    price_paid = max(0.01, min(0.99, trade_price))
    if is_buy_no:
        outcome = not yes_outcome
    else:
        outcome = yes_outcome

    if outcome:
        pnl = (1.0 - price_paid) * bet_size
    else:
        pnl = -price_paid * bet_size

    actual_yes_int = 1 if yes_outcome else 0
    brier_prob = (1.0 - model_prob) if is_buy_no else model_prob
    brier      = round((brier_prob - actual_yes_int) ** 2, 6)

    return {
        "outcome":    outcome,
        "outcome_raw": yes_outcome,
        "pnl":        round(pnl, 4),
        "brier":      brier,
        "price_paid": round(price_paid, 4),
        "bet_size":   round(bet_size, 4),
    }


# ─── 3. RUN BACKTEST ─────────────────────────────────────

def _kelly_bet(p: float, price: float, bankroll: float) -> float:
    """
    Kelly fraction: f = (p*(1-price) - (1-p)*price) / (1-price)
    Applied to current bankroll. Capped at MAX_BET.
    """
    if price <= 0 or price >= 1:
        return 0.0
    b = (1.0 - price) / price   # net odds
    f = (p * b - (1.0 - p)) / b
    if f <= 0:
        return 0.0
    raw_bet = bankroll * f * 0.5  # half-Kelly to better match live sizing
    return min(raw_bet, MAX_BET)


def _bucket_probability(mu: float, sigma: float, low_f: float, high_f: float,
                        emos_mod=None) -> float:
    low_c  = f_to_c(low_f)  if low_f  > -999.0 else -999.0
    high_c = f_to_c(high_f) if high_f <  999.0  else  999.0
    # Exact-degree market: "be exactly X°C" → expand to ±0.5°C window
    if abs(low_c - high_c) < 0.05 and low_c > -999.0 and high_c < 999.0:
        low_c  = low_c  - 0.5
        high_c = high_c + 0.5
    if emos_mod is not None:
        return emos_mod.prob_bucket(mu, sigma, low_c, high_c)

    from math import erf, sqrt

    def _norm_cdf(x, mu, sigma):
        return 0.5 * (1 + erf((x - mu) / (sigma * sqrt(2))))

    if low_c <= -999.0:
        p = _norm_cdf(high_c, mu, sigma)
    elif high_c >= 999.0:
        p = 1.0 - _norm_cdf(low_c, mu, sigma)
    else:
        p = _norm_cdf(high_c, mu, sigma) - _norm_cdf(low_c, mu, sigma)
    return max(0.01, min(0.99, p))


def _proxy_bucket_prices(raw_mean: float, raw_std: float, buckets: list[tuple[float, float]]) -> list[float]:
    """
    Proxy a full daily market surface across all buckets together.

    Key design goals:
    - avoid collapsing every concentrated day into 0.99 + 0.01 + ...
    - allow realistic underround / overround behavior at the basket level
    - remain deterministic and strictly pre-outcome

    We do this in two steps:
    1. use a broader "market sigma" than the raw ensemble spread so the market
       stays fatter-tailed than the sharp model;
    2. apply a power distortion to emulate favorite/longshot bias.
       alpha > 1 compresses the surface into underround (tails underpriced),
       alpha < 1 expands the surface into overround.
    """
    market_sigma = max(raw_std * 2.0, 1.6)
    base_probs = [
        _bucket_probability(raw_mean, market_sigma, low_f, high_f, emos_mod=None)
        for low_f, high_f in buckets
    ]

    uncertainty = max(0.0, min(1.0, (market_sigma - 1.6) / 2.4))
    alpha = 1.18 - 0.35 * uncertainty

    prices = []
    for prob in base_probs:
        distorted = prob ** alpha
        if distorted <= 0.02:
            distorted *= 0.85
        prices.append(round(max(0.001, min(0.999, distorted)), 4))

    return prices


def _confidence_proxy(model_vals, model_prob):
    directional = max(model_prob, 1.0 - model_prob)
    if len(model_vals) <= 1:
        return round(max(0.5, min(0.95, directional)), 4)
    spread = statistics.stdev(model_vals)
    agreement = max(0.5, min(0.95, 1.0 - spread / 6.0))
    conf = 0.6 * directional + 0.4 * agreement
    return round(max(0.5, min(0.95, conf)), 4)


def _passes_min_share_rule(trade_price: float, effective_bet: float) -> bool:
    """
    Mirror the live autotrader's 5-share minimum behavior.
    """
    shares = max(effective_bet / max(trade_price, 0.01), 5.0)
    bumped_cost = shares * trade_price
    return bumped_cost <= MAX_BET + 1e-9


def _bucket_midpoint_c(low_f: float, high_f: float) -> float:
    low_c  = f_to_c(low_f)  if low_f  > -999.0 else -999.0
    high_c = f_to_c(high_f) if high_f <  999.0  else  999.0
    if low_c <= -999.0:
        return high_c - 0.5
    if high_c >= 999.0:
        return low_c + 0.5
    return (low_c + high_c) / 2.0


def run_backtest(city: str = "hong kong",
                 lat: float = 22.3020,
                 lon: float = 114.1739,
                 days: int = 180,
                 coeffs=None,
                 buckets_f=None) -> dict:
    """
    Full 180-day backtest for HK weather markets.
    Uses EMOS Gaussian CDF for probability estimation when coefficients
    are available; falls back to raw-model-mean Gaussian otherwise.

    Returns results dict with per-day records + aggregate stats.
    """
    end_date   = (date.today() - timedelta(days=6)).isoformat()    # ERA5 lag
    start_date = (date.today() - timedelta(days=days + 6)).isoformat()

    print(f"\n[backtest] Fetching {days} days of data for {city.title()}...", flush=True)
    print(f"           {start_date} → {end_date}", flush=True)
    records = fetch_backtest_data(city, lat, lon, start_date, end_date)
    print(f"[backtest] {len(records)} day records loaded.", flush=True)

    if not records:
        print("[backtest] No data — cannot run backtest.", flush=True)
        return {"error": "no_data", "n_days": 0}

    # Load EMOS coefficients for HK
    if coeffs is None:
        try:
            with open(COEFF_FILE) as f:
                coeffs = json.load(f)
        except Exception:
            coeffs = {}

    # Use caller-supplied buckets or fetch from Gamma API
    if buckets_f is None:
        buckets_f = fetch_city_buckets_f(city)

    hk_coeffs  = coeffs.get(city, coeffs.get("hong kong"))
    if isinstance(hk_coeffs, dict) and "12-24" in hk_coeffs:
        hk_coeffs = hk_coeffs["12-24"]

    # Try importing EMOS module for Gaussian CDF
    try:
        import polymarket_emos as emos_mod
        _has_emos = True
    except ImportError:
        emos_mod  = None
        _has_emos = False

    # ── Simulate day-by-day ───────────────────────────────
    bankroll       = INITIAL_BANKROLL
    daily_records  = []
    all_trades     = []
    bucket_stats   = {i: {"n": 0, "wins": 0, "pnl": 0.0, "brier_sum": 0.0}
                      for i in range(len(buckets_f))}
    diagnostics = {
        "directional_rejects": {},
        "arb_rejects": {},
        "selection_rejects": {},
        "candidate_counts": {},
    }

    def _bump(section: str, reason: str, inc: int = 1):
        diagnostics[section][reason] = diagnostics[section].get(reason, 0) + inc

    for rec in records:
        actual_c  = rec["actual_max"]
        gfs       = rec["gfs_max"]
        ecmwf     = rec["ecmwf_max"]
        icon      = rec["icon_max"]

        # Build ensemble mean / inter-model spread for this day
        model_vals = [v for v in [gfs, ecmwf, icon] if v is not None]
        if not model_vals:
            continue

        raw_mean = statistics.mean(model_vals)
        raw_std  = statistics.stdev(model_vals) if len(model_vals) > 1 else 2.0

        # ── EMOS prediction ───────────────────────────────
        if _has_emos and hk_coeffs:
            c = hk_coeffs
            inter_var = raw_std ** 2 if len(model_vals) > 1 else 1.0
            gfs_m   = gfs   if gfs   is not None else raw_mean
            ecmwf_m = ecmwf if ecmwf is not None else raw_mean
            icon_m  = icon  if icon  is not None else raw_mean
            mu    = (c["a"]
                     + c["b1"] * gfs_m
                     + c["b2"] * ecmwf_m
                     + c["b3"] * icon_m)
            sigma = math.sqrt(max(c["c"] + c["d"] * inter_var, 0.01))
        else:
            mu    = raw_mean
            sigma = max(raw_std, 1.0)

        day_pnl    = 0.0
        day_trades = 0
        day_candidates = []
        market_features = []
        yesterday_actual = daily_records[-1]["actual_c"] if daily_records else None
        proxy_prices = _proxy_bucket_prices(raw_mean, raw_std, buckets_f)

        for i, (low_f, high_f) in enumerate(buckets_f):
            model_prob = _bucket_probability(
                mu, sigma, low_f, high_f, emos_mod if _has_emos else None
            )
            market_yes_price = proxy_prices[i]

            # HK recency fade, matching the live model's continuation discount.
            low_c  = f_to_c(low_f)  if low_f  > -999.0 else -999.0
            high_c = f_to_c(high_f) if high_f <  999.0  else  999.0
            if yesterday_actual is not None:
                is_hot_bucket  = low_c >= 28.0 or high_c >= 30.0
                is_cold_bucket = high_c <= 20.0
                if yesterday_actual > 30.0 and is_hot_bucket:
                    model_prob *= 0.85
                elif yesterday_actual < 18.0 and is_cold_bucket:
                    model_prob *= 0.85
                model_prob = max(0.01, min(0.99, model_prob))

            edge = model_prob - market_yes_price
            conf = _confidence_proxy(model_vals, model_prob)
            market_features.append({
                "bucket_idx": i,
                "low_f": low_f,
                "high_f": high_f,
                "model_prob": round(model_prob, 4),
                "market_yes_price": round(market_yes_price, 4),
                "edge": round(edge, 4),
                "conf": round(conf, 4),
                "midpoint_c": _bucket_midpoint_c(low_f, high_f),
                "actual_c": actual_c,
            })

            if market_yes_price <= TAIL_MAX_PRICE and market_yes_price >= TAIL_MIN_PRICE:
                if model_prob / max(market_yes_price, 0.01) < TAIL_RATIO_THRESH:
                    _bump("directional_rejects", "tail_ratio_below_threshold")
                    continue
                direction = "BUY YES"
                trade_prob = model_prob
                trade_price = market_yes_price
                setup_type = "tail"
                tier_name = "L"
                raw_signal_bet = 1.50
            else:
                tier_cfg = live_get_tier(conf, edge)
                if tier_cfg is None:
                    _bump("directional_rejects", "no_tier")
                    continue
                if edge > 0:
                    direction = "BUY YES"
                    trade_prob = model_prob
                    trade_price = market_yes_price
                else:
                    direction = "BUY NO"
                    trade_prob = 1.0 - model_prob
                    trade_price = 1.0 - market_yes_price
                setup_type = "directional"
                tier_name = tier_cfg["name"]
                raw_signal_bet = live_kelly_size(
                    trade_prob,
                    trade_price,
                    bankroll,
                    tier_cfg["kelly"],
                    tier_cfg["max_pct"],
                )

            if trade_price > ENTRY_PRICE_CAP:
                _bump("directional_rejects", "entry_price_cap")
                continue

            if raw_signal_bet < RAW_SIGNAL_MIN_BET and setup_type != "tail":
                _bump("directional_rejects", "raw_bet_below_min")
                continue
            effective_bet = min(raw_signal_bet, MAX_BET)
            if effective_bet < 0.01:
                _bump("directional_rejects", "effective_bet_too_small")
                continue
            if not _passes_min_share_rule(trade_price, effective_bet):
                _bump("directional_rejects", "fails_min_share_rule")
                continue

            ev = live_expected_value(trade_prob, trade_price, effective_bet)
            day_candidates.append({
                "bucket_idx": i,
                "low_f": low_f,
                "high_f": high_f,
                "actual_c": actual_c,
                "model_prob": round(model_prob, 4),
                "market_price": round(market_yes_price, 4),
                "trade_prob": round(trade_prob, 4),
                "direction": direction,
                "setup_type": setup_type,
                "strategy_arm": (
                    "tail"
                    if setup_type == "tail"
                    else f"directional_{tier_name.lower()}"
                ),
                "edge": round(edge, 4),
                "conf": round(conf, 4),
                "tier_name": tier_name,
                "raw_bet": round(raw_signal_bet, 4),
                "bet": round(effective_bet, 4),
                "trade_price": round(trade_price, 4),
                "ev": round(ev, 4),
            })

        if len(market_features) >= 3:
            yes_sum = sum(feature["market_yes_price"] for feature in market_features)
            deviation = yes_sum - 1.0
            if abs(deviation) < ARB_MIN_DEVIATION:
                _bump("arb_rejects", "deviation_below_min")
            else:
                arb_leg_budget = min(
                    ARB_BET_PER_LEG,
                    MAX_BET,
                    max((bankroll * MAX_DAILY_EXPOSURE) / len(market_features), 0.01),
                )
                strategy_arm = (
                    "bucket_sum_arb_loose"
                    if abs(deviation) >= ARB_LOOSE_MIN_DEVIATION
                    else "bucket_sum_arb_strict"
                )
                if strategy_arm == "bucket_sum_arb_strict":
                    _bump("arb_rejects", "deviation_below_loose_threshold", len(market_features))
                arb_candidates = []
                arb_leg_failed = False
                for feature in market_features:
                    trade_price = (
                        feature["market_yes_price"]
                        if deviation < 0
                        else round(1.0 - feature["market_yes_price"], 4)
                    )
                    effective_bet = arb_leg_budget
                    if not _passes_min_share_rule(trade_price, effective_bet):
                        arb_leg_failed = True
                        break
                    arb_candidates.append({
                        "bucket_idx": feature["bucket_idx"],
                        "low_f": feature["low_f"],
                        "high_f": feature["high_f"],
                        "actual_c": actual_c,
                        "model_prob": feature["model_prob"],
                        "market_price": feature["market_yes_price"],
                        "trade_prob": round(1.0 / len(market_features), 4),
                        "direction": "BUY YES" if deviation < 0 else "BUY NO",
                        "setup_type": "bucket_sum_arb",
                        "strategy_arm": strategy_arm,
                        "arb_group": f"{rec['date']}_{'yes' if deviation < 0 else 'no'}",
                        "arb_yes_sum": round(yes_sum, 6),
                        "arb_deviation": round(abs(deviation), 6),
                        "arb_stale_available": False,
                        "edge": round(abs(deviation), 4),
                        "conf": 1.0,
                        "tier_name": "ARB",
                        "raw_bet": arb_leg_budget,
                        "bet": round(effective_bet, 4),
                        "trade_price": round(trade_price, 4),
                        "ev": round(abs(deviation) * effective_bet, 4),
                    })
                if arb_leg_failed:
                    _bump("arb_rejects", "fails_min_share_rule", len(market_features))
                    continue
                day_candidates.extend(arb_candidates)

        # Precision bracket mode: center bucket + best adjacent bucket around the
        # forecast mean when the spread is tight.
        if raw_std <= PRECISION_MAX_STD_C and len(market_features) >= 2:
            ordered = sorted(market_features, key=lambda m: m["midpoint_c"])
            center_idx = min(range(len(ordered)), key=lambda idx: abs(ordered[idx]["midpoint_c"] - mu))
            center = ordered[center_idx]
            adjacent = []
            for idx in [center_idx - 1, center_idx + 1]:
                if 0 <= idx < len(ordered):
                    adjacent.append(ordered[idx])
            if adjacent:
                hedge = max(adjacent, key=lambda m: m["edge"])
                combined_edge = (center["model_prob"] + hedge["model_prob"]) - (
                    center["market_yes_price"] + hedge["market_yes_price"]
                )
                if combined_edge >= PRECISION_MIN_EDGE:
                    hedge_group = f"{rec['date']}_{round(center['midpoint_c'], 2)}"
                    for role, bucket, raw_bet in [
                        ("core", center, PRECISION_RAW_BET),
                        ("hedge", hedge, PRECISION_RAW_BET * 0.5),
                    ]:
                        trade_price = bucket["market_yes_price"]
                        if trade_price > ENTRY_PRICE_CAP:
                            continue
                        effective_bet = min(raw_bet, MAX_BET)
                        if not _passes_min_share_rule(trade_price, effective_bet):
                            continue
                        day_candidates.append({
                            "bucket_idx": bucket["bucket_idx"],
                            "low_f": bucket["low_f"],
                            "high_f": bucket["high_f"],
                            "actual_c": actual_c,
                            "model_prob": bucket["model_prob"],
                            "market_price": bucket["market_yes_price"],
                            "trade_prob": bucket["model_prob"],
                            "direction": "BUY YES",
                            "setup_type": "precision_bracket",
                            "strategy_arm": f"precision_{role}",
                            "hedge_role": role,
                            "hedge_group": hedge_group,
                            "edge": bucket["edge"],
                            "conf": bucket["conf"],
                            "tier_name": "P",
                            "raw_bet": raw_bet,
                            "bet": round(effective_bet, 4),
                            "trade_price": trade_price,
                            "ev": round(live_expected_value(bucket["model_prob"], trade_price, effective_bet), 4),
                        })

        for candidate in day_candidates:
            arm = candidate.get("strategy_arm") or candidate.get("setup_type") or "unknown"
            diagnostics["candidate_counts"][arm] = diagnostics["candidate_counts"].get(arm, 0) + 1

        day_candidates.sort(
            key=lambda s: (
                0 if s.get("strategy_arm") == "stale_quote_capture" else
                1 if s.get("strategy_arm") == "bucket_sum_arb_loose" else
                2 if s["setup_type"] == "precision_bracket" else
                3 if s["setup_type"] == "directional" else
                3,
                0 if s.get("hedge_role") == "core" else 1,
                {"A": 0, "B": 1, "C": 2, "P": 3, "L": 4}.get(s["tier_name"], 9),
                -abs(s["edge"]),
            )
        )

        deployed_today = 0.0
        selected = []
        daily_cap = bankroll * MAX_DAILY_EXPOSURE
        used_buckets = set()
        precision_groups = {}
        handled_arb_groups = set()
        for candidate in day_candidates:
            if candidate.get("strategy_arm") == "bucket_sum_arb_strict":
                _bump("selection_rejects", "bucket_sum_strict_filtered")
                continue
            if candidate.get("setup_type") == "bucket_sum_arb":
                arb_group = candidate.get("arb_group")
                if arb_group in handled_arb_groups:
                    continue
                group_members = [
                    c for c in day_candidates
                    if c.get("arb_group") == arb_group
                ]
                handled_arb_groups.add(arb_group)
                if not group_members:
                    continue
                if any(m.get("strategy_arm") == "bucket_sum_arb_strict" for m in group_members):
                    _bump("selection_rejects", "bucket_sum_strict_filtered", len(group_members))
                    continue
                group_cost = sum(m["bet"] for m in group_members)
                if len(selected) >= LIVE_MAX_PER_CITY:
                    _bump("selection_rejects", "max_per_city_reached", len(group_members))
                    continue
                if deployed_today + group_cost > daily_cap:
                    _bump("selection_rejects", "daily_cap_filtered", len(group_members))
                    continue
                selected.extend(group_members)
                deployed_today += group_cost
                for member in group_members:
                    used_buckets.add(member["bucket_idx"])
                continue
            if candidate["bucket_idx"] in used_buckets:
                _bump("selection_rejects", "bucket_already_used")
                continue
            if candidate.get("setup_type") == "precision_bracket":
                group = candidate.get("hedge_group")
                precision_groups.setdefault(group, 0)
                if candidate.get("hedge_role") == "hedge" and precision_groups[group] == 0:
                    _bump("selection_rejects", "precision_hedge_without_core")
                    continue
            if len(selected) >= LIVE_MAX_PER_CITY:
                _bump("selection_rejects", "max_per_city_reached")
                break
            if deployed_today + candidate["bet"] > daily_cap:
                _bump("selection_rejects", "daily_cap_filtered")
                continue
            selected.append(candidate)
            deployed_today += candidate["bet"]
            used_buckets.add(candidate["bucket_idx"])
            if candidate.get("setup_type") == "precision_bracket":
                precision_groups[candidate["hedge_group"]] += 1

        for candidate in selected:
            result = simulate_market(
                actual_c,
                candidate["low_f"],
                candidate["high_f"],
                candidate["model_prob"],
                candidate["trade_price"],
                candidate["bet"],
                candidate["direction"],
            )

            # Update bankroll
            bankroll  += result["pnl"]
            bankroll   = max(bankroll, 0.01)   # floor at 1¢
            day_pnl   += result["pnl"]
            day_trades += 1

            trade_rec = {"date": rec["date"], "city": city, **candidate, **result}
            all_trades.append(trade_rec)

            s = bucket_stats[candidate["bucket_idx"]]
            s["n"]         += 1
            s["wins"]      += 1 if result["outcome"] else 0
            s["pnl"]       += result["pnl"]
            s["brier_sum"] += result["brier"]

        daily_records.append({
            "date":      rec["date"],
            "city":      city,
            "actual_c":  actual_c,
            "mu":        round(mu, 2),
            "sigma":     round(sigma, 2),
            "day_pnl":   round(day_pnl, 4),
            "n_trades":  day_trades,
            "bankroll":  round(bankroll, 4),
        })

    # ── Aggregate stats ───────────────────────────────────
    if not all_trades:
        return {
            "city":         city,
            "start_date":   start_date,
            "end_date":     end_date,
            "n_days":       len(records),
            "n_bets":       0,
            "total_pnl":    0.0,
            "roi":          0.0,
            "win_rate":     None,
            "brier_score":  None,
            "final_bankroll": INITIAL_BANKROLL,
            "daily_records":  daily_records,
            "bucket_breakdown": {},
            "strategy_breakdown": {},
            "strategy_arm_breakdown": {},
            "diagnostics": diagnostics,
            "backtest_limits": {
                "stale_quote_capture": "unavailable_without_historical_orderbook",
                "bucket_sum_arb_strict": "excluded_from_trade_selection_by_design",
                "bucket_sum_arb_loose_min_deviation": ARB_LOOSE_MIN_DEVIATION,
            },
            "all_trades":   [],
        }

    total_pnl    = sum(t["pnl"]   for t in all_trades)
    total_risked = sum(t["bet"]   for t in all_trades)
    wins         = sum(1          for t in all_trades if t["outcome"])
    brier_scores = [t["brier"]    for t in all_trades]
    mean_brier   = statistics.mean(brier_scores) if brier_scores else None
    win_rate     = wins / len(all_trades) if all_trades else None
    roi          = total_pnl / INITIAL_BANKROLL if INITIAL_BANKROLL > 0 else 0.0

    # Sharpe: daily PnL series
    daily_pnls = [d["day_pnl"] for d in daily_records if d["n_trades"] > 0]
    if len(daily_pnls) > 1:
        mean_daily = statistics.mean(daily_pnls)
        std_daily  = statistics.stdev(daily_pnls)
        sharpe     = round((mean_daily - RISK_FREE_RATE) / std_daily * math.sqrt(252), 4) \
                     if std_daily > 0 else 0.0
    else:
        sharpe = 0.0

    # Max drawdown
    peak      = INITIAL_BANKROLL
    max_dd    = 0.0
    running   = INITIAL_BANKROLL
    for d in daily_records:
        running += d["day_pnl"]
        if running > peak:
            peak = running
        dd = (peak - running) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    # Per-bucket breakdown
    bucket_breakdown = {}
    for i, (low_f, high_f) in enumerate(buckets_f):
        s = bucket_stats[i]
        if s["n"] == 0:
            continue
        label = (f"≤{high_f:.0f}°F" if low_f <= -999.0 else
                 f"≥{low_f:.0f}°F"  if high_f >= 999.0  else
                 f"{low_f:.0f}-{high_f:.0f}°F")
        bucket_breakdown[label] = {
            "n_bets":     s["n"],
            "win_rate":   round(s["wins"] / s["n"], 4) if s["n"] else None,
            "total_pnl":  round(s["pnl"], 4),
            "brier_score": round(s["brier_sum"] / s["n"], 6) if s["n"] else None,
        }

    # Per-strategy breakdown
    strategy_stats = {}
    strategy_arm_stats = {}
    for t in all_trades:
        setup = t.get("setup_type", "unknown")
        s = strategy_stats.setdefault(setup, {
            "n_bets": 0,
            "wins": 0,
            "total_pnl": 0.0,
            "total_risked": 0.0,
            "brier_sum": 0.0,
        })
        s["n_bets"] += 1
        s["wins"] += 1 if t.get("outcome") else 0
        s["total_pnl"] += t["pnl"]
        s["total_risked"] += t["bet"]
        s["brier_sum"] += t["brier"]

        arm = t.get("strategy_arm") or setup
        a = strategy_arm_stats.setdefault(arm, {
            "n_bets": 0,
            "wins": 0,
            "total_pnl": 0.0,
            "total_risked": 0.0,
            "brier_sum": 0.0,
        })
        a["n_bets"] += 1
        a["wins"] += 1 if t.get("outcome") else 0
        a["total_pnl"] += t["pnl"]
        a["total_risked"] += t["bet"]
        a["brier_sum"] += t["brier"]

    strategy_breakdown = {}
    for setup, s in strategy_stats.items():
        n = s["n_bets"]
        strategy_breakdown[setup] = {
            "n_bets": n,
            "win_rate": round(s["wins"] / n, 4) if n else None,
            "total_pnl": round(s["total_pnl"], 4),
            "total_risked": round(s["total_risked"], 4),
            "roi_on_risk": round(s["total_pnl"] / s["total_risked"], 4) if s["total_risked"] > 0 else None,
            "brier_score": round(s["brier_sum"] / n, 6) if n else None,
        }

    strategy_arm_breakdown = {}
    for arm, s in strategy_arm_stats.items():
        n = s["n_bets"]
        strategy_arm_breakdown[arm] = {
            "n_bets": n,
            "win_rate": round(s["wins"] / n, 4) if n else None,
            "total_pnl": round(s["total_pnl"], 4),
            "total_risked": round(s["total_risked"], 4),
            "roi_on_risk": round(s["total_pnl"] / s["total_risked"], 4) if s["total_risked"] > 0 else None,
            "brier_score": round(s["brier_sum"] / n, 6) if n else None,
        }

    return {
        "city":             city,
        "start_date":       start_date,
        "end_date":         end_date,
        "n_days":           len(records),
        "first_trade_date": all_trades[0]["date"] if all_trades else None,
        "n_bets":           len(all_trades),
        "total_pnl":        round(total_pnl, 4),
        "total_risked":     round(total_risked, 4),
        "roi":              round(roi, 4),
        "win_rate":         round(win_rate, 4) if win_rate is not None else None,
        "brier_score":      round(mean_brier, 6) if mean_brier is not None else None,
        "sharpe":           sharpe,
        "max_drawdown":     round(max_dd, 4),
        "initial_bankroll": INITIAL_BANKROLL,
        "final_bankroll":   round(bankroll, 4),
        "daily_records":    daily_records,
        "bucket_breakdown": bucket_breakdown,
        "strategy_breakdown": strategy_breakdown,
        "strategy_arm_breakdown": strategy_arm_breakdown,
        "diagnostics": diagnostics,
        "backtest_limits": {
            "stale_quote_capture": "unavailable_without_historical_orderbook",
            "bucket_sum_arb_strict": "excluded_from_trade_selection_by_design",
            "bucket_sum_arb_loose_min_deviation": ARB_LOOSE_MIN_DEVIATION,
        },
        "all_trades":       all_trades,
        "computed_at":      date.today().isoformat(),
    }


def _canonical_city(city: str) -> str:
    city_slug = (city or "").strip().lower()
    return {
        "new york": "new york city",
        "nyc": "new york city",
    }.get(city_slug, city_slug)


def _quantile(values: list[float], q: float):
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return round(ordered[0], 4)
    pos = (len(ordered) - 1) * q
    low = math.floor(pos)
    high = math.ceil(pos)
    if low == high:
        return round(ordered[low], 4)
    frac = pos - low
    return round(ordered[low] + (ordered[high] - ordered[low]) * frac, 4)


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _empty_stat_bucket() -> dict:
    return {
        "n_bets": 0,
        "wins": 0,
        "total_pnl": 0.0,
        "total_risked": 0.0,
        "brier_sum": 0.0,
    }


def _update_stat_bucket(bucket: dict, trade: dict, pnl_value: float, bet_value: float) -> None:
    bucket["n_bets"] += 1
    bucket["wins"] += 1 if trade.get("outcome") else 0
    bucket["total_pnl"] += pnl_value
    bucket["total_risked"] += bet_value
    if trade.get("brier") is not None:
        bucket["brier_sum"] += float(trade["brier"])


def _finalize_stat_bucket(bucket: dict) -> dict:
    n = bucket["n_bets"]
    return {
        "n_bets": n,
        "win_rate": round(bucket["wins"] / n, 4) if n else None,
        "total_pnl": round(bucket["total_pnl"], 4),
        "total_risked": round(bucket["total_risked"], 4),
        "roi_on_risk": _safe_ratio(bucket["total_pnl"], bucket["total_risked"]),
        "brier_score": round(bucket["brier_sum"] / n, 6) if n else None,
    }


def _aggregate_trade_breakdowns(trades: list[dict], pnl_fn=None, bet_fn=None) -> dict:
    pnl_fn = pnl_fn or (lambda trade: float(trade.get("pnl", 0.0)))
    bet_fn = bet_fn or (lambda trade: float(trade.get("bet", 0.0)))

    strategy_stats = {}
    strategy_arm_stats = {}
    city_stats = {}
    city_arm_stats = {}

    for trade in trades:
        pnl_value = float(pnl_fn(trade))
        bet_value = float(bet_fn(trade))
        setup = trade.get("setup_type", "unknown")
        arm = trade.get("strategy_arm") or setup
        city = trade.get("city", "unknown")
        city_arm = f"{city}::{arm}"

        _update_stat_bucket(strategy_stats.setdefault(setup, _empty_stat_bucket()), trade, pnl_value, bet_value)
        _update_stat_bucket(strategy_arm_stats.setdefault(arm, _empty_stat_bucket()), trade, pnl_value, bet_value)
        _update_stat_bucket(city_stats.setdefault(city, _empty_stat_bucket()), trade, pnl_value, bet_value)
        _update_stat_bucket(city_arm_stats.setdefault(city_arm, _empty_stat_bucket()), trade, pnl_value, bet_value)

    return {
        "strategy_breakdown": {
            key: _finalize_stat_bucket(bucket)
            for key, bucket in sorted(strategy_stats.items())
        },
        "strategy_arm_breakdown": {
            key: _finalize_stat_bucket(bucket)
            for key, bucket in sorted(strategy_arm_stats.items())
        },
        "city_breakdown": {
            key: _finalize_stat_bucket(bucket)
            for key, bucket in sorted(city_stats.items())
        },
        "city_arm_breakdown": {
            key: _finalize_stat_bucket(bucket)
            for key, bucket in sorted(city_arm_stats.items())
        },
    }


def _compute_path_metrics(daily_records: list[dict], starting_bankroll: float) -> tuple[float, float]:
    daily_pnls = [float(d.get("day_pnl", 0.0)) for d in daily_records if d.get("n_trades", 0) > 0]
    if len(daily_pnls) > 1:
        mean_daily = statistics.mean(daily_pnls)
        std_daily = statistics.stdev(daily_pnls)
        sharpe = round((mean_daily - RISK_FREE_RATE) / std_daily * math.sqrt(252), 4) if std_daily > 0 else 0.0
    else:
        sharpe = 0.0

    peak = float(starting_bankroll)
    running = float(starting_bankroll)
    max_dd = 0.0
    for row in daily_records:
        running += float(row.get("day_pnl", 0.0))
        if running > peak:
            peak = running
        dd = (peak - running) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return sharpe, round(max_dd, 4)


def _build_daily_records_from_trades(trades: list[dict], starting_bankroll: float, pnl_fn=None) -> list[dict]:
    pnl_fn = pnl_fn or (lambda trade: float(trade.get("pnl", 0.0)))
    daily_by_date = {}
    for trade in trades:
        trade_date = trade.get("date")
        if not trade_date:
            continue
        entry = daily_by_date.setdefault(
            trade_date,
            {
                "date": trade_date,
                "day_pnl": 0.0,
                "n_trades": 0,
                "cities": {},
            },
        )
        pnl_value = float(pnl_fn(trade))
        city = trade.get("city", "unknown")
        entry["day_pnl"] += pnl_value
        entry["n_trades"] += 1
        city_entry = entry["cities"].setdefault(city, {"day_pnl": 0.0, "n_trades": 0})
        city_entry["day_pnl"] += pnl_value
        city_entry["n_trades"] += 1

    bankroll = float(starting_bankroll)
    daily_records = []
    for trade_date in sorted(daily_by_date):
        entry = daily_by_date[trade_date]
        bankroll = max(0.01, bankroll + entry["day_pnl"])
        daily_records.append(
            {
                "date": trade_date,
                "day_pnl": round(entry["day_pnl"], 4),
                "n_trades": entry["n_trades"],
                "bankroll": round(bankroll, 4),
                "cities": {
                    city: {
                        "day_pnl": round(stats["day_pnl"], 4),
                        "n_trades": stats["n_trades"],
                    }
                    for city, stats in sorted(entry["cities"].items())
                },
            }
        )
    return daily_records


def _summarize_trade_subset(
    trades: list[dict],
    starting_bankroll: float,
    daily_records: list[dict] | None = None,
    pnl_fn=None,
    bet_fn=None,
) -> dict:
    pnl_fn = pnl_fn or (lambda trade: float(trade.get("pnl", 0.0)))
    bet_fn = bet_fn or (lambda trade: float(trade.get("bet", 0.0)))
    daily_records = daily_records or _build_daily_records_from_trades(trades, starting_bankroll, pnl_fn=pnl_fn)

    total_pnl = sum(float(pnl_fn(trade)) for trade in trades)
    total_risked = sum(float(bet_fn(trade)) for trade in trades)
    wins = sum(1 for trade in trades if trade.get("outcome"))
    brier_scores = [float(trade["brier"]) for trade in trades if trade.get("brier") is not None]
    sharpe, max_dd = _compute_path_metrics(daily_records, starting_bankroll)
    breakdowns = _aggregate_trade_breakdowns(trades, pnl_fn=pnl_fn, bet_fn=bet_fn)

    return {
        "initial_bankroll": round(starting_bankroll, 4),
        "final_bankroll": round(starting_bankroll + total_pnl, 4),
        "n_bets": len(trades),
        "total_pnl": round(total_pnl, 4),
        "total_risked": round(total_risked, 4),
        "roi": round(total_pnl / starting_bankroll, 4) if starting_bankroll > 0 else 0.0,
        "win_rate": round(wins / len(trades), 4) if trades else None,
        "brier_score": round(statistics.mean(brier_scores), 6) if brier_scores else None,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "daily_records": daily_records,
        **breakdowns,
    }


def _build_execution_assumptions(
    slippage_bps: float = DEFAULT_EXECUTION_SLIPPAGE_BPS,
    fee_bps: float = DEFAULT_EXECUTION_FEE_BPS,
    fill_rate: float = DEFAULT_FILL_RATE,
    loose_arb_fill_rate: float = DEFAULT_LOOSE_ARB_FILL_RATE,
    stale_fill_rate: float = DEFAULT_STALE_FILL_RATE,
) -> dict:
    return {
        "slippage_bps": max(0.0, float(slippage_bps)),
        "fee_bps": max(0.0, float(fee_bps)),
        "fill_rate": min(1.0, max(0.0, float(fill_rate))),
        "loose_arb_fill_rate": min(1.0, max(0.0, float(loose_arb_fill_rate))),
        "stale_fill_rate": min(1.0, max(0.0, float(stale_fill_rate))),
    }


def _fill_rate_for_trade(trade: dict, assumptions: dict) -> float:
    arm = trade.get("strategy_arm")
    if arm == "bucket_sum_arb_loose":
        return min(assumptions["fill_rate"], assumptions["loose_arb_fill_rate"])
    if arm == "stale_quote_capture":
        return assumptions["stale_fill_rate"]
    return assumptions["fill_rate"]


def _trade_execution_cost(trade: dict, assumptions: dict) -> float:
    bet = float(trade.get("bet", 0.0))
    return bet * (assumptions["slippage_bps"] + assumptions["fee_bps"]) / 10000.0


def _expected_adjusted_trade_pnl(trade: dict, assumptions: dict) -> float:
    fill_rate = _fill_rate_for_trade(trade, assumptions)
    return fill_rate * (float(trade.get("pnl", 0.0)) - _trade_execution_cost(trade, assumptions))


def _expected_adjusted_trade_bet(trade: dict, assumptions: dict) -> float:
    return _fill_rate_for_trade(trade, assumptions) * float(trade.get("bet", 0.0))


def _sample_adjusted_trade_pnl(trade: dict, assumptions: dict, rng: random.Random) -> float:
    fill_rate = _fill_rate_for_trade(trade, assumptions)
    if rng.random() > fill_rate:
        return 0.0
    return float(trade.get("pnl", 0.0)) - _trade_execution_cost(trade, assumptions)


def _trade_days_from_trades(trades: list[dict]) -> list[dict]:
    grouped = {}
    for trade in trades:
        trade_date = trade.get("date")
        if not trade_date:
            continue
        grouped.setdefault(trade_date, []).append(trade)
    return [
        {
            "date": trade_date,
            "trades": grouped[trade_date],
            "n_trades": len(grouped[trade_date]),
        }
        for trade_date in sorted(grouped)
    ]


def _count_unique_cities(trades: list[dict]) -> int:
    return len({trade.get("city") for trade in trades if trade.get("city")})


def _scenario_starting_bankroll(trades: list[dict], fallback: float) -> float:
    city_count = _count_unique_cities(trades)
    if city_count <= 0:
        return fallback
    return round(INITIAL_BANKROLL * city_count, 4)


def _build_execution_adjusted_summary(trades: list[dict], starting_bankroll: float, assumptions: dict) -> dict:
    daily_records = _build_daily_records_from_trades(
        trades,
        starting_bankroll,
        pnl_fn=lambda trade: _expected_adjusted_trade_pnl(trade, assumptions),
    )
    summary = _summarize_trade_subset(
        trades,
        starting_bankroll,
        daily_records=daily_records,
        pnl_fn=lambda trade: _expected_adjusted_trade_pnl(trade, assumptions),
        bet_fn=lambda trade: _expected_adjusted_trade_bet(trade, assumptions),
    )
    summary["execution_assumptions"] = assumptions
    return summary


def _slim_backtest_results(results):
    if not isinstance(results, dict):
        return results
    slim = {}
    for key, value in results.items():
        if key == "all_trades":
            continue
        if key == "city_results" and isinstance(value, dict):
            slim[key] = {
                city: _slim_backtest_results(city_result)
                for city, city_result in value.items()
            }
        else:
            slim[key] = value
    return slim


def _build_portfolio_daily_records(city_results: dict) -> list[dict]:
    daily_by_date = {}
    for city, results in city_results.items():
        for row in results.get("daily_records", []):
            entry = daily_by_date.setdefault(
                row["date"],
                {
                    "date": row["date"],
                    "day_pnl": 0.0,
                    "n_trades": 0,
                    "cities": {},
                },
            )
            entry["day_pnl"] += row.get("day_pnl", 0.0)
            entry["n_trades"] += row.get("n_trades", 0)
            entry["cities"][city] = {
                "day_pnl": row.get("day_pnl", 0.0),
                "n_trades": row.get("n_trades", 0),
                "bankroll": row.get("bankroll"),
            }

    bankroll = INITIAL_BANKROLL * len(city_results)
    portfolio_daily = []
    for day in sorted(daily_by_date):
        entry = daily_by_date[day]
        bankroll = max(0.01, bankroll + entry["day_pnl"])
        portfolio_daily.append({
            "date": entry["date"],
            "day_pnl": round(entry["day_pnl"], 4),
            "n_trades": entry["n_trades"],
            "bankroll": round(bankroll, 4),
            "cities": entry["cities"],
        })
    return portfolio_daily


def run_multi_city_backtest(cities: list[str], days: int = 180, coeffs=None) -> dict:
    city_results = {}
    skipped = {}

    for requested_city in cities:
        city = _canonical_city(requested_city)
        if city in city_results:
            continue
        coords = CITY_COORDS.get(city)
        if coords is None:
            skipped[requested_city] = "unknown_city"
            continue

        lat, lon = coords
        city_buckets = fetch_city_buckets_f(city)
        city_results[city] = run_backtest(
            city=city,
            lat=lat,
            lon=lon,
            days=days,
            coeffs=coeffs,
            buckets_f=city_buckets,
        )

    if not city_results:
        return {
            "error": "no_valid_cities",
            "requested_cities": cities,
            "skipped_cities": skipped,
        }

    portfolio_daily = _build_portfolio_daily_records(city_results)
    all_trades = []
    diagnostics = {
        "directional_rejects": {},
        "arb_rejects": {},
        "selection_rejects": {},
        "candidate_counts": {},
    }
    for results in city_results.values():
        all_trades.extend(results.get("all_trades", []))
        for section, values in results.get("diagnostics", {}).items():
            target = diagnostics.setdefault(section, {})
            for key, value in values.items():
                target[key] = target.get(key, 0) + value

    active_cities = sum(1 for r in city_results.values() if r.get("n_bets", 0) > 0)
    sample_result = next(iter(city_results.values()))
    initial_bankroll = round(INITIAL_BANKROLL * len(city_results), 4)
    summary = _summarize_trade_subset(
        trades=all_trades,
        starting_bankroll=initial_bankroll,
        daily_records=portfolio_daily,
    )

    return {
        "mode": "multi_city",
        "cities": list(city_results),
        "city_results": city_results,
        "skipped_cities": skipped,
        "n_cities": len(city_results),
        "cities_with_trades": active_cities,
        "start_date": sample_result.get("start_date"),
        "end_date": sample_result.get("end_date"),
        "n_days": sample_result.get("n_days"),
        "portfolio_daily_records": portfolio_daily,
        "initial_bankroll": summary["initial_bankroll"],
        "final_bankroll": summary["final_bankroll"],
        "n_bets": summary["n_bets"],
        "total_pnl": summary["total_pnl"],
        "total_risked": summary["total_risked"],
        "roi": summary["roi"],
        "win_rate": summary["win_rate"],
        "brier_score": summary["brier_score"],
        "sharpe": summary["sharpe"],
        "max_drawdown": summary["max_drawdown"],
        "strategy_breakdown": summary["strategy_breakdown"],
        "strategy_arm_breakdown": summary["strategy_arm_breakdown"],
        "city_breakdown": summary["city_breakdown"],
        "city_arm_breakdown": summary["city_arm_breakdown"],
        "diagnostics": diagnostics,
        "all_trades": all_trades,
        "computed_at": date.today().isoformat(),
    }


def run_monte_carlo(
    daily_records: list[dict],
    starting_bankroll: float,
    all_trades: list[dict] | None = None,
    iterations: int = DEFAULT_MONTE_CARLO_ITERATIONS,
    horizon_days: int | None = None,
    seed: int = DEFAULT_MONTE_CARLO_SEED,
    mode: str = DEFAULT_MONTE_CARLO_MODE,
    block_days: int = DEFAULT_MONTE_CARLO_BLOCK_DAYS,
    slippage_bps: float = DEFAULT_EXECUTION_SLIPPAGE_BPS,
    fee_bps: float = DEFAULT_EXECUTION_FEE_BPS,
    fill_rate: float = DEFAULT_FILL_RATE,
    loose_arb_fill_rate: float = DEFAULT_LOOSE_ARB_FILL_RATE,
    stale_fill_rate: float = DEFAULT_STALE_FILL_RATE,
) -> dict:
    if not daily_records and not all_trades:
        return {"error": "no_daily_records"}

    assumptions = _build_execution_assumptions(
        slippage_bps=slippage_bps,
        fee_bps=fee_bps,
        fill_rate=fill_rate,
        loose_arb_fill_rate=loose_arb_fill_rate,
        stale_fill_rate=stale_fill_rate,
    )
    mode = (mode or DEFAULT_MONTE_CARLO_MODE).strip().lower()
    if mode not in {"daily", "trade", "block"}:
        return {"error": "invalid_mode", "mode": mode}

    trade_days = _trade_days_from_trades(all_trades or [])
    sample_days = len(daily_records) or len(trade_days)
    if sample_days <= 0:
        return {"error": "no_daily_records"}
    horizon = horizon_days or sample_days
    rng = random.Random(seed)

    adjusted_daily_records = _build_daily_records_from_trades(
        all_trades or [],
        starting_bankroll,
        pnl_fn=lambda trade: _expected_adjusted_trade_pnl(trade, assumptions),
    ) if all_trades else daily_records
    adjusted_pnl_samples = [float(row.get("day_pnl", 0.0)) for row in adjusted_daily_records]
    if not adjusted_pnl_samples:
        adjusted_pnl_samples = [float(row.get("day_pnl", 0.0)) for row in daily_records]

    trade_pool = list(all_trades or [])
    day_trade_counts = [day["n_trades"] for day in trade_days if day.get("n_trades", 0) > 0]
    synthetic_days = [{"day_pnl": float(row.get("day_pnl", 0.0)), "n_trades": int(row.get("n_trades", 0)), "trades": []} for row in daily_records]
    day_pool = trade_days if trade_days else synthetic_days
    if not day_pool:
        return {"error": "no_daily_records"}

    if mode == "trade" and not trade_pool:
        return {"error": "no_trade_data_for_trade_mode"}

    if mode == "block":
        block_size = max(1, int(block_days or DEFAULT_MONTE_CARLO_BLOCK_DAYS))
        if len(day_pool) <= block_size:
            blocks = [day_pool]
        else:
            blocks = [day_pool[start:start + block_size] for start in range(0, len(day_pool) - block_size + 1)]
    else:
        block_size = 1
        blocks = []

    terminal_bankrolls = []
    max_drawdowns = []
    profitable = 0
    half_or_worse = 0
    busts = 0

    for _ in range(iterations):
        bankroll = float(starting_bankroll)
        peak = bankroll
        max_dd = 0.0
        simulated_days = 0

        while simulated_days < horizon:
            if mode == "trade":
                trade_count = rng.choice(day_trade_counts) if day_trade_counts else 0
                day_pnl = 0.0
                for _trade in range(trade_count):
                    sampled_trade = rng.choice(trade_pool)
                    day_pnl += _sample_adjusted_trade_pnl(sampled_trade, assumptions, rng)
                bankroll = max(0.01, bankroll + day_pnl)
                peak = max(peak, bankroll)
                if peak > 0:
                    max_dd = max(max_dd, (peak - bankroll) / peak)
                simulated_days += 1
                continue

            if mode == "daily":
                sampled_day = rng.choice(day_pool)
                if sampled_day.get("trades"):
                    day_pnl = sum(_sample_adjusted_trade_pnl(trade, assumptions, rng) for trade in sampled_day["trades"])
                else:
                    day_pnl = float(sampled_day.get("day_pnl", 0.0))
                bankroll = max(0.01, bankroll + day_pnl)
                peak = max(peak, bankroll)
                if peak > 0:
                    max_dd = max(max_dd, (peak - bankroll) / peak)
                simulated_days += 1
                continue

            sampled_block = rng.choice(blocks)
            for sampled_day in sampled_block:
                if simulated_days >= horizon:
                    break
                if sampled_day.get("trades"):
                    day_pnl = sum(_sample_adjusted_trade_pnl(trade, assumptions, rng) for trade in sampled_day["trades"])
                else:
                    day_pnl = float(sampled_day.get("day_pnl", 0.0))
                bankroll = max(0.01, bankroll + day_pnl)
                peak = max(peak, bankroll)
                if peak > 0:
                    max_dd = max(max_dd, (peak - bankroll) / peak)
                simulated_days += 1

        terminal_bankrolls.append(bankroll)
        max_drawdowns.append(max_dd)
        profitable += 1 if bankroll > starting_bankroll else 0
        half_or_worse += 1 if bankroll <= starting_bankroll * 0.5 else 0
        busts += 1 if bankroll <= 0.05 else 0

    return {
        "iterations": iterations,
        "seed": seed,
        "mode": mode,
        "block_days": block_size if mode == "block" else None,
        "sample_days": sample_days,
        "sample_trades": len(trade_pool),
        "horizon_days": horizon,
        "starting_bankroll": round(starting_bankroll, 4),
        "mean_daily_pnl": round(statistics.mean(adjusted_pnl_samples), 6),
        "stdev_daily_pnl": round(statistics.stdev(adjusted_pnl_samples), 6) if len(adjusted_pnl_samples) > 1 else 0.0,
        "mean_terminal_bankroll": round(statistics.mean(terminal_bankrolls), 4),
        "median_terminal_bankroll": _quantile(terminal_bankrolls, 0.5),
        "p05_terminal_bankroll": _quantile(terminal_bankrolls, 0.05),
        "p25_terminal_bankroll": _quantile(terminal_bankrolls, 0.25),
        "p75_terminal_bankroll": _quantile(terminal_bankrolls, 0.75),
        "p95_terminal_bankroll": _quantile(terminal_bankrolls, 0.95),
        "best_terminal_bankroll": round(max(terminal_bankrolls), 4),
        "worst_terminal_bankroll": round(min(terminal_bankrolls), 4),
        "prob_profit": round(profitable / iterations, 4),
        "prob_loss": round(1 - (profitable / iterations), 4),
        "prob_half_bankroll_or_worse": round(half_or_worse / iterations, 4),
        "prob_bust": round(busts / iterations, 4),
        "mean_max_drawdown": round(statistics.mean(max_drawdowns), 4),
        "p95_max_drawdown": _quantile(max_drawdowns, 0.95),
        "execution_assumptions": assumptions,
    }


def run_ablation_study(
    results: dict,
    mc_iterations: int,
    mc_horizon_days: int | None,
    seed: int,
    mc_mode: str,
    mc_block_days: int,
    slippage_bps: float,
    fee_bps: float,
    fill_rate: float,
    loose_arb_fill_rate: float,
    stale_fill_rate: float,
) -> list[dict]:
    all_trades = list(results.get("all_trades") or [])
    if not all_trades:
        return []

    assumptions = _build_execution_assumptions(
        slippage_bps=slippage_bps,
        fee_bps=fee_bps,
        fill_rate=fill_rate,
        loose_arb_fill_rate=loose_arb_fill_rate,
        stale_fill_rate=stale_fill_rate,
    )
    city_breakdown = results.get("city_breakdown") or _aggregate_trade_breakdowns(all_trades).get("city_breakdown", {})
    sorted_cities = sorted(city_breakdown.items(), key=lambda item: item[1].get("total_pnl", 0.0))
    bottom_cities = [city for city, _stats in sorted_cities[:DEFAULT_ABLATION_BOTTOM_N]]
    positive_cities = [city for city, stats in city_breakdown.items() if stats.get("total_pnl", 0.0) > 0]

    scenarios = [
        {
            "name": "baseline_all",
            "label": "All executed trades",
            "trades": all_trades,
            "starting_bankroll": results.get("initial_bankroll", _scenario_starting_bankroll(all_trades, INITIAL_BANKROLL)),
        },
        {
            "name": "no_bucket_sum_arb_loose",
            "label": "Exclude bucket_sum_arb_loose",
            "trades": [trade for trade in all_trades if trade.get("strategy_arm") != "bucket_sum_arb_loose"],
            "starting_bankroll": results.get("initial_bankroll", _scenario_starting_bankroll(all_trades, INITIAL_BANKROLL)),
        },
        {
            "name": "precision_only",
            "label": "Precision arms only",
            "trades": [trade for trade in all_trades if str(trade.get("strategy_arm", "")).startswith("precision_")],
            "starting_bankroll": results.get("initial_bankroll", _scenario_starting_bankroll(all_trades, INITIAL_BANKROLL)),
        },
        {
            "name": "positive_cities_only",
            "label": "Positive-PnL cities only",
            "trades": [trade for trade in all_trades if trade.get("city") in positive_cities],
            "starting_bankroll": round(INITIAL_BANKROLL * len(positive_cities), 4) if positive_cities else results.get("initial_bankroll", INITIAL_BANKROLL),
            "selected_cities": positive_cities,
        },
        {
            "name": "drop_bottom_5_cities",
            "label": "Drop worst 5 cities",
            "trades": [trade for trade in all_trades if trade.get("city") not in bottom_cities],
            "starting_bankroll": round(INITIAL_BANKROLL * max(results.get("n_cities", 0) - len(bottom_cities), 1), 4),
            "excluded_cities": bottom_cities,
        },
    ]

    outputs = []
    for scenario in scenarios:
        trades = scenario["trades"]
        starting_bankroll = float(scenario["starting_bankroll"])
        raw_summary = _summarize_trade_subset(trades, starting_bankroll)
        execution_adjusted = _build_execution_adjusted_summary(trades, starting_bankroll, assumptions)
        monte_carlo = run_monte_carlo(
            daily_records=raw_summary["daily_records"],
            starting_bankroll=starting_bankroll,
            all_trades=trades,
            iterations=mc_iterations,
            horizon_days=mc_horizon_days,
            seed=seed,
            mode=mc_mode,
            block_days=mc_block_days,
            slippage_bps=slippage_bps,
            fee_bps=fee_bps,
            fill_rate=fill_rate,
            loose_arb_fill_rate=loose_arb_fill_rate,
            stale_fill_rate=stale_fill_rate,
        )
        outputs.append(
            {
                "name": scenario["name"],
                "label": scenario["label"],
                "n_cities": _count_unique_cities(trades),
                "n_bets": len(trades),
                "selected_cities": scenario.get("selected_cities"),
                "excluded_cities": scenario.get("excluded_cities"),
                "raw": _slim_backtest_results(raw_summary),
                "execution_adjusted": _slim_backtest_results(execution_adjusted),
                "monte_carlo": monte_carlo,
            }
        )
    return outputs


def print_multi_city_backtest_report(results: dict) -> None:
    print("\n" + "=" * 70, flush=True)
    print("POLYMARKET WEATHER  —  MULTI-CITY BACKTEST", flush=True)
    print("=" * 70, flush=True)

    if results.get("error") == "no_valid_cities":
        print("  ERROR: no valid cities were provided.", flush=True)
        return

    print(f"  Cities:         {results.get('n_cities', 0)} total / {results.get('cities_with_trades', 0)} with trades", flush=True)
    print(f"  Period:         {results.get('start_date')} → {results.get('end_date')}", flush=True)
    print(f"  Trading days:   {results.get('n_days', 0)}", flush=True)
    print(f"  Total bets:     {results.get('n_bets', 0)}", flush=True)
    print(f"  Initial roll:   ${results.get('initial_bankroll', 0.0):.2f}", flush=True)
    print(f"  Final roll:     ${results.get('final_bankroll', 0.0):.2f}", flush=True)
    print(f"  Total PnL:      ${results.get('total_pnl', 0.0):+.4f}", flush=True)
    print(f"  ROI:            {results.get('roi', 0.0):+.1%}", flush=True)
    print(f"  Sharpe (ann.):  {results.get('sharpe', 0.0):.2f}", flush=True)
    print(f"  Max drawdown:   {results.get('max_drawdown', 0.0):.1%}", flush=True)
    win_rate = results.get("win_rate")
    brier = results.get("brier_score")
    print(f"  Win rate:       {win_rate:.1%}" if win_rate is not None else "  Win rate:       N/A", flush=True)
    print(f"  Brier score:    {brier:.4f}" if brier is not None else "  Brier score:    N/A", flush=True)

    skipped = results.get("skipped_cities", {})
    if skipped:
        print(f"  Skipped:        {', '.join(f'{city} ({reason})' for city, reason in skipped.items())}", flush=True)

    ranked = sorted(
        results.get("city_results", {}).items(),
        key=lambda item: (-item[1].get("n_bets", 0), item[0]),
    )

    strategy_arm_bd = results.get("strategy_arm_breakdown", {})
    if strategy_arm_bd:
        print(flush=True)
        print("  PORTFOLIO STRATEGY-ARM SUMMARY:", flush=True)
        print(f"  {'Arm':>22s}  {'N':>5s}  {'PnL':>8s}  {'ROI/Risk':>10s}", flush=True)
        print("  " + "-" * 56, flush=True)
        for label, s in sorted(strategy_arm_bd.items(), key=lambda item: item[1].get("total_pnl", 0.0), reverse=True):
            rr_str = f"{s['roi_on_risk']:+.1%}" if s["roi_on_risk"] is not None else "  N/A"
            print(
                f"  {label:>22s}  {s['n_bets']:>5d}  ${s['total_pnl']:>+7.4f}  {rr_str:>10s}",
                flush=True,
            )
    active_rows = [(city, stats) for city, stats in ranked if stats.get("n_bets", 0) > 0]
    if active_rows:
        print(flush=True)
        print("  PER-CITY SUMMARY (cities with trades):", flush=True)
        print(f"  {'City':>15s}  {'Bets':>5s}  {'PnL':>8s}  {'ROI':>8s}  {'WinRate':>8s}", flush=True)
        print("  " + "-" * 60, flush=True)
        for city, stats in active_rows[:20]:
            wr = stats.get("win_rate")
            wr_str = f"{wr:.1%}" if wr is not None else "N/A"
            print(
                f"  {city.title():>15s}  {stats.get('n_bets', 0):>5d}  "
                f"${stats.get('total_pnl', 0.0):>+7.4f}  {stats.get('roi', 0.0):>+7.1%}  {wr_str:>8s}",
                flush=True,
            )
        if len(active_rows) > 20:
            print(f"  ... {len(active_rows) - 20} more cities omitted", flush=True)
    else:
        print("  No cities produced trades under current thresholds.", flush=True)


def print_monte_carlo_report(results: dict) -> None:
    print("\n" + "=" * 70, flush=True)
    print("MONTE CARLO PORTFOLIO SIMULATION", flush=True)
    print("=" * 70, flush=True)

    if results.get("error") == "no_daily_records":
        print("  ERROR: no daily records available for Monte Carlo simulation.", flush=True)
        return
    if results.get("error") == "no_trade_data_for_trade_mode":
        print("  ERROR: trade mode requires trade-level data.", flush=True)
        return

    print(f"  Iterations:          {results.get('iterations', 0)}", flush=True)
    print(f"  Mode:                {results.get('mode', 'unknown')}", flush=True)
    if results.get('mode') == 'block':
        print(f"  Block size:          {results.get('block_days', 0)} days", flush=True)
    print(f"  Horizon:             {results.get('horizon_days', 0)} days", flush=True)
    print(f"  Sample days:         {results.get('sample_days', 0)}", flush=True)
    print(f"  Sample trades:       {results.get('sample_trades', 0)}", flush=True)
    print(f"  Starting bankroll:   ${results.get('starting_bankroll', 0.0):.2f}", flush=True)
    print(f"  Mean daily PnL:      ${results.get('mean_daily_pnl', 0.0):+.4f}", flush=True)
    print(f"  Daily PnL stdev:     ${results.get('stdev_daily_pnl', 0.0):.4f}", flush=True)
    assumptions = results.get('execution_assumptions') or {}
    if assumptions:
        print(flush=True)
        print("  EXECUTION ASSUMPTIONS:", flush=True)
        print(
            f"    Slippage+fees:     {assumptions.get('slippage_bps', 0.0) + assumptions.get('fee_bps', 0.0):.1f} bps"
            f"  (slippage {assumptions.get('slippage_bps', 0.0):.1f} + fees {assumptions.get('fee_bps', 0.0):.1f})",
            flush=True,
        )
        print(f"    Base fill rate:    {assumptions.get('fill_rate', 0.0):.1%}", flush=True)
        print(f"    Loose-arb fill:    {assumptions.get('loose_arb_fill_rate', 0.0):.1%}", flush=True)
        print(f"    Stale fill rate:   {assumptions.get('stale_fill_rate', 0.0):.1%}", flush=True)
    print(flush=True)
    print("  TERMINAL BANKROLL DISTRIBUTION:", flush=True)
    print(f"    Mean:              ${results.get('mean_terminal_bankroll', 0.0):.4f}", flush=True)
    print(f"    Median:            ${results.get('median_terminal_bankroll', 0.0):.4f}", flush=True)
    print(f"    P05 / P95:         ${results.get('p05_terminal_bankroll', 0.0):.4f} / ${results.get('p95_terminal_bankroll', 0.0):.4f}", flush=True)
    print(f"    Worst / Best:      ${results.get('worst_terminal_bankroll', 0.0):.4f} / ${results.get('best_terminal_bankroll', 0.0):.4f}", flush=True)
    print(flush=True)
    print("  RISK METRICS:", flush=True)
    print(f"    Prob. profit:      {results.get('prob_profit', 0.0):.1%}", flush=True)
    print(f"    Prob. loss:        {results.get('prob_loss', 0.0):.1%}", flush=True)
    print(f"    Prob. ≤50% roll:   {results.get('prob_half_bankroll_or_worse', 0.0):.1%}", flush=True)
    print(f"    Prob. bust:        {results.get('prob_bust', 0.0):.1%}", flush=True)
    print(f"    Mean max DD:       {results.get('mean_max_drawdown', 0.0):.1%}", flush=True)
    print(f"    P95 max DD:        {results.get('p95_max_drawdown', 0.0):.1%}", flush=True)


def print_ablation_report(ablation_results: list[dict]) -> None:
    if not ablation_results:
        return

    print("\n" + "=" * 70, flush=True)
    print("ABLATION STUDY", flush=True)
    print("=" * 70, flush=True)
    print(f"  {'Scenario':<28s} {'Bets':>6s} {'Raw ROI':>9s} {'Adj ROI':>9s} {'MC P05':>10s} {'MC Profit%':>11s}", flush=True)
    print("  " + "-" * 86, flush=True)
    for row in ablation_results:
        raw = row.get('raw', {})
        adjusted = row.get('execution_adjusted', {})
        mc = row.get('monte_carlo', {})
        print(
            f"  {row.get('name', 'unknown'):<28s} {raw.get('n_bets', 0):>6d} "
            f"{raw.get('roi', 0.0):>+8.1%} {adjusted.get('roi', 0.0):>+8.1%} "
            f"${mc.get('p05_terminal_bankroll', 0.0):>9.2f} {mc.get('prob_profit', 0.0):>10.1%}",
            flush=True,
        )
        if row.get('excluded_cities'):
            print(f"    excluded: {', '.join(row['excluded_cities'])}", flush=True)
        if row.get('selected_cities'):
            selected = row['selected_cities']
            print(f"    selected: {', '.join(selected[:12])}" + (" ..." if len(selected) > 12 else ""), flush=True)


def _parse_args():
    parser = argparse.ArgumentParser(description="Backtest Polymarket weather strategies and run Monte Carlo simulations.")
    parser.add_argument("--city", default="hong kong", help="single city to backtest")
    parser.add_argument("--cities", default="", help="comma-separated city list for a portfolio backtest")
    parser.add_argument("--all-cities", action="store_true", help="backtest every city with EMOS coefficients")
    parser.add_argument("--days", type=int, default=180, help="historical lookback window")
    parser.add_argument("--monte-carlo", action="store_true", help="run Monte Carlo on the resulting trade stream")
    parser.add_argument("--mc-iterations", type=int, default=DEFAULT_MONTE_CARLO_ITERATIONS, help="number of Monte Carlo paths")
    parser.add_argument("--mc-horizon-days", type=int, default=0, help="simulation horizon in days; 0 uses the sample length")
    parser.add_argument("--mc-mode", choices=["daily", "trade", "block"], default=DEFAULT_MONTE_CARLO_MODE, help="Monte Carlo resampling mode")
    parser.add_argument("--mc-block-days", type=int, default=DEFAULT_MONTE_CARLO_BLOCK_DAYS, help="block length for block-bootstrap Monte Carlo")
    parser.add_argument("--execution-slippage-bps", type=float, default=DEFAULT_EXECUTION_SLIPPAGE_BPS, help="per-trade slippage haircut in bps")
    parser.add_argument("--execution-fee-bps", type=float, default=DEFAULT_EXECUTION_FEE_BPS, help="per-trade fee haircut in bps")
    parser.add_argument("--fill-rate", type=float, default=DEFAULT_FILL_RATE, help="base fill rate for simulated execution")
    parser.add_argument("--loose-arb-fill-rate", type=float, default=DEFAULT_LOOSE_ARB_FILL_RATE, help="fill rate override for bucket_sum_arb_loose")
    parser.add_argument("--stale-fill-rate", type=float, default=DEFAULT_STALE_FILL_RATE, help="fill rate override for stale quote capture")
    parser.add_argument("--ablation", action="store_true", help="run post-backtest ablation study on realized trades")
    parser.add_argument("--seed", type=int, default=DEFAULT_MONTE_CARLO_SEED, help="RNG seed for Monte Carlo")
    return parser.parse_args()


def _resolve_requested_cities(args, coeffs) -> list[str]:
    requested = []
    if args.all_cities:
        requested.extend(sorted(coeffs))
    elif args.cities:
        requested.extend(part.strip() for part in args.cities.split(",") if part.strip())
    elif args.city:
        requested.append(args.city)

    if not requested:
        requested = ["hong kong"]

    resolved = []
    seen = set()
    for city in requested:
        canonical = _canonical_city(city)
        if canonical in seen:
            continue
        seen.add(canonical)
        resolved.append(canonical)
    return resolved


# ─── 4. PRINT BACKTEST REPORT ─────────────────────────────

def print_backtest_report(results: dict) -> None:
    """Print backtest results with PnL curve, per-bucket breakdown, overall stats."""
    print("\n" + "=" * 70, flush=True)
    print("POLYMARKET WEATHER  —  BACKTEST REPORT", flush=True)
    print("=" * 70, flush=True)

    if results.get("error") == "no_data":
        print("  ERROR: No data available for backtest.", flush=True)
        return

    city   = results.get("city", "unknown").title()
    n_days = results.get("n_days", 0)
    n_bets = results.get("n_bets", 0)

    print(f"  City:          {city}", flush=True)
    print(f"  Period:        {results.get('start_date')} → {results.get('end_date')}", flush=True)
    print(f"  Trading days:  {n_days}", flush=True)
    print(f"  Total bets:    {n_bets}", flush=True)
    limits = results.get("backtest_limits", {})
    if limits:
        print(f"  Notes:         stale_quote_capture unavailable in proxy backtest", flush=True)
    print(flush=True)

    if n_bets == 0:
        print("  No bets generated — signal threshold too high or no data.", flush=True)
        diagnostics = results.get("diagnostics", {})
        if diagnostics:
            print(flush=True)
            print("  DIAGNOSTICS:", flush=True)
            for section in ("candidate_counts", "directional_rejects", "arb_rejects", "selection_rejects"):
                values = diagnostics.get(section, {})
                if not values:
                    continue
                print(f"    {section}:", flush=True)
                for key, value in sorted(values.items(), key=lambda kv: (-kv[1], kv[0])):
                    print(f"      {key}: {value}", flush=True)
        return

    roi          = results.get("roi", 0.0)
    win_rate     = results.get("win_rate")
    brier        = results.get("brier_score")
    sharpe       = results.get("sharpe", 0.0)
    max_dd       = results.get("max_drawdown", 0.0)
    total_pnl    = results.get("total_pnl", 0.0)
    total_risked = results.get("total_risked", 0.0)
    init_br      = results.get("initial_bankroll", INITIAL_BANKROLL)
    final_br     = results.get("final_bankroll", init_br)

    print("  PERFORMANCE SUMMARY:", flush=True)
    print(f"    Initial bankroll: ${init_br:.2f}", flush=True)
    print(f"    Final bankroll:   ${final_br:.2f}", flush=True)
    print(f"    Total PnL:        ${total_pnl:+.4f}", flush=True)
    print(f"    Total risked:     ${total_risked:.4f}", flush=True)
    print(f"    ROI:              {roi:+.1%}", flush=True)
    print(f"    Win rate:         {win_rate:.1%}" if win_rate is not None else "    Win rate:         N/A", flush=True)
    print(f"    Brier score:      {brier:.4f}" if brier is not None else "    Brier score:      N/A", flush=True)
    print(f"    Sharpe (ann.):    {sharpe:.2f}", flush=True)
    print(f"    Max drawdown:     {max_dd:.1%}", flush=True)

    # ── Daily PnL curve (sampled — show weekly) ───────────
    daily = results.get("daily_records", [])
    trading_days = [d for d in daily if d["n_trades"] > 0]
    if trading_days:
        print(flush=True)
        print("  DAILY PnL CURVE (trading days only, sample):", flush=True)
        step   = max(1, len(trading_days) // 20)
        cumulative = 0.0
        for i, d in enumerate(trading_days):
            cumulative += d["day_pnl"]
            if i % step == 0 or i == len(trading_days) - 1:
                bar_units = int(abs(cumulative) / 0.05)
                bar_char  = "+" if cumulative >= 0 else "-"
                bar       = bar_char * min(bar_units, 30)
                print(
                    f"    {d['date']}  μ={d['mu']:.1f}°C  "
                    f"n={d['n_trades']}  dayPnL={d['day_pnl']:+.4f}  "
                    f"cum={cumulative:+.4f}  BR=${d['bankroll']:.3f}  |{bar}",
                    flush=True,
                )

    # ── Per-bucket breakdown ──────────────────────────────
    bucket_bd = results.get("bucket_breakdown", {})
    if bucket_bd:
        print(flush=True)
        print("  PER-BUCKET BREAKDOWN:", flush=True)
        print(f"  {'Bucket':>15s}  {'N':>5s}  {'WinRate':>8s}  {'PnL':>8s}  {'Brier':>8s}", flush=True)
        print("  " + "-" * 55, flush=True)
        for label, s in sorted(bucket_bd.items()):
            wr_str = f"{s['win_rate']:.1%}" if s["win_rate"] is not None else "  N/A"
            br_str = f"{s['brier_score']:.4f}" if s["brier_score"] is not None else "  N/A"
            print(
                f"  {label:>15s}  {s['n_bets']:>5d}  "
                f"{wr_str:>8s}  ${s['total_pnl']:>+7.4f}  {br_str:>8s}",
                flush=True,
            )

    strategy_bd = results.get("strategy_breakdown", {})
    if strategy_bd:
        print(flush=True)
        print("  PER-STRATEGY BREAKDOWN:", flush=True)
        print(f"  {'Strategy':>15s}  {'N':>5s}  {'WinRate':>8s}  {'PnL':>8s}  {'ROI/Risk':>10s}  {'Brier':>8s}", flush=True)
        print("  " + "-" * 75, flush=True)
        for label, s in sorted(strategy_bd.items()):
            wr_str = f"{s['win_rate']:.1%}" if s["win_rate"] is not None else "  N/A"
            rr_str = f"{s['roi_on_risk']:+.1%}" if s["roi_on_risk"] is not None else "  N/A"
            br_str = f"{s['brier_score']:.4f}" if s["brier_score"] is not None else "  N/A"
            print(
                f"  {label:>15s}  {s['n_bets']:>5d}  "
                f"{wr_str:>8s}  ${s['total_pnl']:>+7.4f}  {rr_str:>10s}  {br_str:>8s}",
                    flush=True,
                )

    strategy_arm_bd = results.get("strategy_arm_breakdown", {})
    if strategy_arm_bd:
        print(flush=True)
        print("  PER-STRATEGY-ARM BREAKDOWN:", flush=True)
        print(f"  {'Arm':>22s}  {'N':>5s}  {'WinRate':>8s}  {'PnL':>8s}  {'ROI/Risk':>10s}  {'Brier':>8s}", flush=True)
        print("  " + "-" * 82, flush=True)
        for label, s in sorted(strategy_arm_bd.items()):
            wr_str = f"{s['win_rate']:.1%}" if s["win_rate"] is not None else "  N/A"
            rr_str = f"{s['roi_on_risk']:+.1%}" if s["roi_on_risk"] is not None else "  N/A"
            br_str = f"{s['brier_score']:.4f}" if s["brier_score"] is not None else "  N/A"
            print(
                f"  {label:>22s}  {s['n_bets']:>5d}  "
                f"{wr_str:>8s}  ${s['total_pnl']:>+7.4f}  {rr_str:>10s}  {br_str:>8s}",
                flush=True,
            )

    if limits:
        print(flush=True)
        print("  BACKTEST LIMITS:", flush=True)
        print(f"    stale_quote_capture: {limits.get('stale_quote_capture')}", flush=True)
        print(f"    bucket_sum_arb_strict: {limits.get('bucket_sum_arb_strict')}", flush=True)
        print(
            f"    bucket_sum_arb_loose threshold: "
            f"{limits.get('bucket_sum_arb_loose_min_deviation', 0):.1%}",
            flush=True,
        )

    diagnostics = results.get("diagnostics", {})
    if diagnostics:
        print(flush=True)
        print("  DIAGNOSTICS:", flush=True)
        for section in ("candidate_counts", "directional_rejects", "arb_rejects", "selection_rejects"):
            values = diagnostics.get(section, {})
            if not values:
                continue
            print(f"    {section}:", flush=True)
            for key, value in sorted(values.items(), key=lambda kv: (-kv[1], kv[0])):
                print(f"      {key}: {value}", flush=True)

    # ── Go/No-Go verdict ──────────────────────────────────
    READY_ROI    = 0.10
    READY_BRIER  = 0.22
    READY_N_BETS = 20

    roi_ok   = roi   >  READY_ROI
    brier_ok = brier is not None and brier < READY_BRIER
    n_ok     = n_bets >= READY_N_BETS

    print(flush=True)
    print("  SCALE-UP CRITERIA:", flush=True)
    print(f"    ROI > {READY_ROI:.0%}:         {'PASS' if roi_ok   else 'FAIL':>4s}  (actual: {roi:+.1%})", flush=True)
    print(f"    Brier < {READY_BRIER}:       {'PASS' if brier_ok else 'FAIL':>4s}  (actual: {brier:.4f})" \
          if brier is not None else f"    Brier < {READY_BRIER}:       FAIL  (no data)", flush=True)
    print(f"    N bets >= {READY_N_BETS}:       {'PASS' if n_ok     else 'FAIL':>4s}  (actual: {n_bets})", flush=True)

    if roi_ok and brier_ok and n_ok:
        print(flush=True)
        print("  *** READY TO SCALE — all criteria met. Deploy capital. ***", flush=True)
    else:
        reasons = []
        if not roi_ok:   reasons.append(f"ROI gap: {roi - READY_ROI:+.1%}")
        if not brier_ok: reasons.append(f"Brier gap: {(brier or 1.0) - READY_BRIER:+.4f}")
        if not n_ok:     reasons.append(f"need {READY_N_BETS - n_bets} more bets")
        print(f"  Not ready to scale: {'; '.join(reasons)}", flush=True)

    print("=" * 70, flush=True)


# ─── 5. MAIN ──────────────────────────────────────────────

def main():
    import polymarket_emos as emos_mod

    args = _parse_args()
    coeffs = emos_mod.load_coefficients()
    requested_cities = _resolve_requested_cities(args, coeffs)

    run_portfolio = args.all_cities or len(requested_cities) > 1
    if run_portfolio:
        results = run_multi_city_backtest(
            cities=requested_cities,
            days=args.days,
            coeffs=coeffs,
        )
        print_multi_city_backtest_report(results)
        daily_records = results.get("portfolio_daily_records", [])
        starting_bankroll = results.get("initial_bankroll", INITIAL_BANKROLL)
    else:
        city = requested_cities[0]
        coords = CITY_COORDS.get(city)
        if coords is None:
            raise SystemExit(f"Unknown city: {city}")
        lat, lon = coords
        results = run_backtest(
            city=city,
            lat=lat,
            lon=lon,
            days=args.days,
            coeffs=coeffs,
        )
        print_backtest_report(results)
        daily_records = results.get("daily_records", [])
        starting_bankroll = results.get("initial_bankroll", INITIAL_BANKROLL)

    monte_carlo = None
    if args.monte_carlo:
        monte_carlo = run_monte_carlo(
            daily_records=daily_records,
            starting_bankroll=starting_bankroll,
            all_trades=results.get("all_trades", []),
            iterations=max(1, args.mc_iterations),
            horizon_days=args.mc_horizon_days or None,
            seed=args.seed,
            mode=args.mc_mode,
            block_days=max(1, args.mc_block_days),
            slippage_bps=args.execution_slippage_bps,
            fee_bps=args.execution_fee_bps,
            fill_rate=args.fill_rate,
            loose_arb_fill_rate=args.loose_arb_fill_rate,
            stale_fill_rate=args.stale_fill_rate,
        )
        print_monte_carlo_report(monte_carlo)

    ablation = []
    if args.ablation:
        ablation = run_ablation_study(
            results=results,
            mc_iterations=max(500, min(args.mc_iterations, 5000)),
            mc_horizon_days=args.mc_horizon_days or None,
            seed=args.seed,
            mc_mode=args.mc_mode,
            mc_block_days=max(1, args.mc_block_days),
            slippage_bps=args.execution_slippage_bps,
            fee_bps=args.execution_fee_bps,
            fill_rate=args.fill_rate,
            loose_arb_fill_rate=args.loose_arb_fill_rate,
            stale_fill_rate=args.stale_fill_rate,
        )
        print_ablation_report(ablation)

    out_path = os.path.join(_SCRIPT_DIR, "backtest_results.json")
    payload = {
        "args": vars(args),
        "results": _slim_backtest_results(results),
    }
    if monte_carlo is not None:
        payload["monte_carlo"] = monte_carlo
    if ablation:
        payload["ablation"] = ablation
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\n[backtest] Results saved → {out_path}", flush=True)


if __name__ == "__main__":
    main()
