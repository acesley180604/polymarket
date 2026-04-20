"""
Historical Backtest — HK Weather Markets
Uses 6 months of ERA5 actuals + hindcasts to simulate signal generation.
Reports: PnL, Brier score, calibration, Kelly sizing performance.

Run: python3 polymarket_backtest.py
"""

import os, json, time, math, statistics
import requests
from datetime import date, timedelta
from polymarket_core import f_to_c, c_to_f
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

TAIL_MIN_PRICE       = 0.01    # match live extreme-tail zone
TAIL_MAX_PRICE       = 0.05
TAIL_RATIO_THRESH    = 2.0
ARB_MIN_DEVIATION    = 0.04    # match live arb candidate generation
ARB_LOOSE_MIN_DEVIATION = 0.08 # match live loose-arb promotion threshold
ARB_BET_PER_LEG      = 0.50
ENTRY_PRICE_CAP      = 0.72    # match live directional cap
STANDARD_MIN_EDGE    = 0.12    # live-ish proxy since historical market quotes are unavailable
INITIAL_BANKROLL     = 2.0     # $2 USDC paper bankroll
MAX_BET              = 0.30    # match live T1 cap
RAW_SIGNAL_MIN_BET   = 0.50    # live model gate before autotrader cap
MAX_DAILY_EXPOSURE   = ACCOUNT_TIERS[1]["max_daily_pct"]
RISK_FREE_RATE       = 0.0     # Sharpe calculation


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
                 coeffs=None) -> dict:
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
                      for i in range(len(HK_BUCKETS_F))}
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
        proxy_prices = _proxy_bucket_prices(raw_mean, raw_std, HK_BUCKETS_F)

        for i, (low_f, high_f) in enumerate(HK_BUCKETS_F):
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

            trade_rec = {"date": rec["date"], **candidate, **result}
            all_trades.append(trade_rec)

            s = bucket_stats[candidate["bucket_idx"]]
            s["n"]         += 1
            s["wins"]      += 1 if result["outcome"] else 0
            s["pnl"]       += result["pnl"]
            s["brier_sum"] += result["brier"]

        daily_records.append({
            "date":      rec["date"],
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
    for i, (low_f, high_f) in enumerate(HK_BUCKETS_F):
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
    coeffs = emos_mod.load_coefficients()

    results = run_backtest(
        city="hong kong",
        lat=22.3020,
        lon=114.1739,
        days=180,
        coeffs=coeffs,
    )

    print_backtest_report(results)

    # Save results (without the full trade list to keep file small)
    out_path = os.path.join(_SCRIPT_DIR, "backtest_results.json")
    results_slim = {k: v for k, v in results.items() if k != "all_trades"}
    with open(out_path, "w") as f:
        json.dump(results_slim, f, indent=2)
    print(f"\n[backtest] Results saved → {out_path}", flush=True)


if __name__ == "__main__":
    main()
