"""
Smart Execution Layer
- Maker/taker routing based on spread and urgency
- HKO resolution time detection (HK markets resolve at 16:00 UTC = midnight HKT)
- Live HKO temperature feed for same-day edge
- Entry window optimization
"""

import time
import requests
from datetime import datetime, timezone, timedelta, date
from polymarket_core import CLOB_API

# ---------------------------------------------------------------------------
# Module-level caches
# ---------------------------------------------------------------------------
_temp_cache: dict = {}        # {"value": float, "ts": float}
_spread_cache: dict = {}      # {token_id: {"value": float, "ts": float}}

_TEMP_CACHE_TTL = 600         # 10 minutes
_SPREAD_CACHE_TTL = 300       # 5 minutes
_BOOK_CACHE: dict = {}        # {token_id: {"data": {...}, "ts": float}}
_BOOK_CACHE_TTL = 120

HKO_RHRREAD_URL = (
    "https://data.weather.gov.hk/weatherAPI/opendata/weather.php"
    "?dataType=rhrread&lang=en"
)
HKO_FND_URL = (
    "https://data.weather.gov.hk/weatherAPI/opendata/weather.php"
    "?dataType=fnd&lang=en"
)
CLOB_BOOK_URL = CLOB_API + "/book?token_id={token_id}"

HK_RESOLUTION_HOUR_UTC = 16   # 16:00 UTC = midnight HKT


# ---------------------------------------------------------------------------
# 1. fetch_hko_current_temp
# ---------------------------------------------------------------------------
def fetch_hko_current_temp() -> float | None:
    """
    Return current air temperature (°C) at HK Observatory.
    Caches result for 10 minutes. Returns None on any error.
    """
    now = time.time()
    cached = _temp_cache.get("value")
    if cached is not None and (now - _temp_cache.get("ts", 0)) < _TEMP_CACHE_TTL:
        return cached

    try:
        resp = requests.get(HKO_RHRREAD_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # temperature.data is a list of station readings
        for station in data.get("temperature", {}).get("data", []):
            if "Hong Kong Observatory" in station.get("place", ""):
                temp = float(station["value"])
                _temp_cache["value"] = temp
                _temp_cache["ts"] = now
                return temp
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# 2. fetch_hko_forecast_max
# ---------------------------------------------------------------------------
def fetch_hko_forecast_max(date_str: str | None = None) -> float | None:
    """
    Return forecast maximum temperature (°C) for date_str from HKO 9-day forecast.
    date_str format: "YYYYMMDD". Defaults to today.
    Returns None on any error.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    try:
        resp = requests.get(HKO_FND_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        for day in data.get("weatherForecast", []):
            if day.get("forecastDate") == date_str:
                max_temp = day.get("forecastMaxtemp", {}).get("value")
                if max_temp is not None:
                    return float(max_temp)
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# 3. hours_to_resolution
# ---------------------------------------------------------------------------
def hours_to_resolution(market_end_date_str: str) -> float:
    """
    Return hours from now until market resolution.
    HK markets resolve at 16:00 UTC on market_end_date_str.
    Negative value means already resolved.

    market_end_date_str: ISO date string "YYYY-MM-DD"
    """
    end_date = date.fromisoformat(market_end_date_str)
    resolution_dt = datetime(
        end_date.year, end_date.month, end_date.day,
        HK_RESOLUTION_HOUR_UTC, 0, 0,
        tzinfo=timezone.utc,
    )
    now_utc = datetime.now(timezone.utc)
    delta = resolution_dt - now_utc
    return delta.total_seconds() / 3600.0


# ---------------------------------------------------------------------------
# 4. should_use_maker
# ---------------------------------------------------------------------------
def should_use_maker(market_spread: float, hours_to_resolve: float) -> bool:
    """
    Return True to use passive maker order, False to cross with taker order.

    Maker if: spread > 0.03 AND hours_to_resolve > 6
    Taker if: hours_to_resolve < 4 OR spread <= 0.02

    Maker orders historically outperform by 2.5pp (post-Oct 2024 fee change).
    """
    if hours_to_resolve < 4:
        return False
    if market_spread <= 0.02:
        return False
    if market_spread > 0.03 and hours_to_resolve > 6:
        return True
    # Default: use maker when not urgent and spread is reasonable
    return True


def get_book_snapshot(token_id: str, force_refresh: bool = False) -> dict | None:
    now = time.time()
    entry = _BOOK_CACHE.get(token_id)
    if not force_refresh and entry is not None and (now - entry["ts"]) < _BOOK_CACHE_TTL:
        return entry["data"]
    try:
        url = CLOB_BOOK_URL.format(token_id=token_id)
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        book = resp.json()
        bids = sorted(
            (
                {"price": float(level["price"]), "size": float(level["size"])}
                for level in (book.get("bids") or [])
                if level.get("price") is not None and level.get("size") is not None
            ),
            key=lambda level: level["price"],
            reverse=True,
        )
        asks = sorted(
            (
                {"price": float(level["price"]), "size": float(level["size"])}
                for level in (book.get("asks") or [])
                if level.get("price") is not None and level.get("size") is not None
            ),
            key=lambda level: level["price"],
        )
        best_bid = max((float(b["price"]) for b in bids), default=None) if bids else None
        best_ask = min((float(a["price"]) for a in asks), default=None) if asks else None
        if best_bid is None and best_ask is None:
            return None
        if best_bid is None:
            best_bid = best_ask
        if best_ask is None:
            best_ask = best_bid
        data = {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "bid": round(best_bid, 4),
            "ask": round(best_ask, 4),
            "mid": round((best_bid + best_ask) / 2.0, 4),
            "spread": round(best_ask - best_bid, 4),
            "bid_depth": round(sum(level["size"] for level in bids), 4),
            "ask_depth": round(sum(level["size"] for level in asks), 4),
            "bid_notional": round(sum(level["price"] * level["size"] for level in bids), 4),
            "ask_notional": round(sum(level["price"] * level["size"] for level in asks), 4),
            "bids": [
                {"price": round(level["price"], 4), "size": round(level["size"], 4)}
                for level in bids
            ],
            "asks": [
                {"price": round(level["price"], 4), "size": round(level["size"], 4)}
                for level in asks
            ],
        }
        _BOOK_CACHE[token_id] = {"data": data, "ts": now}
        return data
    except Exception:
        return None


def estimate_fill_from_book(
    book: dict | None,
    size_usdc: float,
    side: int = 0,
    reference_price: float | None = None,
) -> dict:
    """
    Walk visible book depth to estimate marketable fill quality.

    BUY side:
      target shares are inferred from size_usdc and reference_price.
    SELL side:
      size_usdc is treated as share size for compatibility with existing sell flow.
    """
    if not book:
        return {
            "fillable": False,
            "estimated_avg_price": None,
            "filled_shares": 0.0,
            "filled_notional": 0.0,
            "unfilled_shares": None,
            "levels_used": 0,
        }

    if side == 0:
        ref = reference_price or book.get("ask") or book.get("mid") or 0.0
        if ref <= 0:
            return {
                "fillable": False,
                "estimated_avg_price": None,
                "filled_shares": 0.0,
                "filled_notional": 0.0,
                "unfilled_shares": None,
                "levels_used": 0,
            }
        target_shares = size_usdc / ref
        levels = list(book.get("asks") or [])
    else:
        target_shares = max(size_usdc, 0.0)
        levels = list(book.get("bids") or [])

    remaining = target_shares
    filled_shares = 0.0
    filled_notional = 0.0
    levels_used = 0

    for level in levels:
        price = float(level.get("price", 0.0) or 0.0)
        size = float(level.get("size", 0.0) or 0.0)
        if price <= 0 or size <= 0:
            continue
        take = min(size, remaining)
        if take <= 0:
            break
        filled_shares += take
        filled_notional += take * price
        remaining -= take
        levels_used += 1
        if remaining <= 1e-9:
            remaining = 0.0
            break

    avg_price = (filled_notional / filled_shares) if filled_shares > 0 else None
    return {
        "fillable": remaining <= 1e-9 and filled_shares > 0,
        "estimated_avg_price": round(avg_price, 6) if avg_price is not None else None,
        "filled_shares": round(filled_shares, 6),
        "filled_notional": round(filled_notional, 6),
        "unfilled_shares": round(remaining, 6),
        "levels_used": levels_used,
    }


def capture_trade_snapshot(
    token_id: str,
    signal_price: float,
    size_usdc: float,
    side: int = 0,
    hours_to_resolve: float = 24,
    setup_type: str = "",
    force_refresh: bool = False,
) -> dict:
    """
    Capture a point-in-time market snapshot for one trade decision.

    This does not create a separate artifact file; it returns a JSON-serializable
    structure intended to be embedded into the canonical trade log.
    """
    book = get_book_snapshot(token_id, force_refresh=force_refresh)
    fill_estimate = estimate_fill_from_book(book, size_usdc, side=side, reference_price=signal_price)
    stale = detect_stale_liquidity(
        token_id,
        signal_price,
        side=side,
        hours_to_resolve=hours_to_resolve,
        setup_type=setup_type,
    )
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "token_id": str(token_id),
        "signal_price": round(float(signal_price), 6) if signal_price is not None else None,
        "size_usdc": round(float(size_usdc), 6),
        "side": "BUY" if side == 0 else "SELL",
        "hours_to_resolution": round(float(hours_to_resolve), 4) if hours_to_resolve is not None else None,
        "setup_type": setup_type or None,
        "book": book,
        "fill_estimate": fill_estimate,
        "stale_quote": {
            "is_stale": stale.get("is_stale", False),
            "edge": stale.get("edge"),
            "execution_price": stale.get("execution_price"),
        },
    }


def detect_stale_liquidity(
    token_id: str,
    signal_price: float,
    side: int = 0,
    hours_to_resolve: float = 24,
    setup_type: str = "",
) -> dict:
    """
    Check whether the visible top-of-book looks stale relative to the signal.

    BUY side:
      if best ask is materially below signal price, we want to cross and take it.
    SELL side:
      if best bid is materially above signal price, we want to hit it.
    """
    book = get_book_snapshot(token_id)
    if not book:
        return {"is_stale": False, "book": None}

    threshold = 0.01
    if setup_type in {"delta_lag", "bucket_sum_arb", "precision_bracket"}:
        threshold = 0.015
    if hours_to_resolve <= 2:
        threshold = min(threshold, 0.01)

    if side == 0:
        stale_edge = signal_price - book["ask"]
        is_stale = stale_edge >= threshold
        execution_price = book["ask"] if is_stale else signal_price
    else:
        stale_edge = book["bid"] - signal_price
        is_stale = stale_edge >= threshold
        execution_price = book["bid"] if is_stale else signal_price

    return {
        "is_stale": is_stale,
        "edge": round(stale_edge, 4),
        "execution_price": round(max(0.01, min(0.99, execution_price)), 4),
        "book": book,
    }


# ---------------------------------------------------------------------------
# 5. get_entry_score
# ---------------------------------------------------------------------------
def get_entry_score(signal: dict, current_hour_utc: int) -> float:
    """
    Return combined entry score in [0, 1].

    Best entry window: 12-15 UTC (4 hours before HK resolution)
      score = 1.0
    Secondary window: 8-12 or 15-17 UTC
      score = 0.7
    Otherwise:
      score = 0.4

    Multiplied by relative EV quality: signal["ev"] / max_ev.
    signal must contain keys: "ev" and "max_ev"
    """
    hour = current_hour_utc

    if 12 <= hour <= 15:
        time_score = 1.0
    elif (8 <= hour < 12) or (15 < hour <= 17):
        time_score = 0.7
    else:
        time_score = 0.4

    ev = signal.get("ev", 0.0)
    max_ev = signal.get("max_ev", 1.0)
    if max_ev <= 0:
        max_ev = 1.0

    ev_ratio = max(0.0, min(1.0, ev / max_ev))
    return time_score * ev_ratio


# ---------------------------------------------------------------------------
# 6. compute_maker_price
# ---------------------------------------------------------------------------
def compute_maker_price(signal_price: float, side: int, spread_pct: float = 0.01) -> float:
    """
    Compute limit order price for a maker (passive) order.

    side: 0 = BUY, 1 = SELL
    BUY:  limit at signal_price - spread_pct  (slightly below ask, queue up)
    SELL: limit at signal_price + spread_pct  (slightly above bid, queue up)

    Rounded to nearest 0.01.
    Caps: BUY >= 0.02, SELL <= 0.98
    """
    if side == 0:  # BUY
        price = signal_price - spread_pct
        price = max(0.02, price)
    else:          # SELL
        price = signal_price + spread_pct
        price = min(0.98, price)

    return round(price, 2)


# ---------------------------------------------------------------------------
# 7. monitor_hk_for_entry
# ---------------------------------------------------------------------------
def monitor_hk_for_entry(
    signals: list,
    check_interval_minutes: int = 15,
    max_wait_hours: float = 4,
) -> list:
    """
    Blocking poll: wait for optimal HK entry window (12-15 UTC), then return
    signals with entry_score > 0.5.

    If max_wait_hours exceeded before window opens, return all signals anyway
    (urgency override).

    Intended for use inside a cron that runs hourly; will block up to
    max_wait_hours in the worst case.
    """
    interval_seconds = check_interval_minutes * 60
    deadline = time.time() + max_wait_hours * 3600

    while True:
        now_utc = datetime.now(timezone.utc)
        current_hour = now_utc.hour

        in_window = 12 <= current_hour <= 15
        timed_out = time.time() >= deadline

        if in_window or timed_out:
            scored = []
            for sig in signals:
                score = get_entry_score(sig, current_hour)
                if score > 0.5 or timed_out:
                    enriched = dict(sig)
                    enriched["entry_score"] = score
                    enriched["entry_triggered_by"] = (
                        "timeout_override" if timed_out else "optimal_window"
                    )
                    scored.append(enriched)
            return scored

        time.sleep(interval_seconds)


# ---------------------------------------------------------------------------
# 8. place_smart_order
# ---------------------------------------------------------------------------
def place_smart_order(
    token_id: str,
    price: float,
    size_usdc: float,
    dry_run: bool,
    spread: float = 0.03,
    hours_to_resolve: float = 24,
    setup_type: str = "",
) -> tuple[float, str, str]:
    """
    Determine optimal execution parameters without placing the order.

    Returns (final_price, order_type_str, execution_notes).
    Does NOT call place_order — returns parameters for the caller to act on.
    """
    stale = detect_stale_liquidity(token_id, price, side=0, hours_to_resolve=hours_to_resolve, setup_type=setup_type)
    if stale.get("is_stale"):
        final_price = stale["execution_price"]
        order_type_str = "STALE_TAKER"
        notes = (
            f"Cross stale ask at {final_price:.2f} "
            f"(signal={price:.2f}, stale_edge={stale['edge']:.3f}, "
            f"setup={setup_type or 'generic'})."
        )
        if dry_run:
            notes = "[DRY_RUN] " + notes
        return final_price, order_type_str, notes

    use_maker = should_use_maker(spread, hours_to_resolve)

    if use_maker:
        # Default to BUY side (side=0); caller adjusts if selling
        final_price = compute_maker_price(price, side=0)
        order_type_str = "MAKER_LIMIT"
        notes = (
            f"Passive limit at {final_price:.2f} "
            f"(spread={spread:.3f}, hrs_to_resolve={hours_to_resolve:.1f}h). "
            "Expected +2.5pp vs taker."
        )
    else:
        final_price = price
        order_type_str = "TAKER_MARKET"
        reason = (
            "urgency (< 4h)" if hours_to_resolve < 4
            else "tight spread (<= 0.02)"
        )
        notes = (
            f"Cross spread at {final_price:.2f} — {reason}. "
            f"size={size_usdc:.2f} USDC."
        )

    if dry_run:
        notes = "[DRY_RUN] " + notes

    return final_price, order_type_str, notes


# ---------------------------------------------------------------------------
# 9. get_market_spread
# ---------------------------------------------------------------------------
def get_market_spread(token_id: str) -> float | None:
    """
    Return best_ask - best_bid for token_id from CLOB order book.
    Caches result for 5 minutes. Returns None on any error.
    """
    now = time.time()
    entry = _spread_cache.get(token_id)
    if entry is not None and (now - entry["ts"]) < _SPREAD_CACHE_TTL:
        return entry["value"]

    try:
        url = CLOB_BOOK_URL.format(token_id=token_id)
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        book = resp.json()

        bids = book.get("bids", [])
        asks = book.get("asks", [])

        if not bids or not asks:
            return None

        best_bid = max(float(b["price"]) for b in bids)
        best_ask = min(float(a["price"]) for a in asks)
        spread = best_ask - best_bid

        _spread_cache[token_id] = {"value": spread, "ts": now}
        return spread

    except Exception:
        return None


# ---------------------------------------------------------------------------
# 10. hko_temp_edge
# ---------------------------------------------------------------------------
def hko_temp_edge(signal: dict, coeffs: dict) -> float:
    """
    Compare live HKO current temp to the model forecast encoded in signal.

    signal must contain:
      - "bucket_low":  float  (lower bound of temperature bucket)
      - "bucket_high": float  (upper bound of temperature bucket)
      - "forecast_mean": float (model's expected temp)

    coeffs (unused directly but available for future calibration):
      - any calibration coefficients

    Returns edge multiplier (multiply signal EV by 1 + edge):
      -1.0  temp already exceeds bucket_high at 2pm HKT → signal is stale, avoid
       0.5  current temp trending toward bucket → slight positive edge
       0.0  current temp within expected range → no update

    Staleness check: if current HKT hour >= 14 (2pm HKT) and current temp
    already exceeds bucket_high, the market will likely resolve outside the
    predicted bucket.
    """
    current_temp = fetch_hko_current_temp()
    if current_temp is None:
        return 0.0

    bucket_low = signal.get("bucket_low", 0.0)
    bucket_high = signal.get("bucket_high", 100.0)
    forecast_mean = signal.get("forecast_mean", (bucket_low + bucket_high) / 2)

    # HKT = UTC + 8
    hkt_hour = (datetime.now(timezone.utc).hour + 8) % 24

    # Stale signal: past 2pm HKT and temp already above bucket
    if hkt_hour >= 14 and current_temp > bucket_high:
        return -1.0

    # Trending toward bucket: current temp is within 1°C below bucket and not yet there
    distance_to_low = bucket_low - current_temp
    if 0 < distance_to_low <= 1.0:
        return 0.5

    # Trending up into bucket from inside
    if bucket_low <= current_temp <= bucket_high:
        # Check if closer to forecast_mean than expected — slight positive confirmation
        deviation = abs(current_temp - forecast_mean)
        if deviation < 1.0:
            return 0.5

    # Current temp already above bucket high (before 2pm) — may resolve out of bucket
    if current_temp > bucket_high:
        return -1.0

    return 0.0


# ---------------------------------------------------------------------------
# CLI smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("HKO current temp:", fetch_hko_current_temp())
    print("HKO forecast max:", fetch_hko_forecast_max())
    print("Hours to next resolution:", hours_to_resolution(
        (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()
    ))
