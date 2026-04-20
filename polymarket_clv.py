"""
Closing Line Value (CLV) Tracker
=================================
Ported from sports betting (Pinnacle/Buchdahl methodology).

CLV = market_close_price - your_entry_price  (for BUY YES)
    = your_entry_price - market_close_price  (for BUY NO)

If 14-day average CLV > 0, you have edge. Period.
Faster signal than waiting for resolution (14 CLV readings = same power as 60 resolved trades).

Run: python3 polymarket_clv.py          # update CLV for all open positions
     python3 polymarket_clv.py --report # print CLV report
"""

import argparse, json, os, statistics, time
from datetime import datetime, timezone, timedelta

import requests

from polymarket_core import ENV, TRADES_JSONL, CLOB_API

# ─── PRICE CACHE ──────────────────────────────────────────────────────────────
_PRICE_CACHE: dict[str, tuple[float, float]] = {}  # token_id → (mid_price, fetched_ts)
_CACHE_TTL = 300  # 5 minutes


def _as_float(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _execution_metrics(trade: dict) -> dict:
    audit = trade.get("execution_audit") or {}
    decision_fill = audit.get("decision_fill_estimate") or {}
    has_audit = bool(audit)
    signal_price = _as_float(audit.get("signal_price"))
    est_avg = _as_float(decision_fill.get("estimated_avg_price"))
    est_slip = est_avg - signal_price if est_avg is not None and signal_price is not None else None
    return {
        "decision_spread": _as_float(audit.get("decision_spread")),
        "decision_ask_depth": _as_float(audit.get("decision_ask_depth")),
        "decision_bid_depth": _as_float(audit.get("decision_bid_depth")),
        "estimated_slippage": est_slip,
        "realized_slippage": _as_float(audit.get("slippage_vs_signal")),
        "fillable": 1.0 if decision_fill.get("fillable") is True else 0.0 if decision_fill else None,
        "realized_fill": (
            1.0 if _as_float(audit.get("realized_avg_price")) is not None else 0.0
        ) if has_audit else None,
    }


def _summarize_trade_group(trades: list[dict]) -> dict:
    clv_vals = [_as_float(t.get("clv")) for t in trades]
    clv_vals = [v for v in clv_vals if v is not None]

    spreads = []
    ask_depths = []
    bid_depths = []
    est_slippage = []
    realized_slippage = []
    fillable = []
    realized_fill = []

    for trade in trades:
        metrics = _execution_metrics(trade)
        if metrics["decision_spread"] is not None:
            spreads.append(metrics["decision_spread"])
        if metrics["decision_ask_depth"] is not None:
            ask_depths.append(metrics["decision_ask_depth"])
        if metrics["decision_bid_depth"] is not None:
            bid_depths.append(metrics["decision_bid_depth"])
        if metrics["estimated_slippage"] is not None:
            est_slippage.append(metrics["estimated_slippage"])
        if metrics["realized_slippage"] is not None:
            realized_slippage.append(metrics["realized_slippage"])
        if metrics["fillable"] is not None:
            fillable.append(metrics["fillable"])
        if metrics["realized_fill"] is not None:
            realized_fill.append(metrics["realized_fill"])

    summary = {"n": len(trades)}
    if clv_vals:
        summary["mean_clv"] = round(statistics.mean(clv_vals), 6)
        summary["positive_rate"] = round(sum(1 for v in clv_vals if v > 0) / len(clv_vals), 4)
    else:
        summary["mean_clv"] = None
        summary["positive_rate"] = None

    summary["mean_spread"] = round(statistics.mean(spreads), 6) if spreads else None
    summary["mean_ask_depth"] = round(statistics.mean(ask_depths), 4) if ask_depths else None
    summary["mean_bid_depth"] = round(statistics.mean(bid_depths), 4) if bid_depths else None
    summary["mean_est_slippage"] = round(statistics.mean(est_slippage), 6) if est_slippage else None
    summary["mean_realized_slippage"] = round(statistics.mean(realized_slippage), 6) if realized_slippage else None
    summary["fillable_rate"] = round(statistics.mean(fillable), 4) if fillable else None
    summary["realized_fill_rate"] = round(statistics.mean(realized_fill), 4) if realized_fill else None
    return summary


# ─── 1. FETCH MARKET CLOSE PRICE ──────────────────────────────────────────────
def fetch_market_close_price(token_id: str):
    """Return mid price (best_bid + best_ask)/2 from CLOB order book.
    Returns None if book is empty or market has resolved.
    Results are cached for 5 minutes.
    """
    now = time.time()
    cached = _PRICE_CACHE.get(token_id)
    if cached and now - cached[1] < _CACHE_TTL:
        return cached[0]

    try:
        url = f"{CLOB_API}/book?token_id={token_id}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        book = resp.json()

        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None

        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        mid = (best_bid + best_ask) / 2.0

        _PRICE_CACHE[token_id] = (mid, now)
        return mid
    except Exception:
        return None


# ─── 2. IS NEAR CLOSE ─────────────────────────────────────────────────────────
def is_near_close(end_date_str: str) -> bool:
    """Return True if we are within the CLV measurement window (14:00–16:00 UTC)
    on the resolution date. HK markets resolve at 16:00 UTC.
    end_date_str is ISO date e.g. '2026-04-21'.
    """
    try:
        resolution_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        # Window: 14:00 UTC to 16:00 UTC on resolution day
        window_start = resolution_date.replace(hour=14, minute=0, second=0)
        window_end = resolution_date.replace(hour=16, minute=0, second=0)
        now = datetime.now(timezone.utc)
        return window_start <= now <= window_end
    except (ValueError, TypeError):
        return False


# ─── 3. UPDATE CLV ────────────────────────────────────────────────────────────
def update_clv(trades_jsonl_path: str) -> list[dict]:
    """For each eligible open trade near resolution, measure CLV and persist it.

    Eligible: clv is None/missing, outcome is None, dry_run is False,
    and the trade end_date is within 14:00–16:00 UTC today.

    Returns list of trades where CLV was written this run.
    """
    if not os.path.exists(trades_jsonl_path):
        print(f"[CLV] trades file not found: {trades_jsonl_path}")
        return []

    with open(trades_jsonl_path, "r") as fh:
        trades = [json.loads(line) for line in fh if line.strip()]

    updated = []
    for trade in trades:
        # Skip dry runs
        if trade.get("dry_run", True):
            continue
        # Skip already-resolved trades
        if trade.get("outcome") is not None:
            continue
        # Skip already-exited trades
        if trade.get("exit_ts"):
            continue
        # Skip trades that already have CLV
        if trade.get("clv") is not None:
            continue
        # Skip trades not in the measurement window
        end_date = trade.get("end_date") or trade.get("end_date_iso") or ""
        if not is_near_close(end_date):
            continue

        token_id = trade.get("token_id")
        if not token_id:
            continue

        current_price = fetch_market_close_price(token_id)
        if current_price is None:
            continue

        entry_price = float(trade.get("entry_price", trade.get("price", 0)))
        direction = trade.get("direction", "BUY YES").upper()

        if "NO" in direction:
            clv = entry_price - current_price
        else:  # BUY YES (default)
            clv = current_price - entry_price

        trade["clv"] = round(clv, 6)
        trade["clv_measured_at"] = datetime.now(timezone.utc).isoformat()
        trade["clv_price"] = round(current_price, 6)
        updated.append(trade)

    if updated:
        with open(trades_jsonl_path, "w") as fh:
            for t in trades:
                fh.write(json.dumps(t) + "\n")

    return updated


# ─── 4. COMPUTE CLV STATS ─────────────────────────────────────────────────────
def compute_clv_stats(trades_jsonl_path: str) -> dict:
    """Aggregate CLV metrics across all measured trades."""
    if not os.path.exists(trades_jsonl_path):
        return _empty_stats()

    with open(trades_jsonl_path, "r") as fh:
        trades = [json.loads(line) for line in fh if line.strip()]

    measured = [t for t in trades if t.get("clv") is not None]
    if not measured:
        return _empty_stats()

    clv_values = [t["clv"] for t in measured]
    n = len(clv_values)
    mean_clv = statistics.mean(clv_values)
    median_clv = statistics.median(clv_values)
    positive_rate = sum(1 for v in clv_values if v > 0) / n

    # By-source breakdown
    by_source: dict[str, list[dict]] = {}
    for t in measured:
        src = t.get("source", "unknown")
        by_source.setdefault(src, []).append(t)

    by_city: dict[str, list[dict]] = {}
    for t in measured:
        city = (t.get("city") or "unknown").lower()
        by_city.setdefault(city, []).append(t)

    by_setup: dict[str, list[dict]] = {}
    by_arm: dict[str, list[dict]] = {}
    for t in measured:
        setup = t.get("setup_type") or "unknown"
        arm = t.get("strategy_arm") or setup
        by_setup.setdefault(setup, []).append(t)
        by_arm.setdefault(arm, []).append(t)

    # Rolling windows — sort by clv_measured_at
    def _measured_at(t):
        ts = t.get("clv_measured_at", "")
        try:
            return datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            return datetime.min.replace(tzinfo=timezone.utc)

    measured_sorted = sorted(measured, key=_measured_at)
    now = datetime.now(timezone.utc)

    def _rolling(days: int):
        cutoff = now - timedelta(days=days)
        vals = [
            t["clv"]
            for t in measured_sorted
            if _measured_at(t) >= cutoff
        ]
        return statistics.mean(vals) if vals else None

    rolling_7d = _rolling(7)
    rolling_14d = _rolling(14)

    # Trend
    if rolling_7d is not None and rolling_14d is not None:
        diff = rolling_7d - rolling_14d
        if diff > 0.005:
            trend = "improving"
        elif diff < -0.005:
            trend = "decaying"
        else:
            trend = "stable"
    else:
        trend = "stable"

    # Verdict
    r14 = rolling_14d if rolling_14d is not None else 0.0
    if n >= 14 and r14 > 0:
        verdict = "EDGE CONFIRMED"
    elif n >= 14 and r14 < 0:
        verdict = "NO EDGE — KILL"
    else:
        verdict = "INSUFFICIENT DATA"

    return {
        "n": n,
        "mean_clv": round(mean_clv, 6),
        "median_clv": round(median_clv, 6),
        "positive_rate": round(positive_rate, 4),
        "execution_summary": _summarize_trade_group(measured),
        "by_source": {k: _summarize_trade_group(v) for k, v in by_source.items()},
        "by_city": {k: _summarize_trade_group(v) for k, v in by_city.items()},
        "by_setup": {k: _summarize_trade_group(v) for k, v in by_setup.items()},
        "by_arm": {k: _summarize_trade_group(v) for k, v in by_arm.items()},
        "rolling_7d": round(rolling_7d, 6) if rolling_7d is not None else None,
        "rolling_14d": round(rolling_14d, 6) if rolling_14d is not None else None,
        "trend": trend,
        "verdict": verdict,
        "_measured_trades": measured_sorted,  # internal — used by print_clv_report
    }


def _empty_stats() -> dict:
    return {
        "n": 0,
        "mean_clv": None,
        "median_clv": None,
        "positive_rate": None,
        "by_source": {},
        "by_city": {},
        "by_setup": {},
        "by_arm": {},
        "rolling_7d": None,
        "rolling_14d": None,
        "trend": "stable",
        "verdict": "INSUFFICIENT DATA",
        "_measured_trades": [],
    }


def _fmt_num(value, places: int = 4) -> str:
    if value is None:
        return "N/A"
    return f"{value:+.{places}f}"


def _fmt_pct(value, places: int = 1) -> str:
    if value is None:
        return "N/A"
    return f"{value*100:.{places}f}%"


# ─── 5. PRINT CLV REPORT ──────────────────────────────────────────────────────
def _sparkline(daily_clvs: list[float]) -> str:
    """ASCII sparkline: + above zero, - below, = exactly zero."""
    line = ""
    for v in daily_clvs:
        if v > 0.001:
            line += "+"
        elif v < -0.001:
            line += "-"
        else:
            line += "="
    return line or "(no data)"


def print_clv_report(stats: dict) -> None:
    verdict = stats["verdict"]
    r14 = stats.get("rolling_14d")
    r7 = stats.get("rolling_7d")
    n = stats["n"]

    border = "═" * 55
    print(f"\n{border}")
    print(f"  CLV REPORT  —  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(border)

    # Prominent verdict
    if verdict == "EDGE CONFIRMED":
        print(f"  VERDICT: ✅  {verdict}")
    elif verdict == "NO EDGE — KILL":
        print(f"  VERDICT: ⛔  {verdict}")
    else:
        print(f"  VERDICT: ⚠️   {verdict}")

    print(f"\n  Readings (n):    {n}")
    mean_str = f"{stats['mean_clv']:+.4f}" if stats["mean_clv"] is not None else "N/A"
    med_str  = f"{stats['median_clv']:+.4f}" if stats["median_clv"] is not None else "N/A"
    pr_str   = f"{stats['positive_rate']*100:.1f}%" if stats["positive_rate"] is not None else "N/A"
    print(f"  Mean CLV:        {mean_str}")
    print(f"  Median CLV:      {med_str}")
    print(f"  Positive rate:   {pr_str}")
    r7_str  = f"{r7:+.4f}" if r7 is not None else "N/A"
    r14_str = f"{r14:+.4f}" if r14 is not None else "N/A"
    print(f"  Rolling 7d CLV:  {r7_str}")
    print(f"  Rolling 14d CLV: {r14_str}  [{stats['trend'].upper()}]")

    exec_summary = stats.get("execution_summary") or {}
    print(f"\n  Execution quality:")
    print(f"    Mean spread:           {_fmt_num(exec_summary.get('mean_spread'))}")
    print(f"    Mean est slippage:     {_fmt_num(exec_summary.get('mean_est_slippage'))}")
    print(f"    Mean realized slip:    {_fmt_num(exec_summary.get('mean_realized_slippage'))}")
    print(f"    Est fillable rate:     {_fmt_pct(exec_summary.get('fillable_rate'))}")
    print(f"    Realized fill rate:    {_fmt_pct(exec_summary.get('realized_fill_rate'))}")

    # Sparkline — one symbol per day for last 14 days
    measured = stats.get("_measured_trades", [])
    if measured:
        def _ts(t):
            try:
                return datetime.fromisoformat(t.get("clv_measured_at", ""))
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        buckets: dict[str, list[float]] = {}
        for t in measured:
            ts = _ts(t)
            if now - ts <= timedelta(days=14):
                day = ts.strftime("%Y-%m-%d")
                buckets.setdefault(day, []).append(t["clv"])
        daily = [statistics.mean(v) for _, v in sorted(buckets.items())]
        print(f"\n  CLV sparkline (14d, each char = 1 day):")
        print(f"  [{_sparkline(daily)}]")

    # By-source breakdown
    if stats["by_source"]:
        print(f"\n  By signal source:")
        for src, d in sorted(stats["by_source"].items()):
            print(f"    {src:<18}  n={d['n']}  mean CLV={d['mean_clv']:+.4f}")

    # By-city breakdown
    if stats["by_city"]:
        print(f"\n  By city:")
        for city, d in sorted(stats["by_city"].items()):
            print(f"    {city:<20}  n={d['n']}  mean CLV={d['mean_clv']:+.4f}")

    if stats["by_setup"]:
        print(f"\n  By setup:")
        for setup, d in sorted(stats["by_setup"].items()):
            print(
                f"    {setup:<20}  n={d['n']}  mean CLV={_fmt_num(d.get('mean_clv'))}  "
                f"spread={_fmt_num(d.get('mean_spread'))}  "
                f"estSlip={_fmt_num(d.get('mean_est_slippage'))}  "
                f"realSlip={_fmt_num(d.get('mean_realized_slippage'))}"
            )

    if stats["by_arm"]:
        print(f"\n  By strategy arm:")
        for arm, d in sorted(stats["by_arm"].items()):
            print(
                f"    {arm:<20}  n={d['n']}  mean CLV={_fmt_num(d.get('mean_clv'))}  "
                f"spread={_fmt_num(d.get('mean_spread'))}  "
                f"estFill={_fmt_pct(d.get('fillable_rate'))}  "
                f"realFill={_fmt_pct(d.get('realized_fill_rate'))}"
            )

    # Kill / scale signals
    print()
    if r14 is not None and n >= 14:
        if r14 < 0:
            print("  ⛔  KILL SIGNAL — negative CLV for 14 days.")
            print("      Stop trading, revisit hypothesis.")
        elif r14 > 0.05:
            print("  ✅  SCALE SIGNAL — CLV positive for 14 days.")
            print("      Increase position size.")

    print(f"{border}\n")


# ─── 6. MAIN ──────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="CLV Tracker")
    parser.add_argument("--report", action="store_true", help="Print CLV report")
    args = parser.parse_args()

    if args.report:
        stats = compute_clv_stats(TRADES_JSONL)
        print_clv_report(stats)
    else:
        updated = update_clv(TRADES_JSONL)
        if updated:
            print(f"[CLV] Updated {len(updated)} trade(s):")
            for t in updated:
                q = t.get("question", t.get("token_id", "?"))[:60]
                print(f"  CLV={t['clv']:+.4f}  entry={t.get('entry_price', '?')}  "
                      f"price@measure={t['clv_price']}  — {q}")
        else:
            print("[CLV] No trades eligible for CLV measurement right now.")
            print("      (Markets must be within 14:00–16:00 UTC on resolution date.)")


if __name__ == "__main__":
    main()
