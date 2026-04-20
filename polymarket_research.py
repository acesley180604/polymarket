"""
Research Pipeline — Hypothesis Testing & Strategy Monitoring
=============================================================
Ported from systematic fund research pipeline (Two Sigma/AQR/Citadel style).

Three hypotheses to test for HK weather:
  H1: Morning HKO temp ratio vs ECMWF → predicts afternoon max direction
  H2: ECMWF-GFS spread > 1.5°C → tails underpriced
  H3: Bucket probability sum > 1.05 → overpriced buckets (longshot bias)

Metric: Information Coefficient (IC) = rank correlation(signal, outcome)
Kill rule: IC <= 0 for 14 consecutive readings → kill hypothesis
"""

import argparse, json, os, statistics
from datetime import datetime, timezone, timedelta

import requests

from polymarket_core import ENV, TRADES_JSONL, CLOB_API, HYPOTHESES_JSON

# ─── PATHS ────────────────────────────────────────────────────────────────────
HYPOTHESES_PATH = HYPOTHESES_JSON

# ─── DEFAULT HYPOTHESIS SCHEMA ────────────────────────────────────────────────
DEFAULT_HYPOTHESES = {
    "H1": {
        "name": "Morning temp ratio",
        "signal": "HKO_08:00_actual / ECMWF_forecast",
        "prediction": "ratio > 0.96 → actual max exceeds consensus bucket",
        "mechanism": "Diurnal heating established by 09:00 HKT persists",
        "status": "testing",
        "trades": [],
    },
    "H2": {
        "name": "Model disagreement",
        "signal": "abs(ECMWF_mean - GFS_mean)",
        "prediction": "spread > 1.5°C → market underprices tail buckets",
        "mechanism": "Model spread = genuine atmospheric uncertainty retail ignores",
        "status": "testing",
        "trades": [],
    },
    "H3": {
        "name": "Longshot bias",
        "signal": "sum of all YES bucket prices for city on that day",
        "prediction": "sum > 1.05 → cheap buckets overpriced",
        "mechanism": "Retail traders overpay cheap buckets (longshot bias)",
        "status": "testing",
        "trades": [],
    },
}


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
    return {
        "spread": _as_float(audit.get("decision_spread")),
        "est_slippage": _as_float(audit.get("decision_fill_estimate", {}).get("estimated_avg_price")),
        "signal_price": _as_float(audit.get("signal_price")),
        "realized_slippage": _as_float(audit.get("slippage_vs_signal")),
        "fillable": 1.0 if decision_fill.get("fillable") is True else 0.0 if decision_fill else None,
        "realized_fill": (
            1.0 if _as_float(audit.get("realized_avg_price")) is not None else 0.0
        ) if has_audit else None,
    }


def _execution_group_summary(trades: list[dict]) -> dict:
    spreads = []
    est_slips = []
    real_slips = []
    fillable = []
    realized_fill = []

    for trade in trades:
        metrics = _execution_metrics(trade)
        if metrics["spread"] is not None:
            spreads.append(metrics["spread"])
        est_avg = metrics["est_slippage"]
        signal_price = metrics["signal_price"]
        if est_avg is not None and signal_price is not None:
            est_slips.append(est_avg - signal_price)
        if metrics["realized_slippage"] is not None:
            real_slips.append(metrics["realized_slippage"])
        if metrics["fillable"] is not None:
            fillable.append(metrics["fillable"])
        if metrics["realized_fill"] is not None:
            realized_fill.append(metrics["realized_fill"])

    return {
        "n": len(trades),
        "mean_spread": round(statistics.mean(spreads), 6) if spreads else None,
        "mean_est_slippage": round(statistics.mean(est_slips), 6) if est_slips else None,
        "mean_realized_slippage": round(statistics.mean(real_slips), 6) if real_slips else None,
        "fillable_rate": round(statistics.mean(fillable), 4) if fillable else None,
        "realized_fill_rate": round(statistics.mean(realized_fill), 4) if realized_fill else None,
    }


def _fmt_num(value, places: int = 4) -> str:
    if value is None:
        return "N/A"
    return f"{value:+.{places}f}"


def _fmt_pct(value, places: int = 1) -> str:
    if value is None:
        return "N/A"
    return f"{value*100:.{places}f}%"


# ─── HYPOTHESIS FILE HELPERS ──────────────────────────────────────────────────
def _load_hypotheses(path: str = HYPOTHESES_PATH) -> dict:
    if os.path.exists(path):
        with open(path, "r") as fh:
            return json.load(fh)
    return {k: dict(v) for k, v in DEFAULT_HYPOTHESES.items()}


def _save_hypotheses(hypotheses: dict, path: str = HYPOTHESES_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(hypotheses, fh, indent=2)


# ─── 2. RECORD HYPOTHESIS TRADE ───────────────────────────────────────────────
def record_hypothesis_trade(
    hypothesis_id: str,
    date: str,
    signal_value: float,
    entry_price: float,
    direction: str,
    question: str,
    hypotheses_path: str = HYPOTHESES_PATH,
) -> None:
    """Append a new trade observation to a hypothesis, with outcome/clv pending."""
    hypotheses = _load_hypotheses(hypotheses_path)
    if hypothesis_id not in hypotheses:
        raise ValueError(f"Unknown hypothesis: {hypothesis_id}")

    record = {
        "date": date,
        "signal_value": signal_value,
        "entry_price": entry_price,
        "direction": direction,
        "question": question,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "outcome": None,
        "clv": None,
    }
    hypotheses[hypothesis_id]["trades"].append(record)
    _save_hypotheses(hypotheses, hypotheses_path)
    print(f"[Research] Recorded {hypothesis_id} trade: {question[:60]}")


# ─── 3. UPDATE HYPOTHESIS OUTCOMES ────────────────────────────────────────────
def update_hypothesis_outcomes(
    trades_jsonl_path: str = TRADES_JSONL,
    hypotheses_path: str = HYPOTHESES_PATH,
) -> None:
    """Cross-reference hypotheses trades with trades.jsonl and fill in outcome/clv."""
    if not os.path.exists(trades_jsonl_path):
        print("[Research] trades.jsonl not found — skipping outcome update.")
        return

    with open(trades_jsonl_path, "r") as fh:
        live_trades = [json.loads(line) for line in fh if line.strip()]

    # Index by (date, question) for fast lookup
    index: dict[tuple[str, str], dict] = {}
    for t in live_trades:
        d = t.get("end_date") or t.get("end_date_iso") or t.get("date", "")
        q = t.get("question", "")
        index[(d, q)] = t

    hypotheses = _load_hypotheses(hypotheses_path)
    updated_count = 0

    for hid, hyp in hypotheses.items():
        for trade in hyp.get("trades", []):
            if trade.get("outcome") is not None and trade.get("clv") is not None:
                continue  # already filled
            key = (trade.get("date", ""), trade.get("question", ""))
            live = index.get(key)
            if not live:
                continue
            changed = False
            if trade.get("outcome") is None and live.get("outcome") is not None:
                trade["outcome"] = live["outcome"]
                changed = True
            if trade.get("clv") is None and live.get("clv") is not None:
                trade["clv"] = live["clv"]
                changed = True
            if changed:
                updated_count += 1

    _save_hypotheses(hypotheses, hypotheses_path)
    print(f"[Research] Updated {updated_count} hypothesis trade record(s).")


# ─── 4. COMPUTE IC ────────────────────────────────────────────────────────────
def compute_ic(trades_list: list[dict]):
    """Spearman rank correlation between signal_value and binary outcome.
    Returns None if fewer than 5 resolved trades.
    """
    resolved = [
        t for t in trades_list
        if t.get("signal_value") is not None and t.get("outcome") is not None
    ]
    if len(resolved) < 5:
        return None

    try:
        from scipy.stats import spearmanr
    except ImportError:
        # Fallback: manual Spearman via rank correlation
        return _manual_spearman(resolved)

    signals = [float(t["signal_value"]) for t in resolved]
    outcomes = [1.0 if t["outcome"] else 0.0 for t in resolved]
    corr, _ = spearmanr(signals, outcomes)
    return round(float(corr), 6)


def _manual_spearman(resolved: list[dict]) -> float:
    """Minimal Spearman correlation without scipy."""
    n = len(resolved)
    signals = [float(t["signal_value"]) for t in resolved]
    outcomes = [1.0 if t["outcome"] else 0.0 for t in resolved]

    def _ranks(vals):
        indexed = sorted(enumerate(vals), key=lambda x: x[1])
        ranks = [0.0] * len(vals)
        for rank, (idx, _) in enumerate(indexed, 1):
            ranks[idx] = float(rank)
        return ranks

    rs = _ranks(signals)
    ro = _ranks(outcomes)
    d2 = sum((rs[i] - ro[i]) ** 2 for i in range(n))
    rho = 1 - (6 * d2) / (n * (n ** 2 - 1))
    return round(rho, 6)


# ─── 5. KILL DECISION ─────────────────────────────────────────────────────────
def kill_decision(hypothesis_id: str, hypotheses: dict) -> dict:
    """Evaluate kill/promote/continue for one hypothesis."""
    hyp = hypotheses.get(hypothesis_id, {})
    trades = hyp.get("trades", [])
    name = hyp.get("name", hypothesis_id)

    ic = compute_ic(trades)
    n = len([t for t in trades if t.get("outcome") is not None])

    # Rolling 14d CLV from hypothesis trades
    now = datetime.now(timezone.utc)

    def _clv_ts(t):
        # hypothesis trades don't have clv_measured_at, use recorded_at as proxy
        try:
            return datetime.fromisoformat(t.get("recorded_at", ""))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    cutoff = now - timedelta(days=14)
    clv_vals = [
        float(t["clv"])
        for t in trades
        if t.get("clv") is not None and _clv_ts(t) >= cutoff
    ]
    rolling_clv = statistics.mean(clv_vals) if clv_vals else None

    # Verdict logic
    ic_val = ic if ic is not None else 0.0
    clv_val = rolling_clv if rolling_clv is not None else 0.0

    total_n = len(trades)

    if total_n < 5:
        verdict = "INSUFFICIENT DATA"
        reason = f"Only {total_n} trade(s) recorded. Need ≥ 5 to evaluate."
    elif total_n >= 14 and (ic_val <= 0 or clv_val < 0):
        verdict = "KILL"
        parts = []
        if ic_val <= 0:
            parts.append(f"IC={ic_val:+.3f} ≤ 0")
        if clv_val < 0:
            parts.append(f"rolling CLV={clv_val:+.4f} < 0")
        reason = "Kill threshold hit: " + ", ".join(parts)
    elif total_n >= 30 and ic_val > 0 and clv_val > 0:
        verdict = "PROMOTE"
        reason = (
            f"Promote criteria met: IC={ic_val:+.3f} > 0, "
            f"CLV={clv_val:+.4f} > 0, n={total_n} ≥ 30"
        )
    elif total_n >= 5:
        verdict = "CONTINUE"
        ic_str = f"{ic_val:+.3f}" if ic is not None else "N/A"
        clv_str = f"{clv_val:+.4f}" if rolling_clv is not None else "N/A"
        reason = f"Accumulating data. IC={ic_str}, CLV={clv_str}, n={total_n}"
    else:
        verdict = "INSUFFICIENT DATA"
        reason = f"n={total_n} < 5 with outcomes"

    return {
        "id": hypothesis_id,
        "name": name,
        "n": total_n,
        "ic": round(ic_val, 6) if ic is not None else None,
        "rolling_clv": round(clv_val, 6) if rolling_clv is not None else None,
        "verdict": verdict,
        "reason": reason,
    }


# ─── 6. RUN KILL DECISIONS ────────────────────────────────────────────────────
def run_kill_decisions(hypotheses_path: str = HYPOTHESES_PATH) -> None:
    """Evaluate all testing hypotheses, print table, mutate status on kill/promote."""
    hypotheses = _load_hypotheses(hypotheses_path)

    border = "─" * 70
    print(f"\n{border}")
    print(f"  HYPOTHESIS KILL/PROMOTE DECISIONS  —  "
          f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(border)
    print(f"  {'ID':<4} {'Hypothesis':<22} {'IC':>7} {'CLV':>8} {'N':>4}  Verdict")
    print(border)

    changed = False
    for hid, hyp in sorted(hypotheses.items()):
        if hyp.get("status") not in ("testing",):
            continue
        d = kill_decision(hid, hypotheses)
        ic_s = f"{d['ic']:+.3f}" if d["ic"] is not None else "  N/A"
        clv_s = f"{d['rolling_clv']:+.4f}" if d["rolling_clv"] is not None else "   N/A"
        print(f"  {hid:<4} {d['name']:<22} {ic_s:>7} {clv_s:>8} {d['n']:>4}  {d['verdict']}")

        if d["verdict"] == "KILL":
            hypotheses[hid]["status"] = "killed"
            print(f"       ⛔ KILLED — {d['reason']}")
            changed = True
        elif d["verdict"] == "PROMOTE":
            hypotheses[hid]["status"] = "live"
            print(f"       ✅ PROMOTED TO LIVE — {d['reason']}")
            changed = True

    print(border + "\n")

    if changed:
        _save_hypotheses(hypotheses, hypotheses_path)
        print("[Research] hypotheses.json updated with status changes.")


# ─── 7. MONITORING DASHBOARD ──────────────────────────────────────────────────
def monitoring_dashboard(
    trades_jsonl_path: str = TRADES_JSONL,
    hypotheses_path: str = HYPOTHESES_PATH,
) -> None:
    """Print the 5-metric systematic fund dashboard."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Load trades
    trades: list[dict] = []
    if os.path.exists(trades_jsonl_path):
        with open(trades_jsonl_path, "r") as fh:
            trades = [json.loads(line) for line in fh if line.strip()]

    live_trades = [t for t in trades if not t.get("dry_run", True)]
    resolved = [t for t in live_trades if t.get("outcome") is not None]
    measured_clv = [t for t in live_trades if t.get("clv") is not None]
    by_setup: dict[str, list[dict]] = {}
    by_arm: dict[str, list[dict]] = {}
    for trade in live_trades:
        setup = trade.get("setup_type") or "unknown"
        arm = trade.get("strategy_arm") or setup
        by_setup.setdefault(setup, []).append(trade)
        by_arm.setdefault(arm, []).append(trade)

    now = datetime.now(timezone.utc)

    def _clv_ts(t):
        try:
            return datetime.fromisoformat(t.get("clv_measured_at", ""))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    # ── Metric 1: IC ──────────────────────────────────────────────────────────
    def _entry_ts(t):
        try:
            return datetime.fromisoformat(t.get("timestamp", t.get("placed_at", "")))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    r14_cutoff = now - timedelta(days=14)
    ic_trades_14d = [
        t for t in resolved
        if _entry_ts(t) >= r14_cutoff
        and t.get("signal_value") is not None
    ]

    # Compute IC using signal_value vs outcome across all hypotheses
    hypotheses = _load_hypotheses(hypotheses_path)
    all_hyp_trades = []
    for hyp in hypotheses.values():
        all_hyp_trades.extend(hyp.get("trades", []))
    overall_ic = compute_ic(all_hyp_trades)

    # ── Metric 2: CLV ─────────────────────────────────────────────────────────
    r7_cutoff = now - timedelta(days=7)
    clv_7d = [t["clv"] for t in measured_clv if _clv_ts(t) >= r7_cutoff]
    clv_14d = [t["clv"] for t in measured_clv if _clv_ts(t) >= r14_cutoff]
    rolling_7d_clv = statistics.mean(clv_7d) if clv_7d else None
    rolling_14d_clv = statistics.mean(clv_14d) if clv_14d else None

    if rolling_7d_clv is not None and rolling_14d_clv is not None:
        diff = rolling_7d_clv - rolling_14d_clv
        clv_trend = "IMPROVING" if diff > 0.005 else ("DECAYING" if diff < -0.005 else "STABLE")
    else:
        clv_trend = "N/A"

    # ── Metric 3: Edge per $ risked ───────────────────────────────────────────
    gross_profit = sum(
        (float(t.get("size", 0)) * (float(t.get("outcome_price", 1.0)) - float(t.get("entry_price", t.get("price", 0)))))
        for t in resolved
        if t.get("outcome") is True
    )
    gross_profit -= sum(
        float(t.get("size", 0)) * float(t.get("entry_price", t.get("price", 0)))
        for t in resolved
        if t.get("outcome") is False
    )
    capital_deployed = sum(
        float(t.get("size", 0)) * float(t.get("entry_price", t.get("price", 0)))
        for t in live_trades
    )
    edge_pct = (gross_profit / capital_deployed * 100) if capital_deployed > 0 else None

    # ── Metric 4: Hit rate vs expected ────────────────────────────────────────
    wins = sum(1 for t in resolved if t.get("outcome") is True)
    actual_wr = (wins / len(resolved) * 100) if resolved else None
    # Model-implied: average entry price for winning side = implied probability
    implied_wr = None
    if resolved:
        implied_vals = []
        for t in resolved:
            ep = float(t.get("entry_price", t.get("price", 0)))
            if "NO" in (t.get("direction", "BUY YES")).upper():
                implied_vals.append(1 - ep)
            else:
                implied_vals.append(ep)
        if implied_vals:
            implied_wr = statistics.mean(implied_vals) * 100
    wr_delta = (actual_wr - implied_wr) if (actual_wr is not None and implied_wr is not None) else None

    # ── Metric 5: Drawdown ────────────────────────────────────────────────────
    # Simplified P&L curve — cumulative over time
    cumulative = 0.0
    peak = 0.0
    for t in sorted(resolved, key=_entry_ts):
        ep = float(t.get("entry_price", t.get("price", 0)))
        sz = float(t.get("size", 0))
        if t.get("outcome") is True:
            pnl = sz * (1.0 - ep)
        else:
            pnl = -sz * ep
        cumulative += pnl
        peak = max(peak, cumulative)

    drawdown_pct = ((peak - cumulative) / peak * 100) if peak > 0 else 0.0

    # ── Print ─────────────────────────────────────────────────────────────────
    border = "═" * 55
    print(f"\n{border}")
    print(f"  STRATEGY MONITORING DASHBOARD  {today}")
    print(border)

    print(f"\n  METRIC 1: Realized IC (signal prediction power)")
    ic_s = f"{overall_ic:+.4f}" if overall_ic is not None else "N/A"
    print(f"    Rolling 14d IC: {ic_s}  [TARGET: > 0]")
    print(f"    Kill threshold: IC < 0 for 14 consecutive days")

    print(f"\n  METRIC 2: CLV (timing edge)")
    r7s  = f"{rolling_7d_clv:+.4f}" if rolling_7d_clv is not None else "N/A"
    r14s = f"{rolling_14d_clv:+.4f}" if rolling_14d_clv is not None else "N/A"
    print(f"    Rolling 7d CLV:  {r7s}  Rolling 14d: {r14s}")
    print(f"    Trend: {clv_trend}")
    print(f"    Kill threshold: rolling 14d CLV < 0")

    print(f"\n  METRIC 2B: Execution quality")
    overall_exec = _execution_group_summary(live_trades)
    print(f"    Mean spread: { _fmt_num(overall_exec['mean_spread']) }")
    print(f"    Mean est slippage: { _fmt_num(overall_exec['mean_est_slippage']) }")
    print(f"    Mean realized slippage: { _fmt_num(overall_exec['mean_realized_slippage']) }")
    print(f"    Est fillable rate: { _fmt_pct(overall_exec['fillable_rate']) }")
    print(f"    Realized fill rate: { _fmt_pct(overall_exec['realized_fill_rate']) }")

    print(f"\n  METRIC 3: Edge per $ risked (turnover-based return)")
    gp_s  = f"${gross_profit:+.2f}" if gross_profit else "$0.00"
    cap_s = f"${capital_deployed:.2f}" if capital_deployed else "$0.00"
    ep_s  = f"{edge_pct:.1f}%" if edge_pct is not None else "N/A"
    print(f"    Gross profit: {gp_s}  |  Capital deployed: {cap_s}")
    print(f"    Edge/$ : {ep_s}  [TARGET: > 3%]")

    print(f"\n  METRIC 4: Hit rate vs expected")
    awr_s = f"{actual_wr:.0f}%" if actual_wr is not None else "N/A"
    iwr_s = f"{implied_wr:.0f}%" if implied_wr is not None else "N/A"
    delta_s = (f"{wr_delta:+.0f}%" if wr_delta is not None else "N/A")
    print(f"    Actual win rate: {awr_s}  |  Model-implied: {iwr_s}")
    print(f"    Delta: {delta_s}  [KILL if delta > 15% for 30 days]")

    print(f"\n  METRIC 5: Drawdown")
    print(f"    Current drawdown from peak: {drawdown_pct:.1f}%")
    print(f"    Half-size threshold: 25%")
    print(f"    Kill threshold: 50%")
    if drawdown_pct >= 50:
        print(f"    ⛔  KILL THRESHOLD BREACHED")
    elif drawdown_pct >= 25:
        print(f"    ⚠️   HALF-SIZE: reduce position size immediately")

    # ── Hypothesis status table ───────────────────────────────────────────────
    print(f"\n{border}")
    print(f"  HYPOTHESIS STATUS")
    for hid, hyp in sorted(hypotheses.items()):
        status = hyp.get("status", "testing")
        d = kill_decision(hid, hypotheses)
        ic_h  = f"{d['ic']:+.3f}" if d["ic"] is not None else " N/A"
        clv_h = f"{d['rolling_clv']:+.4f}" if d["rolling_clv"] is not None else "  N/A"
        n_h   = d["n"]
        verdict_h = d["verdict"]
        print(
            f"  {hid} [{status}] {hyp['name']:<20} | IC: {ic_h} "
            f"| CLV: {clv_h} | n={n_h} → {verdict_h}"
        )

    if by_setup:
        print(f"\n{border}")
        print("  EXECUTION BY SETUP")
        for setup, setup_trades in sorted(by_setup.items()):
            d = _execution_group_summary(setup_trades)
            print(
                f"  {setup:<20} | n={d['n']:<3} | spread={_fmt_num(d['mean_spread'])} "
                f"| estSlip={_fmt_num(d['mean_est_slippage'])} "
                f"| realSlip={_fmt_num(d['mean_realized_slippage'])}"
            )

    if by_arm:
        print(f"\n{border}")
        print("  EXECUTION BY STRATEGY ARM")
        for arm, arm_trades in sorted(by_arm.items()):
            d = _execution_group_summary(arm_trades)
            print(
                f"  {arm:<20} | n={d['n']:<3} | estFill={_fmt_pct(d['fillable_rate'])} "
                f"| realFill={_fmt_pct(d['realized_fill_rate'])} "
                f"| spread={_fmt_num(d['mean_spread'])}"
            )

    print(f"{border}\n")


# ─── 8. FETCH MORNING HKO TEMP ────────────────────────────────────────────────
def fetch_morning_hko_temp():
    """Fetch current temperature from HKO rhrread API (HK Observatory HQ)."""
    try:
        url = "https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=rhrread&lang=en"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # Walk temperature list for HKO HQ station
        temps = data.get("temperature", {}).get("data", [])
        for entry in temps:
            station = entry.get("station", "").lower()
            if "hko" in station or "hong kong observatory" in station or station == "hko":
                return float(entry["value"])

        # Fallback: first available station
        if temps:
            return float(temps[0]["value"])

        return None
    except Exception as e:
        print(f"[Research] HKO fetch failed: {e}")
        return None


# ─── 9. COMPUTE H1 SIGNAL ────────────────────────────────────────────────────
def compute_h1_signal(hko_morning_temp: float, ecmwf_forecast: float) -> float:
    """Return ratio hko_morning_temp / ecmwf_forecast.

    Signal > 0.96 → buy YES on higher bucket
    Signal < 0.94 → buy NO on higher bucket
    Between 0.94 and 0.96 → no trade
    """
    if ecmwf_forecast == 0:
        raise ValueError("ECMWF forecast cannot be zero")
    return round(hko_morning_temp / ecmwf_forecast, 6)


# ─── 10. COMPUTE H3 SIGNAL ───────────────────────────────────────────────────
def compute_h3_signal(city_markets_prices: list[float]) -> float:
    """Sum of all YES bucket prices for a city on a given day.

    Signal > 1.05 → longshot bias present, fade overpriced cheap buckets.
    A fair market where buckets partition the outcome space sums to 1.0.
    Excess above 1.0 = amount of overpricing embedded in the market.
    """
    return round(sum(city_markets_prices), 6)


# ─── 11. MAIN ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Research Pipeline")
    parser.add_argument("--dashboard", action="store_true", help="Print monitoring dashboard")
    parser.add_argument("--decisions", action="store_true", help="Run kill/promote decisions")
    parser.add_argument("--update", action="store_true", help="Update hypothesis outcomes from trades.jsonl")
    args = parser.parse_args()

    if not any([args.dashboard, args.decisions, args.update]):
        parser.print_help()
        return

    if args.update:
        update_hypothesis_outcomes(TRADES_JSONL, HYPOTHESES_PATH)

    if args.decisions:
        run_kill_decisions(HYPOTHESES_PATH)

    if args.dashboard:
        monitoring_dashboard(TRADES_JSONL, HYPOTHESES_PATH)


if __name__ == "__main__":
    main()
