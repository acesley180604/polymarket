"""
Polymarket Weather Delta-Arb
=============================
Strategy: ECMWF/GFS model-update detection vs Polymarket price lag.

NOT probability estimation. We're detecting:
  new_ecmwf_prob - old_ecmwf_prob = model_delta
  current_market_price ≈ old model → market hasn't repriced yet
  → Enter before market catches up (5-30 min window)

Schedule:
  ECMWF IFS 00z available ~06:00 UTC → run at 06:30 UTC
  ECMWF IFS 12z available ~18:00 UTC → run at 18:30 UTC
  GFS   every 6h available ~3.5h after init → run at 03:30/09:30/15:30/21:30 UTC

Run: python3 polymarket_delta_arb.py
Cron: 30 6,18 * * * python3 /Users/acesley/polymarket_delta_arb.py
      30 3,9,15,21 * * * python3 /Users/acesley/polymarket_delta_arb.py
"""

import requests, json, os, time, math, statistics
from datetime import date, timedelta, datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from polymarket_core import ENV as _env, CITY_COORDS, detect_city, parse_temp_range, c_to_f, f_to_c, GAMMA_API, CLOB_API, discord_post, ARB_STATE_JSON, DELTA_SIGNALS_JSONL, market_timing_metrics

# ─── CONFIG ──────────────────────────────────────────────
STATE_FILE   = ARB_STATE_JSON
SIGNAL_LOG   = DELTA_SIGNALS_JSONL
CITY_FILTER  = [c.strip().lower() for c in _env.get("CITY_FILTER", "").split(",") if c.strip()]

DELTA_THRESH     = 0.10   # model prob must shift ≥10pp to be a signal
LAG_THRESH       = 0.08   # market must be ≥8pp behind new model prob
MIN_MODEL_PROB   = 0.05   # ignore near-zero model forecasts
MAX_ENTRY_PRICE  = 0.80   # don't chase markets already close to resolved
MIN_ENTRY_PRICE  = 0.05   # skip near-zero markets (no exit liquidity)


# ─── ENSEMBLE FETCH ───────────────────────────────────────
def fetch_ensemble(lat, lon, target_date_str, model="ecmwf_ifs04"):
    """Fetch ensemble max-temp members for target date. Returns list of floats (°C)."""
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": lat, "longitude": lon,
        "daily": "temperature_2m_max",
        "temperature_unit": "celsius",
        "models": model,
        "start_date": target_date_str,
        "end_date":   target_date_str,
        "timezone": "auto",
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    daily = r.json().get("daily", {})
    members = []
    for key, vals in daily.items():
        if "temperature_2m_max" in key and vals and vals[0] is not None:
            members.append(float(vals[0]))
    return members

def prob_for_bucket(members, lo, hi, unit):
    """Count-based probability from ensemble members."""
    def to_c(v): return f_to_c(v) if unit == "F" else v
    lo_c = to_c(lo); hi_c = to_c(hi)
    if not members: return 0.5
    if lo == -999.0:
        count = sum(1 for m in members if m <= hi_c + 0.5)
    elif hi == 999.0:
        count = sum(1 for m in members if m >= lo_c - 0.5)
    else:
        count = sum(1 for m in members if lo_c <= m <= hi_c)
    raw = count / len(members)
    return max(0.03, min(0.97, raw))

def ensemble_mean(members):
    return statistics.mean(members) if members else 0.0

# ─── STATE FILE ───────────────────────────────────────────
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"version": 2, "forecasts": {}, "market_prices": {}, "last_run": ""}

def save_state(state):
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def state_key(city, target_date):
    return f"{city.replace(' ','_')}_{target_date}"

# ─── MARKET FETCHING ──────────────────────────────────────
def fetch_target_markets():
    """Fetch active markets for tomorrow and day-after-tomorrow."""
    results = []
    for delta in [1, 2]:
        target = (date.today() + timedelta(days=delta)).isoformat()
        for offset in range(3300, 3600, 100):
            try:
                r = requests.get(f"{GAMMA_API}/events",
                    params={"tag_slug":"weather","active":"true","limit":100,"offset":offset},
                    timeout=15)
                events = r.json() if isinstance(r.json(), list) else []
                found = False
                for ev in events:
                    if target not in ev.get("endDate",""):
                        continue
                    found = True
                    for mkt in ev.get("markets", []):
                        if not mkt.get("active") or not mkt.get("clobTokenIds"):
                            continue
                        tokens = mkt["clobTokenIds"]
                        if isinstance(tokens, str):
                            try: tokens = json.loads(tokens)
                            except: tokens = []
                        city, coords = detect_city(mkt.get("question",""))
                        pr = parse_temp_range(mkt.get("question",""))
                        if not city or not pr or not tokens:
                            continue
                        results.append({
                            "question":    mkt["question"],
                            "token_id":    tokens[0],
                            "token_id_no": tokens[1] if len(tokens) > 1 else None,
                            "city": city, "coords": coords,
                            "target_date": target,
                            "lo": pr[0], "hi": pr[1], "unit": pr[2],
                            "created_ts": mkt.get("createdAt") or ev.get("createdAt") or "",
                        })
                if not found and offset > 3300:
                    dates = [ev.get("endDate","")[:10] for ev in events if ev.get("endDate")]
                    if dates and max(dates) < target:
                        break
            except Exception as e:
                print(f"  Fetch error offset {offset}: {e}", flush=True)
    return results

def get_price_batch(token_ids):
    results = {}
    def fetch_one(tid):
        try:
            r = requests.get(f"{CLOB_API}/midpoint", params={"token_id": tid}, timeout=8)
            if r.status_code == 200:
                mid = float(r.json().get("mid", 0) or 0)
                if mid > 0.005:
                    return tid, mid
        except: pass
        try:
            r = requests.get(f"{CLOB_API}/price", params={"token_id": tid, "side":"buy"}, timeout=8)
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

# ─── DISCORD ALERT ────────────────────────────────────────
def send_discord(content: str, embeds: list = None):
    """Thin wrapper over core discord_post; returns True so callers can log success."""
    discord_post(content, embeds)
    return bool(_env.get("DISCORD_BOT_TOKEN", ""))

def build_discord_embed(signal: dict) -> dict:
    delta_arrow = "⬆️" if signal["model_delta"] > 0 else "⬇️"
    color = 0x00ff88 if signal["model_delta"] > 0 else 0xff4444
    urgency_color = 0xff0000 if "HIGH" in signal["urgency"] else 0xffaa00
    return {
        "title": f"{signal['urgency']}  {signal['direction']}  {signal['city'].title()}  {signal['rng']}",
        "color": urgency_color,
        "fields": [
            {"name": "Model jump",    "value": f"{signal['old_prob']:.0%} → {signal['new_prob']:.0%}  ({delta_arrow}{abs(signal['model_delta']):.0%} in {signal['hours_since']:.0f}h)", "inline": True},
            {"name": "Market price",  "value": f"{signal['market_price']:.0%}  (lag {signal['market_lag']:+.0%})", "inline": True},
            {"name": "Trade",         "value": f"Entry {signal['trade_price']:.0%}  •  Bet **${signal['bet']:.2f}**  •  EV **${signal['ev']:+.2f}**", "inline": False},
            {"name": "Forecast",      "value": f"{signal['fc_mean']:.1f}°C  [{signal['source']}]", "inline": True},
            {"name": "Settles",       "value": signal["target_date"], "inline": True},
        ],
        "footer": {"text": signal["question"][:80]},
    }

# ─── SIGNAL LOG ───────────────────────────────────────────
def log_signal(entry):
    with open(SIGNAL_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")

# ─── BANKROLL ─────────────────────────────────────────────
def get_bankroll():
    try:
        import polymarket_model as _pm
        cash, _ = _pm.fetch_usdc_balance()
        if cash > 0:
            return cash
    except Exception:
        pass
    override = _env.get("BANKROLL_OVERRIDE", "")
    if override:
        return float(override)
    return 2.0

def kelly_size(p_win, market_price, bankroll, fraction=1/4, max_pct=0.05):
    if market_price <= 0.01 or market_price >= 0.99: return 0
    b = (1.0 - market_price) / market_price
    q = 1.0 - p_win
    f = (p_win * b - q) / b
    if f <= 0: return 0
    bet = bankroll * f * fraction
    bet = min(bet, max_pct * bankroll)
    return round(max(bet, 0), 2)

# ─── MAIN ─────────────────────────────────────────────────
def run(send_alerts: bool = True):
    now_utc  = datetime.now(timezone.utc)
    bankroll = get_bankroll()
    state    = load_state()
    prev_run = state.get("last_run", "")

    print("=" * 70, flush=True)
    print(f"POLYMARKET DELTA-ARB  {now_utc.strftime('%Y-%m-%d %H:%M UTC')}", flush=True)
    print(f"Strategy: ECMWF update detection → Polymarket lag entry", flush=True)
    print(f"Bankroll: ${bankroll:.2f}  |  Δ-thresh: {DELTA_THRESH:.0%}  |  Lag-thresh: {LAG_THRESH:.0%}", flush=True)
    if prev_run:
        print(f"Last run: {prev_run[:19]} UTC", flush=True)
    print("=" * 70, flush=True)

    # ── 1. Fetch markets ─────────────────────────────────
    print(f"\n[1] Fetching active weather markets (tomorrow + day-after)...", flush=True)
    markets = fetch_target_markets()
    print(f"    {len(markets)} parseable markets", flush=True)

    # ── 2. Fetch prices ───────────────────────────────────
    print(f"\n[2] Fetching {len(markets)} CLOB prices...", flush=True)
    all_token_ids = [m["token_id"] for m in markets]
    prices        = get_price_batch(all_token_ids)
    live_count    = sum(1 for p in prices.values() if p > 0.01)
    print(f"    {live_count}/{len(prices)} live prices", flush=True)

    # ── 3. Fetch ensembles for unique city+date combos ────
    city_dates = {}
    for m in markets:
        key = state_key(m["city"], m["target_date"])
        if key not in city_dates:
            city_dates[key] = (m["city"], m["coords"], m["target_date"])

    print(f"\n[3] Fetching ECMWF + GFS ensembles for {len(city_dates)} city-dates...", flush=True)
    new_forecasts = {}

    def fetch_city_date(key, city, coords, target_date):
        lat, lon = coords
        all_members = []
        source_list = []
        for model, label in [("ecmwf_ifs025","ECMWF"), ("gfs_seamless","GFS"), ("icon_seamless","ICON")]:
            try:
                m = fetch_ensemble(lat, lon, target_date, model)
                if m:
                    all_members.extend(m)
                    source_list.append(label)
            except Exception:
                pass
        source = "+".join(source_list) if source_list else "none"
        return key, city, target_date, all_members, source

    with ThreadPoolExecutor(max_workers=5) as ex:  # 5 workers — avoid 429
        futs = {ex.submit(fetch_city_date, k, c, coords, d): k
                for k, (c, coords, d) in city_dates.items()}
        for fut in as_completed(futs):
            key, city, target_date, members, source = fut.result()
            if members:
                new_forecasts[key] = {
                    "city": city,
                    "date": target_date,
                    "mean": round(ensemble_mean(members), 2),
                    "members": [round(x, 2) for x in members],
                    "n": len(members),
                    "source": source,
                    "fetched_at": now_utc.isoformat(),
                }
                print(f"    {city.title():20s} {target_date}  "
                      f"mean={new_forecasts[key]['mean']:.1f}°C  "
                      f"n={len(members)}  [{source}]", flush=True)

    # ── 4. Detect deltas ──────────────────────────────────
    print(f"\n[4] Detecting model deltas vs stored state...\n", flush=True)

    signals   = []
    unchanged = 0
    no_state  = 0
    no_price  = 0

    for m in markets:
        key          = state_key(m["city"], m["target_date"])
        new_fc       = new_forecasts.get(key)
        market_price = prices.get(m["token_id"], 0)

        if not new_fc or len(new_fc.get("members", [])) < 20:  # skip empty/rate-limited fetches
            no_price += 1; continue
        if market_price < 0.005:
            no_price += 1; continue
        timing = market_timing_metrics(m.get("created_ts"), m.get("target_date"))

        # Compute new model probability for this specific bucket
        new_prob = prob_for_bucket(new_fc["members"], m["lo"], m["hi"], m["unit"])

        # Load previous forecast for delta
        prev_fc = state["forecasts"].get(key)
        if prev_fc and prev_fc.get("members"):
            old_prob     = prob_for_bucket(prev_fc["members"], m["lo"], m["hi"], m["unit"])
            mean_shift   = new_fc["mean"] - prev_fc.get("mean", new_fc["mean"])
            model_delta  = new_prob - old_prob
            hours_ago    = (now_utc - datetime.fromisoformat(
                prev_fc.get("fetched_at", now_utc.isoformat()).replace("Z","+00:00")
            )).total_seconds() / 3600
        else:
            # No prior state — can't compute delta yet, store for next run
            no_state += 1
            old_prob    = None
            mean_shift  = 0
            model_delta = 0
            hours_ago   = 0

        # Delta signal: model jumped AND market hasn't repriced
        if old_prob is not None and abs(model_delta) >= DELTA_THRESH:
            # Determine trade direction
            if model_delta > 0:
                direction    = "BUY YES"
                trade_prob   = new_prob
                trade_price  = market_price
                market_lag   = new_prob - market_price
            else:
                direction    = "BUY NO "
                trade_prob   = 1.0 - new_prob
                trade_price  = 1.0 - market_price
                market_lag   = trade_prob - trade_price

            # Only signal if market is actually lagging
            effective_lag_thresh = LAG_THRESH / max(timing["age_score"], 0.5)
            if market_lag < effective_lag_thresh:
                unchanged += 1; continue

            # Entry price guard
            if trade_price > MAX_ENTRY_PRICE or trade_price < MIN_ENTRY_PRICE:
                unchanged += 1; continue

            bet = kelly_size(trade_prob, trade_price, bankroll, 1/4, 0.05)
            if bet < 0.50:
                continue

            ev = trade_prob * (bet / trade_price - bet) - (1 - trade_prob) * bet

            lo, hi, unit = m["lo"], m["hi"], m["unit"]
            if   lo == -999.0: rng = f"≤{hi}°{unit}"
            elif hi == 999.0:  rng = f"≥{lo}°{unit}"
            elif lo == hi:     rng = f"={lo}°{unit}"
            else:              rng = f"{lo}-{hi}°{unit}"

            order_token = m["token_id"] if direction == "BUY YES" else (m.get("token_id_no") or m["token_id"])
            signals.append({
                "city":         m["city"],
                "target_date":  m["target_date"],
                "rng":          rng,
                "direction":    direction,
                "model_delta":  model_delta,
                "mean_shift":   mean_shift,
                "old_prob":     round(old_prob, 3),
                "new_prob":     round(new_prob, 3),
                "market_price": round(market_price, 3),
                "market_lag":   round(market_lag, 3),
                "trade_price":  round(trade_price, 3),
                "bet":          bet,
                "ev":           round(ev, 2),
                "hours_since":  round(hours_ago, 1),
                "fc_mean":      new_fc["mean"],
                "source":       new_fc["source"],
                "question":     m["question"],
                "token_id":     m["token_id"],
                "order_token":  order_token,
                "urgency":      "🚨 HIGH" if hours_ago < 2 else "⚠️ MED",
                "setup_type":   "delta_lag",
                "strategy_arm": "delta_lag",
                "hypothesis":   "H2",
                "signal_value": round(model_delta, 6),
                "created_ts":   m.get("created_ts", ""),
                "market_age_score": timing["age_score"],
                "market_age_hours": timing["age_hours"],
                "hours_to_resolution": timing["hours_to_resolution"],
            })
        else:
            unchanged += 1

    # ── 5. Print signals ──────────────────────────────────
    signals.sort(key=lambda x: (-abs(x["model_delta"]), -x["market_lag"]))

    if CITY_FILTER:
        signals = [s for s in signals if s.get("city", "").lower() in CITY_FILTER]

    print(f"  Model deltas detected: {len(signals)}", flush=True)
    print(f"  No prior state:        {no_state} (need 1 more run)", flush=True)
    print(f"  Unchanged/filtered:    {unchanged}", flush=True)

    if not signals:
        msg = (f"No delta-arb signals at {now_utc.strftime('%H:%M UTC')}.\n"
               f"Next ECMWF runs: ~06:00 UTC and ~18:00 UTC.")
        print(f"\n  {msg}", flush=True)
        save_state({**state, "forecasts": {**state["forecasts"], **new_forecasts},
                    "market_prices": {**state.get("market_prices",{}),
                                      **{tid:{"price":p,"at":now_utc.isoformat()}
                                         for tid,p in prices.items() if p > 0.01}}})
        return []

    print(flush=True)
    print("━" * 70, flush=True)
    print("DELTA-ARB SIGNALS  (model jumped, market still sleeping)\n", flush=True)

    alert_lines = []
    for i, s in enumerate(signals, 1):
        urgency = s["urgency"]
        delta_arrow = "↑" if s["model_delta"] > 0 else "↓"
        print(f"  {urgency}  #{i}  {s['direction']}  {s['city'].upper():18s}  {s['rng']}", flush=True)
        print(f"         Model: {s['old_prob']:.1%} → {s['new_prob']:.1%} "
              f"({delta_arrow}{abs(s['model_delta']):.0%} in {s['hours_since']:.0f}h)  "
              f"Forecast: {s['fc_mean']:.1f}°C [{s['source']}]", flush=True)
        print(f"         Market: {s['market_price']:.1%}  Lag: {s['market_lag']:+.0%}  "
              f"Entry: {s['trade_price']:.1%}  Bet: ${s['bet']:.2f}  EV: ${s['ev']:+.2f}", flush=True)
        print(f"         Q: {s['question'][:70]}", flush=True)
        print(flush=True)

        alert_lines.append(
            f"{urgency} <b>{s['direction']} {s['city'].title()} {s['rng']}</b>\n"
            f"Model: {s['old_prob']:.0%}→{s['new_prob']:.0%} ({delta_arrow}{abs(s['model_delta']):.0%}) | "
            f"Market still: {s['market_price']:.0%} | Lag: {s['market_lag']:+.0%}\n"
            f"Entry ${s['trade_price']:.2f} | Bet ${s['bet']:.2f} | EV ${s['ev']:+.2f}"
        )

        log_signal({
            "ts": now_utc.isoformat(),
            **{k: s[k] for k in ["city","target_date","rng","direction","model_delta",
                                  "old_prob","new_prob","market_price","market_lag",
                                  "bet","ev","token_id","urgency"]},
            "outcome": None,
        })

    # ── Capital summary ───────────────────────────────────
    total_bet = sum(s["bet"] for s in signals)
    total_ev  = sum(s["ev"]  for s in signals)
    print("━" * 70, flush=True)
    print(f"Total signals: {len(signals)}  |  Deploy: ${total_bet:.2f}  |  Expected EV: ${total_ev:+.2f}", flush=True)
    print(f"Bankroll: ${bankroll:.2f}  |  Exposure: {total_bet/bankroll*100:.1f}%", flush=True)
    print("━" * 70, flush=True)
    print(f"\n  Live mode: orders placed by autotrader.py (DRY_RUN={_env.get('DRY_RUN','false')})", flush=True)

    # ── Discord ───────────────────────────────────────────
    if send_alerts and signals and _env.get("DISCORD_BOT_TOKEN", ""):
        header = (f"🌤 **POLYMARKET DELTA-ARB**  `{now_utc.strftime('%Y-%m-%d %H:%M UTC')}`\n"
                  f"**{len(signals)} signal(s)** | Deploy **${total_bet:.2f}** | EV **${total_ev:+.2f}** | "
                  f"Bankroll **${bankroll:.2f}**")
        embeds = [build_discord_embed(s) for s in signals[:10]]
        ok = send_discord(header, embeds)
        print(f"\n  Discord alert {'sent ✓' if ok else 'FAILED ✗'} ({len(signals)} signals → channel {_env.get('DISCORD_CHANNEL_ID', '')})", flush=True)
    elif send_alerts and signals:
        print("\n  (Add DISCORD_BOT_TOKEN + DISCORD_CHANNEL_ID to polymarket.env for alerts)", flush=True)

    # ── Persist state ─────────────────────────────────────
    new_mkt_prices = {
        tid: {"price": p, "at": now_utc.isoformat()}
        for tid, p in prices.items() if p > 0.01
    }
    save_state({
        **state,
        "forecasts":     {**state["forecasts"],     **new_forecasts},
        "market_prices": {**state.get("market_prices", {}), **new_mkt_prices},
    })
    print(f"\n  State saved → {STATE_FILE}", flush=True)

    return signals

if __name__ == "__main__":
    run()
