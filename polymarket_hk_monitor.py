"""
HK Market Monitor & Dashboard
Polls CLOB for live prices, tracks open positions, computes live P&L,
sends Discord alerts on price spikes or near-resolution opportunities.
"""

import os, json, sys, time, requests
from datetime import datetime, timezone, timedelta
from polymarket_core import ENV as _env, discord_post, TRADES_JSONL as _TRADES_JSONL_DEFAULT, CALIBRATION_STATS_JSON, get_clob_client

# ─── ENV LOADING ──────────────────────────────────────────────────────────────

TRADES_JSONL   = _env.get("TRADES_JSONL", _TRADES_JSONL_DEFAULT)
POLL_INTERVAL  = int(_env.get("POLL_INTERVAL", "300"))   # seconds
ALERT_THRESHOLD = float(_env.get("ALERT_THRESHOLD", "0.08"))  # 8% price move
EXIT_AUTOMATION = _env.get("ENABLE_AUTO_EXIT", "true").lower() == "true"
DRY_RUN_MODE = _env.get("DRY_RUN", "true").lower() == "true"

# ─── PRICE CACHE ──────────────────────────────────────────────────────────────

_price_cache: dict = {}   # token_id → {"data": {...}, "fetched_at": float}
_CACHE_TTL = 120          # 2 minutes

# Discord rate limiting: token_id → last alert timestamp
_last_discord_alert: dict = {}   # token_id → float (epoch)
_DISCORD_COOLDOWN = 300          # 5 minutes between alerts per token

# ─── POSITIONS ────────────────────────────────────────────────────────────────

def load_open_positions() -> list:
    """
    Read trades.jsonl and return open positions.

    Filters:
    - Trade placed within the last 7 days
    - outcome == None  (not yet resolved)
    - result.success == True OR result not containing an error key
      AND dry_run == False
    - Deduplicates by token_id, keeping the latest entry.
    """
    if not os.path.exists(TRADES_JSONL):
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    by_token: dict = {}

    try:
        with open(TRADES_JSONL) as f:
            lines = f.readlines()
    except OSError:
        return []

    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            entry = json.loads(raw)
        except json.JSONDecodeError:
            continue

        # Must not be resolved or already exited
        if entry.get("outcome") is not None or entry.get("exit_ts"):
            continue

        # Must not be dry-run
        if entry.get("dry_run", True):
            continue

        # Must be within the last 7 days
        ts_str = entry.get("ts", "")
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        if ts < cutoff:
            continue

        # Must have succeeded (no error key in result, or explicit success)
        result = entry.get("result", {})
        if isinstance(result, dict):
            if "error" in result:
                continue
            if result.get("status") == "dry_run":
                continue
            # Accept if success key is present and True, or if no error key
        else:
            continue

        token_id = entry.get("token_id", "")
        if not token_id:
            continue

        pos = {
            "token_id":  token_id,
            "entry_price": float(entry.get("price", 0)),
            "bet":         float(entry.get("bet", 0)),
            "direction":   entry.get("direction", "BUY YES").strip(),
            "question":    entry.get("question", ""),
            "city":        entry.get("city", "").title(),
            "end_date":    entry.get("end_date", ""),
            "source":      entry.get("source", ""),
            "ts":          ts,
            "created_ts":  entry.get("created_ts", ""),
        }

        # Deduplicate: keep the latest
        existing = by_token.get(token_id)
        if existing is None or ts > existing["ts"]:
            by_token[token_id] = pos

    return list(by_token.values())


def _load_trade_entries() -> list:
    if not os.path.exists(TRADES_JSONL):
        return []
    try:
        with open(TRADES_JSONL) as f:
            return [json.loads(line) for line in f if line.strip()]
    except Exception:
        return []


def _locate_open_trade_entries(entries: list) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    by_token = {}
    for idx, entry in enumerate(entries):
        if entry.get("outcome") is not None:
            continue
        if entry.get("exit_ts"):
            continue
        if entry.get("dry_run", True):
            continue
        token_id = entry.get("token_id", "")
        if not token_id:
            continue
        result = entry.get("result", {})
        if not isinstance(result, dict):
            continue
        if "error" in result or result.get("status") == "dry_run":
            continue
        ts_str = entry.get("ts", "")
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if ts < cutoff:
            continue
        existing = by_token.get(token_id)
        if existing is None or ts > existing["ts"]:
            by_token[token_id] = {"idx": idx, "ts": ts}
    return {token: meta["idx"] for token, meta in by_token.items()}


# ─── PRICE FETCHING ───────────────────────────────────────────────────────────

def fetch_current_price(token_id: str) -> dict | None:
    """
    GET https://clob.polymarket.com/book?token_id={token_id}
    Returns {bid, ask, mid, spread} or None on failure.
    Results cached for 2 minutes.
    """
    now = time.time()
    cached = _price_cache.get(token_id)
    if cached and (now - cached["fetched_at"]) < _CACHE_TTL:
        return cached["data"]

    url = f"https://clob.polymarket.com/book?token_id={token_id}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        book = resp.json()
    except (requests.RequestException, ValueError):
        return None

    # CLOB returns bids/asks as lists of {price, size} dicts
    bids = book.get("bids") or []
    asks = book.get("asks") or []

    best_bid = max((float(b["price"]) for b in bids), default=None) if bids else None
    best_ask = min((float(a["price"]) for a in asks), default=None) if asks else None

    if best_bid is None and best_ask is None:
        return None

    # Gracefully handle one-sided books
    if best_bid is None:
        best_bid = best_ask
    if best_ask is None:
        best_ask = best_bid

    mid    = (best_bid + best_ask) / 2
    spread = best_ask - best_bid

    data = {
        "bid":    round(best_bid, 4),
        "ask":    round(best_ask, 4),
        "mid":    round(mid, 4),
        "spread": round(spread, 4),
    }
    _price_cache[token_id] = {"data": data, "fetched_at": now}
    return data


# ─── P&L COMPUTATION ──────────────────────────────────────────────────────────

def compute_live_pnl(positions: list) -> list:
    """
    Enrich each position with current price and unrealized P&L.

    For BUY YES (or BUY NO — same share-value logic):
        shares         = bet / entry_price
        current_value  = shares * current_mid
        unrealized_pnl = current_value - bet
        pct_change     = (current_mid - entry_price) / entry_price
    """
    enriched = []
    for pos in positions:
        p = dict(pos)
        token_id    = p["token_id"]
        entry_price = p["entry_price"]
        bet         = p["bet"]

        price_data = fetch_current_price(token_id)

        if price_data is None or entry_price <= 0:
            p.update({
                "current_mid":     None,
                "current_value":   None,
                "unrealized_pnl":  None,
                "pct_change":      None,
                "price_error":     True,
            })
            enriched.append(p)
            continue

        current_mid   = price_data["mid"]
        shares        = bet / entry_price
        current_value = shares * current_mid
        unrealized_pnl = current_value - bet
        pct_change     = (current_mid - entry_price) / entry_price

        p.update({
            "current_mid":    round(current_mid, 4),
            "current_value":  round(current_value, 4),
            "unrealized_pnl": round(unrealized_pnl, 4),
            "pct_change":     round(pct_change, 4),
            "bid":            price_data["bid"],
            "ask":            price_data["ask"],
            "spread":         price_data["spread"],
            "price_error":    False,
        })
        enriched.append(p)

    return enriched


# ─── ALERTS ───────────────────────────────────────────────────────────────────

def _hours_to_resolution(pos: dict) -> float | None:
    """Return hours until end_date, or None if unparseable."""
    end_date_str = pos.get("end_date", "")
    if not end_date_str:
        return None
    now = datetime.now(timezone.utc)
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            end = datetime.strptime(end_date_str, fmt)
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)
            delta = (end - now).total_seconds() / 3600
            return delta
        except ValueError:
            continue
    return None


def check_alerts(enriched_positions: list, last_alert_prices: dict) -> list:
    """
    Return a list of alert strings for positions that meet alert criteria.
    last_alert_prices: {token_id: price_at_last_alert} — used to avoid repeat alerts
    on the same price level (caller should update this dict after sending).
    """
    alerts = []
    now = datetime.now(timezone.utc)

    for pos in enriched_positions:
        if pos.get("price_error"):
            continue

        token_id    = pos["token_id"]
        city        = pos.get("city", "?")
        direction   = pos.get("direction", "")
        current_mid = pos["current_mid"]
        entry_price = pos["entry_price"]
        pct_change  = pos["pct_change"]
        unrealized  = pos["unrealized_pnl"]
        question    = pos.get("question", "")[:60]

        alert_msgs = []

        # 1. Price spike / drop beyond threshold
        if abs(pct_change) > ALERT_THRESHOLD:
            last_alerted_price = last_alert_prices.get(token_id)
            if last_alerted_price is None or abs(current_mid - last_alerted_price) > (ALERT_THRESHOLD * 0.5):
                sign = "+" if pct_change > 0 else ""
                alert_msgs.append(
                    f"PRICE MOVE {sign}{pct_change:.1%} | {city} | {direction} | "
                    f"entry={entry_price:.3f} now={current_mid:.3f} | PnL ${unrealized:+.2f}"
                )
                last_alert_prices[token_id] = current_mid

        # 2. Near resolution (< 2 hours)
        hours_left = _hours_to_resolution(pos)
        if hours_left is not None and 0 < hours_left < 2:
            alert_msgs.append(
                f"RESOLVES SOON {hours_left:.1f}h | {city} | {direction} | "
                f"now={current_mid:.3f} | PnL ${unrealized:+.2f} | {question}"
            )

        # 3. Significant loss: price fell below 0.05 from entry above 0.10
        if current_mid < 0.05 and entry_price > 0.10:
            alert_msgs.append(
                f"NEAR ZERO {current_mid:.3f} | {city} | {direction} | "
                f"entry={entry_price:.3f} | PnL ${unrealized:+.2f}"
            )

        # 4. Near certainty — consider taking profit
        if current_mid > 0.85:
            alert_msgs.append(
                f"NEAR CERTAINTY {current_mid:.3f} | {city} | {direction} | "
                f"PnL ${unrealized:+.2f} (+{pct_change:.1%}) — consider profit take"
            )

        # 5. Avellaneda-Stoikov inventory rule: position current value > 50% of
        # daily budget ($0.60 for T1) → concentrated risk, take partial off
        daily_budget = float(_env.get("BANKROLL_OVERRIDE", 2)) * 0.30
        current_value = pos.get("current_value", 0)
        if current_value > daily_budget * 0.50 and pct_change > 0.15:
            alert_msgs.append(
                f"INVENTORY CONCENTRATION ${current_value:.2f} > 50% daily budget | "
                f"{city} | {direction} | PnL ${unrealized:+.2f} — sell partial, reduce risk"
            )

        for msg in alert_msgs:
            alerts.append(f"[{now.strftime('%H:%M UTC')}] {msg}")

    return alerts


def _daily_budget_reference() -> float:
    try:
        import polymarket_model as _pm
        cash, _ = _pm.fetch_usdc_balance()
        if cash > 0:
            return cash * 0.30
    except Exception:
        pass
    return float(_env.get("BANKROLL_OVERRIDE", 2)) * 0.30


def determine_exit_actions(enriched_positions: list) -> list:
    actions = []
    daily_budget = _daily_budget_reference()
    for pos in enriched_positions:
        if pos.get("price_error"):
            continue
        current_mid = pos.get("current_mid")
        entry_price = pos.get("entry_price", 0.0)
        current_value = pos.get("current_value", 0.0)
        if current_mid is None or entry_price <= 0:
            continue
        hours_left = _hours_to_resolution(pos)
        reason = None
        if current_mid >= entry_price * 2.0:
            reason = "2x profit target"
        elif current_mid < entry_price * 0.40:
            reason = "60% loss stop"
        elif hours_left is not None and 0 <= hours_left < 0.5 and 0.35 < current_mid < 0.65:
            reason = "unresolved at close"
        elif current_value > daily_budget * 0.60 and pos.get("pct_change", 0.0) > 0.15:
            reason = "inventory concentration"
        if reason:
            action = dict(pos)
            action["exit_reason"] = reason
            action["exit_shares"] = max(action["bet"] / action["entry_price"], 0.0)
            action["exit_price"] = pos.get("bid") or pos.get("current_mid")
            actions.append(action)
    return actions


def execute_sell(token_id: str, shares: float, price: float, dry_run: bool = False):
    if dry_run:
        return {"status": "dry_run", "shares": shares, "price": price}
    try:
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import SELL
        client = get_clob_client()
        order_args = client.create_order(
            OrderArgs(token_id=str(token_id), price=float(price), size=float(shares), side=SELL)
        )
        result = client.post_order(order_args, "GTC")
        return result if isinstance(result, dict) else {"success": True, "result": str(result)}
    except Exception as e:
        return {"error": str(e)[:200]}


def persist_exit(action: dict, result: dict) -> bool:
    entries = _load_trade_entries()
    open_indices = _locate_open_trade_entries(entries)
    idx = open_indices.get(action["token_id"])
    if idx is None:
        return False
    trade = entries[idx]
    exit_price = float(action.get("exit_price") or action.get("current_mid") or 0.0)
    shares = float(action.get("exit_shares") or 0.0)
    entry_price = float(trade.get("price", 0.0) or 0.0)
    realized = (exit_price - entry_price) * shares
    trade["exit_ts"] = datetime.now(timezone.utc).isoformat()
    trade["exit_reason"] = action.get("exit_reason")
    trade["exit_price"] = round(exit_price, 6)
    trade["exit_shares"] = round(shares, 6)
    trade["exit_result"] = result
    trade["realized_pnl"] = round(realized, 6)
    trade["closed"] = True
    with open(TRADES_JSONL, "w") as f:
        for row in entries:
            f.write(json.dumps(row) + "\n")
    return True


def run_exit_checks(enriched_positions: list) -> list:
    if not EXIT_AUTOMATION:
        return []
    closed = []
    for action in determine_exit_actions(enriched_positions):
        result = execute_sell(
            token_id=action["token_id"],
            shares=action["exit_shares"],
            price=max(0.01, min(0.99, float(action["exit_price"]))),
            dry_run=DRY_RUN_MODE,
        )
        if result.get("status") == "dry_run":
            print(
                f"[exit] dry-run candidate {action.get('city','?')} {action.get('direction','')} | "
                f"{action['exit_reason']} | entry={action['entry_price']:.3f} "
                f"exit={action['exit_price']:.3f}",
                flush=True,
            )
            continue
        ok = bool(result.get("success") or result.get("orderID"))
        if ok and persist_exit(action, result):
            realized = (action["exit_price"] - action["entry_price"]) * action["exit_shares"]
            msg = (
                f"📤 EXIT {action.get('city','?')} {action.get('direction','')} | "
                f"{action['exit_reason']} | entry={action['entry_price']:.3f} "
                f"exit={action['exit_price']:.3f} | PnL ${realized:+.2f}"
            )
            discord_alert(msg, token_id=f"exit:{action['token_id']}")
            closed.append({**action, "result": result, "realized_pnl": round(realized, 6)})
        elif result.get("error"):
            print(f"[exit] failed {action['token_id'][:18]}... {result['error']}", flush=True)
    return closed


# ─── DISCORD ──────────────────────────────────────────────────────────────────

def discord_alert(message: str, token_id: str = "_global") -> bool:
    """
    Post message to Discord channel via core discord_post.
    Rate-limited: max 1 post per 5 minutes per token_id.
    Returns True if message was sent, False if rate-limited or no creds.
    """
    if not _env.get("DISCORD_BOT_TOKEN", "") or not _env.get("DISCORD_CHANNEL_ID", ""):
        print(f"[DISCORD] (no creds) {message}")
        return False

    now = time.time()
    last = _last_discord_alert.get(token_id, 0)
    if now - last < _DISCORD_COOLDOWN:
        return False

    # Discord has a 2000-char message limit
    discord_post(message[:2000])
    _last_discord_alert[token_id] = now
    return True


# ─── DASHBOARD ────────────────────────────────────────────────────────────────

def _format_time_left(pos: dict) -> str:
    hours = _hours_to_resolution(pos)
    if hours is None:
        return "?"
    if hours < 0:
        return "EXPIRED"
    if hours < 1:
        return f"{int(hours * 60)}m"
    if hours < 24:
        return f"{int(hours)}h {int((hours % 1) * 60)}m"
    days = int(hours // 24)
    hrs  = int(hours % 24)
    return f"{days}d {hrs}h"


def _load_tier_status() -> str | None:
    """Return a one-line tier status string if calibration_stats.json exists."""
    if os.path.exists(CALIBRATION_STATS_JSON):
        try:
            with open(CALIBRATION_STATS_JSON) as f:
                stats = json.load(f)
            resolved = stats.get("resolved_bets")
            if resolved is None:
                resolved = stats.get("n_resolved", 0)
            brier = stats.get("brier_score")
            wr = stats.get("win_rate")
            roi = stats.get("roi")
            tier     = stats.get("tier", "T?")
            brier_str = f"{float(brier):.3f}" if isinstance(brier, (int, float)) else "n/a"
            wr_str = f"{float(wr):.1%}" if isinstance(wr, (int, float)) else "n/a"
            roi_str = f"{float(roi):.1%}" if isinstance(roi, (int, float)) else "n/a"
            return (f"Tier: {tier} | {resolved} resolved | "
                    f"Brier: {brier_str} | WR: {wr_str} | ROI: {roi_str}")
        except (json.JSONDecodeError, OSError):
            return None
    return None


def print_dashboard(enriched_positions: list, stats: dict) -> None:
    """Print a formatted real-time dashboard to the terminal."""
    if sys.stdout.isatty() and os.environ.get("TERM"):
        os.system("clear")

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("=" * 78)
    print(f"  POLYMARKET HK MONITOR  |  {now_str}  |  {len(enriched_positions)} positions")
    print("=" * 78)

    if not enriched_positions:
        print("  No open positions found.")
    else:
        # Header
        print(f"  {'CITY':<14} {'DIRECTION':<10} {'ENTRY':>6} {'NOW':>6} "
              f"{'PnL%':>7} {'PnL$':>8}  {'RESOLVES':>10}")
        print("  " + "-" * 74)

        for pos in enriched_positions:
            city      = pos.get("city", "?")[:13]
            direction = pos.get("direction", "?")[:9]
            entry     = pos.get("entry_price", 0)
            now_mid   = pos.get("current_mid")
            pct       = pos.get("pct_change")
            pnl_usd   = pos.get("unrealized_pnl")
            time_left = _format_time_left(pos)

            if pos.get("price_error") or now_mid is None:
                print(f"  {city:<14} {direction:<10} {entry:>6.3f} {'N/A':>6} "
                      f"{'?':>7} {'?':>8}  {time_left:>10}  [price unavailable]")
                continue

            pct_str = f"{'+' if pct >= 0 else ''}{pct:.1%}"
            pnl_str = f"${pnl_usd:+.2f}"

            # Color hint via simple ASCII indicator
            trend = "^" if pct > 0 else ("v" if pct < 0 else "=")
            print(f"  {city:<14} {direction:<10} {entry:>6.3f} {now_mid:>6.3f} "
                  f"{pct_str:>7} {pnl_str:>8}  {time_left:>10}  {trend}")

    print("=" * 78)

    # Summary row
    valid = [p for p in enriched_positions if not p.get("price_error") and p.get("unrealized_pnl") is not None]
    if valid:
        total_deployed = sum(p.get("bet", 0) for p in valid)
        total_pnl      = sum(p["unrealized_pnl"] for p in valid)
        total_pct      = total_pnl / total_deployed if total_deployed > 0 else 0
        best_pct       = max(p["pct_change"] for p in valid)
        worst_pct      = min(p["pct_change"] for p in valid)
        print(f"  TOTAL  deployed: ${total_deployed:.2f}  "
              f"unrealized: ${total_pnl:+.2f} ({total_pct:+.1%})  "
              f"best: {best_pct:+.1%}  worst: {worst_pct:+.1%}")
    else:
        print("  TOTAL  No valid price data available.")

    # Tier status
    tier_str = _load_tier_status()
    if tier_str:
        print(f"  {tier_str}")

    print("=" * 78)
    print(f"  Next refresh in {POLL_INTERVAL}s  |  Ctrl+C to exit")
    print()


# ─── SUMMARY ──────────────────────────────────────────────────────────────────

def generate_discord_summary() -> str:
    """
    One-line summary for a Discord heartbeat cron.
    Returns a formatted string regardless of whether positions exist.
    """
    positions = load_open_positions()
    enriched  = compute_live_pnl(positions)
    valid     = [p for p in enriched if not p.get("price_error") and p.get("unrealized_pnl") is not None]

    n = len(valid)
    if n == 0:
        return "📊 0 open positions | no live P&L data"

    total_pnl  = sum(p["unrealized_pnl"] for p in valid)
    best_pct   = max(p["pct_change"] for p in valid)
    worst_pct  = min(p["pct_change"] for p in valid)

    return (f"📊 {n} open position{'s' if n != 1 else ''} | "
            f"unrealized P&L: ${total_pnl:+.2f} | "
            f"best: {best_pct:+.0%} worst: {worst_pct:+.0%}")


# ─── MAIN LOOP ────────────────────────────────────────────────────────────────

def run_monitor(continuous: bool = True) -> None:
    """
    Main monitoring loop.

    continuous=True  → loop forever, sleeping POLL_INTERVAL between cycles.
    continuous=False → run exactly once (for cron / --once usage).
    """
    last_alert_prices: dict = {}  # token_id → price when last alerted

    print(f"[monitor] starting — TRADES_JSONL={TRADES_JSONL}  interval={POLL_INTERVAL}s", flush=True)

    while True:
        try:
            positions = load_open_positions()
            enriched  = compute_live_pnl(positions)
            alerts    = check_alerts(enriched, last_alert_prices)
            closed    = run_exit_checks(enriched)

            for alert in alerts:
                # Use a stable token-based key if we can extract one
                # alerts are plain strings; use global cooldown bucket
                discord_alert(alert, token_id="_alert")

            if closed:
                print(f"[monitor] executed {len(closed)} exit(s)", flush=True)
                positions = load_open_positions()
                enriched = compute_live_pnl(positions)

            print_dashboard(enriched, {})

            if not continuous:
                break

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\n[monitor] interrupted — exiting.")
            break
        except Exception as exc:
            print(f"[monitor] ERROR: {exc}", flush=True)
            if not continuous:
                break
            # Back off slightly on unexpected errors before retrying
            time.sleep(min(POLL_INTERVAL, 60))


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    p = argparse.ArgumentParser(
        description="Polymarket HK Monitor — live P&L dashboard and Discord alerts"
    )
    p.add_argument("--once",    action="store_true", help="Run one cycle and exit")
    p.add_argument("--summary", action="store_true", help="Post Discord heartbeat summary and exit")
    args = p.parse_args()

    if args.summary:
        msg = generate_discord_summary()
        discord_alert(msg, token_id="_summary")
        print(msg)
    elif args.once:
        run_monitor(continuous=False)
    else:
        run_monitor(continuous=True)


if __name__ == "__main__":
    main()
