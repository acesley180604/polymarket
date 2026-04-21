"""
Polymarket Auto-Trader
======================
Runs both signal engines, deduplicates, places live orders.

Delta-arb signals (model update detection) take priority.
Conviction model fills remaining daily budget.

Cron:
  CRON_TZ=UTC
  30 6,18 * * *       cd /root/polymarket && python3 polymarket_autotrader.py
  30 3,9,15,21 * * *  cd /root/polymarket && python3 polymarket_autotrader.py --gfs
"""

import sys, json, time, requests, os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from polymarket_core import ENV as _env, CLOB_API, GAMMA_API, discord_post, get_clob_client as _get_clob_client, l2_headers as _l2_headers, TRADES_JSONL as _TRADES_JSONL_DEFAULT, CALIBRATION_STATS_JSON
import polymarket_capital as cap_mod
import polymarket_execution as exec_mod
DRY_RUN      = _env.get("DRY_RUN", "true").lower() == "true"
GFS_ONLY     = "--gfs" in sys.argv
IGNORE_DAILY_CAP = _env.get("IGNORE_DAILY_CAP", "false").lower() == "true"
ARB_LOOSE_MIN_DEVIATION = float(_env.get("ARB_LOOSE_MIN_DEVIATION", "0.08"))
ACCOUNTING_MODE = _env.get("ACCOUNTING_MODE", "paper" if DRY_RUN else "live").strip().lower() or ("paper" if DRY_RUN else "live")
# Comma-separated city filter — only trade these cities. Empty = all cities.
CITY_FILTER  = [c.strip().lower() for c in _env.get("CITY_FILTER", "").split(",") if c.strip()]

TRADE_LOG   = _TRADES_JSONL_DEFAULT
import fcntl as _fcntl
LOCK_FILE = "/tmp/polymarket_autotrader.lock"

def _acquire_lock():
    lf = open(LOCK_FILE, "w")
    try:
        _fcntl.flock(lf, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        return lf
    except OSError:
        print("[LOCK] Another autotrader run is in progress. Exiting.", flush=True)
        lf.close()
        return None



# ─── ORDER PLACEMENT ──────────────────────────────────────
CHAIN_ID   = 137
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
EIP712_DOMAIN = {"name":"Polymarket CTF Exchange","version":"1","chainId":CHAIN_ID,"verifyingContract":CTF_EXCHANGE}
ORDER_TYPES = {"Order":[
    {"name":"salt","type":"uint256"},{"name":"maker","type":"address"},{"name":"signer","type":"address"},
    {"name":"taker","type":"address"},{"name":"tokenId","type":"uint256"},{"name":"makerAmount","type":"uint256"},
    {"name":"takerAmount","type":"uint256"},{"name":"expiration","type":"uint256"},{"name":"nonce","type":"uint256"},
    {"name":"feeRateBps","type":"uint256"},{"name":"side","type":"uint8"},{"name":"signatureType","type":"uint8"},
]}


def _maybe_float(value):
    try:
        if value in (None, "", False):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_execution_audit(signal, result):
    signal_price = _maybe_float(signal.get("signal_price"))
    trade_price = _maybe_float(signal.get("trade_price"))
    decision_snapshot = signal.get("decision_snapshot") or {}
    post_order_snapshot = signal.get("post_order_snapshot") or {}
    decision_book = decision_snapshot.get("book") or {}
    post_book = post_order_snapshot.get("book") or {}

    audit = {
        "signal_price": signal_price,
        "planned_trade_price": trade_price,
        "decision_snapshot_at": decision_snapshot.get("captured_at"),
        "post_order_snapshot_at": post_order_snapshot.get("captured_at"),
        "decision_best_bid": decision_book.get("bid"),
        "decision_best_ask": decision_book.get("ask"),
        "decision_mid": decision_book.get("mid"),
        "decision_spread": decision_book.get("spread"),
        "decision_bid_depth": decision_book.get("bid_depth"),
        "decision_ask_depth": decision_book.get("ask_depth"),
        "decision_fill_estimate": decision_snapshot.get("fill_estimate"),
        "post_order_best_bid": post_book.get("bid"),
        "post_order_best_ask": post_book.get("ask"),
        "post_order_mid": post_book.get("mid"),
        "post_order_spread": post_book.get("spread"),
    }

    realized_avg_price = None
    matched_shares = _maybe_float(result.get("takingAmount"))
    matched_notional = _maybe_float(result.get("makingAmount"))
    if matched_shares and matched_notional and matched_shares > 0:
        realized_avg_price = matched_notional / matched_shares

    audit.update({
        "result_status": result.get("status"),
        "result_order_id": result.get("orderID"),
        "result_transaction_hashes": result.get("transactionsHashes"),
        "matched_shares": round(matched_shares, 6) if matched_shares is not None else None,
        "matched_notional": round(matched_notional, 6) if matched_notional is not None else None,
        "realized_avg_price": round(realized_avg_price, 6) if realized_avg_price is not None else None,
    })

    if signal_price is not None and realized_avg_price is not None:
        audit["slippage_vs_signal"] = round(realized_avg_price - signal_price, 6)
    else:
        audit["slippage_vs_signal"] = None

    decision_ask = _maybe_float(decision_book.get("ask"))
    decision_bid = _maybe_float(decision_book.get("bid"))
    if realized_avg_price is not None:
        audit["slippage_vs_best_ask"] = (
            round(realized_avg_price - decision_ask, 6) if decision_ask is not None else None
        )
        audit["slippage_vs_best_bid"] = (
            round(realized_avg_price - decision_bid, 6) if decision_bid is not None else None
        )
    else:
        audit["slippage_vs_best_ask"] = None
        audit["slippage_vs_best_bid"] = None

    return audit

def place_order(token_id: str, price: float, size_usdc: float, side: int, dry_run: bool,
                max_usdc: float = None):
    """side: 0=BUY, 1=SELL. max_usdc: hard cap — skip if 5-share min would exceed it."""
    side_str = "BUY" if side == 0 else "SELL"
    print(f"  {'[DRY] ' if dry_run else '[LIVE]'} {side_str} token={str(token_id)[:20]}... "
          f"price={price:.3f} size=${size_usdc:.2f}", flush=True)

    if dry_run:
        return {"status": "dry_run", "token_id": token_id, "price": price, "size": size_usdc}

    try:
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import BUY, SELL
        client = _get_clob_client()
        MIN_SHARES = 5.0
        raw_shares = size_usdc / price if side == 0 else size_usdc
        shares = max(raw_shares, MIN_SHARES)  # enforce Polymarket minimum
        bumped_cost = shares * price
        # Polymarket hard minimum $1 per order
        if bumped_cost < 1.0:
            return {"skipped": True, "reason": "below_poly_min_1usd", "cost": bumped_cost}
        if max_usdc is not None and bumped_cost > max_usdc:
            msg = (f"         (skipped: 5-share min would cost ${bumped_cost:.2f}, "
                   f"exceeds tier cap ${max_usdc:.2f})")
            print(msg, flush=True)
            return {"skipped": True, "reason": "below_min_share_size"}
        if shares != raw_shares:
            print(f"         (size bumped to {shares:.2f} shares = ${bumped_cost:.2f} USDC to meet 5-share min)", flush=True)
        order_args = client.create_order(OrderArgs(
            token_id=token_id,
            price=price,
            size=shares,
            side=BUY if side == 0 else SELL,
        ))
        result = client.post_order(order_args, "GTC")
        print(f"         → {str(result)[:120]}", flush=True)
        return result if isinstance(result, dict) else {"success": True, "result": str(result)}
    except Exception as e:
        err = {"error": str(e)[:200]}
        print(f"         → ERROR: {err['error']}", flush=True)
        return err

def log_trade(signal, result, source):
    urgency = signal.get("urgency", "")
    hypothesis = signal.get("hypothesis") or (
        "H3" if urgency == "ladder"
        else "H5" if urgency == "arb"
        else "H2" if signal.get("_source") == "delta"
        else "H1"  # conviction model default
    )
    entry = {
        "ts":          datetime.now(timezone.utc).isoformat(),
        "source":      source,
        "city":        signal.get("city",""),
        "question":    signal.get("question","")[:80],
        "direction":   signal.get("direction",""),
        "price":       signal.get("trade_price", signal.get("yes_price", 0)),
        "bet":         signal.get("bet", 0),
        "ev":          signal.get("ev", 0),
        "model_prob":  signal.get("trade_prob", signal.get("f_prob")),
        "token_id":    signal.get("order_token",""),
        "end_date":    signal.get("end_date", signal.get("target_date", "")),
        "dry_run":     DRY_RUN,
        "accounting_mode": ACCOUNTING_MODE,
        "result":      result,
        "outcome":     None,
        "clv":         None,   # filled by polymarket_clv.py at 14:00-16:00 UTC
        "hypothesis":  hypothesis,
        "signal_value": signal.get("signal_value"),
        "setup_type":  signal.get("setup_type"),
        "strategy_arm": signal.get("strategy_arm") or signal.get("setup_type"),
        "market_age_score": signal.get("market_age_score"),
        "market_age_hours": signal.get("market_age_hours"),
        "hours_to_resolution": signal.get("hours_to_resolution"),
        "created_ts": signal.get("created_ts", ""),
        "execution_type": signal.get("execution_type"),
        "execution_notes": signal.get("execution_notes"),
        "reward_context": signal.get("reward_context"),
        "decision_snapshot": signal.get("decision_snapshot"),
        "post_order_snapshot": signal.get("post_order_snapshot"),
        "execution_audit": _build_execution_audit(signal, result),
        "hedge_group": signal.get("hedge_group"),
        "hedge_role": signal.get("hedge_role"),
    }
    # Never log skipped or failed orders
    r = result or {}
    if r.get("skipped") or r.get("error") or ("min size" in str(r.get("error_message","")).lower()):
        return
    if not (r.get("orderID") or r.get("success") or r.get("status") == "dry_run"):
        return
    with open(TRADE_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")

def signal_to_embed(signal, source, status_override=None):
    status_key = status_override or ("dry_run" if DRY_RUN else "placed")
    color = 0x00ff88 if status_key in ("placed", "dry_run") else 0x888888
    status_map = {
        "placed": "✅ Placed",
        "dry_run": "📋 Dry run",
        "skipped": "⏭ Skipped",
        "failed": "❌ Failed",
    }
    status = status_map.get(status_key, "⏭ Skipped")
    tier    = signal.get("tier_cfg", {}).get("name", "") or signal.get("urgency","")
    return {
        "title":  f"{status}  {signal.get('direction','')}  {signal.get('city','').title()}  {signal.get('rng','')}",
        "color":  color,
        "fields": [
            {"name":"Source",  "value": source, "inline": True},
            {"name":"Tier",    "value": str(tier), "inline": True},
            {"name":"Price",   "value": f"{signal.get('trade_price', signal.get('yes_price',0)):.0%}", "inline": True},
            {"name":"Bet",     "value": f"${signal.get('effective_bet', signal.get('bet',0)):.2f}", "inline": True},
            {"name":"EV",      "value": f"${signal.get('ev',0):+.2f}", "inline": True},
            {"name":"Settles", "value": signal.get("end_date", signal.get("target_date","")), "inline": True},
        ],
        "footer": {"text": signal.get("question","")[:80]},
    }


def _trade_accounting_mode(trade):
    mode = (trade.get("accounting_mode") or "").strip().lower()
    if mode:
        return mode
    return "paper" if trade.get("dry_run") else "live"


def _trade_counts_for_accounting(trade):
    result = trade.get("result", {})
    if not isinstance(result, dict):
        return False
    if trade.get("dry_run"):
        return result.get("status") == "dry_run"
    return bool(result.get("success") or result.get("orderID"))


def _classify_bucket_sum_arb(signal, order_type):
    arb_deviation = _maybe_float(signal.get("arb_deviation"))
    if arb_deviation is None:
        signal_value = _maybe_float(signal.get("signal_value"))
        arb_deviation = abs(signal_value - 1.0) if signal_value is not None else 0.0

    if order_type == "STALE_TAKER":
        return "stale_quote_capture", None

    if arb_deviation >= ARB_LOOSE_MIN_DEVIATION:
        return "bucket_sum_arb_loose", None

    return "bucket_sum_arb_strict", {
        "skipped": True,
        "reason": "arb_requires_stale_or_large_deviation",
        "arb_deviation": round(arb_deviation, 6),
        "loose_min_deviation": round(ARB_LOOSE_MIN_DEVIATION, 6),
    }


def _enrich_signal_reward_context(signal, force_refresh: bool = False):
    token = signal.get("order_token") or signal.get("token_id")
    if not token:
        signal["reward_context"] = {}
        return signal
    signal_price = signal.get("trade_price") or signal.get("yes_price") or signal.get("price") or 0.0
    bet = float(signal.get("bet") or 0.0)
    try:
        signal["reward_context"] = exec_mod.get_reward_context(str(token), float(signal_price or 0.0), bet, force_refresh=force_refresh)
    except Exception as e:
        signal["reward_context"] = {"error": str(e)}
    return signal


def _signal_priority(signal):
    setup_type = signal.get("setup_type") or ""
    strategy_arm = signal.get("strategy_arm") or ""
    reward = signal.get("reward_context") or {}
    reward_score = float(reward.get("reward_score") or 0.0)
    reward_live = 1 if reward.get("eligible") else 0
    reward_size = 1 if reward.get("size_ok_for_rewards") else 0

    if setup_type == "bucket_sum_arb" or strategy_arm in {"bucket_sum_arb_loose", "bucket_sum_arb_candidate"}:
        tier_rank = 0
    elif setup_type == "precision_bracket" or str(strategy_arm).startswith("precision_"):
        tier_rank = 1
    elif strategy_arm in {"directional_b", "conviction_b"}:
        tier_rank = 2
    elif strategy_arm in {"directional_a", "conviction_a"}:
        tier_rank = 4
    else:
        tier_rank = 3

    return (
        tier_rank,
        -reward_size,
        -reward_live,
        -reward_score,
        -abs(float(signal.get("edge", 0.0) or 0.0)),
        -float(signal.get("ev", 0.0) or 0.0),
    )


def _print_reward_summary(signals):
    reward_live = sum(1 for s in signals if (s.get("reward_context") or {}).get("eligible"))
    reward_size = sum(1 for s in signals if (s.get("reward_context") or {}).get("size_ok_for_rewards"))
    if reward_live:
        print(
            f"  [maker] reward-active markets: {reward_live} | rebate-size-qualified: {reward_size}",
            flush=True,
        )




def _resolve_bankroll():
    try:
        import polymarket_model as _pm_tmp
        _live_cash, _ = _pm_tmp.fetch_usdc_balance()
        if _live_cash >= 0:
            print(f"  (live balance: ${_live_cash:.2f})", flush=True)
            return _live_cash
    except Exception as e:
        print(f"  (live balance unavailable: {e})", flush=True)
    fallback = float(_env.get("BANKROLL_OVERRIDE", 2))
    print(f"  (env override: ${fallback:.2f})", flush=True)
    return fallback


def _skip_for_timing(signal):
    hours_left = signal.get("hours_to_resolution")
    setup_type = signal.get("setup_type") or signal.get("urgency") or ""
    if hours_left is None:
        return False
    return hours_left <= 2 and setup_type not in ("bucket_sum_arb", "precision_bracket")


def _trim_precision_groups(signals):
    grouped = {}
    passthrough = []
    for signal in signals:
        if signal.get("setup_type") == "precision_bracket" and signal.get("hedge_group"):
            grouped.setdefault(signal["hedge_group"], []).append(signal)
        else:
            passthrough.append(signal)

    trimmed = []
    for _group, members in grouped.items():
        members.sort(
            key=lambda s: (
                0 if s.get("hedge_role") == "core" else 1,
                -float(s.get("edge", 0.0)),
            )
        )
        core = members[0]
        trimmed.append(core)
        hedges = [m for m in members[1:] if m.get("hedge_role") == "hedge"]
        if hedges:
            best_hedge = hedges[0]
            if float(best_hedge.get("edge", 0.0)) >= max(0.03, float(core.get("edge", 0.0)) * 0.35):
                trimmed.append(best_hedge)
    return passthrough + trimmed

# ─── MAIN ─────────────────────────────────────────────────
# ─── PARALLEL ARB EXECUTION ───────────────────────────────
def execute_arb_group(signals, tier_cfg, dry_run, source="arb"):
    """
    Execute all legs of a bucket-sum arb group in parallel.
    Jane Street principle: all legs must fire simultaneously or the arb breaks.
    Returns list of (signal, result) tuples.
    """
    from collections import defaultdict
    groups = defaultdict(list)
    for s in signals:
        grp = s.get("hedge_group") or (s.get("city","") + "_" + s.get("end_date",""))
        groups[grp].append(s)

    all_results = []
    for grp, legs in groups.items():
        city = legs[0].get("city","?").upper()
        dev = legs[0].get("arb_deviation", 0)
        print(f"\n  [ARB PARALLEL] {city} dev={dev:.3f} legs={len(legs)} — firing all simultaneously", flush=True)

        def _place_leg(s):
            token = s.get("order_token") or s.get("token_id")
            price = s.get("trade_price") or s.get("yes_price", 0)
            bet   = min(float(s.get("bet", 0.50)), tier_cfg["max_bet"])
            s["effective_bet"] = bet
            s["execution_type"] = "ARB_MARKET"
            s["execution_notes"] = f"parallel arb | dev={dev:.3f} | legs={len(legs)}"
            result = place_order(str(token), price, bet, 0, dry_run, max_usdc=tier_cfg["max_bet"])
            return s, result

        with ThreadPoolExecutor(max_workers=min(len(legs), 10)) as ex:
            futures = [ex.submit(_place_leg, s) for s in legs]
            for fut in as_completed(futures):
                s, result = fut.result()
                filled = bool(result.get("success") or result.get("orderID"))
                print(f"    leg {s.get(rng,)} {s.get(direction,)} bet=${s.get(effective_bet,0):.2f} → {OK if filled else FAIL}", flush=True)
                log_trade(s, result, source)
                all_results.append((s, result))

        filled_count = sum(1 for _, r in all_results[-len(legs):] if r.get("success") or r.get("orderID"))
        if filled_count < len(legs):
            print(f"  [ARB WARNING] only {filled_count}/{len(legs)} legs filled — partial arb exposure!", flush=True)
        else:
            total_bet = sum(s.get("effective_bet",0) for s, _ in all_results[-len(legs):])
            print(f"  [ARB COMPLETE] all {len(legs)} legs filled | deployed=${total_bet:.2f} | guaranteed EV=${dev*total_bet/len(legs):.3f}", flush=True)

    return all_results

def run():
    _lock = _acquire_lock()
    if _lock is None:
        return
    now     = datetime.now(timezone.utc)
    bankroll = _resolve_bankroll()
    mode_str = "🔴 LIVE" if not DRY_RUN else "📋 DRY RUN"

    print("=" * 70, flush=True)
    print(f"POLYMARKET AUTO-TRADER  {now.strftime('%Y-%m-%d %H:%M UTC')}  {mode_str}", flush=True)
    print(f"Bankroll: ${bankroll:.2f}", flush=True)
    print("=" * 70, flush=True)

    _tier = cap_mod.get_current_tier(_env)
    _tier_cfg = cap_mod.TIERS[_tier]
    print(f"Tier: {_tier_cfg['name']} | Max bet: ${_tier_cfg['max_bet']:.2f} | Max daily: {_tier_cfg['max_daily_pct']:.0%}", flush=True)

    # Check stop-loss
    if cap_mod.stop_loss_hit(bankroll, bankroll, _tier):
        print("[STOP-LOSS] Drawdown limit hit. No trading today.", flush=True)
        discord_post("🛑 Stop-loss triggered — drawdown limit hit. Bot halted.")
        return

    all_signals = []

    if CITY_FILTER:
        print(f"City filter active: {CITY_FILTER}", flush=True)

    # ── 1. Delta-arb (always run — time-sensitive) ────────
    print("\n[A] DELTA-ARB SCANNER", flush=True)
    try:
        import polymarket_delta_arb as delta_mod
        delta_signals = delta_mod.run(send_alerts=False)
        for s in delta_signals:
            s["_source"] = "delta"
        all_signals.extend(delta_signals)
        print(f"    → {len(delta_signals)} delta signals", flush=True)
    except Exception as e:
        print(f"    Delta scanner error: {e}", flush=True)

    # ── 2. Conviction model (only on ECMWF windows, not GFS-only runs) ──
    if not GFS_ONLY:
        print("\n[B] CONVICTION MODEL", flush=True)
        try:
            import polymarket_model as conv_mod
            conv_signals = conv_mod.run()
            for s in conv_signals:
                s["_source"] = "conviction"
            all_signals.extend(conv_signals)
            print(f"    → {len(conv_signals)} conviction signals", flush=True)
        except Exception as e:
            print(f"    Conviction model error: {e}", flush=True)

    # Apply city filter
    if CITY_FILTER:
        all_signals = [s for s in all_signals if s.get("city","").lower() in CITY_FILTER]
        print(f"After city filter ({CITY_FILTER}): {len(all_signals)} signals", flush=True)

    if not all_signals:
        print("\nNo signals. State saved, nothing to trade.", flush=True)
        discord_post(f"🌤 **{now.strftime('%H:%M UTC')}** — No signals this run.")
        return

    # ── 3. Load today's already-placed questions + tally spend ──
    from datetime import date as _date
    today_str = _date.today().isoformat()
    placed_today_qtns     = set()   # ANY question already bet today (primary dedup key)
    placed_today_yes_qtns = set()   # questions with YES positions (conflict guard)
    placed_today_no_qtns  = set()   # questions with NO positions (conflict guard)
    already_deployed      = 0.0    # USDC already committed today (all successful orders)
    if os.path.exists(TRADE_LOG):
        try:
            with open(TRADE_LOG) as _f:
                for _line in _f:
                    _t = json.loads(_line)
                    _mode = _trade_accounting_mode(_t)
                    _ok = _trade_counts_for_accounting(_t)
                    if _t.get("ts","")[:10] == today_str and _ok and _mode == ACCOUNTING_MODE:
                        _q   = _t.get("question","")
                        _dir = _t.get("direction","").strip()
                        placed_today_qtns.add(_q)
                        already_deployed += _t.get("bet", 0)
                        if _dir == "BUY YES": placed_today_yes_qtns.add(_q)
                        if _dir == "BUY NO":  placed_today_no_qtns.add(_q)
        except Exception: pass

    daily_cap = cap_mod.daily_budget(bankroll, _tier, 0)  # full daily budget

    # Hard stop: if already at/over daily cap, don't place anything
    if already_deployed >= daily_cap and not IGNORE_DAILY_CAP:
        print(f"\n[DAILY CAP HIT] Already deployed ${already_deployed:.2f} today "
              f"(cap ${daily_cap:.2f}). Stopping.", flush=True)
        discord_post(f"🛑 Daily cap hit — ${already_deployed:.2f} deployed. No new orders.")
        return

    if IGNORE_DAILY_CAP:
        remaining_budget = float("inf")
        print(
            f"\n[DAILY CAP OVERRIDE] IGNORE_DAILY_CAP=true | "
            f"already deployed ${already_deployed:.2f} | configured cap ${daily_cap:.2f}",
            flush=True,
        )
    else:
        remaining_budget = daily_cap - already_deployed
    print(f"\n[C] Daily cap: ${daily_cap:.2f}  |  Already deployed: ${already_deployed:.2f}  "
          f"|  Remaining: {'OVERRIDE' if IGNORE_DAILY_CAP else f'${remaining_budget:.2f}'}", flush=True)

    # Deduplicate: by question string (not token ID — token can differ YES/NO between runs)
    deduped = []
    for s in all_signals:
        q = s.get("question", "")
        d = s.get("direction", "").strip()
        if q in placed_today_qtns:
            print(f"  [dedup] already bet today: {q[:55]}...", flush=True); continue
        if d == "BUY YES" and q in placed_today_no_qtns:
            print(f"  [conflict] have NO on this market, skip YES: {q[:50]}", flush=True); continue
        if d == "BUY NO" and q in placed_today_yes_qtns:
            print(f"  [conflict] have YES on this market, skip NO: {q[:50]}", flush=True); continue
        placed_today_qtns.add(q)
        if d == "BUY YES": placed_today_yes_qtns.add(q)
        if d == "BUY NO":  placed_today_no_qtns.add(q)
        if _skip_for_timing(s):
            print(f"  [timing] skip late directional setup: {q[:55]}...", flush=True)
            continue
        deduped.append(s)

    deduped = _trim_precision_groups(deduped)

    if len(deduped) > 1:
        try:
            adjusted_bets = cap_mod.portfolio_kelly(deduped, bankroll, _tier)
            for s, new_bet in zip(deduped, adjusted_bets):
                s["bet"] = new_bet
        except Exception as e:
            print(f"  [portfolio Kelly failed, using single-bet sizes]: {e}", flush=True)

    for s in deduped:
        _enrich_signal_reward_context(s)
    deduped.sort(key=_signal_priority)
    _print_reward_summary(deduped)

    # ── 4. Enforce remaining daily budget ────────────────
    deployed = 0
    final    = []
    for s in deduped:
        capped_bet = min(s["bet"], _tier_cfg["max_bet"])
        if capped_bet <= 0:
            continue
        if deployed + capped_bet > remaining_budget:
            break
        final.append(s)
        deployed += capped_bet

    print(f"\n[C] PLACING ORDERS  ({len(final)} signals | ${deployed:.2f} this run | {mode_str})", flush=True)
    print(f"    Daily cap: ${daily_cap:.2f}  |  Already deployed: ${already_deployed:.2f}  "
          f"|  Stop-loss: -${bankroll*0.15:.2f}", flush=True)

    # ── 5. Place orders ───────────────────────────────────
    # Split: arb signals execute in parallel groups, conviction executes sequentially
    arb_final = [s for s in final if s.get("setup_type") == "bucket_sum_arb"]
    conv_final = [s for s in final if s.get("setup_type") != "bucket_sum_arb"]

    if arb_final:
        print(f"  [ARB] {len(arb_final)} arb legs → parallel execution", flush=True)
        execute_arb_group(arb_final, _tier_cfg, DRY_RUN, source="arb")
        placed_count = len(arb_final)

    placed_embeds = []
    for s in conv_final:
        token    = s.get("order_token") or s.get("token_id")
        signal_price = s.get("trade_price") or s.get("yes_price", 0)
        s["signal_price"] = signal_price
        bet      = min(s["bet"], _tier_cfg["max_bet"])
        s["effective_bet"] = bet
        if bet != s["bet"]:
            print(f"  [tier cap] bet capped ${s['bet']:.2f}→${bet:.2f} (T{_tier} max=${_tier_cfg['max_bet']:.2f})", flush=True)
        side     = 0  # always BUY (YES or NO token directly)
        source   = s.get("_source", "?")
        spread   = exec_mod.get_market_spread(str(token)) or 0.03
        s["reward_context"] = exec_mod.get_reward_context(str(token), signal_price, bet, force_refresh=True)
        s["decision_snapshot"] = exec_mod.capture_trade_snapshot(
            token_id=str(token),
            signal_price=signal_price,
            size_usdc=bet,
            side=side,
            hours_to_resolve=s.get("hours_to_resolution") or 24,
            setup_type=s.get("setup_type", ""),
            force_refresh=True,
        )
        if s["decision_snapshot"].get("reward_context"):
            s["reward_context"] = s["decision_snapshot"].get("reward_context")
        final_price, order_type, exec_notes = exec_mod.place_smart_order(
            token_id=str(token),
            price=signal_price,
            size_usdc=bet,
            dry_run=DRY_RUN,
            spread=spread,
            hours_to_resolve=s.get("hours_to_resolution") or 24,
            setup_type=s.get("setup_type", ""),
            reward_context=s.get("reward_context"),
            book_snapshot=(s.get("decision_snapshot") or {}).get("book"),
        )
        s["execution_type"] = order_type
        s["execution_notes"] = exec_notes
        s["trade_price"] = final_price
        price = final_price
        print(f"  [exec] {order_type} | {exec_notes}", flush=True)

        if s.get("setup_type") == "bucket_sum_arb":
            s["strategy_arm"], skip_result = _classify_bucket_sum_arb(s, order_type)
            if s["strategy_arm"] == "stale_quote_capture":
                s["execution_notes"] += " | strategy=stale_quote_capture"
            elif s["strategy_arm"] == "bucket_sum_arb_loose":
                s["execution_notes"] += (
                    f" | strategy=bucket_sum_arb_loose | "
                    f"arb_deviation={float(s.get('arb_deviation', 0.0)):.3f}"
                )
            if skip_result:
                result = skip_result
                log_trade(s, result, source)
                placed_embeds.append(signal_to_embed(s, source, "skipped"))
                print(
                    "  [exec] skipping weak non-stale bucket-sum arb "
                    f"(deviation={skip_result['arb_deviation']:.3f} < "
                    f"loose threshold {skip_result['loose_min_deviation']:.3f})",
                    flush=True,
                )
                continue

        if s.get("setup_type") == "bucket_sum_arb" and order_type != "STALE_TAKER" and s.get("strategy_arm") == "bucket_sum_arb_loose":
            print("  [exec] allowing loose bucket-sum arb without stale-liquidity because deviation is large enough", flush=True)

        result = place_order(str(token), price, bet, side, DRY_RUN, max_usdc=_tier_cfg["max_bet"])
        s["post_order_snapshot"] = exec_mod.capture_trade_snapshot(
            token_id=str(token),
            signal_price=price,
            size_usdc=bet,
            side=side,
            hours_to_resolve=s.get("hours_to_resolution") or 24,
            setup_type=s.get("setup_type", ""),
            force_refresh=True,
        )
        log_trade(s, result, source)

        ok = result.get("status") == "dry_run" or "orderID" in result or result.get("success")
        if result.get("skipped"):
            embed_status = "skipped"
        elif ok and DRY_RUN:
            embed_status = "dry_run"
        elif ok:
            embed_status = "placed"
        else:
            embed_status = "failed"
        placed_embeds.append(signal_to_embed(s, source, embed_status))
        if not DRY_RUN and ok:
            placed_count += 1
        elif DRY_RUN:
            placed_count += 1

        if ok and s.get("hypothesis") and s.get("signal_value") is not None:
            try:
                import polymarket_research as _res
                _res.record_hypothesis_trade(
                    hypothesis_id=s["hypothesis"],
                    date=s.get("end_date", s.get("target_date", "")),
                    signal_value=float(s["signal_value"]),
                    entry_price=float(price),
                    direction=s.get("direction", ""),
                    question=s.get("question", ""),
                )
            except Exception as e:
                print(f"  [hypothesis record failed]: {e}", flush=True)

        time.sleep(0.3)  # rate limit

    # ── 6. Tier unlock check ─────────────────────────────
    try:
        _stats = cap_mod.load_stats(CALIBRATION_STATS_JSON)
        _new_tier, _reason = cap_mod.check_tier_unlock(_tier, _stats)
        if _new_tier > _tier:
            print(f"\n🎯 TIER UNLOCK AVAILABLE: {cap_mod.TIERS[_new_tier]['name']} — {_reason}", flush=True)
            discord_post(f"🎯 **Tier unlock available!** → {cap_mod.TIERS[_new_tier]['name']}\n{_reason}")
    except Exception: pass

    # ── 7. Discord summary ────────────────────────────────
    total_ev   = sum(s["ev"] for s in final)
    header     = (f"{'📋 DRY RUN' if DRY_RUN else '🔴 LIVE'} **{now.strftime('%H:%M UTC')}**  "
                  f"**{placed_count}/{len(final)} orders** | "
                  f"Deploy **${deployed:.2f}** / ${daily_cap:.2f} | "
                  f"EV **${total_ev:+.2f}**")
    discord_post(header, placed_embeds[:10])

    print(f"\n{'='*70}", flush=True)
    print(f"Done: {placed_count} orders | ${deployed:.2f} deployed | EV ${total_ev:+.2f}", flush=True)
    print(f"Trade log → {TRADE_LOG}", flush=True)

if __name__ == "__main__":
    run()

