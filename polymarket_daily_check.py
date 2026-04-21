#!/usr/bin/env python3
"""Daily verification check for polymarket arb system.
Runs at 09:00 UTC. Checks scale-up gate metrics and sends Discord report.
"""
import json, os, sys, time, subprocess
from datetime import datetime, timezone

sys.path.insert(0, "/root/polymarket")
from polymarket_core import ENV, discord_post

TRADES_FILE   = "/root/polymarket/trades.jsonl"
SCANNER_STATE = "/root/polymarket/scanner_state.json"
SCANNER_SVC   = "polymarket-scanner"

GATE_500  = {"min_arb_resolved": 30,  "min_win_rate": 0.65, "min_fill_rate": 0.20, "min_avg_dev": 0.06}
GATE_2000 = {"min_arb_resolved": 100, "min_win_rate": 0.68, "min_fill_rate": 0.25, "min_avg_dev": 0.06}

def load_trades():
    if not os.path.exists(TRADES_FILE):
        return []
    out = []
    with open(TRADES_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                try: out.append(json.loads(line))
                except: pass
    return out

def is_arb(t):
    return (t.get("setup_type","").startswith("bucket_sum_arb") or
            t.get("strategy_arm","").startswith("bucket_sum_arb") or
            t.get("source","") == "arb")

def scanner_status():
    try:
        r = subprocess.run(["systemctl","is-active", SCANNER_SVC],
                           capture_output=True, text=True, timeout=5)
        active = r.stdout.strip() == "active"
        state  = json.load(open(SCANNER_STATE)) if os.path.exists(SCANNER_STATE) else {}
        age    = round((time.time() - float(state.get("last_trigger", 0))) / 60, 1)
        cash   = float(state.get("last_cash", 0))
        return active, age, cash
    except:
        return False, -1, 0.0

def check(label, val, threshold, fmt=".1%", higher_is_better=True, pending=False):
    if pending:
        return "⏳"
    ok = (val >= threshold) if higher_is_better else (val <= threshold)
    display = format(val, fmt) if fmt else str(val)
    return f"{'✅' if ok else '❌'} {label}: {display} ({'≥' if higher_is_better else '≤'}{format(threshold, fmt) if fmt else threshold})"

def run():
    trades = load_trades()
    arb    = [t for t in trades if is_arb(t)]
    n_arb  = len(arb)

    # Resolution stats
    resolved     = [t for t in arb if t.get("outcome") is not None]
    n_res        = len(resolved)
    wins         = [t for t in resolved if str(t.get("outcome","")).lower() in ("win","true","yes","correct","1")]
    win_rate     = len(wins) / max(n_res, 1)

    # Fill rate
    matched      = [t for t in arb if t.get("result",{}).get("status","").lower() == "matched"]
    live_open    = [t for t in arb if t.get("result",{}).get("status","").lower() in ("live",)]
    fill_rate    = len(matched) / max(n_arb, 1)

    # Deviation quality
    devs = []
    for t in arb:
        notes = t.get("execution_notes","")
        if "arb_deviation=" in notes:
            try: devs.append(float(notes.split("arb_deviation=")[1].split()[0].rstrip(",")))
            except: pass
    avg_dev   = sum(devs) / len(devs) if devs else 0.0
    pct_above = sum(1 for d in devs if d >= 0.06) / max(len(devs), 1)

    # Capital & EV
    capital_deployed = sum(float(t.get("bet",0)) for t in arb)
    pending_ev       = sum(float(t.get("ev",0)) for t in arb
                           if t.get("outcome") is None and
                           t.get("result",{}).get("status","").lower() in ("live","matched"))
    realized_pnl     = sum(float(t.get("pnl") or t.get("profit") or 0) for t in resolved)

    # Days running
    first_ts = None
    for t in arb:
        ts = t.get("ts","")
        if ts:
            try: first_ts = datetime.fromisoformat(ts.replace("Z","+00:00")); break
            except: pass
    days = (datetime.now(timezone.utc) - first_ts).days if first_ts else 0

    # Scanner
    scanner_ok, scanner_age, cash = scanner_status()

    # ── Gate evaluation ──────────────────────────────────────────────────────
    def gate_status(g):
        if n_res < 3:
            return f"⏳ Accumulating data ({n_res}/{g['min_arb_resolved']} resolved)"
        res_ok   = n_res     >= g["min_arb_resolved"]
        wr_ok    = win_rate  >= g["min_win_rate"]    if n_res >= 5 else None
        fill_ok  = fill_rate >= g["min_fill_rate"]
        dev_ok   = avg_dev   >= g["min_avg_dev"]
        checks   = [res_ok, fill_ok, dev_ok] + ([wr_ok] if wr_ok is not None else [])
        passed   = all(checks)
        icons    = {True:"✅", False:"❌", None:"⏳"}
        return (f"{icons[res_ok]}res≥{g['min_arb_resolved']}({n_res})  "
                f"{icons[wr_ok]}wr≥{g['min_win_rate']:.0%}({win_rate:.0%})  "
                f"{icons[fill_ok]}fill≥{g['min_fill_rate']:.0%}({fill_rate:.0%})  "
                f"{icons[dev_ok]}dev≥{g['min_avg_dev']:.0f}%({avg_dev:.1%})  "
                f"→ {'**PASS — DEPOSIT NOW**' if passed else 'hold'}")

    # ── Red flags ────────────────────────────────────────────────────────────
    flags = []
    if not scanner_ok:                    flags.append("Scanner service is DOWN")
    if scanner_ok and scanner_age > 30:   flags.append(f"No scanner trigger in {scanner_age:.0f}m")
    if avg_dev < 0.04 and n_arb > 20:     flags.append(f"Avg arb deviation {avg_dev:.3f} below 0.04 (fee floor concern)")
    if fill_rate < 0.02 and n_arb > 30:   flags.append(f"Fill rate {fill_rate:.1%} near zero — check order pricing")
    from datetime import date as _date; stale = sum(1 for t in arb if t.get("end_date") and (_date.today() - _date.fromisoformat(t["end_date"][:10])).days >= 2 and t.get("outcome") is None)
    if stale > 10 and n_res == 0:         flags.append("Truth resolution broken — 0 resolved after 80+ trades")
    if cash < 0.50 and n_arb > 0:         flags.append(f"Cash ${cash:.2f} — no capital to redeploy")

    health = ("🔴 CRITICAL" if len(flags) >= 2 else
              "🟡 WARNING"  if flags else "🟢 HEALTHY")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = f"""**Polymarket Daily Check** — {now}
Status: {health}  |  Day {days}  |  Cash: ${cash:.2f}

**Arb Funnel**
• Placed: {n_arb}  |  Open: {len(live_open)}  |  Filled: {len(matched)} ({fill_rate:.0%})
• Resolved: {n_res}  |  Win rate: {f'{win_rate:.0%}' if n_res >= 3 else 'TBD'}
• Avg deviation: {avg_dev:.3f}  ({pct_above:.0%} ≥ 0.06 threshold)
• Capital deployed: ${capital_deployed:.2f}  |  Pending EV: ${pending_ev:.2f}
• Realized PnL: ${realized_pnl:.2f}

**$500 Gate**
{gate_status(GATE_500)}

**$2000 Gate**
{gate_status(GATE_2000)}

**Scanner**
{'✅ active' if scanner_ok else '❌ DOWN'}  |  Last trigger: {scanner_age:.0f}m ago"""

    if flags:
        msg += "\n\n**Flags**\n" + "\n".join(f"• {f}" for f in flags)

    return msg, flags

if __name__ == "__main__":
    msg, flags = run()
    print(msg)
    discord_post(msg)
    if flags:
        discord_post("@here Action needed: " + " | ".join(flags))
