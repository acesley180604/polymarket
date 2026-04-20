#!/usr/bin/env python3
"""
Polymarket Trading Journal Bot — t.me/weather_predict_poly_bot
"""
import json, os, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta

TOKEN = "8746644595:AAE2h4IxxSfsvmR3qEMzkBXOsF54F6eb7LE"
BASE  = f"https://api.telegram.org/bot{TOKEN}"

TRADES_FILE  = "/root/polymarket/trades.jsonl"
CAL_FILE     = "/root/polymarket/calibration_stats.json"
ARB_FILE     = "/root/polymarket/arb_state.json"
CHAT_IDS     = "/root/polymarket/tg_chat_ids.json"

# ── helpers ──────────────────────────────────────────────────────────────────

def api(method, **p):
    data = urllib.parse.urlencode({k: (json.dumps(v) if isinstance(v, (dict,list)) else v)
                                    for k, v in p.items()}).encode()
    req = urllib.request.Request(f"{BASE}/{method}", data=data)
    with urllib.request.urlopen(req, timeout=35) as r:
        return json.loads(r.read())

def load_trades():
    try:
        return [json.loads(l) for l in open(TRADES_FILE) if l.strip()]
    except:
        return []

def load_cal():
    try:
        return json.load(open(CAL_FILE))
    except:
        return {}

def save_chat(cid):
    ids = load_chats()
    if cid not in ids:
        ids.append(cid)
        json.dump(ids, open(CHAT_IDS, "w"))

def load_chats():
    try:
        return json.load(open(CHAT_IDS))
    except:
        return []

# ── formatters ───────────────────────────────────────────────────────────────

def fmt_dashboard():
    trades = load_trades()
    open_t  = [t for t in trades if t.get("outcome") is None]
    res_t   = [t for t in trades if t.get("outcome") is not None]
    bet     = sum(t.get("bet", 0) for t in open_t)
    ev      = sum(t.get("ev",  0) for t in open_t)
    cal     = load_cal()

    # last model run
    try:
        arb = json.load(open(ARB_FILE))
        lr  = arb.get("last_run", "")
        dt  = datetime.fromisoformat(lr.replace("Z","+00:00")) + timedelta(hours=8)
        lr_s = dt.strftime("%b %d %H:%M HKT")
    except:
        lr_s = "unknown"

    # top cities
    cities = {}
    for t in open_t:
        c = t.get("city","?").title()
        cities[c] = cities.get(c,0)+1
    top = sorted(cities.items(), key=lambda x:-x[1])[:6]
    city_lines = "  ".join(f"{c}({n})" for c,n in top)

    pnl_line = "⏳ No resolved trades yet"
    if cal.get("win_rate"):
        pnl_line = f"🎯 Win rate: *{cal['win_rate']*100:.1f}%*"
    if cal.get("brier_score"):
        pnl_line += f"  |  Brier: *{cal['brier_score']:.4f}*"

    return "\n".join([
        "📊 *POLYMARKET DASHBOARD*",
        f"🕐 _{datetime.now(timezone.utc).strftime('%b %d %H:%M UTC')}_",
        "",
        "━━ BOOK ━━",
        f"📂 Open positions: *{len(open_t)}*",
        f"💵 Deployed:       *${bet:.2f} USDC*",
        f"📈 Expected EV:    *${ev:.2f}*",
        f"✅ Resolved:       *{len(res_t)}*",
        "",
        "━━ PERFORMANCE ━━",
        pnl_line,
        "",
        "━━ CITIES ━━",
        city_lines or "—",
        "",
        f"🔄 Last run: _{lr_s}_",
    ])

def fmt_journal():
    trades = load_trades()
    today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    t_list = [t for t in trades if t.get("ts","").startswith(today)]
    label  = f"Today {today}"
    if not t_list:
        yd = (datetime.now(timezone.utc)-timedelta(days=1)).strftime("%Y-%m-%d")
        t_list = [t for t in trades if t.get("ts","").startswith(yd)]
        label  = f"Yesterday {yd}"

    bet = sum(t.get("bet",0) for t in t_list)
    ev  = sum(t.get("ev",0)  for t in t_list)

    by_city = {}
    for t in t_list:
        c = t.get("city","?").title()
        by_city.setdefault(c,[]).append(t)

    lines = [
        f"📓 *TRADE JOURNAL — {label}*",
        f"_{len(t_list)} trades | ${bet:.2f} deployed | EV ${ev:.2f}_",
        "",
    ]
    for city, ct in sorted(by_city.items()):
        cb = sum(x.get("bet",0) for x in ct)
        lines.append(f"🌍 *{city}* — {len(ct)} trades  ${cb:.2f}")
        for t in ct[:2]:
            d = "🟢BUY" if "YES" in t.get("direction","") else "🔴NO"
            lines.append(f"  {d} @{t['price']:.3f}  ${t['bet']:.2f}  ev{t.get('ev',0):.3f}")
        if len(ct)>2:
            lines.append(f"  ···+{len(ct)-2} more")
    return "\n".join(lines)

def fmt_positions():
    trades = load_trades()
    open_t = [t for t in trades if t.get("outcome") is None]

    by_date = {}
    for t in open_t:
        d = t.get("end_date","?")
        by_date.setdefault(d,[]).append(t)

    lines = [f"📂 *OPEN POSITIONS* ({len(open_t)} total)", ""]
    for d in sorted(by_date):
        pos = by_date[d]
        b   = sum(x.get("bet",0) for x in pos)
        e   = sum(x.get("ev",0)  for x in pos)
        cities = {}
        for t in pos:
            c = t.get("city","?").title()
            cities[c] = cities.get(c,0)+1
        cs = " ".join(f"{c}×{n}" for c,n in sorted(cities.items()))
        lines += [
            f"📅 *{d}* — {len(pos)} trades  ${b:.2f}  EV${e:.2f}",
            f"   {cs}",
        ]
    return "\n".join(lines)

def fmt_stats():
    trades = load_trades()
    cal    = load_cal()
    arms   = {}
    cities = {}
    for t in trades:
        a = t.get("strategy_arm") or t.get("source","?")
        arms.setdefault(a,{"n":0,"bet":0.0})
        arms[a]["n"]   += 1
        arms[a]["bet"] += t.get("bet",0)
        c = t.get("city","?").title()
        cities.setdefault(c,{"n":0,"bet":0.0})
        cities[c]["n"]   += 1
        cities[c]["bet"] += t.get("bet",0)

    lines = [
        "📈 *PERFORMANCE STATS*",
        f"Total trades:  *{len(trades)}*",
        f"Resolved:      *{cal.get('n_resolved',0)}*",
        f"Open:          *{len(trades)-cal.get('n_resolved',0)}*",
        "",
        "━━ BY STRATEGY ━━",
    ]
    for a, d in sorted(arms.items(), key=lambda x:-x[1]["n"]):
        lines.append(f"  *{a}*: {d['n']} trades  ${d['bet']:.2f}")
    lines += ["","━━ TOP CITIES ━━"]
    for c, d in sorted(cities.items(), key=lambda x:-x[1]["bet"])[:8]:
        lines.append(f"  {c}: {d['n']} trades  ${d['bet']:.2f}")
    if cal.get("win_rate"):
        lines += ["", f"🎯 Win Rate: *{cal['win_rate']*100:.1f}%*"]
    if cal.get("brier_score"):
        lines.append(f"📉 Brier:    *{cal['brier_score']:.4f}*")
    return "\n".join(lines)

def fmt_system():
    checks = [
        ("Autotrader",   "/var/log/polymarket_autotrader.log"),
        ("HK Monitor",   "/root/polymarket/hk_monitor.log"),
        ("CLV",          "/root/polymarket/clv.log"),
        ("Truth",        "/root/polymarket/truth_resolution.log"),
        ("EMOS Train",   "/root/polymarket/emos_train.log"),
        ("Research",     "/root/polymarket/research.log"),
        ("Reddit Scout", "/var/log/cron-reddit-scout-v5.log"),
        ("YouTube Scout","/var/log/cron-youtube-scout.log"),
    ]
    lines = ["⚙️ *SYSTEM STATUS*",""]
    now = datetime.now(timezone.utc)
    for name, path in checks:
        try:
            mtime = os.path.getmtime(path)
            dt    = datetime.fromtimestamp(mtime, tz=timezone.utc)
            mins  = int((now - dt).total_seconds() / 60)
            hk    = dt + timedelta(hours=8)
            ts    = hk.strftime("%b %d %H:%M HKT")
            icon  = "✅" if mins < 120 else ("⚠️" if mins < 480 else "❌")
            lines.append(f"{icon} *{name}*: {ts} ({mins}m ago)")
        except:
            lines.append(f"❓ *{name}*: no log")
    try:
        n = sum(1 for l in open(TRADES_FILE) if l.strip())
        lines += ["", f"📊 Trades on file: *{n}*"]
    except:
        pass
    return "\n".join(lines)

# ── keyboard ─────────────────────────────────────────────────────────────────

KB = {"inline_keyboard": [
    [{"text":"📊 Dashboard","callback_data":"dashboard"},
     {"text":"📓 Journal",  "callback_data":"journal"}],
    [{"text":"📂 Positions","callback_data":"positions"},
     {"text":"📈 Stats",    "callback_data":"stats"}],
    [{"text":"⚙️ System",  "callback_data":"system"},
     {"text":"🔄 Refresh", "callback_data":"dashboard"}],
]}

# ── send / edit ───────────────────────────────────────────────────────────────

def send(cid, text, kb=KB):
    try:
        api("sendMessage", chat_id=cid, text=text,
            parse_mode="Markdown", reply_markup=kb)
    except Exception as e:
        print(f"send err: {e}")

def edit(cid, mid, text, kb=KB):
    try:
        api("editMessageText", chat_id=cid, message_id=mid,
            text=text, parse_mode="Markdown", reply_markup=kb)
    except Exception as e:
        print(f"edit err: {e}")

VIEWS = {
    "dashboard": fmt_dashboard,
    "journal":   fmt_journal,
    "positions": fmt_positions,
    "stats":     fmt_stats,
    "system":    fmt_system,
}

def handle_msg(msg):
    cid  = msg["chat"]["id"]
    text = msg.get("text","").split()[0].lower().rstrip("@")
    save_chat(cid)
    fn   = VIEWS.get(text.lstrip("/"), fmt_dashboard)
    send(cid, fn())

def handle_cb(cb):
    cid = cb["message"]["chat"]["id"]
    mid = cb["message"]["message_id"]
    save_chat(cid)
    try:
        api("answerCallbackQuery", callback_query_id=cb["id"])
    except:
        pass
    fn = VIEWS.get(cb.get("data","dashboard"), fmt_dashboard)
    edit(cid, mid, fn())

# ── main loop ─────────────────────────────────────────────────────────────────

def main():
    print("🤖 Polymarket Journal Bot started")
    offset = 0
    while True:
        try:
            resp = api("getUpdates", offset=offset, timeout=30,
                       allowed_updates=["message","callback_query"])
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                if "message" in upd:
                    handle_msg(upd["message"])
                elif "callback_query" in upd:
                    handle_cb(upd["callback_query"])
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"poll err: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
