#!/usr/bin/env python3
"""
Polymarket Trading Journal Bot — t.me/weather_predict_poly_bot
Telegram polling bot + Flask API server for mini app
"""
import json, os, time, threading, subprocess, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, send_file, request

# ── config ────────────────────────────────────────────────────────────────────
TOKEN        = "8746644595:AAE2h4IxxSfsvmR3qEMzkBXOsF54F6eb7LE"
BASE         = f"https://api.telegram.org/bot{TOKEN}"
POLY_DIR     = "/root/polymarket"
TRADES_FILE  = f"{POLY_DIR}/trades.jsonl"
CAL_FILE     = f"{POLY_DIR}/calibration_stats.json"
ARB_FILE     = f"{POLY_DIR}/arb_state.json"
SCANNER_FILE = f"{POLY_DIR}/scanner_state.json"
CHAT_IDS     = f"{POLY_DIR}/tg_chat_ids.json"
TUNNEL_URL   = f"{POLY_DIR}/.tunnel_url"
MINIAPP_HTML = f"{POLY_DIR}/miniapp.html"
SERVER_PORT  = 3847

app = Flask(__name__)

# ── helpers ───────────────────────────────────────────────────────────────────
def load_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return default if default is not None else {}

def load_trades():
    try:
        return [json.loads(l) for l in open(TRADES_FILE) if l.strip()]
    except:
        return []

def is_arb(t):
    return (t.get("setup_type","").startswith("bucket_sum_arb") or
            t.get("strategy_arm","").startswith("bucket_sum_arb") or
            t.get("source","") == "arb")

def get_tunnel_url():
    try:
        return open(TUNNEL_URL).read().strip()
    except:
        return None

def api(method, **p):
    data = urllib.parse.urlencode(
        {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in p.items()}
    ).encode()
    req = urllib.request.Request(f"{BASE}/{method}", data=data)
    with urllib.request.urlopen(req, timeout=35) as r:
        return json.loads(r.read())

def save_chat(cid):
    ids = load_json(CHAT_IDS, [])
    if cid not in ids:
        ids.append(cid)
        json.dump(ids, open(CHAT_IDS, "w"))

# ── API data builder ──────────────────────────────────────────────────────────
def build_data():
    trades   = load_trades()
    arb      = [t for t in trades if is_arb(t)]
    open_t   = [t for t in trades if t.get("outcome") is None]
    resolved = [t for t in trades if t.get("outcome") is not None]
    arb_res  = [t for t in arb    if t.get("outcome") is not None]
    arb_wins = [t for t in arb_res if str(t.get("outcome","")).lower() in ("win","true","yes","correct","1")]
    matched  = [t for t in arb if t.get("result",{}).get("status","").lower() == "matched"]

    # deviations
    devs = []
    for t in arb:
        n = t.get("execution_notes","")
        if "arb_deviation=" in n:
            try: devs.append(float(n.split("arb_deviation=")[1].split()[0].rstrip(",")))
            except: pass

    cal     = load_json(CAL_FILE, {})
    scanner = load_json(SCANNER_FILE, {})
    now     = time.time()

    # scanner age
    last_trigger = float(scanner.get("last_trigger", 0))
    scanner_age_min = round((now - last_trigger) / 60, 1) if last_trigger else -1

    # system log ages
    def log_age_min(path):
        try:
            return round((now - os.path.getmtime(path)) / 60)
        except:
            return -1

    # scanner health
    try:
        r = subprocess.run(["systemctl","is-active","polymarket-scanner"],
                           capture_output=True, text=True, timeout=3)
        scanner_active = r.stdout.strip() == "active"
    except:
        scanner_active = False

    # recent scanner log lines
    try:
        lines = open(f"{POLY_DIR}/scanner.log").readlines()[-15:]
        scanner_log = [l.strip() for l in lines if l.strip()]
    except:
        scanner_log = []

    cash   = float(scanner.get("last_cash", 0))
    n_arb  = len(arb)
    n_res  = len(arb_res)
    wr     = len(arb_wins) / max(n_res, 1) if n_res >= 3 else None
    fr     = len(matched) / max(n_arb, 1)
    avg_d  = sum(devs) / len(devs) if devs else 0.0
    pct_d  = sum(1 for d in devs if d >= 0.06) / max(len(devs), 1)

    # last 50 trades for table
    trade_rows = []
    for t in sorted(trades, key=lambda x: x.get("ts",""), reverse=True)[:50]:
        trade_rows.append({
            "ts":        t.get("ts","")[:16].replace("T"," "),
            "city":      t.get("city","?").title(),
            "direction": t.get("direction",""),
            "price":     round(float(t.get("price",0)),3),
            "bet":       round(float(t.get("bet",0)),2),
            "status":    t.get("result",{}).get("status","?"),
            "outcome":   t.get("outcome"),
            "setup":     t.get("setup_type") or t.get("source","?"),
            "dev":       next((float(n.split("arb_deviation=")[1].split()[0].rstrip(","))
                               for n in [t.get("execution_notes","")] if "arb_deviation=" in n), None),
            "end_date":  t.get("end_date","?"),
        })

    # pending EV
    pending_ev = sum(float(t.get("ev",0)) for t in arb
                     if t.get("outcome") is None and
                     t.get("result",{}).get("status","").lower() in ("live","matched"))

    # days running
    first_ts = None
    for t in arb:
        ts = t.get("ts","")
        if ts:
            try: first_ts = datetime.fromisoformat(ts.replace("Z","+00:00")); break
            except: pass
    days = (datetime.now(timezone.utc) - first_ts).days if first_ts else 0

    # disk/mem
    try:
        disk = int(subprocess.check_output(["df","--output=pcent","/"],text=True).split()[-1].rstrip("%"))
    except:
        disk = -1
    try:
        mi = open("/proc/meminfo").read()
        total = int([l for l in mi.split("\n") if "MemTotal" in l][0].split()[1])
        avail = int([l for l in mi.split("\n") if "MemAvailable" in l][0].split()[1])
        mem = round((1 - avail/total)*100)
    except:
        mem = -1

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "days_running": days,
        "portfolio": {
            "cash":           round(cash, 2),
            "deployed":       round(sum(float(t.get("bet",0)) for t in open_t), 2),
            "pending_ev":     round(pending_ev, 2),
            "pnl_realized":   0.0,
            "open_positions": len(open_t),
            "total_trades":   len(trades),
            "arb_trades":     n_arb,
        },
        "scanner": {
            "active":           scanner_active,
            "last_trigger_min": scanner_age_min,
            "cash":             round(cash, 2),
            "log":              scanner_log,
        },
        "performance": {
            "arb_total":    n_arb,
            "resolved":     n_res,
            "wins":         len(arb_wins),
            "win_rate":     round(wr, 4) if wr is not None else None,
            "fill_rate":    round(fr, 4),
            "matched":      len(matched),
            "avg_dev":      round(avg_d, 4),
            "pct_above_06": round(pct_d, 4),
            "pending_ev":   round(pending_ev, 2),
        },
        "gate": {
            "g500": {
                "resolved_target": 30,  "wr_target": 0.65,
                "fill_target": 0.20,    "dev_target": 0.06,
                "resolved": n_res,      "win_rate": round(wr,4) if wr else None,
                "fill_rate": round(fr,4), "avg_dev": round(avg_d,4),
            },
            "g2000": {
                "resolved_target": 100, "wr_target": 0.68,
                "fill_target": 0.25,    "dev_target": 0.06,
                "resolved": n_res,      "win_rate": round(wr,4) if wr else None,
                "fill_rate": round(fr,4), "avg_dev": round(avg_d,4),
            },
        },
        "system": {
            "scanner_active":      scanner_active,
            "scanner_age_min":     scanner_age_min,
            "autosell_age_min":    log_age_min(f"{POLY_DIR}/autosell.log"),
            "truth_age_min":       log_age_min(f"{POLY_DIR}/truth_resolution.log"),
            "daily_check_age_min": log_age_min(f"{POLY_DIR}/daily_check.log"),
            "emos_age_min":        log_age_min(f"{POLY_DIR}/emos_train.log"),
            "disk_pct":            disk,
            "mem_pct":             mem,
        },
        "trades": trade_rows,
    }

_data_cache = {"ts": 0, "data": None}
def get_data_cached():
    if time.time() - _data_cache["ts"] > 10:
        _data_cache["data"] = build_data()
        _data_cache["ts"]   = time.time()
    return _data_cache["data"]

# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route("/")
def index():
    resp = send_file(MINIAPP_HTML, mimetype="text/html")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"]        = "no-cache"
    return resp

@app.route("/api/data")
def api_data():
    try:
        data = get_data_cached()
        resp = jsonify({"ok": True, "data": data})
        resp.headers["Cache-Control"] = "no-store"
        return resp
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ── Telegram bot ──────────────────────────────────────────────────────────────
def send(cid, text, kb=None):
    kw = {"chat_id": cid, "text": text, "parse_mode": "Markdown"}
    if kb:
        kw["reply_markup"] = kb
    try:
        api("sendMessage", **kw)
    except Exception as e:
        print(f"send err: {e}")

def edit(cid, mid, text, kb=None):
    kw = {"chat_id": cid, "message_id": mid, "text": text, "parse_mode": "Markdown"}
    if kb:
        kw["reply_markup"] = kb
    try:
        api("editMessageText", **kw)
    except Exception as e:
        print(f"edit err: {e}")

def make_kb(with_webapp=True):
    url = get_tunnel_url()
    rows = [
        [{"text":" Dashboard","callback_data":"dashboard"},
         {"text":" Positions","callback_data":"positions"}],
        [{"text":" Stats",    "callback_data":"stats"},
         {"text":" System",  "callback_data":"system"}],
    ]
    if url and with_webapp:
        rows.insert(0, [{"text":" Open Dashboard", "web_app": {"url": url}}])
    rows.append([{"text":" Refresh","callback_data":"dashboard"}])
    return {"inline_keyboard": rows}

def fmt_dashboard():
    d = build_data()
    p = d["portfolio"]
    perf = d["performance"]
    wr = f"{perf['win_rate']*100:.1f}%" if perf["win_rate"] else "TBD"
    return "\n".join([
        " *POLYMARKET DASHBOARD*",
        f" _{datetime.now(timezone.utc).strftime('%b %d %H:%M UTC')}_  |  Day {d['days_running']}",
        "",
        "━━ BOOK ━━",
        f" Cash:      *${p['cash']:.2f}*",
        f" Deployed:  *${p['deployed']:.2f}*",
        f" Pending EV: *${p['pending_ev']:.2f}*",
        f" Resolved:  *{perf['resolved']}*",
        "",
        "━━ ARB PERFORMANCE ━━",
        f" Win rate: *{wr}*  |  Fill: *{perf['fill_rate']*100:.0f}%*",
        f" Avg dev: *{perf['avg_dev']:.3f}*  |  Trades: *{perf['arb_total']}*",
        "",
        f"$500 gate: *{perf['resolved']}/30* resolved",
    ])

def fmt_positions():
    trades = load_trades()
    open_t = [t for t in trades if t.get("outcome") is None]
    by_date = {}
    for t in open_t:
        d = t.get("end_date","?")
        by_date.setdefault(d,[]).append(t)
    lines = [f" *OPEN POSITIONS* ({len(open_t)} total)", ""]
    for d in sorted(by_date):
        pos = by_date[d]
        bet = sum(float(x.get("bet",0)) for x in pos)
        cities = {}
        for t in pos:
            c = t.get("city","?").title()
            cities[c] = cities.get(c,0)+1
        cs = " ".join(f"{c}×{n}" for c,n in sorted(cities.items()))
        lines += [f" *{d}* — {len(pos)} trades  ${bet:.2f}", f"   {cs}"]
    return "\n".join(lines)

def fmt_stats():
    trades = load_trades()
    arb = [t for t in trades if is_arb(t)]
    arms = {}
    for t in arb:
        a = t.get("strategy_arm") or t.get("source","?")
        arms.setdefault(a,{"n":0,"bet":0.0})
        arms[a]["n"]   += 1
        arms[a]["bet"] += float(t.get("bet",0))
    lines = [f" *STATS*  ({len(arb)} arb trades)", ""]
    for a, d in sorted(arms.items(), key=lambda x:-x[1]["n"]):
        lines.append(f"  *{a[:30]}*: {d['n']} trades  ${d['bet']:.2f}")
    return "\n".join(lines)

def fmt_system():
    d = build_data()["system"]
    def icon(age, warn=120, crit=480):
        if age < 0:    return ""
        if age < warn: return ""
        if age < crit: return ""
        return ""
    lines = [
        " *SYSTEM*", "",
        f"{'' if d['scanner_active'] else ''} Scanner service  _{d['scanner_age_min']:.0f}m ago_",
        f"{icon(d['autosell_age_min'],10,30)} Autosell  _{d['autosell_age_min']}m ago_",
        f"{icon(d['truth_age_min'],130,300)} Truth resolve  _{d['truth_age_min']}m ago_",
        f"{icon(d['daily_check_age_min'],1500,1600)} Daily check  _{d['daily_check_age_min']}m ago_",
        "",
        f" Disk: *{d['disk_pct']}%*  |  RAM: *{d['mem_pct']}%*",
    ]
    return "\n".join(lines)

VIEWS = {
    "dashboard": fmt_dashboard,
    "positions": fmt_positions,
    "stats":     fmt_stats,
    "system":    fmt_system,
}

def handle_msg(msg):
    cid  = msg["chat"]["id"]
    text = msg.get("text","").split()[0].lower().rstrip("@")
    save_chat(cid)
    if text in ("/start", "start"):
        send(cid, fmt_dashboard(), make_kb())
    else:
        fn = VIEWS.get(text.lstrip("/"), fmt_dashboard)
        send(cid, fn(), make_kb())

def handle_cb(cb):
    cid = cb["message"]["chat"]["id"]
    mid = cb["message"]["message_id"]
    save_chat(cid)
    try:
        api("answerCallbackQuery", callback_query_id=cb["id"])
    except:
        pass
    fn = VIEWS.get(cb.get("data","dashboard"), fmt_dashboard)
    edit(cid, mid, fn(), make_kb())

def poll_loop():
    print(" Polymarket Journal Bot started")
    offset = 0
    while True:
        try:
            resp = api("getUpdates", offset=offset, timeout=30,
                       allowed_updates=["message","callback_query"])
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                if "message"       in upd: handle_msg(upd["message"])
                elif "callback_query" in upd: handle_cb(upd["callback_query"])
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"poll err: {e}")
            time.sleep(5)

if __name__ == "__main__":
    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    print(f" Flask server on :{SERVER_PORT}")
    app.run(host="0.0.0.0", port=SERVER_PORT, debug=False, use_reloader=False)
