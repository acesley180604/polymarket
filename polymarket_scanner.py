"""
Autonomous Capital Engine — fully self-managing loop.

Every 60s it:
1. Checks cash balance — if increased since last tick (position resolved), triggers redeploy
2. Scans for arb signals — if found above threshold, triggers autotrader
3. Cooldown 300s between autotrader calls to prevent hammering

No human intervention needed. Runs as systemd service.
"""
import time, subprocess, sys, os, json, requests
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv("/root/polymarket/polymarket.env")

SCAN_INTERVAL  = 60
MIN_DEVIATION  = float(os.environ.get("ARB_MIN_DEVIATION", "0.06"))
TRADER_COOLDOWN = 300        # min seconds between autotrader calls
MIN_CASH_TO_DEPLOY = 2.0     # only redeploy if this much cash is free
LOG_FILE       = "/root/polymarket/scanner.log"
LOCK_FILE      = "/tmp/polymarket_scanner.lock"
STATE_FILE     = "/root/polymarket/scanner_state.json"

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = "[" + ts + "] " + str(msg)
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"last_cash": 0.0, "last_trigger": 0}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def get_cash():
    try:
        result = subprocess.run(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0,\"/root/polymarket\")\n"
             "from dotenv import load_dotenv; load_dotenv(\"/root/polymarket/polymarket.env\")\n"
             "import polymarket_model as pm\n"
             "cash, pos = pm.fetch_usdc_balance()\n"
             "print(\"CASH:\"+str(round(cash,4)))"],
            capture_output=True, text=True, cwd="/root/polymarket", timeout=20
        )
        for line in result.stdout.splitlines():
            if line.startswith("CASH:"):
                return float(line.split(":")[1])
    except Exception:
        pass
    return 0.0

def scan_arb():
    try:
        result = subprocess.run(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0,\"/root/polymarket\")\n"
             "from dotenv import load_dotenv; load_dotenv(\"/root/polymarket/polymarket.env\")\n"
             "import json, os; os.environ[\"DRY_RUN\"]=\"true\"\n"
             "import polymarket_model as m\n"
             "sigs = m.run()\n"
             "arb = [s for s in (sigs or []) if s.get(\"setup_type\")==\"bucket_sum_arb\"]\n"
             "print(\"ARB:\"+json.dumps([{\"city\":s.get(\"city\"),\"dev\":s.get(\"arb_deviation\",0),\"bet\":s.get(\"bet\",0)} for s in arb]))"],
            capture_output=True, text=True, cwd="/root/polymarket", timeout=120
        )
        for line in result.stdout.splitlines():
            if line.startswith("ARB:"):
                return json.loads(line[4:])
    except Exception:
        pass
    return []

def trigger_trader(reason):
    log(">>> AUTOTRADER TRIGGERED: " + reason + " <<<")
    subprocess.run([sys.executable, "/root/polymarket/polymarket_autotrader.py"], cwd="/root/polymarket")
    log(">>> AUTOTRADER DONE <<<")

def main():
    import fcntl
    lf = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("Scanner already running. Exiting.")
        return

    log("Autonomous capital engine started | scan=" + str(SCAN_INTERVAL) + "s | cooldown=" + str(TRADER_COOLDOWN) + "s | min_dev=" + str(round(MIN_DEVIATION*100)) + "%")
    state = load_state()

    while True:
        try:
            now = time.time()
            cash = get_cash()
            cash_delta = cash - state["last_cash"]
            since_last = now - state["last_trigger"]
            can_trade  = since_last >= TRADER_COOLDOWN and cash >= MIN_CASH_TO_DEPLOY

            # Trigger 1: cash increased (position resolved, capital returned)
            if cash_delta >= 1.0 and can_trade:
                log("Cash increased $" + str(round(cash_delta,2)) + " (resolved positions) — redeploying $" + str(round(cash,2)))
                trigger_trader("cash_returned $" + str(round(cash_delta,2)))
                state["last_trigger"] = time.time()
                state["last_cash"] = get_cash()
                save_state(state)
                time.sleep(SCAN_INTERVAL)
                continue

            # Trigger 2: arb signals found above threshold
            arb = scan_arb()
            if arb and can_trade:
                best = max(arb, key=lambda x: x.get("dev",0))
                log("Arb signals: " + str(len(arb)) + " | best=" + str(best.get("city","")) + " dev=" + str(round(best.get("dev",0),3)) + " — deploying $" + str(round(cash,2)))
                trigger_trader("arb_signal dev=" + str(round(best.get("dev",0),3)))
                state["last_trigger"] = time.time()
                state["last_cash"] = get_cash()
                save_state(state)
            elif arb:
                log("Arb found (" + str(len(arb)) + " signals) but in cooldown — " + str(int(TRADER_COOLDOWN - since_last)) + "s remaining")
            elif cash >= MIN_CASH_TO_DEPLOY:
                log("Cash $" + str(round(cash,2)) + " idle — no arb signals above " + str(round(MIN_DEVIATION*100)) + "% threshold")
            else:
                log("Cash $" + str(round(cash,2)) + " — no signals, nothing to do")

            state["last_cash"] = cash
            save_state(state)

        except Exception as e:
            log("Loop error: " + str(e))

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    main()
