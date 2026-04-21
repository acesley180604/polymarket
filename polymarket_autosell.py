"""
Auto-Sell: find weather positions >SELL_THRESHOLD, sell to recycle capital.
Uses data-api for positions + CLOB midpoint for live prices.
Runs every 5 min via cron.
"""
import json, os, requests, time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv("/root/polymarket/polymarket.env")
from polymarket_core import ENV, CLOB_API, discord_post, get_clob_client

DRY_RUN        = ENV.get("DRY_RUN","true").lower() == "true"
SELL_THRESHOLD = float(ENV.get("AUTOSELL_THRESHOLD","0.95"))
DATA_API       = "https://data-api.polymarket.com"
ADDR           = ENV.get("POLY_ADDRESS","")

def fetch_weather_positions():
    try:
        r = requests.get(DATA_API+"/positions?user="+ADDR+"&sizeThreshold=0.01&limit=200", timeout=15)
        if r.status_code == 200:
            all_pos = r.json()
            return [p for p in all_pos
                    if "temperature" in (p.get("title","") or "").lower()
                    and float(p.get("size",0) or 0) >= 1.0]
    except Exception as e:
        print("  [positions] fetch error:", e, flush=True)
    return []

def get_mid(token_id):
    try:
        r = requests.get(f"{CLOB_API}/midpoint", params={"token_id": token_id}, timeout=8)
        if r.status_code == 200:
            return float(r.json().get("mid",0) or 0)
    except: pass
    return 0.0

def sell_position(token_id, size, price, title):
    if DRY_RUN:
        print(f"  [DRY] SELL {size:.0f} @ {price:.3f} — {title[:40]}", flush=True)
        return True
    try:
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import SELL
        client = get_clob_client()
        sell_price = round(max(price - 0.01, 0.85), 3)
        order = client.create_order(OrderArgs(token_id=token_id, price=sell_price, size=size, side=SELL))
        result = client.post_order(order, "GTC")
        ok = isinstance(result, dict) and bool(result.get("orderID") or result.get("success"))
        print(f"  {OK if ok else FAIL} sell {title[:40]} @ {sell_price}", flush=True)
        return ok
    except Exception as e:
        print(f"  SELL ERROR: {str(e)[:100]}", flush=True)
        return False

def run():
    now = datetime.now(timezone.utc)
    print("="*55, flush=True)
    print(f"AUTO-SELL {now.strftime('%Y-%m-%d %H:%M UTC')} threshold={SELL_THRESHOLD:.0%}", flush=True)

    positions = fetch_weather_positions()
    if not positions:
        print("  No weather positions found.", flush=True)
        return

    # Fetch live prices in parallel
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(get_mid, p.get("asset","")): p for p in positions}
        priced = [(futures[f], f.result()) for f in as_completed(futures)]

    sellable = [(p, mid) for p, mid in priced if mid >= SELL_THRESHOLD]
    sellable.sort(key=lambda x: -x[1])

    print(f"  Positions: {len(positions)} | Sellable (>{SELL_THRESHOLD:.0%}): {len(sellable)}", flush=True)
    if not sellable:
        # Show top positions anyway
        top = sorted(priced, key=lambda x: -x[1])[:5]
        for p, mid in top:
            print(f"  {(p.get(title,))[:45]} @ {mid:.3f}", flush=True)
        return

    freed = 0.0
    for p, mid in sellable:
        token = p.get("asset","")
        size  = float(p.get("size",0))
        title = p.get("title","") or ""
        val   = size * mid
        print(f"\n  SELL {title[:48]}", flush=True)
        print(f"  size={size:.0f} mid={mid:.3f} est=${val:.2f}", flush=True)
        ok = sell_position(token, size, mid, title)
        if ok:
            freed += val
        time.sleep(0.3)

    print(f"\n  Freed: ${freed:.2f}", flush=True)

    # Redeploy if meaningful cash freed
    if freed >= 2.0 and not DRY_RUN:
        print("\n  [REDEPLOY] triggering autotrader...", flush=True)
        import subprocess, sys
        subprocess.run([sys.executable, "/root/polymarket/polymarket_autotrader.py"], cwd="/root/polymarket")

if __name__ == "__main__":
    run()
