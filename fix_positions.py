import json, sys
sys.path.insert(0, '/root/polymarket')
from dotenv import dotenv_values
_env = dotenv_values('/root/polymarket/polymarket.env')

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from py_clob_client.constants import POLYGON

creds = ApiCreds(
    api_key=_env["POLY_API_KEY"],
    api_secret=_env["POLY_SECRET"],
    api_passphrase=_env["POLY_PASSPHRASE"],
)
client = ClobClient(
    host="https://clob.polymarket.com", chain_id=POLYGON,
    key=_env["POLY_PRIVATE_KEY"], creds=creds,
    signature_type=1, funder=_env["POLY_ADDRESS"],
)

# Get open orders raw
orders = client.get_orders()
raw = orders if isinstance(orders, list) else orders.get('data', [])
open_orders = [o for o in raw if str(o.get('status','')).upper() in ('LIVE','OPEN')]

print(f"=== OPEN ORDERS: {len(open_orders)} ===")
for o in open_orders:
    print(json.dumps(o, indent=2)[:400])
    print("---")

# Cancel ALL open orders
if open_orders:
    order_ids = [o['id'] for o in open_orders]
    print(f"\nCancelling {len(order_ids)} open orders...")
    for oid in order_ids:
        try:
            r = client.cancel(oid)
            print(f"  Cancelled {oid[:25]}... → {r}")
        except Exception as e:
            print(f"  FAILED {oid[:25]}... → {e}")

# Check remaining balance
import requests, time, hmac, hashlib, base64
from eth_account import Account
def l2_headers():
    ts = str(int(time.time()))
    msg = ts + "GET" + "/balance"
    sec = base64.urlsafe_b64decode(_env["POLY_SECRET"])
    sig = base64.urlsafe_b64encode(hmac.new(sec, msg.encode(), hashlib.sha256).digest()).decode()
    return {"POLY_ADDRESS": Account.from_key(_env["POLY_PRIVATE_KEY"]).address,
            "POLY_API_KEY": _env["POLY_API_KEY"], "POLY_PASSPHRASE": _env["POLY_PASSPHRASE"],
            "POLY_TIMESTAMP": ts, "POLY_SIGNATURE": sig}
try:
    r = requests.get("https://clob.polymarket.com/balance", headers=l2_headers(), timeout=8)
    print(f"\nCurrent USDC balance: {r.text}")
except Exception as e:
    print(f"Balance check failed: {e}")
