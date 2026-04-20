import json, os, sys
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

orders = client.get_orders()
open_orders = [o for o in (orders if isinstance(orders, list) else orders.get('data', []))
               if o.get('status') in ('LIVE', 'OPEN', 'live', 'open')]

print(f"Open orders on CLOB: {len(open_orders)}")
total_locked = 0
for o in open_orders:
    size_matched = float(o.get('size_matched', 0) or 0)
    size = float(o.get('size', 0) or 0)
    remaining = size - size_matched
    price = float(o.get('price', 0) or 0)
    locked = remaining * price
    total_locked += locked
    print(f"  {o['id'][:20]}...  {o.get('side','?')}  price={price:.3f}  "
          f"size={size:.2f}  matched={size_matched:.2f}  locked=${locked:.2f}")
print(f"Total USDC locked: ${total_locked:.2f}")
