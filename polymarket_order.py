"""
Polymarket Order Placer
Signs EIP-712 orders with private key + submits via CLOB API with L2 auth
"""
import requests, json, time, os
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3
from polymarket_core import ENV as _env, CLOB_API, l2_headers

PRIVATE_KEY = _env["POLY_PRIVATE_KEY"]
ADDRESS     = _env["POLY_ADDRESS"]
DRY_RUN     = _env.get("DRY_RUN", "true").lower() == "true"
CHAIN_ID    = 137  # Polygon

# ─── EIP-712 DOMAIN + ORDER TYPE ─────────────────────────
# Polymarket CTF Exchange contract on Polygon
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

EIP712_DOMAIN = {
    "name": "Polymarket CTF Exchange",
    "version": "1",
    "chainId": CHAIN_ID,
    "verifyingContract": CTF_EXCHANGE,
}

ORDER_TYPES = {
    "Order": [
        {"name": "salt",          "type": "uint256"},
        {"name": "maker",         "type": "address"},
        {"name": "signer",        "type": "address"},
        {"name": "taker",         "type": "address"},
        {"name": "tokenId",       "type": "uint256"},
        {"name": "makerAmount",   "type": "uint256"},
        {"name": "takerAmount",   "type": "uint256"},
        {"name": "expiration",    "type": "uint256"},
        {"name": "nonce",         "type": "uint256"},
        {"name": "feeRateBps",    "type": "uint256"},
        {"name": "side",          "type": "uint8"},
        {"name": "signatureType", "type": "uint8"},
    ]
}

SIDE_BUY  = 0
SIDE_SELL = 1

def build_and_sign_order(token_id: str, price: float, size_usdc: float, side: int):
    """
    Build a limit order and sign it with EIP-712.
    price: between 0 and 1 (e.g. 0.08 = 8 cents per share)
    size_usdc: how many USDC to spend
    Returns signed order dict ready to POST
    """
    account = Account.from_key(PRIVATE_KEY)
    signer  = account.address   # key that signs
    maker   = ADDRESS           # Polymarket proxy wallet (holds funds)

    # Convert to chain units (USDC has 6 decimals on Polygon)
    DECIMALS = 10**6
    if side == SIDE_BUY:
        maker_amount = int(size_usdc * DECIMALS)
        taker_amount = int((size_usdc / price) * DECIMALS)
    else:
        shares       = size_usdc
        maker_amount = int(shares * DECIMALS)
        taker_amount = int(shares * price * DECIMALS)

    salt       = int(time.time() * 1000)
    expiration = 0
    nonce      = 0
    fee_bps    = 0

    order_data = {
        "salt":          salt,
        "maker":         maker,
        "signer":        signer,
        "taker":         "0x0000000000000000000000000000000000000000",
        "tokenId":       int(token_id),
        "makerAmount":   maker_amount,
        "takerAmount":   taker_amount,
        "expiration":    expiration,
        "nonce":         nonce,
        "feeRateBps":    fee_bps,
        "side":          side,
        "signatureType": 1,  # POLY_PROXY: signer != maker
    }

    # EIP-712 structured sign
    signed = account.sign_typed_data(
        domain_data=EIP712_DOMAIN,
        message_types=ORDER_TYPES,
        message_data=order_data,
    )

    return {
        "order": {
            **order_data,
            "signature": signed.signature.hex(),
        },
        "owner":     maker,
        "orderType": "GTC",  # Good Till Cancelled
    }

def place_order(token_id: str, price: float, size_usdc: float,
                side: int = SIDE_BUY, dry_run: bool = True):
    """
    Place a limit order on Polymarket CLOB.
    Returns response dict.
    """
    print(f"\n{'[DRY RUN] ' if dry_run else '[LIVE] '}Placing order:")
    print(f"  Token:  {token_id[:30]}...")
    print(f"  Side:   {'BUY' if side==SIDE_BUY else 'SELL'}")
    print(f"  Price:  {price:.4f} ({price*100:.2f}¢ per share)")
    print(f"  Size:   ${size_usdc:.2f} USDC")
    shares = size_usdc / price
    profit = shares - size_usdc
    print(f"  Shares: {shares:.2f}  Max profit: ${profit:.2f}")

    if dry_run:
        print("  → DRY RUN: not submitted")
        return {"status": "dry_run"}

    payload = build_and_sign_order(token_id, price, size_usdc, side)
    body    = json.dumps(payload)
    path    = "/order"
    headers = l2_headers("POST", path, body)

    r = requests.post(f"{CLOB_API}{path}", headers=headers, data=body, timeout=15)
    print(f"  → Response {r.status_code}: {r.text[:300]}")
    return r.json() if r.status_code in (200, 201) else {"error": r.text}

def get_balance(dry_run=True):
    """Check USDC balance"""
    path = f"/balance?address={ADDRESS}"
    r = requests.get(f"{CLOB_API}{path}",
                     headers=l2_headers("GET", path),
                     timeout=10)
    if r.status_code == 200:
        return r.json()
    return {}

def check_open_orders():
    """List all open orders"""
    path = f"/orders?maker={ADDRESS}"
    r = requests.get(f"{CLOB_API}{path}",
                     headers=l2_headers("GET", path),
                     timeout=10)
    if r.status_code == 200:
        return r.json()
    return []

if __name__ == "__main__":
    print("="*60)
    print("Polymarket Order System — Test")
    print(f"Wallet: {ADDRESS}")
    print(f"Mode:   {'DRY RUN' if DRY_RUN else '*** LIVE ***'}")
    print("="*60)

    # Check balance
    print("\nChecking balance...")
    bal = get_balance()
    print(f"Balance: {bal}")

    # Check open orders
    print("\nChecking open orders...")
    orders = check_open_orders()
    print(f"Open orders: {orders}")

    # Test order (Shanghai ≥22°C — best edge from scanner)
    # Token ID from scanner output — fetch it
    print("\nFetching Shanghai ≥22°C market token...")
    import sys
    r = requests.get("https://gamma-api.polymarket.com/events",
                     params={"tag_slug":"weather","active":"true","limit":100,"offset":3300},
                     timeout=15)
    target_token = None
    for ev in r.json():
        if "2026-04-20" in ev.get("endDate","") and "Shanghai" in ev.get("title",""):
            for mkt in ev.get("markets",[]):
                q = mkt.get("question","")
                if "22°C" in q and ("higher" in q.lower() or "above" in q.lower() or "≥" in q):
                    toks = json.loads(mkt["clobTokenIds"]) if isinstance(mkt.get("clobTokenIds"), str) else mkt.get("clobTokenIds",[])
                    if toks:
                        target_token = toks[0]
                        print(f"Found: {q}")
                        print(f"Token: {target_token}")
                        break
            if target_token:
                break

    if target_token:
        # Get current price
        r2 = requests.get(f"{CLOB_API}/midpoint", params={"token_id": target_token}, timeout=8)
        price = float(r2.json().get("mid", 0.085))
        print(f"Current midpoint: {price:.4f} ({price*100:.2f}¢)")

        # Place $5 test order (dry run)
        result = place_order(
            token_id=target_token,
            price=price,
            size_usdc=5.0,
            side=SIDE_BUY,
            dry_run=DRY_RUN,
        )
        print(f"\nResult: {result}")
    else:
        print("Shanghai ≥22°C market not found (may have resolved already)")
        print("Run polymarket_weather_scan.py to find current edges")
