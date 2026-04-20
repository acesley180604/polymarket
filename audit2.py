import json
trades = []
with open('/root/polymarket/trades.jsonl') as f:
    for line in f:
        try: trades.append(json.loads(line.strip()))
        except: pass
live = [t for t in trades if not t.get('dry_run')]
matched = [t for t in live if isinstance(t.get('result'),dict) and t['result'].get('status')=='matched']
on_book = [t for t in live if isinstance(t.get('result'),dict) and t['result'].get('status')=='live']
total_matched = sum(float(t['result'].get('makingAmount',0) or 0) for t in matched)
total_book = sum(t.get('bet',0) for t in on_book)
print(f"Live trades: {len(live)}  |  Matched/filled: {len(matched)}  |  On book (locked): {len(on_book)}")
print(f"USDC actually filled: ${total_matched:.2f}")
print(f"USDC locked on book:  ${total_book:.2f}")
print(f"TOTAL CAPITAL USED:   ${total_matched + total_book:.2f}")
print()
from collections import Counter
qtns = Counter(t['question'][:65] for t in live)
dupes = {q:n for q,n in qtns.items() if n>1}
print(f"Duplicate bets on same market: {len(dupes)}")
for q,n in list(dupes.items())[:10]:
    print(f"  x{n}  {q}")
