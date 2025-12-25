"""fetch_statement_transactions.py

Fetch the latest statement, iterate its `statement_lines` for CARD_TRANSACTION entries,
fetch each transaction by id (GET /developer/v1/transactions/{id}), and write full transactions to JSON.

Usage:
  python fetch_statement_transactions.py
"""
import json
import os

from utils import load_env, load_config
from ramp_client import RampClient

cfg = load_config('config.toml')
env = load_env()
client = RampClient(cfg['ramp']['base_url'], cfg['ramp']['token_url'], env['RAMP_CLIENT_ID'], env['RAMP_CLIENT_SECRET'])
client.authenticate()

stmts = client.get_statements()
if not stmts:
    print('No statements found')
    raise SystemExit(1)

latest = stmts[0]
lines = latest.get('statement_lines') or []
card_tx_ids = [l.get('id') for l in lines if l.get('type') == 'CARD_TRANSACTION']
print(f"Found {len(card_tx_ids)} CARD_TRANSACTION lines in the latest statement")

results = []
for i, tid in enumerate(card_tx_ids):
    url = f"{client.base_url}/transactions/{tid}"
    resp = client.session.get(url)
    if resp.status_code == 200:
        results.append(resp.json())
    else:
        print(f"Warning: transaction {tid} returned status {resp.status_code}")

exports_dir = cfg.get('exports_path', 'exports') if isinstance(cfg, dict) else 'exports'
os.makedirs(exports_dir, exist_ok=True)
out = os.path.join(exports_dir, f'statement_transactions_{latest.get("id")[:8]}_{len(results)}.json')
with open(out, 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2)

print(f"Wrote {out} with {len(results)} transactions")
print('\nSample IDs:')
for r in results[:5]:
    print(r.get('id'), r.get('accounting_date'), r.get('amount'))
