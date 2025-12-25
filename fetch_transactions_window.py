"""fetch_transactions_window.py

Utility: fetch Ramp transactions for a given date window and optionally write JSON to `exports/`.

Usage:
  python fetch_transactions_window.py --start 2025-11-13 --end 2025-12-13 --json

Outputs:
  - exports/transactions_20251113_20251213.json (pretty-printed)
  - prints the count and first 5 transaction ids to stdout
"""
import argparse
import json
import os
from datetime import datetime

from utils import load_env, load_config
from ramp_client import RampClient


def iso_date_str(s: str) -> str:
    return s.replace('-', '')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--start', required=False, default='2025-11-13', help='Start date YYYY-MM-DD')
    p.add_argument('--end', required=False, default='2025-12-13', help='End date YYYY-MM-DD')
    p.add_argument('--out', required=False, default=None, help='Output path (defaults to exports/transactions_<start>_<end>.json)')
    p.add_argument('--json', action='store_true', help='Write output as JSON')
    p.add_argument('--sample', type=int, default=5, help='Print sample N transactions to stdout')
    args = p.parse_args()

    cfg = load_config('config.toml')
    env = load_env()

    client = RampClient(cfg['ramp']['base_url'], cfg['ramp']['token_url'], env['RAMP_CLIENT_ID'], env['RAMP_CLIENT_SECRET'])
    client.authenticate()

    start = args.start
    end = args.end
    print(f"Fetching transactions from {start} to {end}...")
    txs = client.get_transactions(start_date=start, end_date=end)
    print(f"Fetched {len(txs)} transactions from the API")

    if args.json:
        exports_dir = cfg.get('exports_path', 'exports') if isinstance(cfg, dict) else 'exports'
        os.makedirs(exports_dir, exist_ok=True)
        out = args.out or os.path.join(exports_dir, f"transactions_{iso_date_str(start)}_{iso_date_str(end)}.json")
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(txs, f, indent=2)
        print(f"Wrote {out}")

    s = args.sample
    if s > 0:
        print('\nSample transactions:')
        for i, tx in enumerate(txs[:s]):
            print(f"[{i}] id={tx.get('id')} accounting_date={tx.get('accounting_date')} amount={tx.get('amount')}")


if __name__ == '__main__':
    main()
