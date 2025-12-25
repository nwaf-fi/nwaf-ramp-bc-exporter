"""diagnose_transactions_filters.py

Fetch transactions for a date window and run diagnostics by status, state, and date fields.

Usage:
  python diagnose_transactions_filters.py --start 2025-11-13 --end 2025-12-13
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime

from utils import load_env, load_config
from ramp_client import RampClient


def iso_date(s: str) -> str:
    return s.split('T')[0] if s else ''


def summarize(txs, start, end):
    by_state = Counter()
    by_sync_status = Counter()
    by_card = Counter()
    count_in_window_by_accounting_date = 0
    for t in txs:
        by_state[t.get('state') or ''] += 1
        by_sync_status[t.get('sync_status') or ''] += 1
        by_card[t.get('card_id') or ''] += 1
        ad = t.get('accounting_date')
        if ad:
            ds = iso_date(ad)
            if start <= ds <= end:
                count_in_window_by_accounting_date += 1
    return {
        'total': len(txs),
        'by_state': by_state,
        'by_sync_status': by_sync_status,
        'by_card': by_card,
        'count_by_accounting_date_window': count_in_window_by_accounting_date,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--start', default='2025-11-13')
    p.add_argument('--end', default='2025-12-13')
    args = p.parse_args()

    cfg = load_config('config.toml')
    env = load_env()
    client = RampClient(cfg['ramp']['base_url'], cfg['ramp']['token_url'], env['RAMP_CLIENT_ID'], env['RAMP_CLIENT_SECRET'])
    client.authenticate()

    start = args.start
    end = args.end

    print(f"Fetching default transactions for {start} -> {end}")
    txs_default = client.get_transactions(start_date=start, end_date=end)
    s_default = summarize(txs_default, start, end)
    print('\nDefault fetch:')
    print(s_default)

    statuses = ['PENDING', 'CLEARED', 'POSTED', 'VOIDED', 'DECLINED', 'AUTHORIZED', 'SETTLED', 'ALL']
    results = {}
    for st in statuses:
        try:
            txs = client.get_transactions(status=st, start_date=start, end_date=end)
            results[st] = summarize(txs, start, end)
        except Exception as ex:
            results[st] = f'error: {ex}'

    print('\nStatus fetches:')
    for st, res in results.items():
        print(f"{st}: {res}")

    print('\nDone')

if __name__ == '__main__':
    main()
