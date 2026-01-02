"""
Merged Credit Card Statement Exporter

- Fetches the latest Ramp statement and associated transactions
- Filters and normalizes transactions by date and card if needed
- Compares statement and transaction totals
- Exports a Business Central-friendly credit card journal CSV
- Supports strict total matching and flexible date key config

Usage: python merged_cc_statement_export.py [--strict] [--card-filter]
"""
import os
import sys
import argparse
from datetime import datetime
from typing import Any, Dict
from utils import load_env, load_config, _extract_amount
from ramp_client import RampClient
from transform import ramp_credit_card_to_bc_rows


def iso_date(s: str) -> str:
    if not s:
        return ''
    return str(s)[:10]


def main():
    parser = argparse.ArgumentParser(description="Export Ramp credit card statement journal to CSV.")
    parser.add_argument('--strict', action='store_true', help='Fail if statement and transaction totals do not match')
    parser.add_argument('--card-filter', action='store_true', help='Filter transactions by card if statement has card info')
    args = parser.parse_args()

    cfg = load_config('config.toml')
    env = load_env()
    tx_date_key = cfg.get('business_central', {}).get('tx_date_key', 'accounting_date')
    strict_totals = args.strict or cfg.get('business_central', {}).get('cc_strict_totals', False)

    client = RampClient(cfg['ramp']['base_url'], cfg['ramp']['token_url'], env['RAMP_CLIENT_ID'], env['RAMP_CLIENT_SECRET'])
    client.authenticate()

    stmts = client.get_statements()
    if not stmts:
        print('No statements found')
        return

    latest = stmts[0]
    start = latest.get('start_date') or latest.get('period_start') or (latest.get('statement_date') or '')
    end = latest.get('end_date') or latest.get('period_end') or (latest.get('statement_date') or '')
    s_norm = iso_date(start)
    e_norm = iso_date(end)
    print(f"Using statement period: {s_norm} -> {e_norm}")

    # Prefer authoritative transaction list from the statement object if present
    lines = latest.get('statement_lines') or []
    card_tx_ids = [l.get('id') for l in lines if l.get('type') == 'CARD_TRANSACTION']
    filtered = []
    failures = []
    if card_tx_ids:
        print(f"Found {len(card_tx_ids)} CARD_TRANSACTION lines in the statement; fetching each by id (concurrent)")
        from concurrent.futures import ThreadPoolExecutor, as_completed
        max_workers = min(12, max(4, len(card_tx_ids)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(client.session.get, f"{client.base_url}/transactions/{tid}", timeout=15): tid for tid in card_tx_ids}
            for fut in as_completed(futures):
                tid = futures[fut]
                try:
                    resp = fut.result()
                    if resp.status_code == 200:
                        filtered.append(resp.json())
                    else:
                        failures.append((tid, resp.status_code))
                except Exception as exc:
                    failures.append((tid, str(exc)))
        print(f"Fetched {len(filtered)} statement transactions (failures: {len(failures)})")
    else:
        # Fallback: use date-range query and filter by the configured tx_date_key
        txs = client.get_transactions(start_date=start, end_date=end)
        print(f"Fetched {len(txs)} transactions from API for the period")
        if s_norm and e_norm:
            def in_range(tx):
                v = tx.get(tx_date_key) or tx.get('user_transaction_time') or tx.get('posted_at') or tx.get('created_at')
                if not v:
                    return False
                ds = iso_date(str(v))
                return s_norm <= ds <= e_norm
            filtered = [t for t in txs if in_range(t)]
        else:
            filtered = txs
        print(f"Transactions after filtering by {tx_date_key}: {len(filtered)}")

    # Optionally filter by card id if statement has card info and --card-filter is set
    if args.card_filter:
        stmt_card_id = None
        if latest.get('card') and isinstance(latest.get('card'), dict):
            stmt_card_id = latest['card'].get('id') or latest['card'].get('last_four')
        if stmt_card_id:
            filtered = [t for t in filtered if t.get('card') and isinstance(t.get('card'), dict) and (t['card'].get('id') == stmt_card_id or t['card'].get('last_four') == stmt_card_id)]
            print(f"Filtered to {len(filtered)} transactions matching card {stmt_card_id}")

    stmt_total = _extract_amount(latest.get('total_amount') or latest.get('amount') or latest.get('balance') or 0)
    tx_total = sum(_extract_amount(t.get('amount')) for t in filtered)
    print(f"Statement total: {stmt_total}  Transactions total: {tx_total}")

    if strict_totals and abs(stmt_total - tx_total) > 0.01:
        raise RuntimeError('Statement total does not match transaction total (strict mode)')
    elif abs(stmt_total - tx_total) > 0.01:
        print('Warning: totals do not match; continuing (non-strict)')

    # Build DataFrame and write CSV
    df = ramp_credit_card_to_bc_rows(filtered, cfg, write_audit=False)
    exports_dir = cfg.get('exports_path', 'exports') if isinstance(cfg, dict) else 'exports'
    os.makedirs(exports_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%dT%H%M%S')
    s_file = s_norm.replace('-', '') if s_norm else 'start'
    e_file = e_norm.replace('-', '') if e_norm else 'end'
    out = os.path.join(exports_dir, f'merged_cc_statement_journal_{s_file}_{e_file}_{ts}.csv')
    df.to_csv(out, index=False)
    print(f"Wrote {out} (period: {s_norm} -> {e_norm})")

if __name__ == '__main__':
    main()
