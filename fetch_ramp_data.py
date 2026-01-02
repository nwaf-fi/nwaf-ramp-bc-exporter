"""
fetch_ramp_data.py

Unified data fetcher for Ramp API.
Supports: transactions, statements, statement_lines, transfers, bills, reimbursements, vendors
- Output: CSV
- Supports date range or fetch all
- Allows filtering by status (where applicable)

Usage:
  python fetch_ramp_data.py --type transactions --start YYYY-MM-DD --end YYYY-MM-DD --status PAID
  python fetch_ramp_data.py --type statements --all
  python fetch_ramp_data.py --type statement_lines --start YYYY-MM-DD --end YYYY-MM-DD

Requires .env and config.toml for credentials/config.
"""
import argparse
import os
import pandas as pd
from datetime import datetime
from utils import load_env, load_config
from ramp_client import RampClient

def write_csv(data, out_path):
    if not data:
        print(f"No data to write for {out_path}")
        return
    df = pd.DataFrame(data)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(df)} records)")

def fetch_statement_lines(client, statements):
    lines = []
    for stmt in statements:
        for line in stmt.get('statement_lines', []):
            line_copy = line.copy()
            line_copy['statement_id'] = stmt.get('id')
            lines.append(line_copy)
    return lines

def main():
    parser = argparse.ArgumentParser(description="Fetch data from Ramp API and export as CSV.")
    parser.add_argument('--type', required=True, choices=['transactions', 'statements', 'statement_lines', 'transfers', 'bills', 'reimbursements', 'vendors'], help='Data type to fetch')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD)')
    parser.add_argument('--status', help='Status filter (e.g., PAID, APPROVED, etc.)')
    parser.add_argument('--all', action='store_true', help='Fetch all records (ignore date range)')
    args = parser.parse_args()

    cfg = load_config('config.toml')
    env = load_env()
    client = RampClient(cfg['ramp']['base_url'], cfg['ramp']['token_url'], env['RAMP_CLIENT_ID'], env['RAMP_CLIENT_SECRET'])
    client.authenticate()

    exports_dir = cfg.get('exports_path', 'exports') if isinstance(cfg, dict) else 'exports'
    os.makedirs(exports_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%dT%H%M%S')

    fetch_kwargs = {}
    if not args.all:
        if args.start:
            fetch_kwargs['start_date'] = args.start
        if args.end:
            fetch_kwargs['end_date'] = args.end
    if args.status:
        fetch_kwargs['status'] = args.status

    if args.type == 'transactions':
        data = client.get_transactions(**fetch_kwargs)
        out = os.path.join(exports_dir, f'transactions_{ts}.csv')
        write_csv(data, out)
    elif args.type == 'statements':
        data = client.get_statements(**fetch_kwargs)
        out = os.path.join(exports_dir, f'statements_{ts}.csv')
        write_csv(data, out)
    elif args.type == 'statement_lines':
        stmts = client.get_statements(**fetch_kwargs)
        data = fetch_statement_lines(client, stmts)
        out = os.path.join(exports_dir, f'statement_lines_{ts}.csv')
        write_csv(data, out)
    elif args.type == 'transfers':
        data = client.get_transfers(**fetch_kwargs)
        out = os.path.join(exports_dir, f'transfers_{ts}.csv')
        write_csv(data, out)
    elif args.type == 'bills':
        data = client.get_bills(**fetch_kwargs)
        out = os.path.join(exports_dir, f'bills_{ts}.csv')
        write_csv(data, out)
    elif args.type == 'reimbursements':
        data = client.get_reimbursements(**fetch_kwargs)
        out = os.path.join(exports_dir, f'reimbursements_{ts}.csv')
        write_csv(data, out)
    elif args.type == 'vendors':
        data = client.get_vendors()
        out = os.path.join(exports_dir, f'vendors_{ts}.csv')
        write_csv(data, out)
    else:
        print(f"Unknown type: {args.type}")

if __name__ == '__main__':
    main()
