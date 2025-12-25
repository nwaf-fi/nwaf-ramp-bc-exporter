"""cc_statement_export.py

Fetch the most recent Ramp statement, pull transactions for that statement period,
validate totals, and write a credit-card journal CSV using existing transforms.
"""
from datetime import datetime
import os
from typing import Any, Dict

from utils import load_env, load_config
from ramp_client import RampClient
from transform import ramp_credit_card_to_bc_rows


def extract_amount(amount_obj: Any) -> float:
    if isinstance(amount_obj, dict):
        minor = amount_obj.get('amount', 0)
        conv = amount_obj.get('minor_unit_conversion_rate', 100)
        try:
            return float(minor) / float(conv) if conv else float(minor)
        except Exception:
            return 0.0
    try:
        return float(amount_obj or 0.0)
    except Exception:
        return 0.0


def find_nth_latest_statement(statements: list, n: int = 0) -> Dict:
    # Prefer explicit end/start fields; otherwise sort by available date keys
    def stmt_key(s: Dict):
        for k in ('end_date', 'period_end', 'statement_date', 'created_at'):
            v = s.get(k)
            if v:
                try:
                    return datetime.fromisoformat(str(v)[:19])
                except Exception:
                    try:
                        return datetime.strptime(str(v)[:10], '%Y-%m-%d')
                    except Exception:
                        pass
        return datetime.min

    if not statements:
        return {}
    statements_sorted = sorted(statements, key=stmt_key, reverse=True)
    if n < 0 or n >= len(statements_sorted):
        return {}
    return statements_sorted[n]


def main():
    cfg = load_config('config.toml')
    env = load_env()

    client = RampClient(cfg['ramp']['base_url'], cfg['ramp']['token_url'], env['RAMP_CLIENT_ID'], env['RAMP_CLIENT_SECRET'])
    client.authenticate()

    print("Fetching statements...")
    stmts = client.get_statements()
    latest = find_nth_latest_statement(stmts, 0)
    if not latest:
        print("No statements found.")
        return

    # Extract start/end (try multiple possible keys)
    start = latest.get('start_date') or latest.get('period_start') or latest.get('start') or None
    end = latest.get('end_date') or latest.get('period_end') or latest.get('end') or None
    if not start or not end:
        # attempt to infer from statement_date (treat as single-day)
        sd = latest.get('statement_date') or latest.get('created_at')
        if sd:
            start = sd[:10]
            end = sd[:10]

    print(f"Latest statement period: {start} -> {end}")

    # Fetch transactions for that period
    txs = client.get_transactions(start_date=start, end_date=end)
    print(f"Fetched {len(txs)} transactions for period")

    # Optionally filter by card id if statement has card info
    stmt_card_id = None
    if latest.get('card') and isinstance(latest.get('card'), dict):
        stmt_card_id = latest['card'].get('id') or latest['card'].get('last_four')
    if stmt_card_id:
        filtered = []
        for t in txs:
            if t.get('card') and isinstance(t.get('card'), dict):
                if t['card'].get('id') == stmt_card_id or t['card'].get('last_four') == stmt_card_id:
                    filtered.append(t)
        txs = filtered
        print(f"Filtered to {len(txs)} transactions matching card {stmt_card_id}")

    # Use the latest statement period (no fallback) as requested
    stmt_total = extract_amount(latest.get('total_amount') or latest.get('amount') or latest.get('balance') or 0)
    tx_total = sum(extract_amount(t.get('amount')) for t in txs)

    # Now, ensure we include only transactions whose clearing/posted date falls in the statement period
    def tx_date_in_range(tx, start_date_str, end_date_str):
        # prefer `accounting_date` (the Transactions API clearing date), then fallback to other date keys
        date_keys = ['accounting_date', 'clearing_date', 'cleared_at', 'posted_at', 'user_transaction_time', 'settled_at', 'created_at']
        for k in date_keys:
            v = tx.get(k)
            if v:
                try:
                    ds = str(v)[:10]
                    if start_date_str and end_date_str and start_date_str <= ds <= end_date_str:
                        return True
                except Exception:
                    pass
        return False

    # Normalize start/end to YYYY-MM-DD
    s_norm = start[:10] if start else None
    e_norm = end[:10] if end else None
    if s_norm and e_norm:
        txs = [t for t in txs if tx_date_in_range(t, s_norm, e_norm)]
        print(f"Filtered to {len(txs)} transactions by clearing/posted date between {s_norm} and {e_norm}")

    tx_total = sum(extract_amount(t.get('amount')) for t in txs)
    print(f"Statement total: {stmt_total}  Transactions total: {tx_total}")
    if abs(stmt_total - tx_total) > 0.01:
        print("Warning: totals do not match. The journal will still be generated but totals differ.")

    # Build journal DataFrame and write CSV
    df = ramp_credit_card_to_bc_rows(txs, cfg)
    # Allow header style override from config: 'title' (default) or 'snake'
    header_style = cfg.get('business_central', {}).get('cc_header_style', 'title')
    if header_style == 'snake':
        # convert columns to snake_case
        def to_snake(s):
            return s.replace(' ', '_').replace('.', '').replace('-', '_').lower()
        df.columns = [to_snake(c) for c in df.columns]
    exports_dir = cfg.get('exports_path', 'exports') if isinstance(cfg, dict) else 'exports'
    os.makedirs(exports_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%dT%H%M%S')
    out = os.path.join(exports_dir, f'cc_statement_journal_{ts}.csv')
    df.to_csv(out, index=False)
    print(f"Wrote credit-card journal CSV: {out}")


if __name__ == '__main__':
    main()
