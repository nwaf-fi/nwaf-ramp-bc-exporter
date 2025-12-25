"""Fetch Ramp statements (last N months) and export statements, payments, and statement lines.

Outputs CSVs to `exports/`:
 - statements_{ts}.csv
 - statement_payments_{ts}.csv (detailed payments if available)
 - statement_lines_{ts}.csv (one row per statement line; attempts to resolve amounts by fetching transaction details)

Usage: python fetch_statements.py

Requires .env with RAMP_CLIENT_ID and RAMP_CLIENT_SECRET and config.toml
"""
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import toml
import pandas as pd
from ramp_client import RampClient

load_dotenv()
env = os.environ
RAMP_CLIENT_ID = env.get('RAMP_CLIENT_ID')
RAMP_CLIENT_SECRET = env.get('RAMP_CLIENT_SECRET')
if not RAMP_CLIENT_ID or not RAMP_CLIENT_SECRET:
    raise SystemExit('RAMP_CLIENT_ID and RAMP_CLIENT_SECRET must be set in the environment or .env file')

cfg = toml.load('config.toml')
base_url = cfg['ramp'].get('base_url')
token_url = cfg['ramp'].get('token_url')
page_size = cfg['ramp'].get('page_size', 200)

client = RampClient(base_url=base_url, token_url=token_url,
                    client_id=RAMP_CLIENT_ID, client_secret=RAMP_CLIENT_SECRET,
                    enable_sync=False)
print('Authenticating with Ramp...')
client.authenticate()

# Calculate last 6 months
today = datetime.utcnow().date()
start_date = (today - timedelta(days=180)).strftime('%Y-%m-%d')
end_date = today.strftime('%Y-%m-%d')
print(f'Fetching statements from {start_date} to {end_date}...')

statements = client.get_statements(start_date=start_date, end_date=end_date, page_size=page_size)
print(f'Fetched {len(statements)} statement summaries')

statements_rows = []
statement_payments_rows = []
statement_lines_rows = []

# helper to normalize amount values into major units (assume cents -> divide by 100 when integer)
def normalize_amount(amount_obj):
    if amount_obj is None:
        return None
    try:
        if isinstance(amount_obj, dict):
            amt = amount_obj.get('amount')
            conv = amount_obj.get('minor_unit_conversion_rate', 100)
            return float(amt) / float(conv) if amt is not None else None
        elif isinstance(amount_obj, (int, float)):
            # Heuristic: if integer and large, assume minor units (cents)
            if isinstance(amount_obj, int) and abs(amount_obj) > 100000:
                return float(amount_obj) / 100.0
            return float(amount_obj)
        else:
            return float(amount_obj)
    except Exception:
        return None

for s in statements:
    stmt_id = s.get('id')
    # Fetch detailed statement by id to get payments and statement_lines
    try:
        resp = client.session.get(f"{base_url}/statements/{stmt_id}")
        resp.raise_for_status()
        detail = resp.json()
    except Exception as e:
        print(f'Failed to fetch details for statement {stmt_id}: {e}')
        detail = s  # fallback to summary

    start = detail.get('start_date')
    end = detail.get('end_date')
    payments_summary = detail.get('payments', {})
    charges = detail.get('charges', {})
    opening = detail.get('opening_balance', {})
    ending = detail.get('ending_balance', {})
    statement_url = detail.get('statement_url', '')

    statements_rows.append({
        'statement_id': stmt_id,
        'start_date': start,
        'end_date': end,
        'payments_amount': normalize_amount(payments_summary.get('amount') if isinstance(payments_summary, dict) else payments_summary),
        'payments_currency': payments_summary.get('currency_code') if isinstance(payments_summary, dict) else None,
        'charges_amount': normalize_amount(charges.get('amount') if isinstance(charges, dict) else charges),
        'charges_currency': charges.get('currency_code') if isinstance(charges, dict) else None,
        'opening_balance': normalize_amount(opening.get('amount') if isinstance(opening, dict) else opening),
        'ending_balance': normalize_amount(ending.get('amount') if isinstance(ending, dict) else ending),
        'statement_url': statement_url,
        'raw': detail
    })

    # If payments detailed array exists, include them
    payments_detail = detail.get('payments_detail') or detail.get('payments')
    # Some APIs return payments as a summary object; if there's a list under 'payments' use it
    if isinstance(payments_detail, list):
        for p in payments_detail:
            statement_payments_rows.append({
                'statement_id': stmt_id,
                'payment_id': p.get('id'),
                'payment_date': p.get('posted_at') or p.get('created_at') or p.get('date'),
                'amount': normalize_amount(p.get('amount') if isinstance(p.get('amount'), dict) else p.get('amount')),
                'currency': (p.get('amount') or {}).get('currency_code') if isinstance(p.get('amount'), dict) else p.get('currency'),
                'method': p.get('method') or p.get('type') or '',
                'notes': p.get('memo') or p.get('description') or ''
            })

    # Statement lines: try to resolve amounts by fetching transaction/cashback endpoints
    stmt_lines = detail.get('statement_lines') or []
    for line in stmt_lines:
        line_id = line.get('id')
        line_type = line.get('type')
        amount_val = None
        currency = None
        merchant = None
        # Try to fetch transaction-like entities for amount
        # Try transactions endpoint
        tried = False
        if line_id:
            for endpoint in ['transactions', 'cashbacks', 'reimbursements', 'bills']:
                try:
                    tried = True
                    r = client.session.get(f"{base_url}/{endpoint}/{line_id}")
                    if r.status_code == 200:
                        obj = r.json()
                        # Many objects use an 'amount' object
                        amt_obj = obj.get('amount') if isinstance(obj.get('amount'), dict) else obj.get('amount')
                        amount_val = normalize_amount(amt_obj)
                        # currency detection
                        if isinstance(obj.get('amount'), dict):
                            currency = obj.get('amount').get('currency_code')
                        else:
                            currency = obj.get('currency') or currency
                        merchant = obj.get('merchant_name') or (obj.get('merchant') or {}).get('name')
                        break
                except Exception:
                    continue
            if not tried:
                amount_val = None

        statement_lines_rows.append({
            'statement_id': stmt_id,
            'line_id': line_id,
            'type': line_type,
            'amount': amount_val,
            'currency': currency,
            'merchant': merchant
        })

# Write outputs
os.makedirs('exports', exist_ok=True)
ts = datetime.now().strftime('%Y%m%dT%H%M%S')
statements_path = os.path.join('exports', f'statements_{ts}.csv')
lines_path = os.path.join('exports', f'statement_lines_{ts}.csv')
payments_path = os.path.join('exports', f'statement_payments_{ts}.csv')

pd.DataFrame(statements_rows).drop(columns=['raw']).to_csv(statements_path, index=False)
print(f'Wrote statements summary: {statements_path}')

if statement_payments_rows:
    pd.DataFrame(statement_payments_rows).to_csv(payments_path, index=False)
    print(f'Wrote statement payments: {payments_path}')
else:
    print('No detailed statement payments found in the response; only summary amounts written in statements CSV')

pd.DataFrame(statement_lines_rows).to_csv(lines_path, index=False)
print(f'Wrote statement lines: {lines_path}')

print('Done.')
