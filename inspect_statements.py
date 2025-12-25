"""inspect_statements.py
Print statements and basic diagnostics (period dates, totals, transaction_count if present)
"""
from utils import load_env, load_config
from ramp_client import RampClient

cfg = load_config('config.toml')
env = load_env()
client = RampClient(cfg['ramp']['base_url'], cfg['ramp']['token_url'], env['RAMP_CLIENT_ID'], env['RAMP_CLIENT_SECRET'])
client.authenticate()

stmts = client.get_statements()
print(f"Found {len(stmts)} statements")
for i, s in enumerate(stmts[:10]):
    print('---')
    print(i, 'id=', s.get('id'))
    print('period_start=', s.get('start_date') or s.get('period_start'))
    print('period_end=', s.get('end_date') or s.get('period_end'))
    print('statement_date=', s.get('statement_date'))
    print('total_amount=', s.get('total_amount') or s.get('amount') or s.get('balance'))
    # transactions might be present or require a separate API call
    txs = s.get('transactions')
    if txs is not None:
        print('transactions_count_in_statement_object=', len(txs))
    else:
        print('transactions not embedded in statement object')
    print('raw keys=', list(s.keys()))
