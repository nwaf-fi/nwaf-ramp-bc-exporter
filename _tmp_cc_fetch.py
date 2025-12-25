from utils import load_env, load_config
from ramp_client import RampClient
import json

cfg = load_config('config.toml')
env = load_env()
client = RampClient(cfg['ramp']['base_url'], cfg['ramp']['token_url'], env['RAMP_CLIENT_ID'], env['RAMP_CLIENT_SECRET'])
client.authenticate()
stmts = client.get_statements()
if not stmts:
    print('No statements returned')
    raise SystemExit(0)
latest = stmts[0]
start = latest.get('start_date') or latest.get('period_start') or latest.get('start') or (latest.get('statement_date') or '')[:10]
end = latest.get('end_date') or latest.get('period_end') or latest.get('end') or (latest.get('statement_date') or '')[:10]
print('Using statement period:', start[:10], '->', end[:10])
txs = client.get_transactions(start_date=start, end_date=end)
print('Fetched', len(txs), 'transactions')
for i, t in enumerate(txs[:5]):
    snippet = {k: t.get(k) for k in ['id','amount','accounting_date','clearing_date','cleared_at','posted_at','user_transaction_time','created_at','card','line_items']}
    print(f'--- tx #{i+1} ---')
    print(json.dumps(snippet, indent=2))
