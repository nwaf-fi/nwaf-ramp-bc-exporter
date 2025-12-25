"""Fetch transfers from Ramp API and export full JSON objects.

Writes two files to `exports/`:
 - transfers_{ts}.json  -> full JSON array of transfers
 - transfers_{ts}.ndjson -> newline-delimited JSON (one transfer per line)

Usage:
  python fetch_transfers.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]

If no dates provided, fetches all transfers (paginated).
"""
import os
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
import toml
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

parser = argparse.ArgumentParser(description='Fetch transfers from Ramp and export JSON')
parser.add_argument('--start', help='Start date (YYYY-MM-DD) to filter transfers')
parser.add_argument('--end', help='End date (YYYY-MM-DD) to filter transfers')
args = parser.parse_args()

client = RampClient(base_url=base_url, token_url=token_url,
                    client_id=RAMP_CLIENT_ID, client_secret=RAMP_CLIENT_SECRET,
                    enable_sync=False)
print('Authenticating with Ramp...')
client.authenticate()

print('Fetching transfers...')
transfers = client.get_transfers(start_date=args.start, end_date=args.end, page_size=page_size)
print(f'Fetched {len(transfers)} transfers')

# Write outputs
os.makedirs('exports', exist_ok=True)
ts = datetime.now().strftime('%Y%m%dT%H%M%S')
json_path = os.path.join('exports', f'transfers_{ts}.json')
ndjson_path = os.path.join('exports', f'transfers_{ts}.ndjson')

try:
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(transfers, f, ensure_ascii=False, indent=2)
    print(f'Wrote full JSON array: {json_path}')
except Exception as e:
    print(f'Failed to write JSON array: {e}')

try:
    with open(ndjson_path, 'w', encoding='utf-8') as f:
        for t in transfers:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    print(f'Wrote NDJSON (one object per line): {ndjson_path}')
except Exception as e:
    print(f'Failed to write NDJSON: {e}')

print('Done.')
