"""create_accounting_connection.py

Safe CLI helper to create an API-based accounting connection in Ramp.

Usage:
  python ERP_Config/create_accounting_connection.py --payload ERP_Config/chartOfAccounts_ramp_payload.json --dry-run
  python ERP_Config/create_accounting_connection.py --payload path/to/payload.json --apply

By default the script does a dry-run (no writes). To actually perform the
creation, pass --apply (requires proper OAuth client credentials with
sufficient scopes, and confirmation from the user).
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

import sys
from pathlib import Path
# Ensure repo root is on sys.path when running this script directly
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ramp_client import RampClient
from utils import load_env


def main(argv: list | None = None) -> int:
    p = argparse.ArgumentParser(description='Create accounting connection in Ramp (safe dry-run by default)')
    p.add_argument('--payload', '-p', required=True, help='Path to Ramp payload JSON')
    p.add_argument('--apply', action='store_true', help='Actually POST to Ramp to create the connection and upload GL accounts')
    p.add_argument('--provider', default=None, help='Remote provider name to use for the connection (overrides any value in payload)')
    p.add_argument('--base-url', default=None, help='Optional override for Ramp base URL')

    args = p.parse_args(argv)

    payload_path = Path(args.payload)
    if not payload_path.exists():
        print(f"ERROR: payload file not found: {payload_path}")
        return 2

    payload = json.loads(payload_path.read_text(encoding='utf-8'))

    env = load_env()

    cfg = {}
    cfg_base = None
    # Attempt to read base_url from config.toml if present, otherwise require override via --base-url
    try:
        import tomllib
        cfg = tomllib.loads(Path('config.toml').read_text())
        cfg_base = cfg.get('ramp', {}).get('base_url')
    except Exception:
        cfg = {}
        cfg_base = None

    base_url = args.base_url or cfg_base
    if not base_url:
        print('ERROR: Ramp base_url not found. Pass --base-url or set it in config.toml')
        return 2

    client = RampClient(base_url=base_url, token_url=cfg.get('ramp', {}).get('token_url', ''), client_id=env['RAMP_CLIENT_ID'], client_secret=env['RAMP_CLIENT_SECRET'])
    client.authenticate()

    def _to_gl_accounts(raw_payload: dict) -> list:
        # Convert payload['accounts'] (or payload['gl_accounts']) to Ramp's expected
        # `gl_accounts` structure: classification, code, id, name
        acct_list = raw_payload.get('gl_accounts') or raw_payload.get('accounts') or []
        gl = []
        for a in acct_list:
            gl.append({
                'classification': a.get('account_type'),
                'code': a.get('account_number') or a.get('external_id') or a.get('code'),
                'id': a.get('external_id') or a.get('account_number') or a.get('id'),
                'name': a.get('account_name') or a.get('name')
            })
        return gl

    provider = args.provider or payload.get('remote_provider_name') or 'ACCOUNTING_SEED'

    print('-- Performing dry-run (no writes) --')
    print(f"Using remote_provider_name: {provider}")
    ok_conn, info_conn = client.create_accounting_connection({'remote_provider_name': provider}, dry_run=True)
    print('Connection dry-run result:', ok_conn)
    print(info_conn)

    # Prepare GL accounts from the payload
    gl_accounts = _to_gl_accounts(payload)
    print(f"Prepared {len(gl_accounts)} GL accounts for upload (will be sent in batches up to 500)")
    ok_gl, info_gl = client.upload_gl_accounts(gl_accounts, dry_run=True)
    print('GL accounts dry-run result:', ok_gl)
    print(info_gl)

    if args.apply:
        # Confirm with interactive prompt
        yn = input('Are you sure you want to apply this accounting connection to Ramp (this will create or update the connection and upload GL accounts)? [y/N]: ')
        if yn.strip().lower() != 'y':
            print('Aborted by user')
            return 0

        print('-- Applying accounting connection (this will POST to Ramp) --')
        ok_conn, info_conn = client.create_accounting_connection({'remote_provider_name': provider}, dry_run=False)
        print('Connection apply result:', ok_conn)
        print(info_conn)
        if not ok_conn:
            print('Connection creation failed; aborting GL upload.')
            return 3

        print('-- Uploading GL accounts --')
        ok_gl, info_gl = client.upload_gl_accounts(gl_accounts, dry_run=False)
        print('GL accounts upload result:', ok_gl)
        print(info_gl)
        return 0 if (ok_conn and ok_gl) else 4

    return 0


if __name__ == '__main__':
    raise SystemExit(main())