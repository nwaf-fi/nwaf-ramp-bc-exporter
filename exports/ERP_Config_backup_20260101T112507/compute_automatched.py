"""compute_automatched.py

Re-run the GL accounts upload (POST) and compute which returned `ramp_id`s were
not in the `uploaded` list — these are likely automatched accounts (existing
Ramp accounts matched instead of created).

WARNING: This performs a POST to Ramp and will update existing data. Use only
when acceptable.
"""
from __future__ import annotations
import json
from pathlib import Path

from utils import load_env
from ramp_client import RampClient


def main():
    p = Path('ERP_Config/chartOfAccounts_ramp_payload.json')
    if not p.exists():
        print('Payload not found:', p)
        return 2
    payload = json.loads(p.read_text(encoding='utf-8'))
    accounts = payload.get('accounts') or []

    gl = []
    for a in accounts:
        gl.append({
            'classification': a.get('account_type'),
            'code': a.get('account_number') or a.get('external_id') or a.get('code'),
            'id': a.get('external_id') or a.get('account_number') or a.get('id'),
            'name': a.get('account_name') or a.get('name')
        })

    env = load_env()
    import tomllib
    cfg = tomllib.loads(Path('config.toml').read_text())
    client = RampClient(base_url=cfg.get('ramp', {}).get('base_url'), token_url=cfg.get('ramp', {}).get('token_url',''), client_id=env['RAMP_CLIENT_ID'], client_secret=env['RAMP_CLIENT_SECRET'])
    client.authenticate()

    print('Uploading GL accounts (live)...')
    ok, results = client.upload_gl_accounts(gl, dry_run=False)
    print('overall ok:', ok)
    # Extract response dict from first batch
    resp = None
    for r in results:
        if isinstance(r.get('response'), dict):
            resp = r.get('response')
            break
    if not resp:
        print('No response dict found in upload results:', results)
        return 3

    uploaded_ids = set(resp.get('uploaded') or [])
    resp_gl_accounts = resp.get('gl_accounts') or []

    automatched = []
    for a in resp_gl_accounts:
        rid = a.get('ramp_id')
        if rid and rid not in uploaded_ids:
            automatched.append({'code': a.get('code'), 'name': a.get('name'), 'ramp_id': rid, 'provider_name': a.get('provider_name')})

    print('Automatched count:', len(automatched))
    for a in automatched:
        print(json.dumps(a, ensure_ascii=False))

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
