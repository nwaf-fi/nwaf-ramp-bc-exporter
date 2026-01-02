"""report_gl_upload_audit.py

Compare payload GL accounts to current Ramp GL accounts and write an audit CSV
classifying each payload account as:
  - 'pre-existing' (found and created_at <= connection.created_at)
  - 'created' (found and created_at > connection.created_at)
  - 'missing' (not found)

Usage:
  python -m ERP_Config.report_gl_upload_audit --payload ERP_Config/chartOfAccounts_ramp_payload.json
"""
from __future__ import annotations
import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

from utils import load_env
from ramp_client import RampClient


def main(argv=None):
    p = argparse.ArgumentParser(description='Report GL upload audit')
    p.add_argument('--payload', '-p', required=True, help='Path to Ramp payload JSON')
    args = p.parse_args(argv)

    payload_path = Path(args.payload)
    if not payload_path.exists():
        print('ERROR: payload not found:', payload_path)
        return 2

    payload = json.loads(payload_path.read_text(encoding='utf-8'))
    accounts = payload.get('accounts') or payload.get('gl_accounts') or []

    env = load_env()
    try:
        import tomllib
        cfg = tomllib.loads(Path('config.toml').read_text())
    except Exception:
        cfg = {}

    client = RampClient(base_url=cfg.get('ramp', {}).get('base_url'), token_url=cfg.get('ramp', {}).get('token_url',''), client_id=env['RAMP_CLIENT_ID'], client_secret=env['RAMP_CLIENT_SECRET'])
    client.authenticate()

    # get connection created_at timestamp as baseline
    from urllib.parse import urljoin
    base = client.base_url.rstrip('/')
    if 'developer/v1' in base:
        conn_ep = urljoin(base + '/', 'accounting/connection')
    else:
        conn_ep = urljoin(base + '/', 'developer/v1/accounting/connection')
    resp = client.session.get(conn_ep)
    resp.raise_for_status()
    conn = resp.json()
    conn_created = conn.get('created_at') or conn.get('last_linked_at')
    baseline_ts = None
    if conn_created:
        try:
            baseline_ts = datetime.fromisoformat(conn_created)
        except Exception:
            baseline_ts = None

    # fetch existing GL accounts
    existing = client._get_paginated_data('accounting/accounts', page_size=200)
    by_id = {str(e.get('id')): e for e in existing if e.get('id')}
    by_code = {str(e.get('code')): e for e in existing if e.get('code')}

    # Build audit rows
    rows = []
    counts = {'pre-existing': 0, 'created': 0, 'missing': 0}
    for a in accounts:
        ext_id = a.get('external_id') or a.get('account_number') or a.get('id')
        code = a.get('account_number') or a.get('code') or a.get('external_id') or a.get('id')
        name = a.get('account_name') or a.get('name')
        found = None
        rid = None
        created_at = None
        provider = None
        status = 'missing'

        key_id = str(ext_id) if ext_id is not None else None
        key_code = str(code) if code is not None else None
        if key_id and key_id in by_id:
            found = by_id[key_id]
        elif key_code and key_code in by_code:
            found = by_code[key_code]

        if found:
            rid = found.get('ramp_id')
            provider = found.get('provider_name')
            created_at = found.get('created_at')
            try:
                ex_ts = datetime.fromisoformat(created_at) if created_at else None
            except Exception:
                ex_ts = None

            if baseline_ts and ex_ts and ex_ts > baseline_ts:
                status = 'created'
                counts['created'] += 1
            else:
                status = 'pre-existing'
                counts['pre-existing'] += 1
        else:
            counts['missing'] += 1

        rows.append({
            'payload_id': key_id or '',
            'payload_code': key_code or '',
            'payload_name': name or '',
            'status': status,
            'ramp_id': rid or '',
            'existing_name': found.get('name') if found else '',
            'existing_code': found.get('code') if found else '',
            'existing_created_at': created_at or '',
            'provider_name': provider or ''
        })

    ts = datetime.now().strftime('%Y%m%dT%H%M%S')
    out_dir = Path('exports')
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f'coa_upload_audit_{ts}.csv'

    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print('Wrote audit CSV to:', out_path)
    print('Summary counts:', counts)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())