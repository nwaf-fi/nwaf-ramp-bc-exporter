"""
Fetch a small sample of APPROVED bills from Ramp and write them to
`exports/bill_samples_{ts}.ndjson`. Also pretty-print the first 3 bills
to stdout so you can inspect the vendor object and pick the correct
field for `Buy-from Vendor No.`.

This script performs a dry-run read-only fetch and requires your
existing `.env` or secrets configuration.
"""
import os
import json
from datetime import datetime, timedelta
from utils import load_env, load_config
from ramp_client import RampClient


def main():
    # Load environment/config
    if os.path.exists('.env'):
        env = load_env()
    else:
        env = {'RAMP_CLIENT_ID': os.environ.get('RAMP_CLIENT_ID'), 'RAMP_CLIENT_SECRET': os.environ.get('RAMP_CLIENT_SECRET')}

    cfg = load_config()

    client = RampClient(
        base_url=cfg['ramp']['base_url'],
        token_url=cfg['ramp']['token_url'],
        client_id=env.get('RAMP_CLIENT_ID'),
        client_secret=env.get('RAMP_CLIENT_SECRET'),
        enable_sync=False
    )

    print('Authenticating to Ramp...')
    client.authenticate()
    print('Authenticated')

    # Fetch a small window -- last 90 days to increase chance of hits, but limit page size
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=90)
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    print(f'Fetching PAID bills from {start_str} to {end_str} (read-only, sync-ready)...')
    bills = client.get_bills(status='PAID', start_date=start_str, end_date=end_str, page_size=100, sync_ready=True)

    if not bills:
        print('No bills returned by the API in that window.')
        return

    # Write NDJSON of up to 50 sample bills
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir = 'exports'
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f'bill_samples_{ts}.ndjson')

    sample_count = min(len(bills), 50)
    print(f'Writing {sample_count} bills to {out_path} for inspection...')
    with open(out_path, 'w', encoding='utf-8') as f:
        for b in bills[:sample_count]:
            f.write(json.dumps(b, ensure_ascii=False) + '\n')

    # Fetch vendor details for each unique vendor in the sample and write them
    vendor_ids = []
    for b in bills[:sample_count]:
        v = (b.get('vendor') or {}).get('id')
        if v and v not in vendor_ids:
            vendor_ids.append(v)

    vendor_out = os.path.join(out_dir, f'vendor_details_{ts}.ndjson')
    print(f'Fetching vendor details for {len(vendor_ids)} vendors and writing to {vendor_out}...')
    with open(vendor_out, 'w', encoding='utf-8') as vf:
        for vid in vendor_ids:
            vendor_obj = client.get_vendor(vid)
            if vendor_obj is None:
                vendor_obj = {'id': vid, 'error': 'not found or error'}
            vf.write(json.dumps(vendor_obj, ensure_ascii=False) + '\n')

    # Pretty-print the first 3 bills to stdout (keys sorted) for quick inspection
    print('\n=== First 3 bill JSON previews (pretty-printed) ===\n')
    for i, b in enumerate(bills[:3]):
        print(f'--- Bill #{i+1} id={b.get("id")} vendor={b.get("vendor", {}).get("name")} ---')
        # Print vendor object and a short list of top-level keys
        vendor = b.get('vendor') or {}
        top_keys = {k: b.get(k) for k in ['id','invoice_number','bill_date','amount','line_items'] if k in b}
        print('Top-level keys sample:')
        print(json.dumps(top_keys, indent=2, ensure_ascii=False))
        print('\nVendor object (from bill):')
        # Pretty print vendor object so you can see available fields
        print(json.dumps(vendor, indent=2, ensure_ascii=False))
        print('\nVendor details fetched from Accounting Vendors API (if available):')
        vid = vendor.get('id')
        if vid:
            vdet = client.get_vendor(vid)
            print(json.dumps(vdet or {'id': vid, 'error': 'not found'}, indent=2, ensure_ascii=False))
        else:
            print('(no vendor id available)')

        print('\nFirst line item keys:')
        li = (b.get('line_items') or [])[:1]
        if li:
            print(json.dumps(li[0], indent=2, ensure_ascii=False))
        else:
            print('(no line items)')
        print('\n')

    print('Done. Inspect the files and the printed vendor objects and tell me which vendor field should be used for Buy-from Vendor No.')
    print('Bill NDJSON path:', out_path)
    print('Vendor NDJSON path:', vendor_out)


if __name__ == '__main__':
    main()
