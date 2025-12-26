"""
Dry-run script: fetch Ramp bills and run transforms (no writes to Ramp).
Writes outputs to `exports/` and prints a short summary.
"""
import os
from datetime import datetime, timedelta
from utils import load_env, load_config
from ramp_client import RampClient
from transform import ramp_bills_to_general_journal, ramp_bills_to_purchase_invoice_lines, enrich_bills_with_vendor_external_ids


def main():
    # Load env and config
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

    print('Authenticating...')
    client.authenticate()
    print('Authenticated')

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=30)
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    print(f'Fetching PAID bills from {start_str} to {end_str}...')
    bills = client.get_bills(status='PAID', start_date=start_str, end_date=end_str, page_size=cfg['ramp'].get('page_size',200))
    print(f'Fetched {len(bills)} bills')

    os.makedirs('exports', exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    if not bills:
        print('No bills to process')
        return

    # Filter out already-synced bills
    before = len(bills)
    bills = [b for b in bills if not client.is_transaction_synced(b)]
    after = len(bills)
    skipped = before - after
    if skipped:
        print(f'Skipped {skipped} bills already marked synced')

    # Enrich bills with vendor external IDs from Vendor API, then run transforms
    print('Enriching bills with vendor external IDs...')
    bills = enrich_bills_with_vendor_external_ids(bills, client)
    gj_df = ramp_bills_to_general_journal(bills, cfg)
    pi_df = ramp_bills_to_purchase_invoice_lines(bills, cfg)

    gj_path = f'exports/dryrun_purchase_invoices_journal_{ts}.csv'
    pi_path = f'exports/dryrun_purchase_invoices_{ts}.csv'

    gj_df.to_csv(gj_path, index=False)
    pi_df.to_csv(pi_path, index=False)

    print('Wrote:')
    print(' -', gj_path)
    print(' -', pi_path)

    # Simple validation: missing Account No.
    missing_account = pi_df[pi_df['Account No.'].astype(str).str.strip()=='']
    if not missing_account.empty:
        print(f'Warning: {len(missing_account)} invoice lines missing Account No.')
        out = f'exports/dryrun_missing_account_lines_{ts}.csv'
        missing_account.to_csv(out, index=False)
        print(' Wrote missing lines to', out)

    # Balancing rows info in journal
    balancing_rows = gj_df[(gj_df['Debit Amount']>0) & (gj_df['Credit Amount']>0)]
    # unlikely, but report imbalances
    total_debit = gj_df['Debit Amount'].sum()
    total_credit = gj_df['Credit Amount'].sum()
    print(f'Journal totals: Debit={total_debit:.2f} Credit={total_credit:.2f}')

    print('Done')


if __name__ == '__main__':
    main()
