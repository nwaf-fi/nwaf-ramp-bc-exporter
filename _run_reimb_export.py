"""
Local runner: export reimbursement journal entries for a given month.

Usage:
  python _run_reimb_export.py              # defaults to 2026-02
  python _run_reimb_export.py 2026-03      # specify any YYYY-MM
  python _run_reimb_export.py 2026-02-01 2026-02-28  # explicit start/end dates

Outputs two files to exports/:
  BC_Journal_REIMB_<period>_<timestamp>.csv   (import into BC)
  BC_Journal_REIMB_<period>_<timestamp>.xlsx  (human review)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.ramp_client import RampClient
from lib.utils import load_env, load_config
from lib.bc_export import export
from transform import ramp_reimbursements_to_bc_rows

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
args = sys.argv[1:]

if len(args) == 0:
    target_month = '2026-02'
    start_date = '2026-02-01'
    end_date   = '2026-02-28'
    period_label = target_month
elif len(args) == 1:
    target_month = args[0]   # e.g. '2026-03'
    import calendar
    from datetime import datetime
    ym = datetime.strptime(target_month, '%Y-%m')
    last_day = calendar.monthrange(ym.year, ym.month)[1]
    start_date = f"{ym.year:04d}-{ym.month:02d}-01"
    end_date   = f"{ym.year:04d}-{ym.month:02d}-{last_day:02d}"
    period_label = target_month
elif len(args) == 2:
    start_date, end_date = args[0], args[1]
    target_month = None
    period_label = f"{start_date}_{end_date}"
else:
    print("Usage: python _run_reimb_export.py [YYYY-MM | start_date end_date]")
    sys.exit(1)

print(f"\n=== Ramp Reimbursement Export ===")
print(f"Period: {start_date} → {end_date}")

# ---------------------------------------------------------------------------
# Load config and authenticate
# ---------------------------------------------------------------------------
env = load_env()
cfg = load_config()

# Make sure ap_account is set; add a sensible default if missing
bc_cfg = cfg.setdefault('business_central', {})
if not bc_cfg.get('ap_account') and not bc_cfg.get('ap_clearing_account'):
    print("\n⚠️  WARNING: 'ap_account' not found in config.toml [business_central].")
    print("   Defaulting to '20000'. Add ap_account to config.toml to suppress this.")
    bc_cfg['ap_account'] = '20000'

# Attach period filter so the transform function filters by payment_processed_at
if target_month:
    cfg['target_month'] = target_month
else:
    cfg['period'] = {'start': start_date, 'end': end_date}

client = RampClient(
    base_url=cfg['ramp']['base_url'],
    token_url=cfg['ramp']['token_url'],
    client_id=env['RAMP_CLIENT_ID'],
    client_secret=env['RAMP_CLIENT_SECRET']
)
client.authenticate()

# ---------------------------------------------------------------------------
# Fetch reimbursements
# Filter by transaction_date (the expense date on the reimbursement) using the
# Ramp API's from_transaction_date / to_transaction_date datetime string parameters.
# ---------------------------------------------------------------------------
from datetime import datetime, timedelta

from_dt_str = f"{start_date}T00:00:00Z"
to_dt_str   = f"{end_date}T23:59:59Z"

print(f"\nFetching reimbursements from Ramp (transaction_date {start_date} → {end_date})...")
reimbursements = client.get_reimbursements(
    from_transaction_date=from_dt_str,
    to_transaction_date=to_dt_str,
)
print(f"Fetched {len(reimbursements)} reimbursements from Ramp")

if not reimbursements:
    print("No reimbursements found. Exiting.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# Quick diagnostic: show payment_processed_at distribution
# ---------------------------------------------------------------------------
from collections import Counter
pay_months = Counter()
for r in reimbursements:
    ppa = (r.get('payment_processed_at') or '')[:7]  # YYYY-MM
    pay_months[ppa or '(none)'] += 1

print("\npayment_processed_at distribution:")
for month, count in sorted(pay_months.items()):
    marker = " ← target" if month == (target_month or period_label[:7]) else ""
    print(f"  {month}: {count} reimbursements{marker}")

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
print(f"\nTransforming (filtering by payment_processed_at in {period_label})...")
df = ramp_reimbursements_to_bc_rows(reimbursements, cfg)

if df.empty:
    print("\nNo journal rows produced for the selected period. Check the distribution above.")
    sys.exit(0)

detail_rows   = df[df['Document Type'] == 'Invoice']
clearing_rows = df[df['Document Type'] == 'Payment']

print(f"\nJournal rows produced:")
print(f"  Detail lines  (Dr Expense / Cr A/P): {len(detail_rows)}")
print(f"  Clearing lines (Dr A/P / Cr Bank):    {len(clearing_rows)}")
print(f"  Total rows:                            {len(df)}")

print(f"\nDebit total  : ${df['Debit Amount'].sum():,.2f}")
print(f"Credit total : ${df['Credit Amount'].sum():,.2f}")

print("\nSample rows:")
print(df.to_string(index=False, max_rows=15))

# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
xlsx_path, csv_path = export(df, output_dir='exports', prefix=f'REIMB_{period_label}')
print(f"\nExported:")
print(f"  XLSX: {xlsx_path}")
print(f"  CSV:  {csv_path}")
