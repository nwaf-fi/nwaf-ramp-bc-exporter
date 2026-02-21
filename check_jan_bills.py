import toml
import os
from datetime import datetime
from ramp_client import RampClient

# Load config
cfg = toml.load('config.toml')

# Try to get credentials from environment, or prompt user
client_id = os.getenv('RAMP_CLIENT_ID')
client_secret = os.getenv('RAMP_CLIENT_SECRET')

if not client_id or not client_secret:
    print("Credentials not found in environment variables.")
    print("Please enter your Ramp API credentials:")
    client_id = input("RAMP_CLIENT_ID: ").strip()
    client_secret = input("RAMP_CLIENT_SECRET: ").strip()

env = {
    'RAMP_CLIENT_ID': client_id,
    'RAMP_CLIENT_SECRET': client_secret
}

# Initialize client
client = RampClient(
    base_url=cfg['ramp']['base_url'],
    token_url=cfg['ramp']['token_url'],
    client_id=env['RAMP_CLIENT_ID'],
    client_secret=env['RAMP_CLIENT_SECRET'],
    enable_sync=False
)
client.authenticate()

# Fetch all bills
all_bills = client.get_bills(page_size=200, sync_ready=True) or []
print(f'Total bills fetched: {len(all_bills)}')

# Filter by payment.payment_date in Jan 2026
start_date = datetime(2026, 1, 1).date()
end_date = datetime(2026, 1, 31).date()

filtered = []
for bill in all_bills:
    payment_obj = bill.get('payment') or {}
    payment_date_str = payment_obj.get('payment_date')
    
    if payment_date_str:
        try:
            payment_date = datetime.fromisoformat(payment_date_str[:10]).date()
            if start_date <= payment_date <= end_date:
                filtered.append(bill)
                invoice_num = bill.get('invoice_number', 'N/A')
                status = bill.get('status', 'N/A')
                print(f'  Bill {invoice_num}: payment_date={payment_date}, status={status}')
        except Exception as e:
            pass

print(f'\nBills with payment_date in Jan 2026: {len(filtered)}')
