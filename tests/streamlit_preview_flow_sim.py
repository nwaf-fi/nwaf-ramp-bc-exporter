from transform import enrich_bills_with_vendor_external_ids, ramp_bills_to_purchase_invoice_lines, ramp_bills_to_general_journal, ramp_reimbursements_to_bc_rows
from datetime import datetime

class DummyClient:
    def __init__(self):
        pass
    def authenticate(self):
        return True
    def get_bills(self, status='APPROVED', start_date=None, end_date=None, page_size=200):
        return [
            {
                'id': 'B-1',
                'vendor': {'id': 'V-A', 'external_id': 'V-A-EXT', 'name': 'Vendor A'},
                'vendor_invoice_number': 'INV-A-1',
                'bill_date': '2025-11-30T00:00:00Z',
                'line_items': [
                    {'memo': 'Consulting', 'amount': {'amount': 5000, 'minor_unit_conversion_rate': 100}, 'accounting_field_selections': [{'type':'GL_ACCOUNT','external_code':'54010'}]}
                ]
            }
        ]
    def is_transaction_synced(self, obj):
        return False
    def get_reimbursements(self, status='PAID', start_date=None, end_date=None, page_size=200):
        return [
            {'id':'R-1','amount':{'amount':2500,'minor_unit_conversion_rate':100},'line_items':[{'memo':'Reim','amount':{'amount':2500,'minor_unit_conversion_rate':100},'accounting_field_selections':[{'type':'GL_ACCOUNT','external_code':'54010'}]}]}
        ]

cfg = {'business_central': {'batch_name': 'SIM', 'vendor_payable_account': '26010'}}

client = DummyClient()

# Invoice preview path
bills = client.get_bills(status='APPROVED', start_date='2025-11-01', end_date='2025-11-30')
print('Fetched bills:', len(bills))
filtered = [b for b in bills if not client.is_transaction_synced(b)]
enriched = enrich_bills_with_vendor_external_ids(filtered, client)
pi_df = ramp_bills_to_purchase_invoice_lines(enriched, cfg)
gj_df = ramp_bills_to_general_journal(enriched, cfg)
print('PI rows:', len(pi_df) if pi_df is not None else 0)
print('GJ rows:', len(gj_df) if gj_df is not None else 0)

# Reimbursements preview path
reims = client.get_reimbursements(status='PAID', start_date='2025-11-01', end_date='2025-11-30')
print('Fetched reimbursements:', len(reims))
reims_filtered = [r for r in reims if not client.is_transaction_synced(r)]
r_df = ramp_reimbursements_to_bc_rows(reims_filtered, cfg)
print('Reimbursement rows:', len(r_df) if r_df is not None else 0)

print('Simulated preview flow complete at', datetime.now().isoformat())
