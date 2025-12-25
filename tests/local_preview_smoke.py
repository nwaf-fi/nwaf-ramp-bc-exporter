from transform import (
    enrich_bills_with_vendor_external_ids,
    ramp_bills_to_purchase_invoice_lines,
    ramp_bills_to_general_journal,
    ramp_reimbursements_to_bc_rows
)

# Minimal fake client for enrichment
class DummyClient:
    def __init__(self):
        pass
    def is_transaction_synced(self, obj):
        return False
    def get_vendor(self, vid):
        # return a fake vendor with external id
        return {'id': vid, 'external_vendor_id': f'VENDOR-{vid}', 'external_id': f'VENDOR-{vid}', 'name': 'Dummy Vendor'}

# Sample bill
sample_bill = {
    'id': 'B-TEST-1',
    'vendor': {'id': 'V-1', 'external_id': 'V-1-EXT', 'name': 'Vendor 1'},
    'vendor_invoice_number': 'INV-TEST-1',
    'bill_date': '2025-12-10T00:00:00Z',
    'memo': 'Test invoice',
    'line_items': [
        {
            'memo': 'Service line',
            'amount': {'amount': 15000, 'minor_unit_conversion_rate': 100},
            'accounting_field_selections': [
                {'type': 'GL_ACCOUNT', 'external_code': '54010'},
                {'type': 'OTHER', 'category_info': {'type': 'OTHER', 'external_id': 'Department'}, 'external_code': '001'},
                {'type': 'OTHER', 'category_info': {'type': 'OTHER', 'external_id': 'Activity Code'}, 'external_code': '01'},
            ]
        }
    ]
}

sample_reim = {
    'id': 'R-TEST-1',
    'amount': {'amount': 5000, 'minor_unit_conversion_rate': 100},
    'memo': 'Reimbursement test',
    'line_items': [
        {'memo': 'Reim line', 'amount': {'amount': 5000, 'minor_unit_conversion_rate': 100}, 'accounting_field_selections': [{'type': 'GL_ACCOUNT', 'external_code': '54010'}]}
    ]
}

cfg = {'business_central': {'batch_name': 'TEST', 'vendor_payable_account': '26010'}}

client = DummyClient()

print('--- Enriching bills ---')
enriched = enrich_bills_with_vendor_external_ids([sample_bill], client)
print('Enriched vendor id:', enriched[0].get('vendor', {}).get('external_vendor_id_resolved', None))

print('\n--- Purchase Invoice lines ---')
pi_df = ramp_bills_to_purchase_invoice_lines(enriched, cfg)
print(pi_df.head().to_string(index=False))

print('\n--- General Journal from bills ---')
gj_df = ramp_bills_to_general_journal(enriched, cfg)
print(gj_df.head().to_string(index=False))

print('\n--- Reimbursements to BC rows ---')
reim_df = ramp_reimbursements_to_bc_rows([sample_reim], cfg)
print(reim_df.head().to_string(index=False))
