from transform import ramp_bills_to_general_journal
import os

# Sample bill matching structure expected from Ramp
sample_bill = {
    'id': '80938',
    'vendor': {'id': 'V-001', 'external_id': 'V-001-EXT', 'name': 'Parametric'},
    'vendor_invoice_number': '80938',
    'bill_date': '2025-11-09T00:00:00Z',
    'memo': 'Parametric Q3 2025 management fee',
    'line_items': [
        {
            'memo': 'Parametric management fee',
            'amount': {'amount': 2525800, 'minor_unit_conversion_rate': 100},
            'accounting_field_selections': [
                {'type': 'GL_ACCOUNT', 'external_code': '54010'},
                {'type': 'OTHER', 'category_info': {'type': 'OTHER', 'external_id': 'Department'}, 'external_code': '025'},
                {'type': 'OTHER', 'category_info': {'type': 'OTHER', 'external_id': 'Activity Code'}, 'external_code': '99'},
            ]
        },
        {
            'memo': 'To record invoice from Parametric',
            'amount': {'amount': 2525800, 'minor_unit_conversion_rate': 100},
            'accounting_field_selections': [
                {'type': 'GL_ACCOUNT', 'external_code': '26010'},
                {'type': 'OTHER', 'category_info': {'type': 'OTHER', 'external_id': 'Department'}, 'external_code': '025'},
                {'type': 'OTHER', 'category_info': {'type': 'OTHER', 'external_id': 'Activity Code'}, 'external_code': '99'},
            ]
        }
    ]
}

cfg = {'business_central': {'batch_name': 'CONTROLLER', 'vendor_payable_account': '26010'}}

os.makedirs('exports', exist_ok=True)

df = ramp_bills_to_general_journal([sample_bill], cfg)
print(df.head().to_string(index=False))

out = 'exports/test_journal_output.csv'
df.to_csv(out, index=False)
print(f"Wrote {out}")
