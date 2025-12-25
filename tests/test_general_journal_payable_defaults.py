import pandas as pd
from transform import ramp_bills_to_general_journal


def test_payable_balancing_line_has_default_dimensions():
    cfg = {'business_central': {'batch_name': 'TESTBATCH', 'vendor_payable_account': '26010'}}

    # Bill with a single line that will cause a balancing payable line to be created
    sample_bill = {
        'id': 'B-001',
        'vendor': {'id': 'V-001', 'external_id': 'V-001-EXT', 'name': 'VendorCo'},
        'vendor_invoice_number': 'INV-001',
        'bill_date': '2025-12-01T00:00:00Z',
        'memo': 'Single-line invoice',
        'line_items': [
            {
                'memo': 'Service charge',
                'amount': {'amount': 10000, 'minor_unit_conversion_rate': 100},  # $100.00
                'accounting_field_selections': [
                    {'type': 'GL_ACCOUNT', 'external_code': '54010'},
                ],
            }
        ]
    }

    df = ramp_bills_to_general_journal([sample_bill], cfg)

    # There should be at least one payable balancing line with Account No matching vendor_payable_account
    payable_rows = df[df['Account No.'] == cfg['business_central']['vendor_payable_account']]
    assert not payable_rows.empty, "Expected a payable balancing line"

    # Default dimensions should be set
    assert all(payable_rows['Department Code'] == '000')
    assert all(payable_rows['Activity Code'] == '00')
