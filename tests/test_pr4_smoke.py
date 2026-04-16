import importlib
import os
import pandas as pd

def test_core_imports():
    modules = [
        'lib.ramp_client',
        'lib.utils',
        'lib.bc_export',
        'app.main',
    ]
    for m in modules:
        importlib.import_module(m)


def test_ramp_credit_card_transform_simple():
    from transform import ramp_credit_card_to_bc_rows

    cfg = {
        'business_central': {
            'template_name': 'GENERAL',
            'batch_name': 'ACCOUNTANT',
            'ramp_card_account': '26100',
            'bank_account': 'NT'
        }
    }

    tx = {
        'id': 'T1',
        'amount': 123.45,
        'posted_at': '2026-04-01T12:00:00Z',
        'line_items': [
            {
                'accounting_field_selections': [
                    {'type': 'GL_ACCOUNT', 'external_code': '5000'}
                ]
            }
        ],
        'merchant_name': 'Test Merchant',
        'memo': 'Office supplies'
    }

    df = ramp_credit_card_to_bc_rows([tx], cfg, write_audit=False, statement=None)
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 1
    row = df.iloc[0]
    assert row['Account No.'] == '5000'
    assert float(row['Debit Amount']) == 123.45
