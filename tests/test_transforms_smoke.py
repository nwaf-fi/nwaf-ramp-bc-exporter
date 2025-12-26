import pytest
import pandas as pd

from transform import (
    ramp_bills_to_bc_rows,
    ramp_reimbursements_to_bc_rows,
    enrich_bills_with_vendor_external_ids,
)


def sample_cfg():
    return {
        'business_central': {
            'template_name': 'GENERAL',
            'batch_name': 'TEST_BATCH',
            'vendor_payable_account': '26000',
            'ramp_card_account': '11000',
            'bank_account': '11005',
        },
        'gl_mapping': {'ramp_gl_account_key': 'unused_in_this_test'},
    }


def test_ramp_bills_to_bc_rows_basic():
    cfg = sample_cfg()
    bills = [
        {
            'id': 'B1',
            'amount': {'amount': 12500, 'minor_unit_conversion_rate': 100},
            'bill_date': '2025-12-25T12:00:00Z',
            'memo': 'Office supplies',
            'vendor': {'id': 'v1', 'name': 'Vendor One'},
            'line_items': [
                {
                    'memo': 'Stationery',
                    'accounting_field_selections': [
                        {'category_info': {'type': 'GL_ACCOUNT'}, 'external_code': '4000'},
                        {'category_info': {'type': 'OTHER', 'external_id': 'Department'}, 'external_code': '001'},
                        {'category_info': {'type': 'OTHER', 'external_id': 'Activity Code'}, 'external_code': '01'},
                    ],
                }
            ],
        }
    ]

    df = ramp_bills_to_bc_rows(bills, cfg)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape[0] == 1
    row = df.iloc[0]
    assert row['Debit Amount'] == 125.00
    assert row['Account No.'] == '4000'
    assert row['Bal. Account No.'] == '26000'
    assert row['Department Code'] == '001'
    assert row['Activity Code'] == '01'


class FakeClient:
    def __init__(self, vendors=None, single_vendor=None):
        self._vendors = vendors or []
        self._single = single_vendor or {}

    def get_vendors(self):
        return self._vendors

    def get_vendor(self, vid):
        return self._single.get(vid)


def test_enrich_bills_with_vendor_external_ids():
    bills = [
        {'id': 'b1', 'vendor': {'id': 'v1', 'name': 'Vendor One'}},
        {'id': 'b2', 'vendor': {'id': 'v2', 'name': 'Vendor Two'}},
        {'id': 'b3'},
    ]

    vendors = [
        {'id': 'v1', 'external_vendor_id': 'EID-1'},
    ]
    single = {'v2': {'external_id': 'EID-2'}}
    client = FakeClient(vendors=vendors, single_vendor=single)

    enriched = enrich_bills_with_vendor_external_ids(bills, client)
    assert enriched[0]['vendor']['external_vendor_id_resolved'] == 'EID-1'
    assert enriched[1]['vendor']['external_vendor_id_resolved'] == 'EID-2'
    # bills without vendor should have vendor dict with resolved key set to empty string
    assert enriched[2]['vendor']['external_vendor_id_resolved'] == ''


def test_ramp_reimbursements_to_bc_rows_basic():
    cfg = sample_cfg()
    reimbursements = [
        {
            'id': 'R1',
            'created_at': '2025-12-24T09:00:00Z',
            'user': {'name': 'Alice'},
            'line_items': [
                {
                    'amount': {'amount': 2000, 'minor_unit_conversion_rate': 100},
                    'accounting_field_selections': [
                        {'type': 'GL_ACCOUNT', 'external_code': '5000'},
                        {'type': 'OTHER', 'category_info': {'external_id': 'Department'}, 'external_code': '002'},
                    ],
                }
            ],
        }
    ]

    df = ramp_reimbursements_to_bc_rows(reimbursements, cfg)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.iloc[0]['Debit Amount'] == 20.00
    assert df.iloc[0]['Account No.'] == '5000'
    assert df.iloc[0]['Department Code'] == '002'
