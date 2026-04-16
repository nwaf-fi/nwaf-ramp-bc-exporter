import hashlib
import pandas as pd

from transform import ramp_credit_card_to_bc_rows


def _md5_of_df_csv(df: pd.DataFrame) -> str:
    csv = df.to_csv(index=False)
    return hashlib.md5(csv.encode('utf-8')).hexdigest()


def test_credit_card_transform_checksum():
    cfg = {
        'business_central': {
            'template_name': 'GENERAL',
            'batch_name': 'ACCOUNTANT',
            'ramp_card_account': '26100',
            'bank_account': 'NT'
        }
    }

    tx = {
        'id': 'CHK1',
        'amount': 50.00,
        'posted_at': '2026-04-01T12:00:00Z',
        'line_items': [
            {'accounting_field_selections': [{'type': 'GL_ACCOUNT', 'external_code': '7000'}]}
        ],
        'merchant_name': 'Checksum Merchant',
        'memo': 'Stationery'
    }

    df = ramp_credit_card_to_bc_rows([tx], cfg, write_audit=False, statement=None)
    md5 = _md5_of_df_csv(df)

    # Known-good checksum for this transform shape/value using current transform logic.
    expected = 'b515a45db333622ac93acdda9c7e3880'
    assert md5 == expected, f"Checksum mismatch: {md5} != {expected}"
