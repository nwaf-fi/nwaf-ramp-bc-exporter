"""
Unit tests for ramp_reimbursements_to_bc_rows (A/P clearing workflow).
Run with:  python -m pytest tests/test_reimbursements_ap_clearing.py -v
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
import pandas as pd
from transform import ramp_reimbursements_to_bc_rows, BC_COLUMN_ORDER

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
AP_ACCOUNT = '20500'
BANK_ACCOUNT = 'NT'

BASE_CFG = {
    'business_central': {
        'ap_account': AP_ACCOUNT,
        'bank_account': BANK_ACCOUNT,
        'template_name': 'GENERAL',
        'batch_name': 'RAMP_REIMB',
    }
}

def _make_line_item(amount_cents: int, gl: str, dept: str = '100', activity: str = '10') -> dict:
    return {
        'amount': {'amount': amount_cents, 'minor_unit_conversion_rate': 100},
        'accounting_field_selections': [
            {'type': 'GL_ACCOUNT', 'external_code': gl},
            {'type': 'OTHER', 'external_code': dept,
             'category_info': {'external_id': 'Department'}},
            {'type': 'OTHER', 'external_code': activity,
             'category_info': {'external_id': 'Activity Code'}},
        ],
    }

def _make_reimb(id_: str, employee: str, payment_date: str, line_items: list,
                created_at: str = '2026-01-15T00:00:00Z', memo: str = '',
                transaction_date: str = '', payment_batch_id: str = '') -> dict:
    return {
        'id': id_,
        'user_full_name': employee,
        'payment_processed_at': f'{payment_date}T00:00:00Z' if payment_date else None,
        'transaction_date': transaction_date or payment_date,
        'payment_batch_id': payment_batch_id or None,
        'created_at': created_at,
        'memo': memo,
        'line_items': line_items,
    }


# ---------------------------------------------------------------------------
# Test 1 – One reimbursement, 5 line items for Employee A on 2026-02-10
#          → 5 detail lines + 1 clearing line
# ---------------------------------------------------------------------------
def test_single_reimbursement_five_line_items():
    items = [_make_line_item(5000, '63000') for _ in range(5)]  # 5 × $50
    reimbs = [_make_reimb('R001', 'Alice Smith', '2026-02-10', items)]
    cfg = dict(BASE_CFG, target_month='2026-02')
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)

    assert list(df.columns) == BC_COLUMN_ORDER, "Column order must match BC_COLUMN_ORDER"

    detail = df[df['Document No.'] == 'REIMB-R001']
    clearing = df[df['Document Type'] == 'Payment']

    assert len(detail) == 5, f"Expected 5 detail lines, got {len(detail)}"
    assert len(clearing) == 1, f"Expected 1 clearing line, got {len(clearing)}"

    # Detail lines: Account No. = expense GL, Bal. Account No. = A/P
    assert (detail['Account No.'] == '63000').all()
    assert (detail['Bal. Account No.'] == AP_ACCOUNT).all()
    assert (detail['Document Type'] == 'Invoice').all()

    # Clearing line: Account No. = A/P, Bal. Account No. = Bank
    clr = clearing.iloc[0]
    assert clr['Account No.'] == AP_ACCOUNT
    assert clr['Bal. Account No.'] == BANK_ACCOUNT
    assert clr['Document Type'] == 'Payment'
    assert abs(clr['Debit Amount'] - 250.00) < 0.01, f"Clearing total wrong: {clr['Debit Amount']}"

    # Detail Debit Amount totals same
    assert abs(detail['Debit Amount'].sum() - 250.00) < 0.01

    # Posting dates: detail lines use transaction_date, clearing uses payment_processed_at
    assert (detail['Posting Date'] == '02/10/2026').all(), "Detail lines should post on transaction_date"
    assert clr['Posting Date'] == '02/10/2026', "Clearing line should post on payment_processed_at"


# ---------------------------------------------------------------------------
# Test 2 – Two reimbursements for Employee A on the same payment date
#          → still only 1 clearing line
# ---------------------------------------------------------------------------
def test_two_reimbursements_same_employee_same_date():
    items_a = [_make_line_item(10000, '63000')]  # $100
    items_b = [_make_line_item(7500, '63100')]   # $75
    reimbs = [
        _make_reimb('R002', 'Bob Jones', '2026-02-10', items_a, payment_batch_id='BATCH-0210-A'),
        _make_reimb('R003', 'Bob Jones', '2026-02-10', items_b, payment_batch_id='BATCH-0210-A'),
    ]
    cfg = dict(BASE_CFG, target_month='2026-02')
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)

    clearing = df[df['Document Type'] == 'Payment']
    assert len(clearing) == 1, f"Expected 1 clearing line for Bob Jones, got {len(clearing)}"
    assert abs(clearing.iloc[0]['Debit Amount'] - 175.00) < 0.01


# ---------------------------------------------------------------------------
# Test 3 – Same employee, two different payment dates in the same month
#          → one clearing line per payment date
# ---------------------------------------------------------------------------
def test_same_employee_two_payment_dates():
    items = [_make_line_item(5000, '63000')]
    reimbs = [
        _make_reimb('R004', 'Carol White', '2026-02-05', items),
        _make_reimb('R005', 'Carol White', '2026-02-20', items),
    ]
    cfg = dict(BASE_CFG, target_month='2026-02')
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)

    clearing = df[df['Document Type'] == 'Payment']
    assert len(clearing) == 2, f"Expected 2 clearing lines, got {len(clearing)}"
    # Each clearing line = $50
    for _, row in clearing.iterrows():
        assert abs(row['Debit Amount'] - 50.00) < 0.01


# ---------------------------------------------------------------------------
# Test 4 – Month filter Feb 2026; items outside Feb must be excluded
# ---------------------------------------------------------------------------
def test_month_filter_excludes_outside_period():
    items = [_make_line_item(5000, '63000')]
    reimbs = [
        _make_reimb('R006', 'Dan Lee', '2026-02-15', items),   # IN  Feb 2026
        _make_reimb('R007', 'Dan Lee', '2026-01-31', items),   # OUT Jan 2026
        _make_reimb('R008', 'Dan Lee', '2026-03-01', items),   # OUT Mar 2026
    ]
    cfg = dict(BASE_CFG, target_month='2026-02')
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)

    assert 'REIMB-R006' in df['Document No.'].values
    assert 'REIMB-R007' not in df['Document No.'].values
    assert 'REIMB-R008' not in df['Document No.'].values

    clearing = df[df['Document Type'] == 'Payment']
    assert len(clearing) == 1


# ---------------------------------------------------------------------------
# Test 5 – Missing payment_processed_at with active period filter → skip
# ---------------------------------------------------------------------------
def test_missing_payment_date_produces_detail_only():
    """With no payment_processed_at: Pass 1 still generates the expense detail line
    (transaction_date falls back to created_at for the period check), but Pass 2
    produces no clearing line — the A/P liability exists but cash hasn't been paid."""
    items = [_make_line_item(5000, '63000')]
    reimbs = [
        _make_reimb('R009', 'Eve Green', '', items, created_at='2026-02-10T00:00:00Z'),
    ]
    cfg = dict(BASE_CFG, target_month='2026-02')
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)
    # Expense detail IS present (created_at '2026-02-10' is in period)
    assert 'REIMB-R009' in df['Document No.'].values, "Detail line expected even without payment date"
    # No clearing line — not yet paid
    clearing = df[df['Document Type'] == 'Payment']
    assert clearing.empty, "No clearing line expected when payment_processed_at is missing"


# ---------------------------------------------------------------------------
# Test 6 – Missing payment_processed_at WITHOUT period filter → use created_at
# ---------------------------------------------------------------------------
def test_missing_payment_date_falls_back_to_created_at_no_filter():
    items = [_make_line_item(5000, '63000')]
    reimbs = [
        _make_reimb('R010', 'Frank Hall', '', items, created_at='2026-02-10T00:00:00Z'),
    ]
    cfg = dict(BASE_CFG)  # no target_month, no period
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)
    assert 'REIMB-R010' in df['Document No.'].values
    # Posting date should be 02/10/2026 (from created_at)
    detail = df[df['Document No.'] == 'REIMB-R010']
    assert detail.iloc[0]['Posting Date'] == '02/10/2026'


# ---------------------------------------------------------------------------
# Test 7 – Line item missing GL account is skipped (not the whole reimbursement)
# ---------------------------------------------------------------------------
def test_line_item_missing_gl_skipped():
    good = _make_line_item(5000, '63000')
    bad_item = {
        'amount': {'amount': 3000, 'minor_unit_conversion_rate': 100},
        'accounting_field_selections': [],   # no GL account
    }
    reimbs = [_make_reimb('R011', 'Grace Kim', '2026-02-12', [good, bad_item])]
    cfg = dict(BASE_CFG, target_month='2026-02')
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)

    detail = df[df['Document No.'] == 'REIMB-R011']
    assert len(detail) == 1, "Only the valid line item should produce a detail row"
    # Clearing line should reflect only the valid $50
    clearing = df[df['Document Type'] == 'Payment']
    assert abs(clearing.iloc[0]['Debit Amount'] - 50.00) < 0.01


# ---------------------------------------------------------------------------
# Test 8 – Column order matches BC_COLUMN_ORDER in all scenarios
# ---------------------------------------------------------------------------
def test_output_columns_match_bc_column_order():
    items = [_make_line_item(5000, '63000')]
    reimbs = [_make_reimb('R012', 'Henry Wu', '2026-02-18', items)]
    cfg = dict(BASE_CFG, target_month='2026-02')
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)
    assert list(df.columns) == BC_COLUMN_ORDER


# ---------------------------------------------------------------------------
# Test 9 – period dict filter (start/end) instead of target_month
# ---------------------------------------------------------------------------
def test_period_dict_filter():
    items = [_make_line_item(5000, '63000')]
    reimbs = [
        _make_reimb('R013', 'Iris Chen', '2026-02-01', items),
        _make_reimb('R014', 'Iris Chen', '2026-02-28', items),
        _make_reimb('R015', 'Iris Chen', '2026-03-01', items),
    ]
    cfg = {**BASE_CFG, 'period': {'start': '2026-02-01', 'end': '2026-02-28'}}
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)
    assert 'REIMB-R013' in df['Document No.'].values
    assert 'REIMB-R014' in df['Document No.'].values
    assert 'REIMB-R015' not in df['Document No.'].values


def test_detail_uses_transaction_date_clearing_uses_payment_date():
    """Detail line posts on transaction_date; clearing line posts on payment_processed_at."""
    items = [_make_line_item(5000, '63000')]
    reimbs = [_make_reimb('R016', 'Jane Doe', '2026-02-25',  # payment_processed_at
                          items, transaction_date='2026-02-01')]  # expense date differs
    cfg = dict(BASE_CFG, target_month='2026-02')
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)

    detail = df[df['Document No.'] == 'REIMB-R016']
    clearing = df[df['Document Type'] == 'Payment']

    assert not detail.empty, "Expected a detail row for REIMB-R016"
    assert not clearing.empty, "Expected a clearing row"
    assert detail.iloc[0]['Posting Date'] == '02/01/2026', "Detail should use transaction_date"
    assert clearing.iloc[0]['Posting Date'] == '02/25/2026', "Clearing should use payment_processed_at"


# ---------------------------------------------------------------------------
# Test 11 – Independent passes: Jan expense paid in Feb
#           Feb filter → no detail line (Jan transaction_date), but YES clearing
# ---------------------------------------------------------------------------
def test_independent_passes_prior_period_expense_paid_in_filter_period():
    """Pass 1 (transaction_date) and Pass 2 (payment_processed_at) are independent.
    A January expense paid in February will appear as a clearing line in a February
    run, even though no detail line is included for that period."""
    items = [_make_line_item(8000, '63000')]  # $80
    reimbs = [
        _make_reimb('R017', 'Karl Nord', '2026-02-12', items,
                    transaction_date='2026-01-20'),  # expense in Jan, paid in Feb
    ]
    cfg = dict(BASE_CFG, target_month='2026-02')
    df = ramp_reimbursements_to_bc_rows(reimbs, cfg)

    # Pass 1: transaction_date Jan 20 is outside Feb → no detail line
    assert 'REIMB-R017' not in df['Document No.'].values, \
        "Detail line should be excluded (Jan transaction_date outside Feb filter)"

    # Pass 2: payment_processed_at Feb 12 is inside Feb → clearing line present
    clearing = df[df['Document Type'] == 'Payment']
    assert not clearing.empty, "Clearing line expected for Feb payment of Jan expense"
    assert abs(clearing.iloc[0]['Debit Amount'] - 80.00) < 0.01
    assert clearing.iloc[0]['Posting Date'] == '02/12/2026'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
