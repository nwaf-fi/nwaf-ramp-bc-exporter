# transform.py

import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
import os
import json
from typing import Iterable

# Define the standard BC General Journal column order
BC_COLUMN_ORDER = [
    'Journal Template Name', 'Journal Batch Name', 'Posting Date', 
    'Document Date', 'Document Type', 'Document No.', 'Account Type', 'Account No.', 
    'Description', 'Debit Amount', 'Credit Amount', 'Bal. Account Type', 
    'Bal. Account No.', 'Department Code', 'Activity Code'
]

# Column order for the credit-card style export requested by the user
CC_COLUMN_ORDER = [
    'Date', 'Merchant', 'Posting Date', 'Description', 'Account Type',
    'Account', 'Account Name', 'Department', 'Activity', 'Debit', 'Credit'
]

def ramp_to_bc_rows(transactions: List[Dict[str, Any]], cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Converts a list of Ramp transactions into a DataFrame suitable for BC import,
    using the G/L Account number already coded in the Ramp transaction data.
    """
    if not transactions:
        print("No transactions provided for transformation.")
        return pd.DataFrame()

    print(f"--- Transforming {len(transactions)} transactions using direct G/L mapping ---")
    
    journal_lines = []
    
    # Configuration from config.toml
    bc_cfg = cfg['business_central']
    
    # --- !!! CRITICAL: VERIFY AND SET THE CORRECT KEY HERE !!! ---
    # This key must match the field in the Ramp API response that holds the BC G/L Account No.
    RAMP_GL_ACCOUNT_KEY = cfg['gl_mapping']['ramp_gl_account_key'] 
    
    for index, t in enumerate(transactions):
        # 1. Extract and standardize data
        amount_major_units = t.get('amount', 0)  # Already in major units (dollars)
        
        # Use transaction date for posting date
        trans_date_str = t.get('user_transaction_time', datetime.now().strftime('%Y-%m-%d'))
        try:
            posting_dt = datetime.strptime(trans_date_str[:10], '%Y-%m-%d')
            posting_date_str = posting_dt.strftime('%m/%d/%Y')
        except Exception:
            posting_date_str = datetime.now().strftime('%m/%d/%Y')
        
        doc_no = f"RAMP-{t.get('id', index)}" 
        description = t.get('memo', t.get('merchant_name', 'Ramp Transaction'))
        
        # 2. EXTRACT ACCOUNTING DIMENSIONS FROM LINE ITEMS
        # Look in line_items[0].accounting_field_selections for all types
        trans_gl_account = None
        department_code = None
        activity_code = None
        
        line_items = t.get('line_items', [])
        if line_items and line_items[0].get('accounting_field_selections'):
            for selection in line_items[0]['accounting_field_selections']:
                if selection.get('type') == 'GL_ACCOUNT':
                    trans_gl_account = str(selection.get('external_code', '')).strip()
                elif selection.get('type') == 'OTHER':
                    external_id = selection.get('category_info', {}).get('external_id')
                    if external_id == 'Department':
                        department_code = str(selection.get('external_code', '')).strip()
                    elif external_id == 'Activity Code':
                        activity_code = str(selection.get('external_code', '')).strip()
        
        if not trans_gl_account or trans_gl_account in ('None', 'null', ''):
             print(f"⚠️ Warning: Transaction {doc_no} is missing a G/L Account code. Skipping.")
             continue # Skip transactions that are not fully coded

        # 3. CARD TRANSACTIONS: Use "Payment" document type (accounting best practice)
        # Credit card transactions are classified as payments since they represent
        # the disbursement/payment to merchants/vendors
        gl_debit = amount_major_units  # Debit the expense account
        gl_credit = 0.0
        bank_debit = 0.0
        bank_credit = amount_major_units  # Credit the bank account
        doc_type = 'Payment'  # Appropriate for disbursements/payments

        # 4. Create the journal line
        journal_lines.append({
            'Journal Template Name': bc_cfg.get('template_name', 'GENERAL'),
            'Journal Batch Name': bc_cfg.get('batch_name', 'ACCOUNTANT'),
            'Posting Date': posting_date_str,
            'Document Date': posting_date_str,
            'Document Type': doc_type,
            'Document No.': doc_no,
            'Account Type': 'G/L Account',
            'Account No.': trans_gl_account, # DIRECTLY USE THE RAMP-CODED ACCOUNT
            'Description': description,
            'Debit Amount': round(gl_debit, 2),
            'Credit Amount': round(gl_credit, 2),
            'Bal. Account Type': 'G/L Account',
            'Bal. Account No.': str(bc_cfg['ramp_card_account']),
            'Department Code': department_code or '',
            'Activity Code': activity_code or '',
        })

    df_output = pd.DataFrame(journal_lines)
    if df_output.empty:
        print("No valid transactions found with G/L account codes. Returning empty DataFrame.")
        return pd.DataFrame(columns=BC_COLUMN_ORDER)
    return df_output[BC_COLUMN_ORDER]


def fetch_vendor_external_ids(ramp_client, vendor_ids: Iterable[str]) -> Dict[str, str]:
    """
    Given a RampClient and an iterable of vendor UUIDs, fetch vendor records
    from the Vendor API and return a mapping vendor_id -> external id value.

    The function looks for common field names where the UI "External ID" may be stored
    (e.g. `external_vendor_id`, `external_id`, `remote_code`, `accounting_vendor_remote_id`).
    If no external id is found for a vendor, the mapping will contain an empty string.
    """
    vendor_map: Dict[str, str] = {}
    if not vendor_ids:
        return vendor_map

    # Prefer fetching the full vendor list once (more efficient and avoids
    # per-id 404 issues). Build a mapping vendor.id -> vendor record.
    try:
        all_vendors = ramp_client.get_vendors()
    except Exception:
        all_vendors = []

    lookup_by_id: Dict[str, Dict] = {}
    for v in all_vendors:
        vid = v.get('id')
        if vid:
            lookup_by_id[vid] = v

    unique_ids = set(vendor_ids)
    for vid in unique_ids:
        if not vid:
            continue
        v = lookup_by_id.get(vid)
        if not v:
            # fallback to per-id lookup if not in list
            try:
                v = ramp_client.get_vendor(vid)
            except Exception:
                v = None

        ext = ""
        if v and isinstance(v, dict):
            for k in ("external_vendor_id", "external_id", "externalId", "remote_code", "remoteCode", "accounting_vendor_remote_id", "externalCode"):
                val = v.get(k)
                if val:
                    ext = str(val)
                    break
        vendor_map[vid] = ext
    return vendor_map


def enrich_bills_with_vendor_external_ids(bills: List[Dict[str, Any]], ramp_client) -> List[Dict[str, Any]]:
    """
    For each bill in `bills`, fetch the corresponding vendor record via
    `ramp_client.get_vendor()` and attach a resolved `external_vendor_id` into
    `bill['vendor']['external_vendor_id_resolved']` when available.

    Returns the modified list of bills (mutates in-place but also returns it).
    """
    if not bills:
        return bills

    vendor_ids = [b.get('vendor', {}).get('id') for b in bills if b.get('vendor')]
    vendor_map = fetch_vendor_external_ids(ramp_client, vendor_ids)

    for b in bills:
        v = b.get('vendor') or {}
        vid = v.get('id')
        resolved = vendor_map.get(vid, "")
        # Attach resolved external id for downstream transforms
        if 'vendor' not in b:
            b['vendor'] = {}
        b['vendor']['external_vendor_id_resolved'] = resolved

    return bills


def ramp_bills_to_bc_rows(bills: List[Dict[str, Any]], cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Converts Ramp bills into Business Central journal entries.
    Bills are typically vendor invoices that need payment.
    """
    if not bills:
        print("No bills provided for transformation.")
        return pd.DataFrame()

    print(f"--- Transforming {len(bills)} bills ---")
    
    journal_lines = []
    bc_cfg = cfg['business_central']
    
    for index, bill in enumerate(bills):
        # Extract bill data
        amount_obj = bill.get('amount', {})
        if isinstance(amount_obj, dict):
            minor_amount = amount_obj.get('amount', 0)
            conversion_rate = amount_obj.get('minor_unit_conversion_rate', 100)
            amount = minor_amount / conversion_rate
        else:
            # Fallback if amount is already a number
            amount = float(amount_obj) if amount_obj else 0.0
        
        # For bank reconciliation: use payment date for posting date
        # and bill_date (invoice date) for document date
        # payment_date is a direct field on the bill object per Ramp API docs
        paid_date = bill.get('payment_date') or bill.get('paid_at') or bill.get('settled_at')
        bill_date = bill.get('bill_date') or bill.get('issued_at') or bill.get('created_at')
        
        # Posting date = payment date (for bank reconciliation)
        try:
            if paid_date:
                posting_dt = datetime.fromisoformat(paid_date[:19])
                posting_date = posting_dt.strftime('%m/%d/%Y')
            elif bill_date:
                posting_dt = datetime.fromisoformat(bill_date[:19])
                posting_date = posting_dt.strftime('%m/%d/%Y')
            else:
                posting_date = datetime.now().strftime('%m/%d/%Y')
        except Exception:
            try:
                date_str = paid_date or bill_date
                if date_str:
                    posting_dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
                    posting_date = posting_dt.strftime('%m/%d/%Y')
                else:
                    posting_date = datetime.now().strftime('%m/%d/%Y')
            except Exception:
                posting_date = datetime.now().strftime('%m/%d/%Y')
        
        # Document/Invoice date = bill_date (original invoice date)
        try:
            if bill_date:
                doc_dt = datetime.fromisoformat(bill_date[:19])
                document_date = doc_dt.strftime('%m/%d/%Y')
            else:
                document_date = posting_date
        except Exception:
            try:
                if bill_date:
                    doc_dt = datetime.strptime(bill_date[:10], '%Y-%m-%d')
                    document_date = doc_dt.strftime('%m/%d/%Y')
                else:
                    document_date = posting_date
            except Exception:
                document_date = posting_date
        
        doc_no = f"BILL-{bill.get('id', index)}"
        
        # Get description from bill memo, or line item memo, or fallback
        description = bill.get('memo')
        if not description:
            line_items = bill.get('line_items', [])
            if line_items and line_items[0].get('memo'):
                description = line_items[0]['memo']
        if not description:
            description = f"Bill from {bill.get('vendor', {}).get('name', 'Unknown Vendor')}"
        
        # Extract accounting dimensions from line items
        gl_account = None
        department_code = None
        activity_code = None
        
        line_items = bill.get('line_items', [])
        if line_items and line_items[0].get('accounting_field_selections'):
            for selection in line_items[0]['accounting_field_selections']:
                category_type = selection.get('category_info', {}).get('type')
                if category_type == 'GL_ACCOUNT':
                    gl_account = str(selection.get('external_code', '')).strip()
                elif category_type == 'OTHER':
                    external_id = selection.get('category_info', {}).get('external_id')
                    if external_id == 'Department':
                        department_code = str(selection.get('external_code', '')).strip()
                    elif external_id == 'Activity Code':
                        activity_code = str(selection.get('external_code', '')).strip()
        
        # Bills create payables: Debit Expense, Credit Vendor Payable
        # Use the coded expense account if available, otherwise suspense account
        expense_account = gl_account if gl_account and gl_account not in ('None', 'null', '') else bc_cfg.get('vendor_payable_account', '26000')
        
        journal_lines.append({
            'Journal Template Name': bc_cfg.get('template_name', 'GENERAL'),
            'Journal Batch Name': bc_cfg.get('batch_name', 'ACCOUNTANT'),
            'Posting Date': posting_date,
            'Document Date': document_date,
            'Document Type': 'Invoice',  # Bills are invoices from vendors
            'Document No.': doc_no,
            'Account Type': 'G/L Account',
            'Account No.': str(expense_account),  # Use coded expense account (as string)
            'Description': description,
            'Debit Amount': round(amount, 2),  # Debit the expense account
            'Credit Amount': 0.0,
            'Bal. Account Type': 'G/L Account',
            'Bal. Account No.': str(bc_cfg.get('vendor_payable_account', '26000')),  # Credit vendor payable (as string)
            'Department Code': str(department_code or ''),
            'Activity Code': str(activity_code or ''),
        })

    df_output = pd.DataFrame(journal_lines)
    if df_output.empty:
        return pd.DataFrame(columns=BC_COLUMN_ORDER)
    return df_output[BC_COLUMN_ORDER]


def ramp_reimbursements_to_bc_rows(reimbursements: List[Dict[str, Any]], cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Converts Ramp reimbursements into Business Central journal entries using an
    A/P clearing workflow.

    Two independent passes, each with its own period filter:

      Pass 1 – Expense/detail lines, filtered by transaction_date:
        Dr  Expense G/L account  (employee-coded)
        Cr  A/P clearing account (via Bal. Account No.)
        Posting Date = transaction_date
        Document No. = REIMB-{reimbursement_id}
        Included when: transaction_date falls within the target period.
        Reimbursements with no payment_processed_at ARE included — liability
        is recognized when the expense occurred, not when it was paid.

      Pass 2 – Clearing/payment lines, filtered by payment_processed_at:
        Dr  A/P clearing account
        Cr  Northern Trust bank  (via Bal. Account No.)
        Posting Date = payment_processed_at date
        Document No. = payment_batch_id (groups all reimbursements paid in the
                        same bank transaction under one clearing line)
        Included when: payment_processed_at falls within the target period.
        These lines are independent of Pass 1 — a February run will pick up
        payments for January expenses, and that is intentional.
        Reimbursements with no payment_processed_at are skipped in Pass 2.

    The two passes are run over the full reimbursements list independently.
    A single reimbursement may appear in Pass 1 only (unpaid), Pass 2 only
    (paid in this period but expensed in a prior period), or both (same period).

    Config keys read from cfg['business_central']:
      ap_account / ap_clearing_account  – A/P liability / clearing G/L account
      bank_account                       – Northern Trust bank G/L account
      template_name, batch_name         – passed through to BC columns

    Optional period filtering:
      cfg['period'] = {'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}
      OR cfg['target_month'] = 'YYYY-MM'
    """
    from decimal import Decimal, ROUND_HALF_UP
    import re, calendar

    if not reimbursements:
        print("No reimbursements provided for transformation.")
        return pd.DataFrame()

    bc_cfg = cfg.get('business_central', {})
    ap_account = str(
        bc_cfg.get('ap_account') or bc_cfg.get('ap_clearing_account', '26000')
    )
    bank_account = str(bc_cfg.get('bank_account', 'NT'))
    template_name = bc_cfg.get('template_name', 'GENERAL')
    batch_name = bc_cfg.get('batch_name', 'RAMP_REIMB')

    # -------------------------------------------------------------------------
    # Resolve period filter
    # -------------------------------------------------------------------------
    filter_start: str | None = None
    filter_end: str | None = None
    period_cfg = cfg.get('period')
    target_month = cfg.get('target_month')

    if period_cfg and isinstance(period_cfg, dict):
        filter_start = period_cfg.get('start')
        filter_end = period_cfg.get('end')
    elif target_month:
        try:
            ym = datetime.strptime(target_month, '%Y-%m')
            last_day = calendar.monthrange(ym.year, ym.month)[1]
            filter_start = ym.strftime('%Y-%m-01')
            filter_end = f"{ym.year:04d}-{ym.month:02d}-{last_day:02d}"
        except ValueError:
            print(f"⚠️ Warning: Invalid target_month '{target_month}'. Period filter ignored.")

    period_active = bool(filter_start and filter_end)

    def _in_period(date_str: str) -> bool:
        return filter_start <= date_str <= filter_end  # type: ignore[operator]

    def _parse_amount(amount_obj) -> Decimal:
        if isinstance(amount_obj, dict):
            minor = Decimal(str(amount_obj.get('amount', 0)))
            rate = Decimal(str(amount_obj.get('minor_unit_conversion_rate', 100)))
            return minor / rate
        return Decimal(str(amount_obj)) if amount_obj else Decimal('0')

    def _fmt_date(date_iso10: str) -> str:
        try:
            return datetime.strptime(date_iso10, '%Y-%m-%d').strftime('%m/%d/%Y')
        except Exception:
            return datetime.now().strftime('%m/%d/%Y')

    def _extract_coding(line_item: dict) -> tuple[str | None, str, str]:
        """Return (gl_account, department_code, activity_code) from a line item."""
        gl_account = None
        department_code = ''
        activity_code = ''
        for selection in line_item.get('accounting_field_selections', []):
            sel_type = selection.get('type')
            if sel_type == 'GL_ACCOUNT':
                gl_account = str(selection.get('external_code', '')).strip() or None
            elif sel_type == 'OTHER':
                ext_id = (selection.get('category_info') or {}).get('external_id')
                if ext_id == 'Department':
                    department_code = str(selection.get('external_code', '')).strip()
                elif ext_id == 'Activity Code':
                    activity_code = str(selection.get('external_code', '')).strip()
        return gl_account, department_code, activity_code

    print(f"--- Transforming {len(reimbursements)} reimbursements (A/P clearing workflow) ---")
    if period_active:
        print(f"    Expense period (Pass 1): transaction_date in [{filter_start}, {filter_end}]")
        print(f"    Payment period (Pass 2): payment_processed_at in [{filter_start}, {filter_end}]")

    # =========================================================================
    # PASS 1 — Expense / detail lines (filtered by transaction_date)
    # =========================================================================
    detail_lines: list = []

    for index, reimbursement in enumerate(reimbursements):
        doc_no = f"REIMB-{reimbursement.get('id', index)}"
        employee_name = reimbursement.get('user_full_name') or 'Unknown Employee'

        transaction_date_raw = reimbursement.get('transaction_date') or ''
        transaction_date_iso = transaction_date_raw[:10] if transaction_date_raw else ''

        if not transaction_date_iso:
            # Fall back to accounting_date, then created_at
            for fallback_field in ('accounting_date', 'created_at'):
                raw = reimbursement.get(fallback_field) or ''
                if raw:
                    transaction_date_iso = raw[:10]
                    break
            if not transaction_date_iso:
                transaction_date_iso = datetime.now().strftime('%Y-%m-%d')

        # Apply period filter to transaction_date
        if period_active and not _in_period(transaction_date_iso):
            continue

        transaction_date_str = _fmt_date(transaction_date_iso)
        description = reimbursement.get('memo') or f"Reimbursement – {employee_name}"

        line_items = reimbursement.get('line_items', [])
        if not line_items:
            print(f"⚠️ Warning: {doc_no} has no line items. Skipping.")
            continue

        for line_index, line_item in enumerate(line_items):
            amount = _parse_amount(line_item.get('amount', {}))
            gl_account, department_code, activity_code = _extract_coding(line_item)

            if not gl_account:
                print(
                    f"⚠️ Warning: {doc_no} line {line_index} has no G/L Account. Skipping line."
                )
                continue

            detail_lines.append({
                'Journal Template Name': template_name,
                'Journal Batch Name': batch_name,
                'Posting Date': transaction_date_str,
                'Document Date': transaction_date_str,
                'Document Type': 'Invoice',
                'Document No.': doc_no,
                'Account Type': 'G/L Account',
                'Account No.': gl_account,
                'Description': description,
                'Debit Amount': float(amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
                'Credit Amount': 0.0,
                'Bal. Account Type': 'G/L Account',
                'Bal. Account No.': ap_account,
                'Department Code': department_code or '000',
                'Activity Code': activity_code or '00',
            })

    # =========================================================================
    # PASS 2 — Clearing / payment lines (filtered by payment_processed_at)
    # Grouped by payment_batch_id — one clearing line per bank payment batch.
    # Document No. = payment_batch_id so it ties directly to the bank statement.
    # =========================================================================

    # clearing_batches keyed by payment_batch_id
    # value: {'total', 'payment_date_iso', 'payment_id', 'employee_names': set,
    #         'dept', 'activity'}
    clearing_batches: Dict[str, dict] = {}

    for reimbursement in reimbursements:
        payment_processed_at = reimbursement.get('payment_processed_at') or ''
        payment_date_iso = payment_processed_at[:10] if payment_processed_at else ''

        # Skip unpaid reimbursements entirely in Pass 2
        if not payment_date_iso:
            continue

        # Apply period filter to payment date
        if period_active and not _in_period(payment_date_iso):
            continue

        payment_batch_id = reimbursement.get('payment_batch_id') or ''
        if not payment_batch_id:
            # No batch ID — treat each reimbursement as its own payment
            # (edge case; use reimbursement id as a stable fallback key)
            payment_batch_id = f"SOLO-{reimbursement.get('id', '')}"

        employee_name = reimbursement.get('user_full_name') or 'Unknown Employee'
        payment_id = reimbursement.get('payment_id') or ''

        line_items = reimbursement.get('line_items', [])
        for line_item in line_items:
            amount = _parse_amount(line_item.get('amount', {}))
            gl_account, department_code, activity_code = _extract_coding(line_item)

            if not gl_account:
                continue  # already warned in Pass 1

            if payment_batch_id not in clearing_batches:
                clearing_batches[payment_batch_id] = {
                    'total': Decimal('0'),
                    'payment_date_iso': payment_date_iso,
                    'payment_id': payment_id,
                    'employee_names': set(),
                    'dept': department_code or '000',
                    'activity': activity_code or '00',
                }

            batch = clearing_batches[payment_batch_id]
            batch['total'] += amount
            batch['employee_names'].add(employee_name)

            # Prefer first non-default dept/activity seen in the batch
            if (not batch['dept'] or batch['dept'] == '000') and department_code:
                batch['dept'] = department_code
            if (not batch['activity'] or batch['activity'] == '00') and activity_code:
                batch['activity'] = activity_code

    clearing_lines: list = []
    for payment_batch_id, batch in clearing_batches.items():
        group_total = batch['total'].quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        payment_date_str = _fmt_date(batch['payment_date_iso'])
        names = ', '.join(sorted(batch['employee_names']))
        payment_id_ref = f" (Ref: {batch['payment_id']})" if batch['payment_id'] else ''
        description = f"Reimbursement payment – {names}{payment_id_ref}"

        # Document No. is the payment_batch_id — ties directly to the bank statement
        doc_no = payment_batch_id

        clearing_lines.append({
            'Journal Template Name': template_name,
            'Journal Batch Name': batch_name,
            'Posting Date': payment_date_str,
            'Document Date': payment_date_str,
            'Document Type': 'Payment',
            'Document No.': doc_no,
            'Account Type': 'G/L Account',
            'Account No.': ap_account,
            'Description': description,
            'Debit Amount': float(group_total),
            'Credit Amount': 0.0,
            'Bal. Account Type': 'G/L Account',
            'Bal. Account No.': bank_account,
            'Department Code': batch['dept'],
            'Activity Code': batch['activity'],
        })

    all_lines = detail_lines + clearing_lines
    df_output = pd.DataFrame(all_lines)
    if df_output.empty:
        print("No valid lines produced. Returning empty DataFrame.")
        return pd.DataFrame(columns=BC_COLUMN_ORDER)

    print(f"✓ Pass 1: {len(detail_lines)} expense detail lines")
    print(f"✓ Pass 2: {len(clearing_lines)} clearing lines ({len(clearing_batches)} payment batches)")
    return df_output[BC_COLUMN_ORDER]


def ramp_cashbacks_to_bc_rows(cashbacks: List[Dict[str, Any]], cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Converts Ramp cashbacks into Business Central journal entries.
    Cashbacks are rewards/credits from credit card usage.
    """
    if not cashbacks:
        print("No cashbacks provided for transformation.")
        return pd.DataFrame()

    print(f"--- Transforming {len(cashbacks)} cashbacks ---")
    
    journal_lines = []
    bc_cfg = cfg['business_central']
    
    for index, cashback in enumerate(cashbacks):
        amount_obj = cashback.get('amount', {})
        if isinstance(amount_obj, dict):
            minor_amount = amount_obj.get('amount', 0)
            conversion_rate = amount_obj.get('minor_unit_conversion_rate', 100)
            amount = minor_amount / conversion_rate
        else:
            # Fallback if amount is already a number
            amount = float(amount_obj) if amount_obj else 0.0
            
        earned_date = cashback.get('earned_at', '')
        try:
            if earned_date:
                posting_dt = datetime.strptime(earned_date[:10], '%Y-%m-%d')
                posting_date_str = posting_dt.strftime('%m/%d/%Y')
            else:
                posting_date_str = datetime.now().strftime('%m/%d/%Y')
        except Exception:
            posting_date_str = datetime.now().strftime('%m/%d/%Y')
        
        doc_no = f"CASHBACK-{cashback.get('id', index)}"
        description = f"Cashback reward - {cashback.get('description', 'Credit card cashback')}"
        
        # Cashbacks: Debit Cashback Income, Credit Bank/Card
        journal_lines.append({
            'Journal Template Name': bc_cfg.get('template_name', 'GENERAL'),
            'Journal Batch Name': bc_cfg.get('batch_name', 'RAMP_CASHBACK'),
            'Posting Date': posting_date_str,
            'Document Date': posting_date_str,
            'Document Type': 'Payment',
            'Document No.': doc_no,
            'Account Type': 'G/L Account',
            'Account No.': str(bc_cfg.get('other_income_account', '40000')),  # Other income account (as string)
            'Description': description,
            'Debit Amount': round(amount, 2),
            'Credit Amount': 0.0,
            'Bal. Account Type': 'G/L Account',
            'Bal. Account No.': str(bc_cfg.get('bank_account', 'NT')),
            'Department Code': '',
            'Activity Code': '',
        })

    df_output = pd.DataFrame(journal_lines)
    if df_output.empty:
        return pd.DataFrame(columns=BC_COLUMN_ORDER)
    return df_output[BC_COLUMN_ORDER]


def ramp_statements_to_bc_rows(statements: List[Dict[str, Any]], cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Converts Ramp statements into Business Central journal entries.
    Statements summarize credit card activity periods.
    """
    if not statements:
        print("No statements provided for transformation.")
        return pd.DataFrame()

    print(f"--- Transforming {len(statements)} statements ---")
    
    journal_lines = []
    bc_cfg = cfg['business_central']
    
    for index, statement in enumerate(statements):
        # Statements might contain summary information
        # This is a placeholder - actual implementation depends on statement structure
        total_amount_obj = statement.get('total_amount', {})
        if isinstance(total_amount_obj, dict):
            minor_amount = total_amount_obj.get('amount', 0)
            conversion_rate = total_amount_obj.get('minor_unit_conversion_rate', 100)
            total_amount = minor_amount / conversion_rate
        else:
            # Fallback if amount is already a number
            total_amount = float(total_amount_obj) if total_amount_obj else 0.0
            
        statement_date = statement.get('statement_date', '')
        try:
            if statement_date:
                posting_dt = datetime.strptime(statement_date[:10], '%Y-%m-%d')
                posting_date_str = posting_dt.strftime('%m/%d/%Y')
            else:
                posting_date_str = datetime.now().strftime('%m/%d/%Y')
        except Exception:
            posting_date_str = datetime.now().strftime('%m/%d/%Y')
        
        doc_no = f"STMT-{statement.get('id', index)}"
        description = f"Credit card statement - {statement.get('card', {}).get('last_four', 'XXXX')}"
        
        # Statement summary (if needed for reconciliation)
        # This might be more of an informational entry
        journal_lines.append({
            'Journal Template Name': bc_cfg.get('template_name', 'GENERAL'),
            'Journal Batch Name': bc_cfg.get('batch_name', 'RAMP_STMTS'),
            'Posting Date': posting_date_str,
            'Document Date': posting_date_str,
            'Document Type': '',
            'Document No.': doc_no,
            'Account Type': 'G/L Account',
            'Account No.': str(bc_cfg.get('ramp_card_account', '26100')),
            'Description': description,
            'Debit Amount': 0.0,
            'Credit Amount': round(total_amount, 2),
            'Bal. Account Type': '',
            'Bal. Account No.': '',
            'Department Code': '',
            'Activity Code': '',
        })

    df_output = pd.DataFrame(journal_lines)
    if df_output.empty:
        return pd.DataFrame(columns=BC_COLUMN_ORDER)
    return df_output[BC_COLUMN_ORDER]


def ramp_credit_card_to_bc_rows(transactions: List[Dict[str, Any]], cfg: Dict[str, Any], write_audit: bool = True, statement: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Converts Ramp credit-card transactions into a single-line-per-transaction
    Business Central-friendly DataFrame using the user's requested column
    ordering and formatting.

    When `write_audit` is True (default), an audit CSV with exported Ramp ids
    will be written to `exports/` for traceability. Set to False to suppress
    audit file creation (used by v2 CC exporter to avoid extra files).

    Expected mapping (per user):
      - Date = posting_date (MM/DD/YYYY)
      - Merchant = merchant_name
      - Posting Date = payment date (user-definable field; falls back to transaction date)
      - Description = description
      - Account Type = 'G/L Account'
      - Account = trans_gl_account (from accounting_field_selections)
      - Account Name = ''
      - Department = department_code or ''
      - Activity = activity_code or ''
      - Debit = transaction expense (positive)
      - Credit = refunds (positive)
    
    A final line is added that debits the credit card payable account (26100)
    and credits the bank account (NT) for the total statement amount.
    """
    if not transactions:
        print("No credit-card transactions provided for transformation.")
        return pd.DataFrame()

    print(f"--- Transforming {len(transactions)} credit-card transactions ---")

    journal_lines = []
    audit_rows = []
    bc_cfg = cfg.get('business_central', {})

    # Optional payment date override configured in config.toml
    payment_date_field = bc_cfg.get('payment_date_field')

    for index, t in enumerate(transactions):
        # Amount (assume already in major units). Handle negative amounts as refunds.
        raw_amount = t.get('amount', 0)
        try:
            amount_major_units = float(raw_amount)
        except Exception:
            amount_major_units = 0.0

        is_refund = amount_major_units < 0
        amt = abs(amount_major_units)

        # Determine the primary transaction/posting date (Date column)
        date_field = t.get('posted_at') or t.get('user_transaction_time') or t.get('created_at')
        if date_field:
            try:
                date_val = datetime.strptime(str(date_field)[:10], '%Y-%m-%d')
            except Exception:
                # fallback to now
                date_val = datetime.now()
        else:
            date_val = datetime.now()

        date_str = date_val.strftime('%m/%d/%Y')

        # Posting Date column: prefer configured payment date field, else use a payment_date key, else use date_field
        posting_raw = None
        if payment_date_field:
            posting_raw = t.get(payment_date_field)
        posting_raw = posting_raw or t.get('payment_date') or t.get('settled_at') or date_field
        try:
            posting_dt = datetime.strptime(str(posting_raw)[:10], '%Y-%m-%d')
        except Exception:
            posting_dt = date_val
        posting_date_str = posting_dt.strftime('%m/%d/%Y')

        merchant = t.get('merchant_name') or t.get('merchant', {}).get('name') or ''
        description = t.get('description') or t.get('memo') or merchant or ''

        # Extract GL account and dimensions from the first line item if present
        trans_gl_account = None
        department_code = ''
        activity_code = ''
        line_items = t.get('line_items', [])
        if line_items and line_items[0].get('accounting_field_selections'):
            for selection in line_items[0]['accounting_field_selections']:
                # Two styles observed in other helpers: type == 'GL_ACCOUNT' or category_info.type == 'GL_ACCOUNT'
                if selection.get('type') == 'GL_ACCOUNT' or selection.get('category_info', {}).get('type') == 'GL_ACCOUNT':
                    trans_gl_account = str(selection.get('external_code', '')).strip()
                elif selection.get('type') == 'OTHER' or selection.get('category_info', {}).get('type') == 'OTHER':
                    external_id = selection.get('category_info', {}).get('external_id')
                    if external_id == 'Department':
                        department_code = str(selection.get('external_code', '')).strip()
                    elif external_id == 'Activity Code':
                        activity_code = str(selection.get('external_code', '')).strip()

        if not trans_gl_account or trans_gl_account in ('None', 'null', ''):
            print(f"⚠️ Warning: Transaction {t.get('id', index)} is missing a G/L Account code. Skipping.")
            continue

        # Debit for normal expenses, Credit for refunds (both positive numbers)
        if is_refund:
            gl_debit = 0.0
            gl_credit = round(amt, 2)
        else:
            gl_debit = round(amt, 2)
            gl_credit = 0.0

        # Build BC-style journal line per requested headers
        doc_no = f"RAMP-{t.get('id', index)}"
        # Description format: "{merchant_name} | {memo}" (omit pipe if memo empty)
        memo = t.get('memo') or ''
        if merchant and memo:
            description = f"{merchant} | {memo}"
        elif merchant:
            description = merchant
        else:
            description = memo or ''

        journal_lines.append({
            'Journal Template Name': bc_cfg.get('template_name', 'GENERAL'),
            'Journal Batch Name': bc_cfg.get('batch_name', 'ACCOUNTANT'),
            'Posting Date': posting_date_str,
            'Document Date': posting_date_str,
            'Document Type': 'Payment',
            'Document No.': doc_no,
            'Account Type': 'G/L Account',
            'Account No.': str(trans_gl_account),
            'Description': description,
            'Debit Amount': gl_debit,
            'Credit Amount': gl_credit,
            'Bal. Account Type': 'G/L Account',
            'Bal. Account No.': str(bc_cfg.get('ramp_card_account', '26100')),
            'Department Code': str(department_code or ''),
            'Activity Code': str(activity_code or ''),
        })

        audit_rows.append({
            'ramp_id': t.get('id'),
            'doc_no': doc_no,
            'date': date_str,
            'posting_date': posting_date_str,
            'merchant': merchant,
            'amount': amount_major_units,
            'account': trans_gl_account,
            'department': department_code or '',
            'activity': activity_code or '',
        })

    # Add final payment line: Debit 26100 (CC Payable), Credit NT (Bank)
    if journal_lines and statement:
        # Calculate total amount from transactions
        total_amount = sum(line['Debit Amount'] - line['Credit Amount'] for line in journal_lines)
        
        # Get statement date range for description
        start_date = (statement.get('start_date') or '')[:10]
        end_date = (statement.get('end_date') or '')[:10]
        payment_desc = f"CC Statement Payment {start_date} to {end_date}"
        
        # Use statement end date as posting date
        try:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            payment_posting_date = end_dt.strftime('%m/%d/%Y')
        except Exception:
            payment_posting_date = datetime.now().strftime('%m/%d/%Y')
        
        # Statement ID for document number
        stmt_doc_no = f"RAMP-STMT-{statement.get('id', 'PAYMENT')}"
        
        # Add payment line
        journal_lines.append({
            'Journal Template Name': bc_cfg.get('template_name', 'GENERAL'),
            'Journal Batch Name': bc_cfg.get('batch_name', 'ACCOUNTANT'),
            'Posting Date': payment_posting_date,
            'Document Date': payment_posting_date,
            'Document Type': 'Payment',
            'Document No.': stmt_doc_no,
            'Account Type': 'G/L Account',
            'Account No.': str(bc_cfg.get('ramp_card_account', '26100')),
            'Description': payment_desc,
            'Debit Amount': round(total_amount, 2),
            'Credit Amount': 0.0,
            'Bal. Account Type': 'G/L Account',
            'Bal. Account No.': str(bc_cfg.get('bank_account', 'NT')),
            'Department Code': '000',
            'Activity Code': '00',
        })
    
    df_output = pd.DataFrame(journal_lines)
    if df_output.empty:
        print("No valid credit-card transactions found. Returning empty DataFrame.")
        return pd.DataFrame(columns=BC_COLUMN_ORDER)

    # Ensure exports directory exists
    exports_dir = cfg.get('exports_path', 'exports') if isinstance(cfg, dict) else 'exports'
    os.makedirs(exports_dir, exist_ok=True)

    # Optionally write an audit CSV for traceability
    if write_audit:
        ts = datetime.now().strftime('%Y%m%dT%H%M%S')
        audit_path = os.path.join(exports_dir, f'cc_export_audit_{ts}.csv')
        try:
            pd.DataFrame(audit_rows).to_csv(audit_path, index=False)
            print(f"Wrote audit CSV with exported Ramp IDs to: {audit_path}")
        except Exception as e:
            print(f"Failed to write audit CSV: {e}")

    # Cast department/activity/account no. to strings and ensure column order matches BC template
    df_output = df_output.astype({
        'Account No.': str,
        'Bal. Account No.': str,
        'Department Code': str,
        'Activity Code': str
    })

    return df_output[BC_COLUMN_ORDER]


def ramp_bills_to_purchase_invoice_lines(bills: List[Dict[str, Any]], cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert Ramp bills into a flat, one-row-per-bill-line CSV suitable for
    importing into Business Central Purchase Invoices via CSV.

    Columns produced (minimum set based on example):
      - No. (left blank so BC assigns)
      - Buy-from Vendor No. (Ramp vendor external id)
      - Buy-from Vendor Name
      - Vendor Invoice No. (vendor invoice number from Ramp)
      - Location Code (from config or blank)
      - Assigned User ID (optional from bill)
      - Line Description
      - Account No. (G/L account for the line)
      - Department (maps to Shortcut Dimension 1 Code)
      - Activity (maps to Shortcut Dimension 2 Code)
      - Amount (line amount in major units)
      - VAT Code (default from config)

    Returns a DataFrame with the columns in a conservative order suitable for import.
    """
    if not bills:
        return pd.DataFrame()

    rows = []
    bc_cfg = cfg.get('business_central', {}) if isinstance(cfg, dict) else {}
    default_vat = bc_cfg.get('default_vat_code', '')
    location_code = str(bc_cfg.get('location_code', ''))
    # Vendor lookup field for BC 'Buy-from Vendor No.' (configurable; default to 'external_id')
    # We prefer any previously-resolved vendor external id attached by enrichment
    vendor_lookup_field = bc_cfg.get('vendor_lookup_field', 'external_id')

    for bill in bills:
        vendor = bill.get('vendor') or {}
        # Use resolved external id first (from enrichment), then configured lookup field,
        # then common vendor fields as fallbacks.
        buy_from_no = ''
        if vendor:
            buy_from_no = (
                vendor.get('external_vendor_id_resolved') or
                vendor.get(vendor_lookup_field) or
                vendor.get('external_vendor_id') or
                vendor.get('external_id') or
                vendor.get('remote_code') or
                vendor.get('id') or
                ''
            )
        buy_from_name = vendor.get('name') or ''
        # Vendor invoice no: try common fields
        vendor_invoice_no = bill.get('vendor_invoice_number') or bill.get('invoice_number') or bill.get('document_number') or bill.get('id')

        # Date extraction: for Purchase Invoice, use bill_date (invoice date) for both posting and document dates
        # payment_date is a direct field on the bill object per Ramp API docs
        paid_date = bill.get('payment_date') or bill.get('paid_at') or bill.get('settled_at')
        bill_date = bill.get('bill_date') or bill.get('issued_at') or bill.get('created_at')
        
        # Purchase Invoice: posting date = document date = invoice date (bill_date)
        # Format dates for Business Central (MM/DD/YYYY)
        try:
            if bill_date:
                posting_date = datetime.fromisoformat(bill_date[:19]).strftime('%m/%d/%Y')
            else:
                posting_date = ''
        except Exception:
            try:
                if bill_date:
                    posting_date = datetime.strptime(bill_date[:10], '%Y-%m-%d').strftime('%m/%d/%Y')
                else:
                    posting_date = ''
            except Exception:
                posting_date = ''
        
        # Document date same as posting date for purchase invoices
        document_date = posting_date

        line_items = bill.get('line_items', [])
        if not line_items:
            # Fallback to whole-bill row
            amount_obj = bill.get('amount', {})
            if isinstance(amount_obj, dict):
                amt = amount_obj.get('amount', 0) / amount_obj.get('minor_unit_conversion_rate', 100)
            else:
                try:
                    amt = float(amount_obj)
                except Exception:
                    amt = 0.0

            rows.append({
                'No.': '',
                'Buy-from Vendor No.': str(buy_from_no),
                'Buy-from Vendor Name': buy_from_name,
                'Vendor Invoice No.': vendor_invoice_no,
                'Posting Date': posting_date,
                'Document Date': document_date,
                'Location Code': location_code,
                'Assigned User ID': str(bill.get('assigned_user_id', '')),
                'Line Description': bill.get('memo', ''),
                'Account No.': str(bc_cfg.get('vendor_payable_account', '')),
                'Department': '',
                'Activity': '',
                'Amount': round(amt, 2),
                'VAT Code': default_vat,
            })
            continue

        for li in line_items:
            # Determine line amount
            amount_obj = li.get('amount') or li.get('total') or li.get('value') or {}
            if isinstance(amount_obj, dict):
                minor_amount = amount_obj.get('amount', 0)
                conv = amount_obj.get('minor_unit_conversion_rate', 100)
                amount = float(minor_amount) / float(conv) if conv else float(minor_amount)
            else:
                try:
                    amount = float(amount_obj)
                except Exception:
                    # Try top-level bill amount fallback
                    bill_amount = bill.get('amount', {})
                    if isinstance(bill_amount, dict):
                        amount = float(bill_amount.get('amount', 0)) / float(bill_amount.get('minor_unit_conversion_rate', 100))
                    else:
                        try:
                            amount = float(bill_amount)
                        except Exception:
                            amount = 0.0

            # Extract accounting selections
            gl_account = ''
            department_code = ''
            activity_code = ''
            accounting_fields = li.get('accounting_field_selections') or []
            for selection in accounting_fields:
                if selection.get('type') == 'GL_ACCOUNT' or selection.get('category_info', {}).get('type') == 'GL_ACCOUNT':
                    gl_account = str(selection.get('external_code', '')).strip()
                elif selection.get('type') == 'OTHER' or selection.get('category_info', {}).get('type') == 'OTHER':
                    external_id = selection.get('category_info', {}).get('external_id')
                    if external_id == 'Department':
                        department_code = str(selection.get('external_code', '')).strip()
                    elif external_id == 'Activity Code':
                        activity_code = str(selection.get('external_code', '')).strip()

            rows.append({
                'No.': '',
                'Buy-from Vendor No.': str(buy_from_no),
                'Buy-from Vendor Name': buy_from_name,
                'Vendor Invoice No.': vendor_invoice_no,
                'Posting Date': posting_date,
                'Document Date': document_date,
                'Location Code': location_code,
                'Assigned User ID': str(bill.get('assigned_user_id', '')),
                'Line Description': li.get('memo') or li.get('description') or bill.get('memo', ''),
                'Account No.': str(gl_account or ''),
                'Department': str(department_code or ''),
                'Activity': str(activity_code or ''),
                'Amount': round(amount, 2),
                'VAT Code': default_vat,
            })

    df = pd.DataFrame(rows)
    # Ensure columns order similar to example + extras
    cols = ['No.', 'Buy-from Vendor No.', 'Buy-from Vendor Name', 'Vendor Invoice No.', 'Posting Date', 'Document Date', 'Location Code', 'Assigned User ID', 'Line Description', 'Account No.', 'Department', 'Activity', 'Amount', 'VAT Code']
    return df[cols]


def ramp_bills_to_general_journal(bills: List[Dict[str, Any]], cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Create a General Journal-format DataFrame for bills that posts expense debits
    per bill-line and a single credit to Vendor Payable per bill.

    Columns follow the example 'General Journals_accountant_format.csv' and include:
      Batch Name,Document No.,Approval Status,Posting Date,Description,Account Type,Account No.,Account Name,Department Code,Activity Code,Debit Amount,Credit Amount,...
    """
    if not bills:
        return pd.DataFrame()

    bc_cfg = cfg.get('business_central', {}) if isinstance(cfg, dict) else {}
    batch_name = bc_cfg.get('batch_name', 'CONTROLLER')
    # Default vendor payable account should be 26000 unless overridden in config
    vendor_payable = str(bc_cfg.get('vendor_payable_account', '26000'))
    # Optional exact GL -> payable mapping (preferred): e.g. {'54010': '26010'}
    gl_to_payable_map = bc_cfg.get('gl_to_payable_map', {'54010': '26010'})
    # Normalize mapping keys/values to strings for reliable lookup
    try:
        gl_to_payable_map = {str(k): str(v) for k, v in (gl_to_payable_map or {}).items()}
    except Exception:
        gl_to_payable_map = {}
    # Optional prefix mapping as a fallback: e.g. {'54': '26'} will map 54xxx -> 26xxx
    payable_prefix_map = bc_cfg.get('payable_prefix_map', {}) or {}
    try:
        payable_prefix_map = {str(k): str(v) for k, v in payable_prefix_map.items()}
    except Exception:
        payable_prefix_map = {}
    bank_account = str(bc_cfg.get('bank_account', 'NT'))
    bank_account_name = str(bc_cfg.get('bank_account_name', 'Northern Trust DDA'))

    rows = []
    # Track payable accounts encountered per-bill and debit GLs so balancing line
    # can use a mapped payable when available (prefer mapping from expense GL -> payable)
    encountered_payables = []
    encountered_debit_gls = []
    for bill in bills:
        # reset encountered payables and debit GLs for this bill
        encountered_payables = []
        encountered_debit_gls = []
        
        # Date extraction:
        # - bill_date: invoice date (for Expense→A/P entry)
        # - paid_date: scheduled payment date or paid date (for A/P→Bank entry)
        # payment_date is a direct field on the bill object per Ramp API docs
        paid_date = bill.get('payment_date') or bill.get('paid_at') or bill.get('settled_at')
        bill_date = bill.get('bill_date') or bill.get('issued_at') or bill.get('created_at')
        
        # Invoice entry: posting date = document date = invoice date
        # Business Central expects MM/DD/YYYY format
        try:
            if bill_date:
                invoice_posting_date = datetime.fromisoformat(bill_date[:19]).strftime('%m/%d/%Y')
            else:
                invoice_posting_date = datetime.now().strftime('%m/%d/%Y')
        except Exception:
            try:
                if bill_date:
                    invoice_posting_date = datetime.strptime(bill_date[:10], '%Y-%m-%d').strftime('%m/%d/%Y')
                else:
                    invoice_posting_date = datetime.now().strftime('%m/%d/%Y')
            except Exception:
                invoice_posting_date = datetime.now().strftime('%m/%d/%Y')
        
        # Payment entry: posting date = scheduled/paid date
        try:
            if paid_date:
                payment_posting_date = datetime.fromisoformat(paid_date[:19]).strftime('%m/%d/%Y')
            elif bill_date:
                # Fallback to bill_date if no payment date
                payment_posting_date = invoice_posting_date
            else:
                payment_posting_date = datetime.now().strftime('%m/%d/%Y')
        except Exception:
            try:
                date_str = paid_date or bill_date
                if date_str:
                    payment_posting_date = datetime.strptime(date_str[:10], '%Y-%m-%d').strftime('%m/%d/%Y')
                else:
                    payment_posting_date = datetime.now().strftime('%m/%d/%Y')
            except Exception:
                payment_posting_date = datetime.now().strftime('%m/%d/%Y')
        
        vendor_invoice_no = bill.get('vendor_invoice_number') or bill.get('invoice_number') or bill.get('document_number') or bill.get('id')
        description = bill.get('memo') or f"Bill {vendor_invoice_no}"

        # Sum per-bill amounts and track debits/credits
        total_amount = 0.0
        total_debits = 0.0
        total_credits = 0.0
        line_items = bill.get('line_items', [])
        for li in line_items:
            amount_obj = li.get('amount') or li.get('total') or {}
            if isinstance(amount_obj, dict):
                amt = amount_obj.get('amount', 0) / amount_obj.get('minor_unit_conversion_rate', 100)
            else:
                try:
                    amt = float(amount_obj)
                except Exception:
                    amt = 0.0

            # Extract GL account and dimensions
            gl_account = ''
            department_code = ''
            activity_code = ''
            accounting_fields = li.get('accounting_field_selections') or []
            for selection in accounting_fields:
                if selection.get('type') == 'GL_ACCOUNT' or selection.get('category_info', {}).get('type') == 'GL_ACCOUNT':
                    gl_account = str(selection.get('external_code', '')).strip()
                elif selection.get('type') == 'OTHER' or selection.get('category_info', {}).get('type') == 'OTHER':
                    external_id = selection.get('category_info', {}).get('external_id')
                    if external_id == 'Department':
                        department_code = str(selection.get('external_code', '')).strip()
                    elif external_id == 'Activity Code':
                        activity_code = str(selection.get('external_code', '')).strip()

            total_amount += amt
            # If the line's GL account is the vendor payable account, treat as a credit
            payable_acct = vendor_payable
            # Support explicit exact GL -> payable mapping first
            if gl_account and str(gl_account) in gl_to_payable_map:
                payable_acct = gl_to_payable_map.get(str(gl_account))
            else:
                # Fallback: support derived payable accounts based on prefix mapping
                if gl_account:
                    for pfx, tgt in payable_prefix_map.items():
                        if str(gl_account).startswith(str(pfx)):
                            # keep trailing digits
                            suffix = str(gl_account)[len(pfx):]
                            derived = f"{tgt}{suffix}"
                            payable_acct = derived
                            break
            # record payable encountered for this bill
            if payable_acct not in encountered_payables:
                encountered_payables.append(str(payable_acct))

            # If this line is a debit (i.e., not a payable line), record the GL for potential mapping
            if not (gl_account and str(gl_account).strip() == str(payable_acct).strip()):
                # treat as debit GL
                if gl_account:
                    encountered_debit_gls.append(str(gl_account))
            if gl_account and str(gl_account).strip() == str(payable_acct).strip():
                # Credit to payable
                rows.append({
                    'Batch Name': batch_name,
                    'Document No.': vendor_invoice_no,
                    'Approval Status': '',
                    'Posting Date': invoice_posting_date,
                    'Document Date': invoice_posting_date,
                    'Description': li.get('memo') or description,
                    'Account Type': 'G/L Account',
                    'Account No.': str(gl_account),
                    'Account Name': '',
                    # For payable (liability) accounts, set canonical dimension defaults
                    'Department Code': '000',
                    'Activity Code': '00',
                    'Debit Amount': 0.0,
                    'Credit Amount': round(amt, 2),
                })
                total_credits += amt
            else:
                # Debit expense line
                # Debit expense line
                # If GL missing, fallback to configured vendor_payable (intent: suspend to payable)
                fallback_account = str(bc_cfg.get('vendor_payable_account', vendor_payable))
                rows.append({
                    'Batch Name': batch_name,
                    'Document No.': vendor_invoice_no,
                    'Approval Status': '',
                    'Posting Date': invoice_posting_date,
                    'Document Date': invoice_posting_date,
                    'Description': li.get('memo') or description,
                    'Account Type': 'G/L Account',
                    'Account No.': str(gl_account or fallback_account),
                    'Account Name': '',
                    'Department Code': str(department_code or ''),
                    'Activity Code': str(activity_code or ''),
                    'Debit Amount': round(amt, 2),
                    'Credit Amount': 0.0,
                })
                total_debits += amt

        # Two-step journal entries for paid bills:
        # Entry 1: Record the invoice (Expense → A/P)
        # Entry 2: Record the payment (A/P → Bank)
        
        # Determine which payable account to use for this bill
        payable_for_balance = vendor_payable
        for debit_gl in encountered_debit_gls:
            mapped = gl_to_payable_map.get(str(debit_gl))
            if mapped:
                payable_for_balance = str(mapped)
                break
        else:
            # fallback: prefer any non-default payable encountered
            for p in encountered_payables:
                if p and p != vendor_payable:
                    payable_for_balance = p
                    break
        
        # Calculate the total amount for balancing entries
        imbalance = round(total_debits - total_credits, 2)
        if abs(imbalance) > 0.0001:
            # Entry 1: Invoice - Credit A/P to balance the expense debits (use invoice date)
            rows.append({
                'Batch Name': batch_name,
                'Document No.': vendor_invoice_no,
                'Approval Status': '',
                'Posting Date': invoice_posting_date,
                'Document Date': invoice_posting_date,
                'Description': f"Invoice {vendor_invoice_no}",
                'Account Type': 'G/L Account',
                'Account No.': str(payable_for_balance),
                'Account Name': '',
                'Department Code': '000',
                'Activity Code': '00',
                'Debit Amount': 0.0,
                'Credit Amount': round(imbalance, 2),
            })
            
            # Entry 2: Payment - Debit A/P and Credit Bank (use scheduled/paid date)
            rows.append({
                'Batch Name': batch_name,
                'Document No.': vendor_invoice_no,
                'Approval Status': '',
                'Posting Date': payment_posting_date,
                'Document Date': payment_posting_date,
                'Description': f"Payment for Invoice {vendor_invoice_no}",
                'Account Type': 'G/L Account',
                'Account No.': str(payable_for_balance),
                'Account Name': '',
                'Department Code': '000',
                'Activity Code': '00',
                'Debit Amount': round(imbalance, 2),
                'Credit Amount': 0.0,
            })
            
            rows.append({
                'Batch Name': batch_name,
                'Document No.': vendor_invoice_no,
                'Approval Status': '',
                'Posting Date': payment_posting_date,
                'Document Date': payment_posting_date,
                'Description': f"Payment for Invoice {vendor_invoice_no}",
                'Account Type': 'G/L Account',
                'Account No.': str(bank_account),
                'Account Name': bank_account_name,
                'Department Code': '000',
                'Activity Code': '00',
                'Debit Amount': 0.0,
                'Credit Amount': round(imbalance, 2),
            })

    # Build DataFrame from accumulated rows
    df = pd.DataFrame(rows)

    # Add extra columns from provided template, fill with sensible defaults or totals
    extra_cols = [
        'Sustainability Account No.', 'Total Emission CO2', 'Total Emission CH4', 'Total Emission N2O',
        'IRS 1099 Reporting Period', 'IRS 1099 Form No.', 'IRS 1099 Form Box No.',
        'NumberOfJournalRecords', 'Total Debit', 'Total Credit', 'Balance', 'Total Balance'
    ]

    for c in extra_cols:
        df[c] = ''

    # Compute simple aggregates
    number_of_records = len(df)
    total_balance = df['Debit Amount'].fillna(0).sum() - df['Credit Amount'].fillna(0).sum()

    # Populate aggregate and per-row summary fields
    df['NumberOfJournalRecords'] = number_of_records
    # Per-row Total Debit/Credit should reflect each row's amounts (only one populated)
    df['Total Debit'] = df['Debit Amount'].fillna(0).apply(lambda x: round(float(x), 2) if x and float(x) != 0.0 else 0.00)
    df['Total Credit'] = df['Credit Amount'].fillna(0).apply(lambda x: round(float(x), 2) if x and float(x) != 0.0 else 0.00)
    # Per-row Balance = Debit - Credit
    df['Balance'] = (df['Debit Amount'] - df['Credit Amount']).round(2)
    # Keep Total Balance as the overall batch balance
    df['Total Balance'] = round(total_balance, 2)

    # Ensure consistent ordering to match your CSV template
    cols = [
        'Batch Name','Document No.','Approval Status','Posting Date','Document Date','Description','Account Type','Account No.','Account Name',
        'Department Code','Activity Code','Debit Amount','Credit Amount',
        'Sustainability Account No.','Total Emission CO2','Total Emission CH4','Total Emission N2O',
        'IRS 1099 Reporting Period','IRS 1099 Form No.','IRS 1099 Form Box No.',
        'NumberOfJournalRecords','Total Debit','Total Credit','Balance','Total Balance'
    ]

    # Ensure all columns exist
    for c in cols:
        if c not in df.columns:
            df[c] = ''

    # Reorder
    return df[cols]