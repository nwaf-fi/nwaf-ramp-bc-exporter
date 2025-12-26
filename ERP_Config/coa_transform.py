"""coa_transform.py

Validate and transform a local ERP Chart of Accounts CSV into a Ramp-compatible
account payload JSON suitable for use when creating an API-based accounting
connection.

Usage:
    python ERP_Config/coa_transform.py \
        --input ERP_Config/chartOfAccounts.csv \
        --out-json ERP_Config/chartOfAccounts_ramp_payload.json \
        --out-csv ERP_Config/chartOfAccounts_validated.csv

The script performs basic validation and normalization and prints a short
report. It exits with code 0 when no validation "errors" are found, and
non-zero when errors require manual corrections.
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# Mapping from CSV category values to Ramp account_type values
ACCOUNT_TYPE_MAP = {
    'Assets': 'Asset',
    'Liabilities': 'Liability',
    'Equity': 'Equity',
    'Income': 'Revenue',
    'Expense': 'Expense'
}

REQUIRED_COLUMNS = ['number', 'displayName', 'category']


def normalize_row(row: pd.Series, default_currency: str = 'USD', allow_posting: bool = True) -> Dict:
    """Return transformed account dict for Ramp payload."""
    acct_num = str(row['number']).strip()
    acct_name = str(row['displayName']).strip()
    cat = str(row.get('category', '')).strip()
    sub = str(row.get('subCategory', '')).strip() if row.get('subCategory') is not None else ''

    account_type = ACCOUNT_TYPE_MAP.get(cat, None)

    return {
        'account_number': acct_num,
        'account_name': acct_name,
        'account_type': account_type,
        'sub_type': sub or None,
        'currency': default_currency,
        'allow_posting': bool(allow_posting),
        'external_id': acct_num
    }


def validate_df(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    # Columns
    for c in REQUIRED_COLUMNS:
        if c not in df.columns:
            errors.append(f"Missing required column: {c}")

    if errors:
        return errors, warnings

    # Check for empty or duplicate account numbers
    if df['number'].isnull().any() or (df['number'].astype(str).str.strip() == '').any():
        errors.append('One or more account numbers are missing or empty.')

    dupes = df['number'].astype(str).str.strip().duplicated(keep=False)
    if dupes.any():
        duplicates = df.loc[dupes, 'number'].astype(str).tolist()
        errors.append(f'Duplicate account numbers found: {sorted(set(duplicates))}')

    # Check account names
    if df['displayName'].isnull().any() or (df['displayName'].astype(str).str.strip() == '').any():
        errors.append('One or more accounts are missing displayName (account_name).')

    # Category mapping
    unknown_cats = sorted(set(df['category'].astype(str).unique()) - set(ACCOUNT_TYPE_MAP.keys()))
    if unknown_cats:
        warnings.append(f'Unknown categories found and will need review or mapping: {unknown_cats}')

    return errors, warnings


def transform(df: pd.DataFrame, default_currency: str = 'USD', allow_posting: bool = True) -> Dict:
    accounts = []
    for _, row in df.iterrows():
        acct = normalize_row(row, default_currency=default_currency, allow_posting=allow_posting)
        # If account_type couldn't be mapped, set to None and leave for review
        accounts.append(acct)
    return {'accounts': accounts}


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description='Validate and transform COA CSV to Ramp payload JSON')
    p.add_argument('--input', '-i', default='ERP_Config/chartOfAccounts.csv')
    p.add_argument('--out-json', '-j', default='ERP_Config/chartOfAccounts_ramp_payload.json')
    p.add_argument('--out-csv', '-c', default='ERP_Config/chartOfAccounts_validated.csv')
    p.add_argument('--currency', default='USD')
    p.add_argument('--no-posting', action='store_true', help='Mark allow_posting=False for all accounts')

    args = p.parse_args(argv)

    inp = Path(args.input)
    out_json = Path(args.out_json)
    out_csv = Path(args.out_csv)

    if not inp.exists():
        print(f"ERROR: Input file not found: {inp}")
        return 2

    df = pd.read_csv(inp, dtype=str).fillna('')

    errors, warnings = validate_df(df)

    print('COA Validation Report')
    print('---------------------')
    if errors:
        print('Errors:')
        for e in errors:
            print(' -', e)
    else:
        print('No validation errors found.')

    if warnings:
        print('Warnings:')
        for w in warnings:
            print(' -', w)
    else:
        print('No warnings.')

    # Transform regardless; upstream can reject accounts with missing mapping
    payload = transform(df, default_currency=args.currency, allow_posting=not args.no_posting)

    # Write outputs
    try:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        with open(out_json, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        print(f'Wrote ramp payload JSON to: {out_json}')

        # Write a validated CSV (normalized fields)
        normalized_rows = []
        for acct in payload['accounts']:
            normalized_rows.append({
                'account_number': acct['account_number'],
                'account_name': acct['account_name'],
                'account_type': acct['account_type'] or '',
                'sub_type': acct.get('sub_type') or '',
                'currency': acct['currency'],
                'allow_posting': acct['allow_posting'],
                'external_id': acct['external_id']
            })
        ndf = pd.DataFrame(normalized_rows)
        ndf.to_csv(out_csv, index=False)
        print(f'Wrote normalized CSV to: {out_csv}')
    except Exception as ex:
        print('ERROR writing outputs:', ex)
        return 3

    # Exit non-zero when errors exist to force human review
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
