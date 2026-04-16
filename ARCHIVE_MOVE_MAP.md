# Archive Move Map

This file lists files moved into `archive/legacy_scripts/` and their original paths.

- `merged_cc_statement_export.py`  -> `archive/legacy_scripts/merged_cc_statement_export.py` (shim left at original path)
- `fetch_ramp_data.py`            -> `archive/legacy_scripts/fetch_ramp_data.py` (shim left at original path)
- `inspect_statements.py`         -> `archive/legacy_scripts/inspect_statements.py` (shim left at original path)
- `check_jan_bills.py`           -> `archive/legacy_scripts/check_jan_bills.py` (shim left at original path)
- `diagnose_transactions_filters.py` -> `archive/legacy_scripts/diagnose_transactions_filters.py` (shim left at original path)
- `ui/invoices_backup.py`        -> `archive/legacy_scripts/ui_invoices_backup.py` (shim left at original path)

Notes:
- Each original file now contains a small shim that emits a DeprecationWarning and re-exports from the archive location to remain non-breaking.
- These archived copies are intended to be read-only history; further maintenance should be done in `app/`/`lib/` implementations.
