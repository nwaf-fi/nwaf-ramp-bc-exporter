
import requests
import json
import uuid
from typing import Dict, List, Optional
from urllib.parse import urljoin
from datetime import date, datetime, timezone


def _date_to_iso(d) -> str:
    """Convert a date or datetime to ISO 8601 UTC string for Ramp API filters."""
    if isinstance(d, datetime):
        return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    if isinstance(d, date):
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    return str(d)

class RampClient:
    def _build_endpoint(self, path: str) -> str:
        """
        Helper to construct full API endpoint, handling '/developer/v1' duplication logic.
        """
        base = self.base_url.rstrip('/')
        if 'developer/v1' in base:
            endpoint = urljoin(base + '/', path.lstrip('/'))
        else:
            endpoint = urljoin(base + '/', 'developer/v1/' + path.lstrip('/'))
        return endpoint
    def __init__(self, base_url: str, token_url: str, client_id: str, client_secret: str, enable_sync: bool = False):
        # Normalize base_url to avoid duplicated segments like '/developer/v1/developer/v1'
        b = base_url.rstrip('/')
        if '/developer/v1' in b:
            first = b.find('/developer/v1')
            # Keep only up to the first '/developer/v1' occurrence
            b = b[: first + len('/developer/v1')]
        self.base_url = b
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = requests.Session()
        self._token = None
        # When enable_sync is True, mark_transaction_synced will perform a POST against
        # Ramp's sync endpoint. Default is False to avoid accidental writes.
        self.enable_sync = enable_sync
        self.granted_scopes = None


    def authenticate(self):
        # Request all possible scopes - OAuth will grant only the ones allowed for this client
        all_scopes = "transactions:read bills:read reimbursements:read cashbacks:read statements:read accounting:read accounting:write transfers:read vendors:read"
        tried = []
        # If a specific token_url was provided, try it first; otherwise derive from base_url
        candidates = [self.token_url]
        # Add common alternate token endpoints on same host if token_url is provided
        try:
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(self.token_url)
            if parsed.scheme and parsed.netloc:
                base_auth = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
                # common token paths
                for p in ("/oauth/token", "/oauth2/token", "/oauth2/v2/token"):
                    candidate = base_auth + p
                    if candidate not in candidates:
                        candidates.append(candidate)
        except Exception:
            pass

        last_exc = None
        for url in candidates:
            tried.append(url)
            try:
                resp = self.session.post(
                    url,
                    data={"grant_type": "client_credentials", "scope": all_scopes},
                    auth=(self.client_id, self.client_secret),
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                self._token = data.get("access_token")
                self.granted_scopes = data.get("scope", "")
                print(f"🔑 OAuth token granted from {url} with scopes: {self.granted_scopes}")
                self.session.headers.update({"Authorization": f"Bearer {self._token}"})
                return self._token
            except Exception as ex:
                last_exc = ex
                # Log and try next candidate
                print(f"Auth attempt to {url} failed: {ex}")

        # If we get here, all attempts failed
        print("Failed to obtain OAuth token. Tried the following token endpoints:")
        for t in tried:
            print(f" - {t}")
        # Re-raise the last exception to let callers handle it if desired
        if last_exc:
            raise last_exc
        raise RuntimeError("Unable to obtain OAuth token; no token endpoint succeeded")

    def get_transactions(self, status: Optional[str] = None,
                        start_date: Optional[str] = None, end_date: Optional[str] = None,
                        page_size: int = 200, **extra_params) -> List[Dict]:
        """Fetch transactions from Ramp API. Accepts extra query parameters such as
        `has_no_sync_commits=True` to only return transactions that have not been synced."""
        return self._get_paginated_data("transactions", status, start_date, end_date, page_size, **extra_params)


    def get_bills(self, status: Optional[str] = None,
                  from_issued_date: Optional[str] = None, to_issued_date: Optional[str] = None,
                  start_date: Optional[str] = None, end_date: Optional[str] = None,
                  page_size: int = 200, **extra_params) -> List[Dict]:
        """Fetch bills from Ramp API. Accepts extra query parameters such as
        `sync_ready=True` to only return bills ready to be synced to ERP.
        
        Date filtering:
        - from_issued_date/to_issued_date: Filter by invoice issue date (deprecated for payment reconciliation)
        - start_date/end_date: Filter by payment send date (preferred for bank reconciliation)
        """
        return self._get_paginated_data("bills", status=status, from_issued_date=from_issued_date, to_issued_date=to_issued_date, start_date=start_date, end_date=end_date, page_size=page_size, **extra_params)

    def get_all_bills(
        self,
        from_paid_at: Optional[str] = None,
        to_paid_at: Optional[str] = None,
        page_size: int = 100,
    ) -> list:
        """
        Fetch bills using server-side from_paid_at/to_paid_at filters and
        start-based pagination (page_size 2-100 per Ramp docs).
        paid_at is a top-level field on each bill object.
        """
        all_bills = []
        url = self._build_endpoint("bills")
        params = {"page_size": page_size}
        if from_paid_at:
            params["from_paid_at"] = from_paid_at
        if to_paid_at:
            params["to_paid_at"] = to_paid_at
        print(f"🔍 Fetching bills with params: {params}")

        page_num = 0
        next_url = None
        while True:
            page_num += 1
            if next_url:
                resp = self.session.get(next_url)
            else:
                resp = self.session.get(url, params=params)
            resp.raise_for_status()
            response = resp.json()

            data = response.get("data") or []
            all_bills.extend(data)
            print(f"📄 Page {page_num}: fetched {len(data)} items (total so far: {len(all_bills)})")

            page_info = response.get("page") or {}
            next_url = page_info.get("next")
            if next_url:
                print(f"🔄 Next URL found, fetching next page...")
            else:
                break

        print(f"✅ Retrieved {len(all_bills)} total bills across {page_num} page(s)")
        return all_bills

    def get_reimbursements(self, status: Optional[str] = None,
                          start_date: Optional[str] = None, end_date: Optional[str] = None,
                          page_size: int = 200, **extra_params) -> List[Dict]:
        """Fetch reimbursements from Ramp API. Accepts extra query parameters such as
        `has_no_sync_commits=True` to only return reimbursements not yet synced."""
        return self._get_paginated_data("reimbursements", status, start_date, end_date, page_size, **extra_params)

    def get_cashbacks(self, status: Optional[str] = None,
                      start_date: Optional[str] = None, end_date: Optional[str] = None,
                      page_size: int = 200, **extra_params) -> List[Dict]:
        """Fetch cashbacks from Ramp API"""
        return self._get_paginated_data("cashbacks", status, start_date, end_date, page_size, **extra_params)

    def get_statements(self, status: Optional[str] = None,
                       start_date: Optional[str] = None, end_date: Optional[str] = None,
                       page_size: int = 200, **extra_params) -> List[Dict]:
        """Fetch statements from Ramp API"""
        return self._get_paginated_data("statements", status, start_date, end_date, page_size, **extra_params)

    def get_transfers(self, start_date: Optional[str] = None, end_date: Optional[str] = None,
                      page_size: int = 200) -> List[Dict]:
        """Fetch transfers from Ramp API (paginated).

        Returns a list of transfer objects as returned by the API. Supports
        optional `start_date` and `end_date` filters (YYYY-MM-DD).
        """
        return self._get_paginated_data("transfers", None, start_date, end_date, page_size)

    def get_vendor(self, vendor_id: str) -> Optional[Dict]:
        """
        Fetch a single Vendor record by vendor_id using the Vendor API:
        GET /developer/v1/vendors/{vendor_id}

        Returns the vendor JSON or None on error.
        """
        try:
            url = self._build_endpoint(f"vendors/{vendor_id}")
            resp = self.session.get(url)
            if resp.status_code == 200:
                return resp.json()
            else:
                # Non-200 -- return None but don't raise to keep dry-run robust
                print(f"Warning: vendor lookup returned status {resp.status_code} for vendor_id={vendor_id}")
                return None
        except Exception as ex:
            print(f"Exception fetching vendor {vendor_id}: {ex}")
            return None

    def get_vendors(self, page_size: int = 200) -> List[Dict]:
        """
        Fetch all Accounting Vendor records using the
        GET /developer/v1/accounting/vendors/ endpoint.

        Returns a list of vendor objects. Handles pagination by
        following `next` or `next_cursor` values returned by the API.
        Non-200 responses will be logged and result in an empty list.
        """
        try:
            url = self._build_endpoint("vendors/")
            params = {"limit": page_size}
            results: List[Dict] = []
            next_cursor = None
            while True:
                if next_cursor:
                    params["cursor"] = next_cursor
                resp = self.session.get(url, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("data") or []
                    results.extend(items)
                    next_cursor = data.get("next") or data.get("next_cursor")
                    if not next_cursor:
                        break
                else:
                    print(f"Warning: vendors list returned status {resp.status_code}")
                    break
            return results
        except Exception as ex:
            print(f"Exception fetching vendors: {ex}")
            return []

    def mark_transaction_synced(self, transaction_id: str, sync_reference: str = None) -> bool:
        """Convenience wrapper that returns only a boolean success flag.
        Delegates to `mark_transaction_synced_with_message` which provides diagnostics.
        """
        ok, _ = self.mark_transaction_synced_with_message(transaction_id, sync_reference)
        return ok

    def mark_transaction_synced_with_message(self, transaction_id: str, sync_reference: str = None):
        """
        Mark a transaction as synced to Business Central using the canonical
        `/developer/v1/accounting/syncs` endpoint and return a tuple: (ok: bool, message: str).

        The request payload uses the transactions-array shape and we log the endpoint,
        payload (truncated), and response (status + truncated body) to aid diagnostics.

        Per Ramp docs, only transactions with sync_status=SYNC_READY can be posted to
        /accounting/syncs. Transactions must be marked SYNC_READY by users in the Ramp UI
        before they can be synced via API. This method pre-checks a transaction's sync_status
        and skips (returns False) if the transaction is not SYNC_READY.
        """
        # Pre-check: verify the transaction is SYNC_READY before attempting to sync
        if getattr(self, 'enable_sync', False):
            try:
                txn_url = self._build_endpoint(f'transactions/{transaction_id}')
                check_resp = self.session.get(txn_url, timeout=10)
                if check_resp.status_code == 200:
                    txn_data = check_resp.json()
                    sync_status = txn_data.get('sync_status', '')
                    if sync_status != 'SYNC_READY':
                        msg = (f"Skipped: transaction {transaction_id} has sync_status='{sync_status}', "
                               f"not 'SYNC_READY'. Users must mark it ready in the Ramp UI before it can be synced.")
                        print(f"⚠️  {msg}")
                        return False, msg
                else:
                    print(f"⚠️  Could not pre-check sync_status for {transaction_id}: HTTP {check_resp.status_code}")
            except Exception as pre_ex:
                print(f"⚠️  sync_status pre-check failed for {transaction_id}: {pre_ex}")

        # Dry run behavior: avoid accidental writes
        if not getattr(self, 'enable_sync', False):
            msg = f"[DRY RUN] Would mark transaction {transaction_id} as synced (sync_reference: {sync_reference})"
            print(f"🔍 {msg}")
            return True, msg

        # For live syncs, delegate to the canonical batch `post_accounting_syncs` helper
        try:
            successful_syncs = [{'id': transaction_id}]
            if sync_reference:
                successful_syncs[0]['reference_id'] = sync_reference

            # Use dry_run=False because enable_sync is True here
            ok, info = self.post_accounting_syncs(successful_syncs=successful_syncs, failed_syncs=[], sync_type='TRANSACTION_SYNC', dry_run=False)

            # Determine endpoint consistently to avoid duplicate segments in messages
            base = self.base_url.rstrip('/')
            if 'developer/v1' in base:
                endpoint = urljoin(base + '/', 'accounting/syncs')
            else:
                endpoint = urljoin(base + '/', 'developer/v1/accounting/syncs')

            # Normalize returned info into a message string for callers
            if ok:
                sync_id = ''
                if isinstance(info, dict):
                    body = info.get('response', {})
                    if isinstance(body, dict):
                        sync_id = body.get('sync_id', '')
                    http_status = info.get('status', 201)
                    msg = f"{http_status} at {endpoint}" + (f" (sync_id: {sync_id})" if sync_id else '')
                else:
                    msg = str(info)
                print(f"✅ Sync success for transaction {transaction_id}: {msg}")
                return True, msg
            else:
                # info may be dict with status/response or error string
                if isinstance(info, dict):
                    msg = f"{info.get('status')} at {endpoint}: {str(info.get('response'))[:1000]}"
                else:
                    msg = str(info)
                print(f"❌ Sync failed for transaction {transaction_id}: {msg}")
                return False, msg
        except Exception as ex:
            print(f"❌ Sync exception for transaction {transaction_id}: {ex}")
            return False, str(ex)

    def mark_bill_synced(self, bill_id: str, sync_reference: str = None) -> bool:
        """Convenience wrapper to mark a single bill as synced. Returns boolean."""
        ok, _ = self.mark_bill_synced_with_message(bill_id, sync_reference)
        return ok

    def mark_bill_synced_with_message(self, bill_id: str, sync_reference: str = None):
        """
        Mark a bill as synced using the canonical `/developer/v1/accounting/syncs` endpoint.

        Returns (ok: bool, message: str). When `enable_sync` is False the method
        performs a dry-run and returns a preview message.
        """
        # Dry run behavior: avoid accidental writes
        if not getattr(self, 'enable_sync', False):
            msg = f"[DRY RUN] Would mark bill {bill_id} as synced (sync_reference: {sync_reference})"
            print(f"🔍 {msg}")
            return True, msg

        try:
            # Use batch helper for consistency with other sync types
            successful_syncs = [{'id': bill_id}]
            if sync_reference:
                successful_syncs[0]['reference_id'] = sync_reference

            ok, info = self.post_accounting_syncs(successful_syncs=successful_syncs, failed_syncs=[], sync_type='BILL_SYNC', dry_run=False)

            base = self.base_url.rstrip('/')
            if 'developer/v1' in base:
                endpoint = urljoin(base + '/', 'accounting/syncs')
            else:
                endpoint = urljoin(base + '/', 'developer/v1/accounting/syncs')

            if ok:
                sync_id = ''
                if isinstance(info, dict):
                    body = info.get('response', {})
                    if isinstance(body, dict):
                        sync_id = body.get('sync_id', '')
                    http_status = info.get('status', 201)
                    msg = f"{http_status} at {endpoint}" + (f" (sync_id: {sync_id})" if sync_id else '')
                else:
                    msg = str(info)
                print(f"✅ Sync success for bill {bill_id}: {msg}")
                return True, msg
            else:
                if isinstance(info, dict):
                    msg = f"{info.get('status')} at {endpoint}: {str(info.get('response'))[:1000]}"
                else:
                    msg = str(info)
                print(f"❌ Sync failed for bill {bill_id}: {msg}")
                return False, msg
        except Exception as ex:
            print(f"❌ Sync exception for bill {bill_id}: {ex}")
            return False, str(ex)

    def post_accounting_syncs(self, successful_syncs: list = None, failed_syncs: list = None, sync_type: str = 'TRANSACTION_SYNC', idempotency_key: str = None, dry_run: bool = True):
        """Post a syncs report to `POST /developer/v1/accounting/syncs`.

        Constructs a payload with required top-level fields per Ramp docs:
          - idempotency_key (string UUID, required)
          - sync_type (required): TRANSACTION_SYNC | BILL_SYNC | BILL_PAYMENT_SYNC |
                                  REIMBURSEMENT_SYNC | TRANSFER_SYNC | CASHBACK_SYNC
          - successful_syncs: list of {id: <txn_id>, reference_id: <erp_ref>}  (reference_id optional)
          - failed_syncs: list of {id: <txn_id>, error: {message: ...}}

        Only transactions with sync_status=SYNC_READY can be included in successful_syncs
        for TRANSACTION_SYNC — see Ramp accounting guide.

        Successful response is HTTP 201 with body: {"sync_id": "<uuid>"}.

        When `dry_run` is True the function will not POST and will instead return
        (True, {'endpoint': endpoint, 'payload_preview': payload_preview}).
        When `dry_run` is False, the function will POST and return (ok, info).
        """
        endpoint = self._build_endpoint('accounting/syncs')

        if successful_syncs is None:
            successful_syncs = []
        if failed_syncs is None:
            failed_syncs = []

        # Ensure we do not pass malformed entries
        def _normalize_success(s):
            obj = {'id': s.get('id') or s.get('transaction_id') or s.get('transactionId')}
            ref = s.get('reference_id') or s.get('referenceId')
            if ref:  # omit reference_id entirely when absent — do not send null
                obj['reference_id'] = ref
            return obj

        def _normalize_failed(f):
            return {'id': f.get('id') or f.get('transaction_id') or f.get('transactionId'), 'error': f.get('error') or {'message': f.get('message') or 'Unknown error'}}

        # Build payload but omit empty arrays to avoid server-side schema quirks
        payload = {
            'idempotency_key': idempotency_key or str(uuid.uuid4()),
            'sync_type': sync_type,
        }
        if successful_syncs:
            payload['successful_syncs'] = [_normalize_success(s) for s in successful_syncs]
        if failed_syncs:
            payload['failed_syncs'] = [_normalize_failed(f) for f in failed_syncs]

        # Pretty preview for dry-run
        try:
            preview = json.dumps(payload, ensure_ascii=False)[:1000]
        except Exception:
            preview = '<unserializable-payload>'

        print(f"📡 Accounting syncs endpoint: {endpoint}")
        print(f"🔎 Sync payload preview: {preview}")

        if dry_run:
            return True, {'endpoint': endpoint, 'payload_preview': preview}

        try:
            resp = self.session.post(endpoint, json=payload, timeout=30)
            status = resp.status_code
            try:
                data = resp.json()
            except Exception:
                data = resp.text
            if 200 <= status < 300:
                return True, {'status': status, 'response': data}
            else:
                return False, {'status': status, 'response': data}
        except Exception as ex:
            return False, {'error': str(ex)}

    def is_transaction_synced(self, transaction: Dict) -> bool:
        """Heuristic check whether a transaction object is already marked as synced.

        This checks common fields in Ramp responses such as: 'synced', 'sync_status', or metadata.
        Returns True if any indicator suggests the transaction has been synced previously.
        """
        if not transaction or not isinstance(transaction, dict):
            return False

        # Common patterns
        # 1. Top-level boolean flag
        if transaction.get('synced') is True:
            return True

        # 2. sync_status object
        ss = transaction.get('sync_status') or transaction.get('sync') or {}
        if isinstance(ss, dict):
            if ss.get('synced') is True:
                return True

        # 3. metadata field or attributes
        meta = transaction.get('metadata') or transaction.get('attributes') or {}
        if isinstance(meta, dict):
            if meta.get('synced') is True or meta.get('is_synced') is True:
                return True

        # Default: assume not synced
        return False
        
        # PRODUCTION CODE (uncomment when ready):
        # url = f"{self.base_url}/transactions/{transaction_id}/sync"
        # data = {"synced": True, "sync_system": "business_central"}
        # if sync_reference:
        #     data["sync_reference"] = sync_reference
        #     
        # try:
        #     resp = self.session.post(url, json=data)
        #     return resp.status_code == 200
        # except Exception:
        #     return False

    def get_sync_status(self, transaction_id: str) -> Dict:
        """
        Get sync status for a transaction.
        Returns sync metadata if available.
        """
        try:
            url = f"{self.base_url}/transactions/{transaction_id}"
            resp = self.session.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("sync_status", {})
            return {}
        except Exception:
            return {}

    def check_accounting_sync_enabled(self) -> bool:
        """
        Check whether the current accounting connection supports the API-based
        accounting sync endpoint (`/developer/v1/accounting/syncs`). Caches result
        on the client instance to avoid repeated calls.
        Returns True when the endpoint accepts the request (2xx) and False when
        the server indicates the accounting connection does not support API syncs
        (DEVELOPER_7089 or 4xx/404 responses).
        """
        if getattr(self, '_accounting_sync_enabled', None) is not None:
            return self._accounting_sync_enabled

        endpoint = self._build_endpoint('accounting/syncs')

        # Use canonical syncs helper for capability checks. We perform a dry-run preview
        # to validate the payload shape Ramp expects without sending writes.
        try:
            ok, info = self.post_accounting_syncs(
                successful_syncs=[{'id': '__cap_check__', 'reference_id': 'cap_check'}],
                failed_syncs=[],
                sync_type='TRANSACTION_SYNC',
                dry_run=True
            )
            if ok:
                self._accounting_sync_enabled = True
                payload_preview = info.get('payload_preview') if isinstance(info, dict) else str(info)
                self._accounting_sync_message = f"dry-run preview at {endpoint}: {str(payload_preview)[:1000]}"
                return True
            else:
                # `info` may be a dict with status/response or an error string
                self._accounting_sync_enabled = False
                if isinstance(info, dict) and 'response' in info:
                    self._accounting_sync_message = f"{info.get('status')} at {endpoint}: {str(info.get('response'))[:1000]}"
                else:
                    self._accounting_sync_message = str(info)
                return False
        except Exception as ex:
            self._accounting_sync_enabled = False
            self._accounting_sync_message = str(ex)
            return False

    def create_accounting_connection(self, connection_payload: dict, dry_run: bool = True):
        """
        Create (or update) an API-based accounting connection in Ramp.

        - `connection_payload` should be the dict payload as documented by Ramp's
          `POST /accounting/connection` endpoint (usually includes provider, config, and chart-of-accounts payload).
        - When `dry_run` is True the method will not perform the POST; it will
          instead return the endpoint and a short payload preview to let the
          caller inspect before applying.

        Returns a tuple `(ok: bool, info: str|dict)` where `ok` indicates whether
        the operation was successful (or the dry-run was prepared), and `info`
        contains either the response or a diagnostic message.
        """
        endpoint = self._build_endpoint('accounting/connection')

        # Prepare a small debug payload preview
        try:
            preview = json.dumps(connection_payload)[:1000]
        except Exception:
            preview = '<unserializable-payload>'

        print(f"📦 Accounting connection endpoint: {endpoint}")
        print(f"🔎 Payload preview: {preview}")

        if dry_run:
            return True, {'endpoint': endpoint, 'payload_preview': preview}

        try:
            resp = self.session.post(endpoint, json=connection_payload, timeout=60)
            status = resp.status_code
            try:
                data = resp.json()
            except Exception:
                data = resp.text
            if 200 <= status < 300:
                return True, {'status': status, 'response': data}
            else:
                return False, {'status': status, 'response': data}
        except Exception as ex:
            return False, {'error': str(ex)}

    def upload_gl_accounts(self, gl_accounts: List[Dict], dry_run: bool = True, batch_size: int = 500):
        """Upload GL accounts to Ramp in batches using POST /developer/v1/accounting/accounts.

        - `gl_accounts` should be a list of dicts matching Ramp's GL account schema
          (e.g., keys like `classification`, `code`, `id`, `name`).
        - When `dry_run` is True the method will not POST but will return a
          per-batch preview summary.

        Returns `(ok: bool, results: List[Dict])` where each result dict contains
        batch number, ok status, count, and server response (or preview info).
        """
        endpoint = self._build_endpoint('accounting/accounts')

        results = []
        total = len(gl_accounts)
        if total == 0:
            return True, [{'batch': 0, 'ok': True, 'count': 0, 'status': 'no_accounts'}]

        for i in range(0, total, batch_size):
            batch_num = (i // batch_size) + 1
            batch = gl_accounts[i:i + batch_size]
            payload = {'gl_accounts': batch}
            try:
                preview = json.dumps(payload)[:1000]
            except Exception:
                preview = '<unserializable-payload>'
            print(f"🔎 Upload GL accounts preview (batch {batch_num}/{(total+batch_size-1)//batch_size}): {preview}")

            if dry_run:
                results.append({'batch': batch_num, 'ok': True, 'status': 'dry-run', 'count': len(batch)})
                continue

            try:
                resp = self.session.post(endpoint, json=payload, timeout=60)
                status = resp.status_code
                try:
                    data = resp.json()
                except Exception:
                    data = resp.text
                ok = (200 <= status < 300)
                results.append({'batch': batch_num, 'ok': ok, 'status': status, 'response': data, 'count': len(batch)})
                # Per docs batch uploads are all-or-nothing per call; stop on failure
                if not ok:
                    break
            except Exception as ex:
                results.append({'batch': batch_num, 'ok': False, 'error': str(ex), 'count': len(batch)})
                break

        overall_ok = all(r.get('ok') for r in results)
        return overall_ok, results

    def _get_paginated_data(self, endpoint: str, status: Optional[str] = None,
                           start_date: Optional[str] = None, end_date: Optional[str] = None,
                           page_size: int = 200, **extra_params) -> List[Dict]:
        """Generic method for paginated API calls. Any extra keyword args will be
        added as query string parameters, allowing server-side filtering (e.g.
        has_no_sync_commits=True or sync_ready=True)."""
        url = self._build_endpoint(endpoint)
        params = {}
        if status:
            params["status"] = status
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        params["limit"] = page_size

        # Merge extra params into query string (useful for server-side sync filters)
        if extra_params:
            # Handle specific date filters for bills if they exist in extra_params
            if 'from_issued_date' in extra_params:
                params["start_date"] = extra_params.pop('from_issued_date')
            if 'to_issued_date' in extra_params:
                params["end_date"] = extra_params.pop('to_issued_date')
            
            for k, v in extra_params.items():
                # Convert booleans to lowercase strings which Ramp API expects
                if isinstance(v, bool):
                    params[k] = str(v).lower()
                else:
                    params[k] = v

        # Debug: Print the actual parameters being sent
        print(f"🔍 API Request: {endpoint}")
        print(f"🔍 Parameters: {params}")

        results: List[Dict] = []
        next_cursor = None
        next_url = None
        page_num = 0
        while True:
            page_num += 1
            if next_url:
                # Follow the full next URL directly (used by bills endpoint)
                resp = self.session.get(next_url)
            else:
                if next_cursor:
                    params["cursor"] = next_cursor
                resp = self.session.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data") or []
            results.extend(items)
            print(f"📄 Page {page_num}: fetched {len(items)} items (total so far: {len(results)})")
            # Support both cursor-based and next-URL-based pagination
            next_cursor = data.get("next") or data.get("next_cursor")
            page_info = data.get("page") or {}
            next_url = page_info.get("next") if not next_cursor else None
            if next_cursor:
                print(f"🔄 Next cursor found, fetching next page...")
            elif next_url:
                print(f"🔄 Next URL found, fetching next page...")
            if not next_cursor and not next_url:
                break
        
        print(f"✅ Retrieved {len(results)} total items from {endpoint} across {page_num} page(s)")
        return results
