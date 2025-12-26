
import requests
from typing import Dict, List, Optional
from urllib.parse import urljoin

class RampClient:
    def __init__(self, base_url: str, token_url: str, client_id: str, client_secret: str, enable_sync: bool = False):
        self.base_url = base_url.rstrip('/')
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
                  start_date: Optional[str] = None, end_date: Optional[str] = None,
                  page_size: int = 200, **extra_params) -> List[Dict]:
        """Fetch bills from Ramp API. Accepts extra query parameters such as
        `sync_ready=True` to only return bills ready to be synced to ERP."""
        return self._get_paginated_data("bills", status, start_date, end_date, page_size, **extra_params)

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
            url = urljoin(self.base_url + '/', f"vendors/{vendor_id}")
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
            url = urljoin(self.base_url + '/', "vendors/")
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
        """
        Mark a transaction as synced to Business Central.
        This would typically update transaction metadata to indicate sync status.
        
        NOTE: Currently in testing mode - does not actually update Ramp.
        Requires accounting:write scope to be enabled.
        """
        # If not enabled, behave as dry-run so we don't update production accidentally
        if not getattr(self, 'enable_sync', False):
            print(f"🔍 [DRY RUN] Would mark transaction {transaction_id} as synced (sync_reference: {sync_reference})")
            return True

        # PRODUCTION: attempt to call Ramp API to mark sync status
        url = f"{self.base_url}/transactions/{transaction_id}/sync"
        data = {"synced": True, "sync_system": "business_central"}
        if sync_reference:
            data["sync_reference"] = sync_reference

        try:
            resp = self.session.post(url, json=data)
            # Consider 200/201/204 as success
            return resp.status_code >= 200 and resp.status_code < 300
        except Exception:
            return False

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

    def _get_paginated_data(self, endpoint: str, status: Optional[str] = None,
                           start_date: Optional[str] = None, end_date: Optional[str] = None,
                           page_size: int = 200, **extra_params) -> List[Dict]:
        """Generic method for paginated API calls. Any extra keyword args will be
        added as query string parameters, allowing server-side filtering (e.g.
        has_no_sync_commits=True or sync_ready=True)."""
        url = urljoin(self.base_url + '/', endpoint.lstrip('/'))
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
            for k, v in extra_params.items():
                # Convert booleans to lowercase strings which Ramp API expects
                if isinstance(v, bool):
                    params[k] = str(v).lower()
                else:
                    params[k] = v

        results: List[Dict] = []
        next_cursor = None
        while True:
            if next_cursor:
                params["cursor"] = next_cursor
            resp = self.session.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data") or []
            results.extend(items)
            next_cursor = data.get("next") or data.get("next_cursor")
            if not next_cursor:
                break
        return results
