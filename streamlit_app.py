import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import sys
import os
import streamlit.components.v1 as components
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Page configuration must be the first Streamlit command in the script
st.set_page_config(
    page_title="Ramp → Business Central Export",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Client-side fix: if Azure redirected to a subpath (e.g. /oauth2callback),
# redirect the browser to the app root while preserving query params so
# Streamlit's static assets and WebSocket endpoints load from the correct root.
components.html(
        """
        <script>
        (function() {
            try {
                const p = window.location.pathname || '/';
                if (p && p !== '/' && p.includes('oauth2callback')) {
                    const q = window.location.search || '';
                    // Replace so back button doesn't loop
                    // Use top-level navigation to avoid embedding identity provider pages inside frames
                    try {
                        if (window.top && window.top !== window) {
                            window.top.location.replace('/' + q);
                        } else {
                            window.location.replace('/' + q);
                        }
                    } catch (e) {
                        // If cross-origin access to window.top is denied, fallback to setting top location via document
                        try { document.location = '/' + q; } catch(e2) { window.location.replace('/' + q);} 
                    }
                }
            } catch (e) {
                // ignore
            }
        })();
        </script>
        """,
        height=0,
)
# MSAL-based in-app authentication for Streamlit Community Cloud (Azure AD)
from auth.azure_auth import ensure_authenticated, REDIRECT_URI

# Ensure the user is authenticated and obtain identity values
user_name, user_email = ensure_authenticated()



# `user_name` and `user_email` are provided by ensure_authenticated() (auth.azure_auth)

# Enhanced sidebar user profile
st.sidebar.markdown("---")
st.sidebar.markdown("### 👤 User Profile")
st.sidebar.success(f"**{user_name}**")
if user_email and user_email != user_name:
    st.sidebar.caption(f"📧 {user_email}")
st.sidebar.markdown("---")

# Logout action clears the session token and optionally provides Azure logout link
if st.sidebar.button("🚪 Log out", use_container_width=True):
    st.session_state.pop(SESSION_TOKEN_KEY, None)
    st.query_params.clear()
    logout_url = (
        f"https://login.microsoftonline.com/common/oauth2/v2.0/logout?post_logout_redirect_uri={REDIRECT_URI}"
    )
    st.success("✅ You have been logged out successfully.")
    st.markdown(f"[🔐 Sign in again]({logout_url})")
    st.stop()

# Add current directory to path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import load_env, load_config
from ramp_client import RampClient
from transform import (ramp_credit_card_to_bc_rows, ramp_bills_to_bc_rows,
                      ramp_reimbursements_to_bc_rows, ramp_cashbacks_to_bc_rows,
                      ramp_statements_to_bc_rows, ramp_bills_to_purchase_invoice_lines,
                      ramp_bills_to_general_journal)
from bc_export import export

# Load institutional stylesheet
def load_css():
    css_file = os.path.join(os.path.dirname(__file__), 'assets', 'styles.css')
    if os.path.exists(css_file):
        with open(css_file) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    else:
        # Fallback inline minimal styles if CSS file not found
        st.markdown("""
        <style>
        .app-header { background-color: #1a1f36; color: white; padding: 1.5rem 2rem; margin: -2rem -2rem 2rem -2rem; border-bottom: 3px solid #3498db; }
        .app-header h1 { font-size: 1.75rem; font-weight: 600; margin: 0; }
        .app-header p { margin: 0.5rem 0 0 0; color: #cbd5e0; font-size: 0.95rem; }
        </style>
        """, unsafe_allow_html=True)

load_css()

# Institutional header
st.markdown("""
<div class="app-header">
    <h1>Ramp → Business Central Export</h1>
    <p>Financial Data Integration Platform | Northwest Area Foundation</p>
</div>
""", unsafe_allow_html=True)

# Load configuration
try:
    # For local development
    if os.path.exists('.env'):
        env = load_env()
    else:
        # For Streamlit Cloud - use st.secrets
        env = {
            'RAMP_CLIENT_ID': st.secrets.get('RAMP_CLIENT_ID'),
            'RAMP_CLIENT_SECRET': st.secrets.get('RAMP_CLIENT_SECRET')
        }

    cfg = load_config()
except Exception as e:
    st.error("Configuration Error: Unable to load required settings.")
    st.markdown("Please contact your system administrator.")
    st.stop()


# Post-export sync options (defined before tabs so other panels can reference safely)
st.sidebar.markdown("")
mark_transactions_after_export = st.sidebar.checkbox(
    "Mark exported transactions in Ramp as synced",
    value=False,
    help="If checked, the app will mark exported transactions as synced in Ramp. This is a dry-run unless 'Enable live Ramp sync' is checked."
)
enable_live_ramp_sync = st.sidebar.checkbox(
    "Enable live Ramp sync (will POST to Ramp)",
    value=False,
    help="Enable sending a request to Ramp to mark transactions as synced. Requires accounting:write scope and should be used cautiously."
)

# --- New: Tabbed exports panel (Credit Cards, Invoices, Reimbursements) ---
st.markdown("---")
st.header("Exports by Type")
cc_tab, inv_tab, reimb_tab = st.tabs(["Credit Cards", "Invoices", "Reimbursements"])

# Amount helper moved to `utils._extract_amount`
from utils import _extract_amount, _write_sync_audit

with cc_tab:
    st.subheader("Credit Card Statement Export")
    st.write("This panel automatically fetches the latest card statement and prepares a Business Central journal CSV for download.")

    auto_fetch = True  # per UX requirement: automatically fetch latest statement on render

    if auto_fetch:
        with st.spinner("Authenticating and fetching latest statement..."):
            try:
                client = RampClient(
                    base_url=cfg['ramp']['base_url'],
                    token_url=cfg['ramp']['token_url'],
                    client_id=env['RAMP_CLIENT_ID'],
                    client_secret=env['RAMP_CLIENT_SECRET'],
                    enable_sync=st.session_state.get('enable_live_ramp_sync', False)
                )
                client.authenticate()

                stmts = client.get_statements()
                if not stmts:
                    st.info("No statements found for this account")
                else:
                    latest_stmt = stmts[0]
                    s = (latest_stmt.get('start_date') or '')[:10]
                    e = (latest_stmt.get('end_date') or '')[:10]
                    st.markdown(f"**Statement period:** {s} → {e}")

                    # Get authoritative statement totals (try charges -> balance_sections -> ending_balance)
                    stmt_charges = _extract_amount(latest_stmt.get('charges') or {})
                    if not stmt_charges:
                        # try balance_sections
                        bsecs = latest_stmt.get('balance_sections') or []
                        if bsecs:
                            stmt_charges = _extract_amount(bsecs[0].get('charges') or {})
                    st.write(f"Statement charges (major units): **${stmt_charges:,.2f}**")

                    # Collect CARD_TRANSACTION ids from statement_lines
                    lines = latest_stmt.get('statement_lines') or []
                    card_ids = [l.get('id') for l in lines if l.get('type') == 'CARD_TRANSACTION']
                    st.write(f"Statement transaction count: **{len(card_ids)}**")

                    # Fetch each transaction by id (authoritative) — use concurrency for speed
                    fetched = []
                    failures = []
                    if card_ids:
                        max_workers = min(12, max(4, len(card_ids)))
                        with st.spinner(f"Fetching {len(card_ids)} transactions (concurrent, workers={max_workers})..."):
                            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                                futures = {ex.submit(client.session.get, f"{client.base_url}/transactions/{tid}", timeout=15): tid for tid in card_ids}
                                for fut in as_completed(futures):
                                    tid = futures[fut]
                                    try:
                                        resp = fut.result()
                                        if resp.status_code == 200:
                                            fetched.append(resp.json())
                                        else:
                                            failures.append((tid, resp.status_code))
                                    except Exception as exc:
                                        failures.append((tid, str(exc)))

                    tx_total = sum(_extract_amount(t.get('amount')) for t in fetched)
                    st.write(f"Transactions total (sum of amounts): **${tx_total:,.2f}**")
                    if failures:
                        st.warning(f"{len(failures)} transactions failed to fetch (see logs).")

                    strict_mode = st.checkbox("Abort export if totals mismatch (strict)", value=False, help="When enabled, the export will be disabled if statement total does not match transaction sum.")
                    mismatch = abs(stmt_charges - tx_total) > 0.01
                    if mismatch:
                        st.warning("Statement total does not match the transaction total.")
                        if strict_mode:
                            st.error("Strict mode enabled: export disabled due to totals mismatch.")

                    # Prepare transactions for transform (force posting date to accounting_date)
                    for t in fetched:
                        if t.get('accounting_date'):
                            t['payment_date'] = t.get('accounting_date')

                    df = ramp_credit_card_to_bc_rows(fetched, cfg, write_audit=False)

                    st.subheader("Preview (first 10 rows)")
                    if df is None or df.empty:
                        st.info("No credit-card rows generated. Check mapping or that transactions are fully coded with GL accounts.")
                    else:
                        st.dataframe(df.head(10), use_container_width=True)

                    # Provide download button (disable when strict and mismatch)
                    if df is not None and not df.empty and (not (strict_mode and mismatch)):
                        csv_bytes = df.to_csv(index=False).encode('utf-8')
                        fname = f"v2_cc_statement_journal_{s.replace('-', '')}_{e.replace('-', '')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
                        st.download_button("Download CC Journal (Latest Statement)", data=csv_bytes, file_name=fname, mime='text/csv')

            except Exception as ex:
                st.error(f"Error fetching statement: {ex}")

with inv_tab:
    st.subheader("Purchase Invoice Export (Bills)")
    st.write("Generate Purchase Invoices CSV from approved Ramp bills in the selected date range.")

    # Local date range inputs for the Invoices panel (defaults to global sidebar dates)
    col_a, col_b = st.columns(2)
    with col_a:
        inv_start = st.date_input("Invoices: Start Date", value=st.session_state.get('inv_start_date', start_date if 'start_date' in globals() else datetime.now().replace(day=1)), key='inv_start')
    with col_b:
        inv_end = st.date_input("Invoices: End Date", value=st.session_state.get('inv_end_date', end_date if 'end_date' in globals() else datetime.now()), key='inv_end')

    include_audit = st.checkbox("Write audit NDJSON (export original bill objects)", value=False, key='pi_include_audit')
    confirm_mark = st.checkbox("I confirm: mark exported bills as synced (requires confirmation below)", value=False, key='pi_confirm_mark')

    # Offer a non-destructive preview that mirrors the 'Generate' behavior but does not cache or mark
    if st.button("Preview Purchase Invoices for date range", key='preview_pi_btn'):
        with st.spinner("Fetching bills for preview..."):
            try:
                client = RampClient(
                    base_url=cfg['ramp']['base_url'],
                    token_url=cfg['ramp']['token_url'],
                    client_id=env['RAMP_CLIENT_ID'],
                    client_secret=env['RAMP_CLIENT_SECRET'],
                    enable_sync=False
                )
                client.authenticate()

                start_date_str = inv_start.strftime('%Y-%m-%d')
                end_date_str = inv_end.strftime('%Y-%m-%d')

                bills = client.get_bills(status='APPROVED', start_date=start_date_str, end_date=end_date_str, page_size=cfg['ramp'].get('page_size', 200))
                total_bills = len(bills) if isinstance(bills, list) else 0
                if not bills:
                    st.info('No approved bills found for the specified period.')
                else:
                    st.success(f"Retrieved {total_bills} bills (preview)")

                    # Filter out already-synced for preview too
                    bills_preview = [b for b in bills if not client.is_transaction_synced(b)]

                    # Enrich bills with vendor external ids from the Vendors API so
                    # 'Buy-from Vendor No.' is populated from vendor.external_id (authoritative)
                    try:
                        from transform import enrich_bills_with_vendor_external_ids
                        bills_preview = enrich_bills_with_vendor_external_ids(bills_preview, client)
                    except Exception:
                        # If enrichment fails, continue with best-effort fallback values
                        pass

                    pi_df = ramp_bills_to_purchase_invoice_lines(bills_preview, cfg)
                    gj_df = ramp_bills_to_general_journal(bills_preview, cfg)

                    bill_total = 0.0
                    for b in bills_preview:
                        amt = b.get('amount') or b.get('total') or {}
                        if isinstance(amt, dict):
                            bill_total += (amt.get('amount', 0) / amt.get('minor_unit_conversion_rate', 100))
                        else:
                            try:
                                bill_total += float(amt)
                            except Exception:
                                pass

                    pi_total = pi_df['Amount'].sum() if pi_df is not None and not pi_df.empty and 'Amount' in pi_df.columns else 0.0

                    st.write(f"Bills count after filtering: **{len(bills_preview)}**  — Total amount: **${bill_total:,.2f}**")
                    st.write(f"Purchase Invoice lines total (preview): **${pi_total:,.2f}**")

                    st.subheader("Preview - Purchase Invoice (first 10 rows)")
                    if pi_df is None or pi_df.empty:
                        st.info("No purchase invoice rows generated. Check mapping and bill line items.")
                    else:
                        st.dataframe(pi_df.head(10), use_container_width=True)

                    st.subheader("Preview - General Journal (first 10 rows)")
                    if gj_df is None or gj_df.empty:
                        st.info("No general journal rows generated.")
                    else:
                        st.dataframe(gj_df.head(10), use_container_width=True)

            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                st.error("Error during preview. See details below.")
                with st.expander("Preview error details (expand for stack trace)"):
                    st.code(tb)
                import logging
                logging.exception("Preview error: %s", tb)

    # Clear previous cached results when date range changes
    if st.session_state.get('inv_start') != inv_start or st.session_state.get('inv_end') != inv_end:
        st.session_state.pop('inv_bills', None)
        st.session_state.pop('inv_pi_df', None)
        st.session_state.pop('inv_gj_df', None)
        st.session_state['inv_start'] = inv_start
        st.session_state['inv_end'] = inv_end

    if st.button("Generate Purchase Invoices for date range", key='gen_pi_btn'):
        with st.spinner("Fetching bills and preparing export..."):
            try:
                client = RampClient(
                    base_url=cfg['ramp']['base_url'],
                    token_url=cfg['ramp']['token_url'],
                    client_id=env['RAMP_CLIENT_ID'],
                    client_secret=env['RAMP_CLIENT_SECRET'],
                    enable_sync=st.session_state.get('enable_live_ramp_sync', False)
                )
                client.authenticate()

                start_date_str = inv_start.strftime('%Y-%m-%d')
                end_date_str = inv_end.strftime('%Y-%m-%d')

                bills = client.get_bills(status='APPROVED', start_date=start_date_str, end_date=end_date_str, page_size=cfg['ramp'].get('page_size', 200))
                total_bills = len(bills) if isinstance(bills, list) else 0
                if not bills:
                    st.info('No approved bills found for the specified period.')
                    st.session_state.pop('inv_bills', None)
                    st.stop()

                st.success(f"Retrieved {total_bills} bills (pre-filter)")

                # Remove already-synced bills when possible
                before = len(bills)
                bills = [b for b in bills if not client.is_transaction_synced(b)]
                after = len(bills)
                skipped = before - after
                if skipped:
                    st.info(f"Skipped {skipped} bills already marked synced in Ramp")

                # Enrich bills with vendor external ids from Vendors API to populate
                # Buy-from Vendor No. (prefer vendor.external_vendor_id_resolved)
                try:
                    from transform import enrich_bills_with_vendor_external_ids
                    bills = enrich_bills_with_vendor_external_ids(bills, client)
                except Exception:
                    pass

                # Run transforms
                pi_df = ramp_bills_to_purchase_invoice_lines(bills, cfg)
                gj_df = ramp_bills_to_general_journal(bills, cfg)

                # Totals diagnostics
                bill_total = 0.0
                for b in bills:
                    amt = b.get('amount') or b.get('total') or {}
                    if isinstance(amt, dict):
                        bill_total += (amt.get('amount', 0) / amt.get('minor_unit_conversion_rate', 100))
                    else:
                        try:
                            bill_total += float(amt)
                        except Exception:
                            pass

                pi_total = pi_df['Amount'].sum() if pi_df is not None and not pi_df.empty and 'Amount' in pi_df.columns else 0.0

                # Store results in session for subsequent actions
                st.session_state['inv_bills'] = bills
                st.session_state['inv_pi_df'] = pi_df
                st.session_state['inv_gj_df'] = gj_df
                st.session_state['inv_bill_total'] = bill_total

                st.write(f"Bills count after filtering: **{len(bills)}**  — Total amount: **${bill_total:,.2f}**")
                st.write(f"Purchase Invoice lines total: **${pi_total:,.2f}**")

                st.subheader("Purchase Invoice Preview (first 10 rows)")
                if pi_df is None or pi_df.empty:
                    st.info("No purchase invoice rows generated. Check mapping and bill line items.")
                else:
                    st.dataframe(pi_df.head(10), use_container_width=True)

                st.subheader("General Journal Preview (first 10 rows)")
                if gj_df is None or gj_df.empty:
                    st.info("No general journal rows generated.")
                else:
                    st.dataframe(gj_df.head(10), use_container_width=True)

                # Provide downloads (CSV & Excel)
                if pi_df is not None and not pi_df.empty:
                    csv_bytes = pi_df.to_csv(index=False).encode('utf-8')
                    fname = f"purchase_invoices_{inv_start.strftime('%Y%m%d')}_{inv_end.strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
                    st.download_button("Download Purchase Invoices CSV", data=csv_bytes, file_name=fname, mime='text/csv')

                    # Excel
                    excel_buf = BytesIO()
                    with pd.ExcelWriter(excel_buf, engine='openpyxl') as writer:
                        pi_df.to_excel(writer, sheet_name='PurchaseInvoices', index=False)
                    excel_buf.seek(0)
                    fname_x = fname.replace('.csv', '.xlsx')
                    st.download_button("Download Purchase Invoices Excel", data=excel_buf, file_name=fname_x, mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

                # General Journal
                if gj_df is not None and not gj_df.empty:
                    gj_csv = gj_df.to_csv(index=False).encode('utf-8')
                    gj_name = f"purchase_invoices_journal_{inv_start.strftime('%Y%m%d')}_{inv_end.strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
                    st.download_button("Download Purchase Invoices General Journal CSV", data=gj_csv, file_name=gj_name, mime='text/csv')

                # Optional audit NDJSON (write only when user requests)
                if include_audit:
                    ts = datetime.now().strftime('%Y%m%dT%H%M%S')
                    audit_path = f"exports/purchase_invoices_audit_{ts}.ndjson"
                    try:
                        os.makedirs('exports', exist_ok=True)
                        with open(audit_path, 'w', encoding='utf-8') as af:
                            for b in bills:
                                af.write(json.dumps(b, ensure_ascii=False) + "\n")
                        with open(audit_path, 'rb') as f:
                            st.download_button("Download Bills Audit (NDJSON)", f, file_name=os.path.basename(audit_path), mime='application/x-ndjson')
                    except Exception:
                        st.warning("Could not write audit NDJSON file.")

            except Exception as e:
                st.error(f"Error generating purchase invoices: {e}")

    # If generation has completed and bills are present, offer a separate mark-as-synced action
    bills_cached = st.session_state.get('inv_bills')
    if bills_cached:
        st.markdown('---')
        st.subheader('Post-generation actions')
        st.write(f"{len(bills_cached)} bills prepared for export (total ${st.session_state.get('inv_bill_total', 0.0):,.2f}).")

        if st.checkbox('Enable marking these bills as synced (dry-run unless live sync enabled)', value=False, key='pi_enable_mark'):
            if not st.session_state.get('pi_confirm_mark'):
                st.warning('Please check the confirmation checkbox above to enable marking.')
            else:
                if st.button('Mark these bills as synced in Ramp', key='pi_mark_btn'):
                    with st.spinner('Marking bills as synced...'):
                        try:
                            client = RampClient(
                                base_url=cfg['ramp']['base_url'],
                                token_url=cfg['ramp']['token_url'],
                                client_id=env['RAMP_CLIENT_ID'],
                                client_secret=env['RAMP_CLIENT_SECRET'],
                                enable_sync=st.session_state.get('enable_live_ramp_sync', False)
                            )
                            client.authenticate()

                            sync_ref = f"BC_BillExport_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            results = []
                            progress = st.progress(0)
                            total = len(bills_cached)
                            i = 0
                            for b in bills_cached:
                                i += 1
                                tid = b.get('id')
                                ok = client.mark_transaction_synced(tid, sync_reference=sync_ref)
                                results.append({'timestamp': datetime.now().isoformat(), 'transaction_id': tid, 'ok': ok, 'message': ''})
                                progress.progress(i / total)

                            successes = sum(1 for r in results if r['ok'])
                            failures = len(results) - successes
                            if st.session_state.get('enable_live_ramp_sync', False):
                                st.success(f"Ramp sync complete: {successes} succeeded, {failures} failed.")
                            else:
                                st.info(f"Dry run complete: {successes} would be marked synced (no live requests were sent).")

                            audit_path = _write_sync_audit(results, sync_ref, user_email=user_email)
                            if audit_path:
                                st.markdown(f"Audit CSV written to `{audit_path}`")
                                with open(audit_path, 'rb') as f:
                                    st.download_button("Download bills sync audit CSV", f, file_name=os.path.basename(audit_path))

                        except Exception as e:
                            st.error(f"Error marking bills as synced: {e}")

with reimb_tab:
    st.subheader("Reimbursements Export")
    st.write("Export reimbursements (PAID) as BC general journal rows for the selected date range.")

    include_audit = st.checkbox("Write reimbursements audit NDJSON (export original objects)", value=False, key='reim_include_audit')
    mark_synced = st.checkbox("Mark exported reimbursements as synced in Ramp (dry-run unless live sync enabled)", value=False, key='reim_mark_synced')

    # Local date range inputs for Reimbursements (fallback to global sidebar dates if present)
    col_a, col_b = st.columns(2)
    with col_a:
        reim_start = st.date_input("Reimbursements: Start Date", value=st.session_state.get('reim_start_date', start_date if 'start_date' in globals() else datetime.now().replace(day=1)), key='reim_start')
    with col_b:
        reim_end = st.date_input("Reimbursements: End Date", value=st.session_state.get('reim_end_date', end_date if 'end_date' in globals() else datetime.now()), key='reim_end')

    # Add preview action for reimbursements (non-destructive)
    if st.button("Preview Reimbursements for date range", key='preview_reim_btn'):
        with st.spinner("Fetching reimbursements for preview..."):
            try:
                client = RampClient(
                    base_url=cfg['ramp']['base_url'],
                    token_url=cfg['ramp']['token_url'],
                    client_id=env['RAMP_CLIENT_ID'],
                    client_secret=env['RAMP_CLIENT_SECRET'],
                    enable_sync=False
                )
                client.authenticate()
                start_date_str = reim_start.strftime('%Y-%m-%d')
                end_date_str = reim_end.strftime('%Y-%m-%d')
                reims = client.get_reimbursements(status='PAID', start_date=start_date_str, end_date=end_date_str, page_size=cfg['ramp'].get('page_size', 200))

                if not reims:
                    st.info('No reimbursements found for the specified period.')
                else:
                    st.success(f"Retrieved {len(reims)} reimbursements (preview)")

                    # Filter already-synced
                    reims_preview = [r for r in reims if not client.is_transaction_synced(r)]

                    r_df = ramp_reimbursements_to_bc_rows(reims_preview, cfg)

                    # totals
                    reim_total = 0.0
                    for r in reims_preview:
                        amt = r.get('amount') or r.get('total') or {}
                        if isinstance(amt, dict):
                            reim_total += (amt.get('amount', 0) / amt.get('minor_unit_conversion_rate', 100))
                        else:
                            try:
                                reim_total += float(amt)
                            except Exception:
                                pass

                    rdf_total = r_df['Debit Amount'].sum() if r_df is not None and not r_df.empty and 'Debit Amount' in r_df.columns else 0.0
                    st.write(f"Reimbursements count after filtering: **{len(reims_preview)}**  — Total amount: **${reim_total:,.2f}**")
                    st.write(f"Reimbursement journal debit total (preview): **${rdf_total:,.2f}**")

                    st.subheader("Preview - Reimbursements (first 10 rows)")
                    if r_df is None or r_df.empty:
                        st.info('No reimbursement rows generated. Check mapping and data.')
                    else:
                        st.dataframe(r_df.head(10), use_container_width=True)

                    # Provide downloads
                    if r_df is not None and not r_df.empty:
                        csv_bytes = r_df.to_csv(index=False).encode('utf-8')
                        fname = f"reimbursements_{reim_start.strftime('%Y%m%d')}_{reim_end.strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
                        st.download_button("Download Reimbursements CSV (preview)", data=csv_bytes, file_name=fname, mime='text/csv')

            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                st.error("Error during preview. See details below.")
                with st.expander("Preview error details (expand for stack trace)"):
                    st.code(tb)
                import logging
                logging.exception("Preview error: %s", tb)

    if st.button("Generate Reimbursements for date range"):
        with st.spinner("Fetching reimbursements and preparing export..."):
            try:
                client = RampClient(
                    base_url=cfg['ramp']['base_url'],
                    token_url=cfg['ramp']['token_url'],
                    client_id=env['RAMP_CLIENT_ID'],
                    client_secret=env['RAMP_CLIENT_SECRET'],
                    enable_sync=st.session_state.get('enable_live_ramp_sync', False)
                )
                client.authenticate()
                start_date_str = reim_start.strftime('%Y-%m-%d')
                end_date_str = reim_end.strftime('%Y-%m-%d')
                reims = client.get_reimbursements(status='PAID', start_date=start_date_str, end_date=end_date_str, page_size=cfg['ramp'].get('page_size', 200))

                if not reims:
                    st.info('No reimbursements found for the specified period.')
                    st.stop()

                st.success(f"Retrieved {len(reims)} reimbursements (pre-filter)")

                # Filter already-synced
                before = len(reims)
                reims = [r for r in reims if not client.is_transaction_synced(r)]
                after = len(reims)
                skipped = before - after
                if skipped:
                    st.info(f"Skipped {skipped} reimbursements already marked synced in Ramp")

                r_df = ramp_reimbursements_to_bc_rows(reims, cfg)

                # totals
                reim_total = 0.0
                for r in reims:
                    amt = r.get('amount') or r.get('total') or {}
                    if isinstance(amt, dict):
                        reim_total += (amt.get('amount', 0) / amt.get('minor_unit_conversion_rate', 100))
                    else:
                        try:
                            reim_total += float(amt)
                        except Exception:
                            pass

                rdf_total = r_df['Debit Amount'].sum() if r_df is not None and not r_df.empty and 'Debit Amount' in r_df.columns else 0.0
                st.write(f"Reimbursements count after filtering: **{len(reims)}**  — Total amount: **${reim_total:,.2f}**")
                st.write(f"Reimbursement journal debit total: **${rdf_total:,.2f}**")

                st.subheader("Reimbursements Preview (first 10 rows)")
                if r_df is None or r_df.empty:
                    st.info('No reimbursement rows generated. Check mapping and data.')
                else:
                    st.dataframe(r_df.head(10), use_container_width=True)

                # Provide downloads
                if r_df is not None and not r_df.empty:
                    csv_bytes = r_df.to_csv(index=False).encode('utf-8')
                    fname = f"reimbursements_{reim_start.strftime('%Y%m%d')}_{reim_end.strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
                    st.download_button("Download Reimbursements CSV", data=csv_bytes, file_name=fname, mime='text/csv')

                # Optional audit
                if include_audit:
                    ts = datetime.now().strftime('%Y%m%dT%H%M%S')
                    audit_path = f"exports/reimbursements_audit_{ts}.ndjson"
                    try:
                        os.makedirs('exports', exist_ok=True)
                        with open(audit_path, 'w', encoding='utf-8') as af:
                            for r in reims:
                                af.write(json.dumps(r, ensure_ascii=False) + "\n")
                        with open(audit_path, 'rb') as f:
                            st.download_button("Download Reimbursements Audit (NDJSON)", f, file_name=os.path.basename(audit_path), mime='application/x-ndjson')
                    except Exception:
                        st.warning('Could not write audit NDJSON file.')

                # Optionally mark reimbursements as synced
                if mark_synced and reims:
                    st.info('Preparing to mark reimbursements as synced in Ramp...')
                    sync_ref = f"BC_ReimExport_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    results = []
                    progress = st.progress(0)
                    total = len(reims)
                    i = 0
                    for r in reims:
                        i += 1
                        tid = r.get('id')
                        ok = client.mark_transaction_synced(tid, sync_reference=sync_ref)
                        results.append({'timestamp': datetime.now().isoformat(), 'transaction_id': tid, 'ok': ok, 'message': ''})
                        progress.progress(i / total)

                    successes = sum(1 for res in results if res['ok'])
                    failures = len(results) - successes
                    if st.session_state.get('enable_live_ramp_sync', False):
                        st.success(f"Ramp sync complete: {successes} succeeded, {failures} failed.")
                    else:
                        st.info(f"Dry run complete: {successes} would be marked synced (no live requests were sent).")

            except Exception as e:
                st.error(f"Error generating reimbursements: {e}")

# End tabbed panel

# Sidebar configuration
st.sidebar.markdown('<div class="section-header">Export Configuration</div>', unsafe_allow_html=True)

# --- Sidebar: Latest statement period widget ---
if 'latest_statement' not in st.session_state:
    st.session_state.latest_statement = None
    st.session_state.latest_statement_at = None

with st.sidebar.expander('📄 Latest Card Statement', expanded=True):
    if st.button('Refresh latest statement', key='refresh_statement'):
        # Force refetch next render
        st.session_state.latest_statement = None
        st.session_state.latest_statement_at = None

    if st.session_state.latest_statement is None:
        # Try to fetch and cache
        try:
            sc = RampClient(base_url=cfg['ramp']['base_url'], token_url=cfg['ramp']['token_url'], client_id=env['RAMP_CLIENT_ID'], client_secret=env['RAMP_CLIENT_SECRET'])
            sc.authenticate()
            stmts = sc.get_statements()
            if stmts:
                st.session_state.latest_statement = stmts[0]
                st.session_state.latest_statement_at = datetime.now().isoformat()
        except Exception:
            st.write('Could not fetch latest statement')

    stmt = st.session_state.get('latest_statement')
    if stmt:
        s = (stmt.get('start_date') or '')[:10]
        e = (stmt.get('end_date') or '')[:10]
        st.markdown(f"**Period:** {s} → {e}")
        # try derive charges
        charges = _extract_amount(stmt.get('charges') or {})
        if not charges:
            bsecs = stmt.get('balance_sections') or []
            if bsecs:
                charges = _extract_amount(bsecs[0].get('charges') or {})
        st.markdown(f"**Charges:** ${charges:,.2f}")
        st.markdown(f"*Fetched at {st.session_state.latest_statement_at}*")
    else:
        st.markdown('_No statement cached_')

# Note: Per-tab controls
st.sidebar.markdown("**Per-tab controls**")
st.sidebar.info("Date ranges, export generation, and downloads are managed in each export tab (Credit Cards, Invoices, Reimbursements). Use the relevant tab to preview, generate, and download exports.")

# System Overview (compact sidebar version)
st.sidebar.markdown("### System Overview")
st.sidebar.markdown("- **Secure Microsoft Azure AD authentication**\n- **Real-time API integration with Ramp**\n- **Business Central-compatible exports (CSV, Excel)**\n- **Per-tab previews and dry-run-first export flows**")


def run_export(selected_types, start_date, end_date, cfg, env):
    """Run the export process and display results"""

    with st.spinner("Authenticating with Ramp API..."):
        try:
            client = RampClient(
                base_url=cfg['ramp']['base_url'],
                token_url=cfg['ramp']['token_url'],
                client_id=env['RAMP_CLIENT_ID'],
                client_secret=env['RAMP_CLIENT_SECRET'],
                enable_sync=st.session_state.get('enable_live_ramp_sync', False)
            )
            client.authenticate()
            st.success("Authentication successful")
        except Exception as e:
            st.error("Authentication failed. Please contact administrator.")
            return

    # Check available endpoints
    with st.spinner("Checking API availability..."):
        available_endpoints = check_available_endpoints(client, cfg)

    # Filter selected types to only available ones
    available_selected_types = [t for t in selected_types if available_endpoints.get(t, False)]

    if not available_selected_types:
        st.error("None of the selected data types are available with your current API permissions")
        return

    if len(available_selected_types) < len(selected_types):
        unavailable = [t for t in selected_types if t not in available_selected_types]
        st.warning(f"Some data types are not available: {', '.join(unavailable)}")

    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Fetch and combine data
    combined_df = None
    total_records = 0
    processed_types = 0

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    exported_transaction_ids = set()

    for data_type in available_selected_types:
        status_text.text(f"Fetching {data_type} from {start_date_str} to {end_date_str}...")

        try:
            data, df, processed_ids = fetch_data_for_type(client, data_type, start_date_str, end_date_str, cfg)

            if data:
                st.success(f"Retrieved {len(data)} {data_type} records")
                total_records += len(data)

                # Combine dataframes
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                # Collect processed transaction ids for sync marking
                if processed_ids and data_type == 'transactions':
                    for tid in processed_ids:
                        exported_transaction_ids.add(str(tid))
            else:
                st.info(f"No {data_type} data found for the specified period")

        except Exception as e:
            st.error(f"Error fetching {data_type}: {str(e)}")
            continue

        processed_types += 1
        progress_bar.progress(processed_types / len(available_selected_types))

    progress_bar.empty()
    status_text.empty()

    if combined_df is None or combined_df.empty:
        st.error("No data found for any of the specified types and periods.")
        return

    # Display summary
    st.success(f"Export complete: {total_records} records processed from {len(available_selected_types)} data sources")

    # Display data preview
    st.subheader("Data Preview")
    st.dataframe(combined_df.head(10), use_container_width=True)

    # Export files
    st.subheader("Download Export Files")

    # Create Excel file
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        combined_df.to_excel(writer, sheet_name='Journal_Entries', index=False)
    excel_buffer.seek(0)

    # Create CSV file
    csv_buffer = BytesIO()
    combined_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    col1, col2 = st.columns(2)

    with col1:
        st.download_button(
            label="Download Excel (.xlsx)",
            data=excel_buffer,
            file_name=f"Ramp_BC_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    with col2:
        st.download_button(
            label="Download CSV (.csv)",
            data=csv_buffer,
            file_name=f"Ramp_BC_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )

    # Show a manual button to mark exported transactions as synced
    if exported_transaction_ids:
        st.markdown("---")
        st.subheader("Post-export actions")
        st.write(f"{len(exported_transaction_ids)} exported transaction IDs collected for potential sync with Ramp.")
        st.caption("Use the button below to mark exported transactions as synced in Ramp. This will be a dry run unless 'Enable live Ramp sync' is checked in the sidebar.")

        if st.button("Mark as synced in Ramp", key="mark_synced_button"):
            st.info("Starting marking process — this may take a moment...")
            results = []
            sync_ref = f"BCExport_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            progress = st.progress(0)
            total = len(exported_transaction_ids)
            i = 0
            for tid in list(exported_transaction_ids):
                i += 1
                ok = client.mark_transaction_synced(tid, sync_reference=sync_ref)
                results.append({'timestamp': datetime.now().isoformat(), 'transaction_id': tid, 'ok': ok, 'message': ''})
                progress.progress(i / total)

            successes = sum(1 for r in results if r['ok'])
            failures = len(results) - successes

            if st.session_state.get('enable_live_ramp_sync', False):
                st.success(f"Ramp sync complete: {successes} succeeded, {failures} failed.")
            else:
                st.info(f"Dry run complete: {successes} would be marked synced (no live requests were sent).")

            # Write audit CSV and provide download
            audit_path = _write_sync_audit(results, sync_ref, user_email=user_email)
            if audit_path:
                st.markdown(f"Audit CSV written to `{audit_path}`")
                with open(audit_path, 'rb') as f:
                    st.download_button("Download sync audit CSV", f, file_name=os.path.basename(audit_path))

    # If requested, mark exported transactions in Ramp (dry-run unless live sync enabled)
    if mark_transactions_after_export and exported_transaction_ids:
        st.info(f"Preparing to mark {len(exported_transaction_ids)} exported transactions as synced in Ramp...")
        results = []
        sync_ref = f"BCExport_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        for tid in exported_transaction_ids:
            ok = client.mark_transaction_synced(tid, sync_reference=sync_ref)
            results.append({
                'timestamp': datetime.now().isoformat(),
                'transaction_id': tid,
                'ok': ok,
                'message': ''
            })

        audit_path = _write_sync_audit(results, sync_ref, user_email=user_email)

        successes = sum(1 for r in results if r['ok'])
        failures = len(results) - successes

        if st.session_state.get('enable_live_ramp_sync', False):
            st.success(f"Ramp sync complete: {successes} succeeded, {failures} failed.")
        else:
            st.info(f"Dry run complete: {successes} would be marked synced (no live requests were sent).")

        if audit_path:
            st.markdown(f"Audit CSV written to `{audit_path}`")
            with open(audit_path, 'rb') as f:
                st.download_button("Download sync audit CSV", f, file_name=os.path.basename(audit_path))

def check_available_endpoints(client, cfg):
    """Check which API endpoints are available based on OAuth scopes."""
    endpoints_to_check = {
        'transactions': 'transactions:read',
        'bills': 'bills:read',
        'reimbursements': 'reimbursements:read',
        'cashbacks': 'cashbacks:read',
        'statements': 'statements:read',
        'accounting': 'accounting:read'
    }

    available = {}

    for endpoint, required_scope in endpoints_to_check.items():
        try:
            if endpoint == 'accounting':
                # For accounting, test a different endpoint or method
                url = f"{cfg['ramp']['base_url']}/transactions"
                resp = client.session.get(url, params={'limit': 1})
                available[endpoint] = resp.status_code == 200
            else:
                url = f"{cfg['ramp']['base_url']}/{endpoint}"
                resp = client.session.get(url, params={'limit': 1})
                available[endpoint] = resp.status_code == 200
        except Exception:
            available[endpoint] = False

    return available

def fetch_data_for_type(client, data_type, start_date, end_date, cfg):
    """Fetch data for a specific type and return (data, dataframe, processed_ids)

    processed_ids is a list of string ids for the items that were successfully
    transformed into DataFrame rows (parsed from the "Document No." column when present).
    """
    if data_type == 'transactions':
        data = client.get_transactions(
            status=cfg['ramp'].get('status_filter'),
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        # Filter out already-synced items when possible
        if isinstance(data, list) and data:
            before = len(data)
            data = [d for d in data if not client.is_transaction_synced(d)]
            after = len(data)
            if after < before:
                st.info(f"Skipped {before-after} transactions that were already marked synced in Ramp")

        df = ramp_credit_card_to_bc_rows(data, cfg)
    elif data_type == 'bills':
        data = client.get_bills(
            status='APPROVED',
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        if isinstance(data, list) and data:
            before = len(data)
            data = [d for d in data if not client.is_transaction_synced(d)]
            after = len(data)
            if after < before:
                st.info(f"Skipped {before-after} bills that were already marked synced in Ramp")
        df = ramp_bills_to_bc_rows(data, cfg)
    elif data_type == 'reimbursements':
        data = client.get_reimbursements(
            status='PAID',
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        if isinstance(data, list) and data:
            before = len(data)
            data = [d for d in data if not client.is_transaction_synced(d)]
            after = len(data)
            if after < before:
                st.info(f"Skipped {before-after} reimbursements that were already marked synced in Ramp")
        df = ramp_reimbursements_to_bc_rows(data, cfg)
    elif data_type == 'cashbacks':
        data = client.get_cashbacks(
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        if isinstance(data, list) and data:
            before = len(data)
            data = [d for d in data if not client.is_transaction_synced(d)]
            after = len(data)
            if after < before:
                st.info(f"Skipped {before-after} cashbacks that were already marked synced in Ramp")
        df = ramp_cashbacks_to_bc_rows(data, cfg)
    elif data_type == 'statements':
        data = client.get_statements(
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        if isinstance(data, list) and data:
            before = len(data)
            data = [d for d in data if not client.is_transaction_synced(d)]
            after = len(data)
            if after < before:
                st.info(f"Skipped {before-after} statements that were already marked synced in Ramp")
        df = ramp_statements_to_bc_rows(data, cfg)
    else:
        raise ValueError(f"Unknown data type: {data_type}")

    # Derive processed ids from DataFrame if possible
    processed_ids = []
    try:
        if df is not None and not df.empty and 'Document No.' in df.columns:
            for val in df['Document No.'].tolist():
                if not val:
                    continue
                # Often Document No. values are formatted like PREFIX-<id>
                if isinstance(val, str) and '-' in val:
                    _id = val.split('-', 1)[1]
                else:
                    _id = str(val)
                processed_ids.append(_id)
    except Exception:
        processed_ids = []

    return data, df, processed_ids


# Audit helper moved to `utils._write_sync_audit`

# Footer
st.markdown("""
<div class="footer">
    <div class="footer-title">Northwest Area Foundation</div>
    <p>Financial Data Integration Platform | Ramp → Business Central Export</p>
    <div class="footer-meta">Secure Enterprise Solution | Protected by Microsoft Azure AD</div>
</div>
""", unsafe_allow_html=True)