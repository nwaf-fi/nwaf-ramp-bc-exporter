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


# Add current directory to path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import load_env, load_config
from ramp_client import RampClient
from transform import (ramp_credit_card_to_bc_rows, ramp_bills_to_bc_rows,
                      ramp_reimbursements_to_bc_rows, ramp_cashbacks_to_bc_rows,
                      ramp_statements_to_bc_rows, ramp_bills_to_purchase_invoice_lines,
                      ramp_bills_to_general_journal)
from bc_export import export

from ui.layout import load_css, render_header, render_sidebar

# Apply styles and render the header (layout helpers are extracted into ui/layout.py)
load_css()
render_header()
# Render sidebar content (user profile and system overview)
render_sidebar(user_name, user_email)

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

from ui.credit_cards import render_credit_cards_tab

with cc_tab:
    render_credit_cards_tab(cfg, env)

from ui.invoices import render_invoices_tab
from ui.reimbursements import render_reimbursements_tab

with inv_tab:
    render_invoices_tab(cfg, env)

with reimb_tab:
    render_reimbursements_tab(cfg, env)

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