import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import sys
import os
import streamlit.components.v1 as components

# -------------------------
# Page configuration
# -------------------------
st.set_page_config(
    page_title="Ramp → Business Central Export",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -------------------------
# Azure redirect fix (unchanged)
# -------------------------
components.html(
    """
    <script>
    (function() {
        try {
            const p = window.location.pathname || '/';
            if (p && p !== '/' && p.includes('oauth2callback')) {
                const q = window.location.search || '';
                try {
                    if (window.top && window.top !== window) {
                        window.top.location.replace('/' + q);
                    } else {
                        window.location.replace('/' + q);
                    }
                } catch (e) {
                    try { document.location = '/' + q; }
                    catch(e2) { window.location.replace('/' + q); }
                }
            }
        } catch (e) {}
    })();
    </script>
    """,
    height=0,
)

# -------------------------
# Authentication
# -------------------------
from auth.azure_auth import ensure_authenticated, REDIRECT_URI
user_name, user_email = ensure_authenticated()

# -------------------------
# Path setup
# -------------------------
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# -------------------------
# Internal imports
# -------------------------
from utils import load_env, load_config, _extract_amount, _write_sync_audit
from ramp_client import RampClient
from transform import (
    ramp_credit_card_to_bc_rows,
    ramp_bills_to_bc_rows,
    ramp_reimbursements_to_bc_rows,
    ramp_cashbacks_to_bc_rows,
    ramp_statements_to_bc_rows,
)
from ui.layout import load_css, render_header, render_sidebar

# -------------------------
# UI chrome
# -------------------------
load_css()
render_header()
render_sidebar(user_name, user_email)

# -------------------------
# Configuration loading
# -------------------------
try:
    if os.path.exists('.env'):
        env = load_env()
    else:
        env = {
            'RAMP_CLIENT_ID': st.secrets.get('RAMP_CLIENT_ID'),
            'RAMP_CLIENT_SECRET': st.secrets.get('RAMP_CLIENT_SECRET')
        }
    cfg = load_config()
except Exception:
    st.error("Configuration Error: Unable to load required settings.")
    st.markdown("Please contact your system administrator.")
    st.stop()

# ============================================================
# MAIN CONTENT
# ============================================================

st.markdown("""
<div class="info-card">
    <h3>Export Financial Data</h3>
    <p>
        Select a data source below to preview, export, and download
        Business Central–compatible files from Ramp.
    </p>
</div>
""", unsafe_allow_html=True)

# -------------------------
# Tabs
# -------------------------
cc_tab, inv_tab, reimb_tab = st.tabs([
    "💳 Credit Card Transactions",
    "🧾 Vendor Invoices",
    "💸 Employee Reimbursements"
])

from ui.credit_cards import render_credit_cards_tab
from ui.invoices import render_invoices_tab
from ui.reimbursements import render_reimbursements_tab

with cc_tab:
    render_credit_cards_tab(cfg, env)

with inv_tab:
    render_invoices_tab(cfg, env)

with reimb_tab:
    render_reimbursements_tab(cfg, env)

# ============================================================
# SIDEBAR: STATUS & CONTEXT
# ============================================================

st.sidebar.markdown('<div class="section-header">System Status</div>', unsafe_allow_html=True)

if 'latest_statement' not in st.session_state:
    st.session_state.latest_statement = None
    st.session_state.latest_statement_at = None

with st.sidebar.expander('📄 Latest Credit Card Statement', expanded=False):
    if st.button('Refresh latest statement', key='refresh_statement'):
        st.session_state.latest_statement = None
        st.session_state.latest_statement_at = None

    if st.session_state.latest_statement is None:
        try:
            sc = RampClient(
                base_url=cfg['ramp']['base_url'],
                token_url=cfg['ramp']['token_url'],
                client_id=env['RAMP_CLIENT_ID'],
                client_secret=env['RAMP_CLIENT_SECRET']
            )
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

        charges = _extract_amount(stmt.get('charges') or {})
        if not charges:
            bsecs = stmt.get('balance_sections') or []
            if bsecs:
                charges = _extract_amount(bsecs[0].get('charges') or {})

        st.markdown(f"**Charges:** ${charges:,.2f}")
        st.caption(f"Fetched at {st.session_state.latest_statement_at}")
    else:
        st.markdown("_No statement cached_")

# ============================================================
# EXPORT ENGINE (LOGIC UNCHANGED)
# ============================================================

def run_export(selected_types, start_date, end_date, cfg, env):

    st.markdown("""
    <div class="info-card">
        <h3>Running Export</h3>
        <p>
            Authenticating with Ramp, retrieving data, transforming records,
            and preparing Business Central–ready export files.
        </p>
    </div>
    """, unsafe_allow_html=True)

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
        except Exception:
            st.error("Authentication failed. Please contact administrator.")
            return

    with st.spinner("Checking API availability..."):
        available_endpoints = check_available_endpoints(client, cfg)

    available_selected_types = [t for t in selected_types if available_endpoints.get(t, False)]

    if not available_selected_types:
        st.error("None of the selected data types are available.")
        return

    progress_bar = st.progress(0)
    status_text = st.empty()

    combined_df = None
    total_records = 0
    exported_transaction_ids = set()

    for i, data_type in enumerate(available_selected_types, start=1):
        status_text.text(f"Fetching {data_type}...")

        try:
            data, df, processed_ids = fetch_data_for_type(
                client,
                data_type,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                cfg
            )

            if data:
                total_records += len(data)
                combined_df = df if combined_df is None else pd.concat([combined_df, df], ignore_index=True)
                for tid in processed_ids:
                    exported_transaction_ids.add(str(tid))

        except Exception as e:
            st.error(f"Error fetching {data_type}: {str(e)}")

        progress_bar.progress(i / len(available_selected_types))

    progress_bar.empty()
    status_text.empty()

    if combined_df is None or combined_df.empty:
        st.error("No data returned for the selected period.")
        return

    st.success(f"Export complete: {total_records} records processed")

    st.subheader("Preview")
    st.dataframe(combined_df.head(10), use_container_width=True)

    st.markdown("""
    <div class="info-card">
        <h3>Download Files</h3>
        <p>Your export is ready.</p>
    </div>
    """, unsafe_allow_html=True)

    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        combined_df.to_excel(writer, index=False)
    excel_buffer.seek(0)

    csv_buffer = BytesIO()
    combined_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "⬇️ Download Excel (.xlsx)",
            excel_buffer,
            f"Ramp_BC_Export_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    with c2:
        st.download_button(
            "⬇️ Download CSV (.csv)",
            csv_buffer,
            f"Ramp_BC_Export_{datetime.now():%Y%m%d_%H%M%S}.csv",
            mime="text/csv",
            use_container_width=True
        )

    # ⚠️ Advanced actions (unchanged logic)
    if exported_transaction_ids:
        with st.expander("⚠️ Post-export actions (Advanced)", expanded=False):
            st.write(f"{len(exported_transaction_ids)} transactions prepared for sync.")
            mark_after_export = st.checkbox("Mark exported transactions as synced", value=False)
            enable_live_sync = st.checkbox("Enable live Ramp sync", value=False)

            if mark_after_export:
                st.warning("This action updates Ramp records.")
                confirm = st.checkbox("I confirm this action")
                if confirm and st.button("Mark transactions as synced"):
                    results = []
                    for tid in exported_transaction_ids:
                        ok = client.mark_transaction_synced(tid)
                        results.append(ok)

                    if enable_live_sync:
                        st.success("Ramp sync completed.")
                    else:
                        st.info("Dry run completed.")

# -------------------------
# Footer
# -------------------------
st.markdown("""
<div class="footer">
    <div class="footer-title">Northwest Area Foundation</div>
    <p>Ramp → Business Central Export Platform</p>
    <div class="footer-meta">Secure Enterprise Solution · Microsoft Azure AD</div>
</div>
""", unsafe_allow_html=True)
