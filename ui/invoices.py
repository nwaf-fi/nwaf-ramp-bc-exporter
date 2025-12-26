import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from ramp_client import RampClient
from transform import (ramp_bills_to_purchase_invoice_lines,
                       ramp_bills_to_general_journal,
                       enrich_bills_with_vendor_external_ids)
from utils import _write_sync_audit


def render_invoices_tab(cfg, env):
    st.subheader("Purchase Invoice Export (Bills)")
    st.write("Generate Purchase Invoices CSV from approved Ramp bills in the selected date range.")

    # Local date range inputs for the Invoices panel (defaults to global sidebar dates)
    col_a, col_b = st.columns(2)
    with col_a:
        inv_start = st.date_input("Invoices: Start Date", value=st.session_state.get('inv_start_date', datetime.now().replace(day=1)), key='invoices_inv_start')
    with col_b:
        inv_end = st.date_input("Invoices: End Date", value=st.session_state.get('inv_end_date', datetime.now()), key='invoices_inv_end')

    include_audit = st.checkbox("Write audit NDJSON (export original bill objects)", value=False, key='invoices_pi_include_audit')
    confirm_mark = st.checkbox("I confirm: mark exported bills as synced (requires confirmation below)", value=False, key='invoices_pi_confirm_mark')

    # Offer a non-destructive preview that mirrors the 'Generate' behavior but does not cache or mark
    if st.button("Preview Purchase Invoices for date range", key='invoices_preview_pi_btn'):
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

                # Ask server to only return bills that are ready to sync
                bills = client.get_bills(status='PAID', start_date=start_date_str, end_date=end_date_str, page_size=cfg['ramp'].get('page_size', 200), sync_ready=True)
                total_bills = len(bills) if isinstance(bills, list) else 0
                if not bills:
                    st.info('No paid bills found for the specified period.')
                else:
                    st.success(f"Retrieved {total_bills} bills (preview)")

                    # Filter out already-synced for preview too
                    bills_preview = [b for b in bills if not client.is_transaction_synced(b)]

                    # Exclude items previously marked synced in-session (optimistic client-side filter)
                    synced_inv_ids = set(st.session_state.get('synced_invoices', []))
                    if synced_inv_ids:
                        before_local = len(bills_preview)
                        bills_preview = [b for b in bills_preview if b.get('id') not in synced_inv_ids]
                        local_filtered = before_local - len(bills_preview)
                        if local_filtered:
                            st.info(f"{local_filtered} bills excluded because they were previously marked synced in this session.")

                    # Enrich bills with vendor external ids from the Vendors API so
                    # 'Buy-from Vendor No.' is populated from vendor.external_id (authoritative)
                    try:
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

    if st.button("Generate Purchase Invoices for date range", key='invoices_gen_pi_btn'):
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

                # Ask server to only return bills that are ready to sync
                bills = client.get_bills(status='PAID', start_date=start_date_str, end_date=end_date_str, page_size=cfg['ramp'].get('page_size', 200), sync_ready=True)
                total_bills = len(bills) if isinstance(bills, list) else 0
                if not bills:
                    st.info('No paid bills found for the specified period.')
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

                # Exclude items previously marked synced in-session
                synced_inv_ids = set(st.session_state.get('synced_invoices', []))
                if synced_inv_ids:
                    before_local = len(bills)
                    bills = [b for b in bills if b.get('id') not in synced_inv_ids]
                    local_filtered = before_local - len(bills)
                    if local_filtered:
                        st.info(f"{local_filtered} bills excluded because they were previously marked synced in this session.")

                # Enrich bills with vendor external ids from Vendors API to populate
                # Buy-from Vendor No. (prefer vendor.external_vendor_id_resolved)
                try:
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
                    st.download_button("Download Purchase Invoices CSV", data=csv_bytes, file_name=fname, mime='text/csv', key='invoices_download_pi_csv')

                    # Excel
                    excel_buf = BytesIO()
                    with pd.ExcelWriter(excel_buf, engine='openpyxl') as writer:
                        pi_df.to_excel(writer, sheet_name='PurchaseInvoices', index=False)
                    excel_buf.seek(0)
                    fname_x = fname.replace('.csv', '.xlsx')
                    st.download_button("Download Purchase Invoices Excel", data=excel_buf, file_name=fname_x, mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', key='invoices_download_pi_xlsx')

                # General Journal
                if gj_df is not None and not gj_df.empty:
                    gj_csv = gj_df.to_csv(index=False).encode('utf-8')
                    gj_name = f"purchase_invoices_journal_{inv_start.strftime('%Y%m%d')}_{inv_end.strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
                    st.download_button("Download Purchase Invoices General Journal CSV", data=gj_csv, file_name=gj_name, mime='text/csv', key='invoices_download_gj_csv')

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
                            st.download_button("Download Bills Audit (NDJSON)", f, file_name=os.path.basename(audit_path), mime='application/x-ndjson', key='invoices_download_audit_ndjson')
                    except Exception:
                        st.warning("Could not write audit NDJSON file.")

                # If generation has completed and bills are present, offer a separate mark-as-synced action
                bills_cached = st.session_state.get('inv_bills')
                if bills_cached:
                    st.markdown('---')
                    st.subheader('Post-generation actions')
                    st.write(f"{len(bills_cached)} bills prepared for export (total ${st.session_state.get('inv_bill_total', 0.0):,.2f}).")

                    if st.checkbox('Enable marking these bills as synced (dry-run unless live sync enabled)', value=False, key='invoices_pi_enable_mark'):
                        if not st.session_state.get('invoices_pi_confirm_mark'):
                            st.warning('Please check the confirmation checkbox above to enable marking.')
                        else:
                            if st.button('Mark these bills as synced in Ramp', key='invoices_pi_mark_btn'):
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
                                            ok, msg = client.mark_transaction_synced_with_message(tid, sync_reference=sync_ref)
                                            results.append({'timestamp': datetime.now().isoformat(), 'transaction_id': tid, 'ok': ok, 'message': msg})
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
                                                st.download_button("Download bills sync audit CSV", f, file_name=os.path.basename(audit_path), key='invoices_download_sync_audit_csv')

                                    except Exception as e:
                                        st.error(f"Error marking bills as synced: {e}")

            except Exception as e:
                st.error(f"Error generating purchase invoices: {e}")