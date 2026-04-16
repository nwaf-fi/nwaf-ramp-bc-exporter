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
    st.write("Generate Purchase Invoices CSV from approved Ramp bills by payment send date (for bank reconciliation).")

    # Local date range inputs for the Invoices panel (defaults to global sidebar dates)
    col_a, col_b = st.columns(2)
    with col_a:
        inv_start = st.date_input("Payment Send Date: Start", value=st.session_state.get('inv_start_date', datetime.now().replace(day=1)), key='invoices_inv_start')
    import warnings
    warnings.warn(
        "This archived UI file was moved to archive.legacy_scripts.ui_invoices_backup; import from there if needed.",
        DeprecationWarning,
        stacklevel=2,
    )
    from archive.legacy_scripts.ui_invoices_backup import *
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

                # Filter by payment send date for bank reconciliation
                # Note: Only fetch PAID bills since OPEN bills don't have paid_at dates yet
                from_payment_dt = inv_start
                to_payment_dt = inv_end

                # Fetch only PAID bills (these have paid_at dates for bank reconciliation)
                bills_paid = client.get_bills(status='PAID', page_size=cfg['ramp'].get('page_size', 200), sync_ready=True)
                
                all_bills = bills_paid or []
                
                # Filter client-side by payment date
                bills = []
                for b in all_bills:
                    # payment_date is nested in payment.payment_date, paid_at is direct field
                    payment_info = b.get('payment') or {}
                    payment_date_str = payment_info.get('payment_date') or b.get('paid_at')
                    if payment_date_str:
                        try:
                            payment_dt = datetime.fromisoformat(payment_date_str[:10])
                            if from_payment_dt <= payment_dt.date() <= to_payment_dt:
                                bills.append(b)
                        except:
                            pass
                
                total_bills = len(bills) if isinstance(bills, list) else 0
                if not bills:
                    st.info('No open or paid bills found for the specified period.')
                    st.session_state.pop('inv_bills', None)
                    st.stop()
                
                st.success(f"Retrieved {total_bills} bills from Ramp")

                # Remove already-synced bills when possible
                # before = len(bills)
                # bills = [b for b in bills if not client.is_transaction_synced(b)]
                # after = len(bills)
                # skipped = before - after
                # if skipped:
                #     st.info(f"Skipped {skipped} bills already marked synced in Ramp")

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

                # Calculate totals and count from DataFrame to ensure consistency
                pi_total = pi_df['Amount'].sum() if pi_df is not None and not pi_df.empty and 'Amount' in pi_df.columns else 0.0
                bill_total = pi_total  # Use same source for total amount

                # Store results in session for subsequent actions
                st.session_state['inv_bills'] = bills
                st.session_state['inv_pi_df'] = pi_df
                st.session_state['inv_gj_df'] = gj_df
                st.session_state['inv_bill_total'] = bill_total

                # Count unique Document No. values (BC purchase invoice header) when available; otherwise fall back to bill list length.
                bill_count_filtered = len(pi_df['Document No.'].unique()) if pi_df is not None and not pi_df.empty and 'Document No.' in pi_df.columns else len(bills)

                st.write(f"Bills count after filtering: **{bill_count_filtered}**  — Total amount: **${bill_total:,.2f}**")
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
                            # Capability check
                            sync_supported = client.check_accounting_sync_enabled()
                            if not sync_supported:
                                st.warning('Your accounting connection does not support API-based syncing. See Ramp docs to upgrade the connection.')
                                st.markdown('[Ramp accounting docs](https://docs.ramp.com/developer-api/v1/guides/accounting)')

                            enable_live = st.checkbox('Enable live Ramp sync (will POST to Ramp)', value=False, key='inv_enable_live_sync', disabled=(not sync_supported))
                            if not enable_live:
                                if sync_supported:
                                    st.info('Dry-run mode: no live requests will be sent. Toggle above to perform live requests.')
                                else:
                                    st.info('API-based sync is not available for this accounting connection. Choose Local-only to record syncs locally.')

                            local_only = False
                            if not sync_supported:
                                local_only = st.checkbox('Record these bills as synced locally only (no Ramp API calls)', value=True, key='inv_local_only')

                            if st.button('Mark these bills as synced in Ramp', key='invoices_pi_mark_btn'):
                                with st.spinner('Marking bills as synced...'):
                                    try:
                                        sync_ref = f"BC_BillExport_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                                        results = []
                                        progress = st.progress(0)
                                        total = len(bills_cached)
                                        i = 0

                                        if local_only:
                                            for b in bills_cached:
                                                i += 1
                                                tid = b.get('id')
                                                results.append({'timestamp': datetime.now().isoformat(), 'transaction_id': tid, 'ok': True, 'message': 'LOCAL_ONLY'})
                                                progress.progress(i / total)
                                        else:
                                            client = RampClient(
                                                base_url=cfg['ramp']['base_url'],
                                                token_url=cfg['ramp']['token_url'],
                                                client_id=env['RAMP_CLIENT_ID'],
                                                client_secret=env['RAMP_CLIENT_SECRET'],
                                                enable_sync=enable_live
                                            )
                                            client.authenticate()

                                            for b in bills_cached:
                                                i += 1
                                                tid = b.get('id')
                                                ok, msg = client.mark_transaction_synced_with_message(tid, sync_reference=sync_ref)
                                                results.append({'timestamp': datetime.now().isoformat(), 'transaction_id': tid, 'ok': ok, 'message': msg})
                                                progress.progress(i / total)

                                        successes = sum(1 for r in results if r['ok'])
                                        failures = len(results) - successes
                                        if (not local_only) and st.session_state.get('enable_live_ramp_sync', False):
                                            st.success(f"Ramp sync complete: {successes} succeeded, {failures} failed.")
                                        elif local_only:
                                            st.success(f"Local-only: {successes} recorded locally as synced.")
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