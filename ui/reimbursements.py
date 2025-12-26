import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import json

from transform import ramp_reimbursements_to_bc_rows
from utils import _write_sync_audit, get_ramp_client


def render_reimbursements_tab(cfg, env):
    st.subheader("Reimbursements Export")
    st.write("Export reimbursements (PAID) as BC general journal rows for the selected date range.")

    include_audit = st.checkbox("Write reimbursements audit NDJSON (export original objects)", value=False, key='reim_include_audit')
    mark_synced = st.checkbox("Mark exported reimbursements as synced in Ramp (dry-run unless live sync enabled)", value=False, key='reim_mark_synced')

    # Local date range inputs for Reimbursements (fallback to global sidebar dates if present)
    col_a, col_b = st.columns(2)
    with col_a:
        reim_start = st.date_input("Reimbursements: Start Date", value=st.session_state.get('reim_start_date', datetime.now().replace(day=1)), key='reim_start')
    with col_b:
        reim_end = st.date_input("Reimbursements: End Date", value=st.session_state.get('reim_end_date', datetime.now()), key='reim_end')

    # Add preview action for reimbursements (non-destructive)
    if st.button("Preview Reimbursements for date range", key='preview_reim_btn'):
        with st.spinner("Fetching reimbursements for preview..."):
            try:
                client = get_ramp_client(cfg, env, enable_sync=False)
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
                client = get_ramp_client(cfg, env, enable_sync=st.session_state.get('enable_live_ramp_sync', False))
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
