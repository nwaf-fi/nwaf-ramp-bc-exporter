import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import json
import os

from ramp_client import RampClient
from transform import ramp_reimbursements_to_bc_rows
from utils import _write_sync_audit


def render_reimbursements_tab(cfg, env):
    st.subheader("Reimbursements Export")
    st.write("Export reimbursements (PAID) as BC general journal rows for the selected date range.")

    include_audit = st.checkbox("Write reimbursements audit NDJSON (export original objects)", value=False, key='reimbursements_reim_include_audit')
    mark_synced = st.checkbox("Mark exported reimbursements as synced in Ramp (dry-run unless live sync enabled)", value=False, key='reimbursements_reim_mark_synced')

    # Local date range inputs for Reimbursements (fallback to global sidebar dates if present)
    col_a, col_b = st.columns(2)
    with col_a:
        reim_start = st.date_input("Reimbursements: Start Date", value=st.session_state.get('reim_start_date', datetime.now().replace(day=1)), key='reimbursements_reim_start')
    with col_b:
        reim_end = st.date_input("Reimbursements: End Date", value=st.session_state.get('reim_end_date', datetime.now()), key='reimbursements_reim_end')

    # Add preview action for reimbursements (non-destructive)
    if st.button("Preview Reimbursements for date range", key='reimbursements_preview_reim_btn'):
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
                # Ask server to only return reimbursements that haven't been synced yet
                reims = client.get_reimbursements(status='PAID', start_date=start_date_str, end_date=end_date_str, page_size=cfg['ramp'].get('page_size', 200), has_no_sync_commits=True)

                if not reims:
                    st.info('No reimbursements found for the specified period.')
                else:
                    st.success(f"Retrieved {len(reims)} reimbursements (preview)")

                    # Filter already-synced
                    reims_preview = [r for r in reims if not client.is_transaction_synced(r)]

                    # Additional date filtering: Ramp APIs may apply different date fields; ensure we only include reimbursements whose
                    # relevant date falls within the selected start/end (inclusive). Prefer 'paid_at' then 'posted_at', then 'created_at'.
                    def _reim_date_str(r):
                        for k in ('paid_at', 'posted_at', 'created_at'):
                            v = r.get(k)
                            if v:
                                return v[:10]
                        return None

                    filtered = []
                    filtered_out = 0
                    for r in reims_preview:
                        ds = _reim_date_str(r)
                        if ds:
                            try:
                                d = datetime.fromisoformat(ds).date()
                                if d >= reim_start and d <= reim_end:
                                    filtered.append(r)
                                else:
                                    filtered_out += 1
                            except Exception:
                                # If parsing fails, keep the reimbursement to be safe
                                filtered.append(r)
                        else:
                            # No date available — keep for manual inspection
                            filtered.append(r)

                    reims_preview = filtered

                    # Exclude items already synced in this session (optimistic local filter)
                    synced_ids = set(st.session_state.get('synced_reimbursements', []))
                    if synced_ids:
                        before_local = len(reims_preview)
                        reims_preview = [r for r in reims_preview if r.get('id') not in synced_ids]
                        local_filtered = before_local - len(reims_preview)
                        if local_filtered:
                            st.info(f"{local_filtered} reimbursements excluded because they were previously marked synced in this session.")

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

                    if filtered_out:
                        st.info(f"{filtered_out} reimbursements were excluded because their dates fall outside the selected range.")

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
                        st.download_button("Download Reimbursements CSV (preview)", data=csv_bytes, file_name=fname, mime='text/csv', key='reimbursements_download_preview_csv')

            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                st.error("Error during preview. See details below.")
                with st.expander("Preview error details (expand for stack trace)"):
                    st.code(tb)
                import logging
                logging.exception("Preview error: %s", tb)


    if st.button("Generate Reimbursements JE for date range", key='reimbursements_gen_reim_btn'):
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
                # Ask server to only return reimbursements that haven't been synced yet
                reims = client.get_reimbursements(status='PAID', start_date=start_date_str, end_date=end_date_str, page_size=cfg['ramp'].get('page_size', 200), has_no_sync_commits=True)

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

                # Additional date filtering (prefer 'paid_at', 'posted_at', then 'created_at')
                def _reim_date_str(r):
                    for k in ('paid_at', 'posted_at', 'created_at'):
                        v = r.get(k)
                        if v:
                            return v[:10]
                    return None

                filtered = []
                filtered_out = 0
                for r in reims:
                    ds = _reim_date_str(r)
                    if ds:
                        try:
                            d = datetime.fromisoformat(ds).date()
                            if d >= reim_start and d <= reim_end:
                                filtered.append(r)
                            else:
                                filtered_out += 1
                        except Exception:
                            filtered.append(r)
                    else:
                        filtered.append(r)

                reims = filtered
                if filtered_out:
                    st.info(f"{filtered_out} reimbursements were excluded because their dates fall outside the selected range.")

                # Exclude items previously synced in this session
                synced_ids = set(st.session_state.get('synced_reimbursements', []))
                if synced_ids:
                    before_local = len(reims)
                    reims = [r for r in reims if r.get('id') not in synced_ids]
                    local_filtered = before_local - len(reims)
                    if local_filtered:
                        st.info(f"{local_filtered} reimbursements excluded because they were previously marked synced in this session.")

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
                    st.download_button("Download Reimbursements CSV", data=csv_bytes, file_name=fname, mime='text/csv', key='reimbursements_download_csv')

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
                            st.download_button("Download Reimbursements Audit (NDJSON)", f, file_name=os.path.basename(audit_path), mime='application/x-ndjson', key='reimbursements_download_audit_ndjson')
                    except Exception:
                        st.warning('Could not write audit NDJSON file.')

                # Optionally mark reimbursements as synced (interactive flow)
                if mark_synced and reims:
                    with st.expander('Mark reimbursements as synced', expanded=False):
                        st.write(f"{len(reims)} reimbursements are available to mark as synced.")

                        enable_live = st.checkbox('Enable live Ramp sync (will POST to Ramp)', value=False, key='reim_enable_live_sync')
                        if not enable_live:
                            st.info('Dry-run mode: no live requests will be sent. Toggle the checkbox above to perform live requests.')

                        if st.checkbox('I confirm: mark these reimbursements as synced', value=False, key='reim_confirm_mark'):
                            if st.button('Mark reimbursements as synced in Ramp', key='reim_mark_btn'):
                                with st.spinner('Marking reimbursements as synced...'):
                                    sync_ref = f"BC_ReimExport_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                                    results = []
                                    progress = st.progress(0)
                                    total = len(reims)
                                    i = 0

                                    # Use a dedicated client for marking with explicit enable_sync
                                    marker_client = RampClient(
                                        base_url=cfg['ramp']['base_url'],
                                        token_url=cfg['ramp']['token_url'],
                                        client_id=env['RAMP_CLIENT_ID'],
                                        client_secret=env['RAMP_CLIENT_SECRET'],
                                        enable_sync=enable_live
                                    )
                                    marker_client.authenticate()

                                    for r in reims:
                                        i += 1
                                        tid = r.get('id')
                                        ok = marker_client.mark_transaction_synced(tid, sync_reference=sync_ref)
                                        results.append({'timestamp': datetime.now().isoformat(), 'transaction_id': tid, 'ok': ok, 'message': ''})
                                        progress.progress(i / total)

                                    successes = sum(1 for res in results if res['ok'])
                                    failures = len(results) - successes

                                    if enable_live:
                                        st.success(f"Ramp sync complete: {successes} succeeded, {failures} failed.")
                                    else:
                                        st.info(f"Dry run complete: {successes} would be marked synced (no live requests were sent).")

                                    # Write audit CSV
                                    user_email_local = globals().get('user_email', '')
                                    audit_path = _write_sync_audit(results, sync_ref, user_email=user_email_local)
                                    if audit_path:
                                        with open(audit_path, 'rb') as f:
                                            st.download_button("Download reimbursements sync audit CSV", f, file_name=os.path.basename(audit_path), key='reimbursements_download_sync_audit_csv')

                                    # Show success/failure lists
                                    res_df = pd.DataFrame(results)
                                    if not res_df.empty:
                                        st.subheader('Sync Results')
                                        st.write(res_df)

                                    # If live sync actually performed and succeeded for some items, add to session exclusion set
                                    if enable_live and successes:
                                        synced_ids = set(st.session_state.get('synced_reimbursements', []))
                                        for rres in results:
                                            if rres.get('ok'):
                                                synced_ids.add(str(rres.get('transaction_id')))
                                        st.session_state['synced_reimbursements'] = list(synced_ids)
                                        st.success('Successful syncs have been recorded in-session and will be omitted from future pulls.')

            except Exception as e:
                st.error(f"Error generating reimbursements: {e}")
