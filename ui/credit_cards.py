import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
import pandas as pd
from ramp_client import RampClient
from transform import ramp_credit_card_to_bc_rows
from utils import _extract_amount, _write_sync_audit


def render_credit_cards_tab(cfg, env):
    st.subheader("Credit Card Statement Export")
    st.write("Select a credit card statement from the dropdown to export transactions for that period.")

    # Fetch available statements on load
    if 'cc_statements' not in st.session_state:
        with st.spinner("Loading available statements..."):
            try:
                client = RampClient(
                    base_url=cfg['ramp']['base_url'],
                    token_url=cfg['ramp']['token_url'],
                    client_id=env['RAMP_CLIENT_ID'],
                    client_secret=env['RAMP_CLIENT_SECRET'],
                    enable_sync=False
                )
                client.authenticate()
                stmts = client.get_statements()
                st.session_state['cc_statements'] = stmts if stmts else []
            except Exception as e:
                st.error(f"Error loading statements: {e}")
                st.session_state['cc_statements'] = []

    stmts = st.session_state.get('cc_statements', [])
    
    if not stmts:
        st.info("No statements found for this account")
        if st.button("Refresh Statements", key='cc_refresh_statements'):
            st.session_state.pop('cc_statements', None)
            st.rerun()
    else:
        # Create statement options for dropdown
        statement_options = []
        for stmt in stmts:
            start_date = (stmt.get('start_date') or '')[:10]
            end_date = (stmt.get('end_date') or '')[:10]
            stmt_id = stmt.get('id', 'unknown')
            # Format: "Dec 01, 2025 - Dec 31, 2025 (ID: abc123)"
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                label = f"{start_dt.strftime('%b %d, %Y')} - {end_dt.strftime('%b %d, %Y')}"
            except:
                label = f"{start_date} - {end_date}"
            statement_options.append((label, stmt_id, start_date, end_date, stmt))
        
        # Dropdown to select statement
        selected_idx = st.selectbox(
            "Select Statement Period",
            range(len(statement_options)),
            format_func=lambda i: statement_options[i][0],
            key='cc_statement_selector'
        )
        
        selected_stmt_data = statement_options[selected_idx]
        selected_label, selected_id, start_date_str, end_date_str, latest_stmt = selected_stmt_data
        
        # Display selected statement info
        st.markdown(f"**Statement period:** {start_date_str} → {end_date_str}")
        
        # Auto-populate date fields (for reference/display)
        col_a, col_b = st.columns(2)
        with col_a:
            try:
                start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            except:
                start_date_obj = datetime.now().date()
            st.date_input("Start Date", value=start_date_obj, key='cc_start_date', disabled=True)
        with col_b:
            try:
                end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            except:
                end_date_obj = datetime.now().date()
            st.date_input("End Date", value=end_date_obj, key='cc_end_date', disabled=True)
        
        # Generate button
        if st.button("Generate Credit Card Journal for Selected Statement", key='cc_gen_stmt_btn'):
            with st.spinner("Fetching transactions for selected statement..."):
                try:
                    client = RampClient(
                        base_url=cfg['ramp']['base_url'],
                        token_url=cfg['ramp']['token_url'],
                        client_id=env['RAMP_CLIENT_ID'],
                        client_secret=env['RAMP_CLIENT_SECRET'],
                        enable_sync=st.session_state.get('enable_live_ramp_sync', False)
                    )
                    client.authenticate()

                    # Get authoritative statement totals (try charges -> balance_sections -> ending_balance)
                    stmt_charges = _extract_amount(latest_stmt.get('charges') or {})
                    if not stmt_charges:
                        bsecs = latest_stmt.get('balance_sections') or []
                        if bsecs:
                            stmt_charges = _extract_amount(bsecs[0].get('charges') or {})
                    st.write(f"Statement charges (major units): **${stmt_charges:,.2f}**")

                    # Collect CARD_TRANSACTION ids from statement_lines
                    lines = latest_stmt.get('statement_lines') or []
                    card_ids = [l.get('id') for l in lines if l.get('type') == 'CARD_TRANSACTION']
                    st.write(f"Statement transaction count: **{len(card_ids)}**")

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

                    strict_mode = st.checkbox("Abort export if totals mismatch (strict)", value=False, help="When enabled, the export will be disabled if statement total does not match transaction sum.", key='cc_strict_mode')
                    mismatch = abs(stmt_charges - tx_total) > 0.01
                    if mismatch:
                        st.warning("Statement total does not match the transaction total.")
                        if strict_mode:
                            st.error("Strict mode enabled: export disabled due to totals mismatch.")

                    # Exclude items previously synced in this session (optimistic client-side filter)
                    synced_cc_ids = set(st.session_state.get('synced_cc_transactions', []))
                    if synced_cc_ids:
                        before_local = len(fetched)
                        fetched = [t for t in fetched if t.get('id') not in synced_cc_ids]
                        local_filtered = before_local - len(fetched)
                        if local_filtered:
                            st.info(f"{local_filtered} transactions excluded because they were previously marked synced in this session.")

                    for t in fetched:
                        if t.get('accounting_date'):
                            t['payment_date'] = t.get('accounting_date')

                    df = ramp_credit_card_to_bc_rows(fetched, cfg, write_audit=False, statement=latest_stmt)

                    st.subheader("Preview (first 10 rows)")
                    if df is None or df.empty:
                        st.info("No credit-card rows generated. Check mapping or that transactions are fully coded with GL accounts.")
                    else:
                        st.dataframe(df.head(10), use_container_width=True)

                    if df is not None and not df.empty and (not (strict_mode and mismatch)):
                        csv_bytes = df.to_csv(index=False).encode('utf-8')
                        fname = f"v2_cc_statement_journal_{start_date_str.replace('-', '')}_{end_date_str.replace('-', '')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
                        st.download_button("Download CC Journal (Selected Statement)", data=csv_bytes, file_name=fname, mime='text/csv', key='cc_download_journal')

                        st.download_button("Download CC Journal (Selected Statement)", data=csv_bytes, file_name=fname, mime='text/csv', key='cc_download_journal')

                        # Post-generation sync actions (interactive, per-run)
                        cc_cached = fetched
                        if cc_cached:
                            st.markdown('---')
                            st.subheader('Post-generation actions')
                            st.write(f"{len(cc_cached)} transactions prepared for export (total ${tx_total:,.2f}).")

                            with st.expander('Mark transactions as synced', expanded=False):
                                # Check whether API-based accounting sync is supported by the connected accounting connection
                                sync_supported = client.check_accounting_sync_enabled()
                                if not sync_supported:
                                    st.warning('Your accounting connection does not support API-based syncing. See the Ramp docs and consider upgrading the accounting connection to an API-enabled connection.')
                                    st.markdown('[Ramp accounting docs](https://docs.ramp.com/developer-api/v1/guides/accounting)')

                                # Live sync toggle: disabled if API sync is not supported
                                enable_live = st.checkbox('Enable live Ramp sync (will POST to Ramp)', value=False, key='cc_enable_live_sync', disabled=(not sync_supported))
                                if not enable_live:
                                    if sync_supported:
                                        st.info('Dry-run mode: no live requests will be sent. Toggle above to perform live requests.')
                                    else:
                                        st.info('API-based sync is not available for this accounting connection. Choose Local-only to record syncs locally.')

                                local_only = False
                                if not sync_supported:
                                    local_only = st.checkbox('Record these transactions as synced locally only (no Ramp API calls)', value=True, key='cc_local_only')

                                if st.checkbox('I confirm: mark these transactions as synced', value=False, key='cc_confirm_mark'):
                                    if st.button('Mark these transactions as synced in Ramp', key='cc_mark_btn'):
                                        with st.spinner('Marking transactions as synced...'):
                                            sync_ref = f"BC_CCExport_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                                            results = []
                                            progress = st.progress(0)
                                            total = len(cc_cached)
                                            i = 0

                                            if local_only:
                                                # Record local-only successes
                                                for t in cc_cached:
                                                    i += 1
                                                    tid = t.get('id')
                                                    results.append({'timestamp': datetime.now().isoformat(), 'transaction_id': tid, 'ok': True, 'message': 'LOCAL_ONLY'})
                                                    progress.progress(i / total)
                                            else:
                                                marker = RampClient(
                                                    base_url=cfg['ramp']['base_url'],
                                                    token_url=cfg['ramp']['token_url'],
                                                    client_id=env['RAMP_CLIENT_ID'],
                                                    client_secret=env['RAMP_CLIENT_SECRET'],
                                                    enable_sync=enable_live
                                                )
                                                marker.authenticate()

                                                for t in cc_cached:
                                                    i += 1
                                                    tid = t.get('id')
                                                    ok, msg = marker.mark_transaction_synced_with_message(tid, sync_reference=sync_ref)
                                                    results.append({'timestamp': datetime.now().isoformat(), 'transaction_id': tid, 'ok': ok, 'message': msg})
                                                    progress.progress(i / total)

                                            successes = sum(1 for r in results if r['ok'])
                                            failures = len(results) - successes

                                            if (not local_only) and enable_live:
                                                st.success(f"Ramp sync complete: {successes} succeeded, {failures} failed.")
                                            elif local_only:
                                                st.success(f"Local-only: {successes} recorded locally as synced.")
                                            else:
                                                st.info(f"Dry run complete: {successes} would be marked synced (no live requests were sent).")

                                            user_email_local = globals().get('user_email', '')
                                            audit_path = _write_sync_audit(results, sync_ref, user_email=user_email_local)
                                            if audit_path:
                                                with open(audit_path, 'rb') as f:
                                                    st.download_button("Download CC sync audit CSV", f, file_name=os.path.basename(audit_path), key='cc_download_sync_audit_csv')

                                            res_df = pd.DataFrame(results)
                                            if not res_df.empty:
                                                st.subheader('Sync Results')
                                                st.write(res_df)

                                            # Record successful syncs in-session to exclude from future pulls
                                            if (not local_only) and enable_live and successes:
                                                synced_ids = set(st.session_state.get('synced_cc_transactions', []))
                                                for rres in results:
                                                    if rres.get('ok'):
                                                        synced_ids.add(str(rres.get('transaction_id')))
                                                st.session_state['synced_cc_transactions'] = list(synced_ids)

                except Exception as ex:
                    st.error(f"Error fetching statement: {ex}")
