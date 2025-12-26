import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
from ramp_client import RampClient
from transform import ramp_credit_card_to_bc_rows
from utils import _extract_amount, _write_sync_audit


def render_credit_cards_tab(cfg, env):
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

                    for t in fetched:
                        if t.get('accounting_date'):
                            t['payment_date'] = t.get('accounting_date')

                    df = ramp_credit_card_to_bc_rows(fetched, cfg, write_audit=False)

                    st.subheader("Preview (first 10 rows)")
                    if df is None or df.empty:
                        st.info("No credit-card rows generated. Check mapping or that transactions are fully coded with GL accounts.")
                    else:
                        st.dataframe(df.head(10), use_container_width=True)

                    if df is not None and not df.empty and (not (strict_mode and mismatch)):
                        csv_bytes = df.to_csv(index=False).encode('utf-8')
                        fname = f"v2_cc_statement_journal_{s.replace('-', '')}_{e.replace('-', '')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
                        st.download_button("Download CC Journal (Latest Statement)", data=csv_bytes, file_name=fname, mime='text/csv', key='cc_download_journal')

                        # Post-generation sync actions (enable optional marking of transactions as synced)
                        cc_cached = fetched
                        if cc_cached:
                            st.markdown('---')
                            st.subheader('Post-generation actions')
                            st.write(f"{len(cc_cached)} transactions prepared for export (total ${tx_total:,.2f}).")

                            if st.checkbox('Enable marking these transactions as synced (dry-run unless live sync enabled)', value=False, key='cc_enable_mark'):
                                if not st.session_state.get('cc_confirm_mark'):
                                    st.checkbox('I confirm: mark exported credit card transactions as synced', value=False, key='cc_confirm_mark')
                                else:
                                    if st.button('Mark these transactions as synced in Ramp', key='cc_mark_btn'):
                                        with st.spinner('Marking transactions as synced...'):
                                            try:
                                                client2 = RampClient(
                                                    base_url=cfg['ramp']['base_url'],
                                                    token_url=cfg['ramp']['token_url'],
                                                    client_id=env['RAMP_CLIENT_ID'],
                                                    client_secret=env['RAMP_CLIENT_SECRET'],
                                                    enable_sync=st.session_state.get('enable_live_ramp_sync', False)
                                                )
                                                client2.authenticate()

                                                sync_ref = f"BC_CCExport_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                                                results = []
                                                progress = st.progress(0)
                                                total = len(cc_cached)
                                                i = 0
                                                for t in cc_cached:
                                                    i += 1
                                                    tid = t.get('id')
                                                    ok = client2.mark_transaction_synced(tid, sync_reference=sync_ref)
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
                                                    with open(audit_path, 'rb') as f:
                                                        st.download_button("Download CC sync audit CSV", f, file_name=os.path.basename(audit_path), key='cc_download_sync_audit_csv')

                                            except Exception as e:
                                                st.error(f"Error marking credit card transactions as synced: {e}")

            except Exception as ex:
                st.error(f"Error fetching statement: {ex}")
