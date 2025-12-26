import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from ramp_client import RampClient
from transform import ramp_credit_card_to_bc_rows
from utils import _extract_amount, get_ramp_client


def render_credit_cards_tab(cfg, env):
    st.subheader("Credit Card Statement Export")
    st.write("This panel automatically fetches the latest card statement and prepares a Business Central journal CSV for download.")

    auto_fetch = True  # per UX requirement: automatically fetch latest statement on render

    if auto_fetch:
        with st.spinner("Authenticating and fetching latest statement..."):
            try:
                client = get_ramp_client(cfg, env, enable_sync=st.session_state.get('enable_live_ramp_sync', False))

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

                    strict_mode = st.checkbox("Abort export if totals mismatch (strict)", value=False, help="When enabled, the export will be disabled if statement total does not match transaction sum.")
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
                        st.download_button("Download CC Journal (Latest Statement)", data=csv_bytes, file_name=fname, mime='text/csv')

            except Exception as ex:
                st.error(f"Error fetching statement: {ex}")
