import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import os
import json

from ramp_client import RampClient
from lib.ramp_client import _date_to_iso
from transform import (ramp_bills_to_purchase_invoice_lines,
                       ramp_bills_to_general_journal,
                       enrich_bills_with_vendor_external_ids)


def render_invoices_tab(cfg, env):
    st.subheader("Purchase Invoice Export (Bills)")
    st.write("Generate Purchase Invoices CSV from Ramp bills filtered by payment send date (for bank reconciliation).")

    # Debug section
    with st.expander("🔍 Debug: Test Bill Count by Date Range"):
        st.write("Quick test to count bills with payment dates in a range (does not generate exports)")
        debug_col1, debug_col2 = st.columns(2)
        with debug_col1:
            debug_start = st.date_input("Test Start Date", value=datetime(2026, 1, 1).date(), key='debug_start')
        with debug_col2:
            debug_end = st.date_input("Test End Date", value=datetime(2026, 1, 31).date(), key='debug_end')
        
        if st.button("Count Bills in Range", key='debug_count_btn'):
            with st.spinner("Fetching all bills from Ramp (paginating all pages)..."):
                try:
                    client = RampClient(
                        base_url=cfg['ramp']['base_url'],
                        token_url=cfg['ramp']['token_url'],
                        client_id=env['RAMP_CLIENT_ID'],
                        client_secret=env['RAMP_CLIENT_SECRET'],
                        enable_sync=False
                    )
                    client.authenticate()
                    
                    # Fetch ALL bills using pagination (no API date filter available)
                    # Must filter client-side by payment.payment_date
                    all_bills = client.get_all_bills() or []
                    st.info(f"Total bills fetched from API (all bills, paginated): {len(all_bills)}")
                    
                    # Analyze date fields in all bills
                    bills_with_payment_obj = 0
                    bills_with_payment_date = 0
                    bills_with_issued_at = 0
                    bills_with_due_at = 0
                    bills_with_paid_at = 0
                    
                    filtered = []
                    for bill in all_bills:
                        payment_obj = bill.get('payment')
                        if payment_obj:
                            bills_with_payment_obj += 1
                            if payment_obj.get('payment_date'):
                                bills_with_payment_date += 1
                        
                        if bill.get('issued_at'):
                            bills_with_issued_at += 1
                        if bill.get('due_at'):
                            bills_with_due_at += 1
                        if bill.get('paid_at'):
                            bills_with_paid_at += 1
                        
                        # Since we used API filters, all bills should be in range
                        # But let's still verify client-side
                        payment_date_str = payment_obj.get('payment_date') if payment_obj else None
                        
                        if payment_date_str:
                            try:
                                payment_date = datetime.fromisoformat(payment_date_str[:10]).date()
                                if debug_start <= payment_date <= debug_end:
                                    filtered.append(bill)
                            except:
                                pass
                    
                    # Show date field statistics
                    st.write("**Date field analysis:**")
                    st.write(f"- Bills with payment object: {bills_with_payment_obj}")
                    st.write(f"- Bills with payment.payment_date: {bills_with_payment_date}")
                    st.write(f"- Bills with issued_at: {bills_with_issued_at}")
                    st.write(f"- Bills with due_at: {bills_with_due_at}")
                    st.write(f"- Bills with paid_at: {bills_with_paid_at}")
                    
                    st.success(f"✅ API returned {len(all_bills)} bills | Client-side verified: **{len(filtered)}** bills")
                    
                    # Show sample payment dates from fetched bills
                    if all_bills:
                        st.write("**Sample date values from first 10 bills (all date fields):**")
                        for i, bill in enumerate(all_bills[:10]):
                            payment_obj = bill.get('payment') or {}
                            st.write(f"{i+1}. Invoice #{bill.get('invoice_number', 'N/A')}:")
                            st.write(f"   - issued_at: {bill.get('issued_at', 'N/A')[:10] if bill.get('issued_at') else 'N/A'}")
                            st.write(f"   - due_at: {bill.get('due_at', 'N/A')[:10] if bill.get('due_at') else 'N/A'}")
                            st.write(f"   - paid_at: {bill.get('paid_at', 'N/A')[:10] if bill.get('paid_at') else 'N/A'}")
                            st.write(f"   - payment.payment_date: {payment_obj.get('payment_date', 'N/A')[:10] if payment_obj.get('payment_date') else 'N/A'}")
                            st.write(f"   - payment.effective_date: {payment_obj.get('effective_date', 'N/A')[:10] if payment_obj.get('effective_date') else 'N/A'}")
                    
                    if filtered and len(filtered) <= 20:
                        st.write("**Bills matching date range:**")
                        for bill in filtered[:20]:
                            payment_obj = bill.get('payment') or {}
                            payment_date_str = payment_obj.get('payment_date')
                            st.write(f"- Invoice #{bill.get('invoice_number', 'N/A')}: {payment_date_str[:10] if payment_date_str else 'N/A'} (Status: {bill.get('status', 'N/A')})")
                    
                except Exception as e:
                    st.error(f"Error: {e}")
                    import traceback
                    st.code(traceback.format_exc())

    # Date range inputs
    col_a, col_b = st.columns(2)
    with col_a:
        inv_start = st.date_input("Payment Send Date: Start", 
                                   value=st.session_state.get('inv_start_date', datetime.now().replace(day=1)), 
                                   key='invoices_inv_start')
    with col_b:
        inv_end = st.date_input("Payment Send Date: End", 
                                 value=st.session_state.get('inv_end_date', datetime.now()), 
                                 key='invoices_inv_end')

    # Clear cache when dates change
    if (st.session_state.get('cached_inv_start') != inv_start or 
        st.session_state.get('cached_inv_end') != inv_end):
        st.session_state.pop('inv_bills', None)
        st.session_state.pop('inv_pi_df', None)
        st.session_state.pop('inv_gj_df', None)
        st.session_state['cached_inv_start'] = inv_start
        st.session_state['cached_inv_end'] = inv_end

    # Generate button
    if st.button("Generate Purchase Invoices", key='invoices_generate_btn'):
        with st.spinner("Fetching all bills from Ramp (paginating all pages)..."):
            try:
                # Initialize client
                client = RampClient(
                    base_url=cfg['ramp']['base_url'],
                    token_url=cfg['ramp']['token_url'],
                    client_id=env['RAMP_CLIENT_ID'],
                    client_secret=env['RAMP_CLIENT_SECRET'],
                    enable_sync=False
                )
                client.authenticate()

                # Fetch bills filtered server-side by paid_at date range
                from_paid_at = inv_start.strftime('%Y-%m-%dT00:00:00+00:00')
                to_paid_at = inv_end.strftime('%Y-%m-%dT23:59:59+00:00')
                all_bills = client.get_all_bills(
                    from_paid_at=from_paid_at,
                    to_paid_at=to_paid_at,
                ) or []

                # paid_at is a top-level field; also keep any bills missing paid_at
                # that fall in range via fallback to payment.payment_date
                filtered_bills = []
                for bill in all_bills:
                    paid_at_str = bill.get('paid_at')
                    if paid_at_str:
                        try:
                            paid_date = datetime.fromisoformat(paid_at_str[:10]).date()
                            if inv_start <= paid_date <= inv_end:
                                filtered_bills.append(bill)
                                continue
                        except:
                            pass
                    # Fallback: check nested payment.payment_date
                    payment_obj = bill.get('payment') or {}
                    payment_date_str = payment_obj.get('payment_date')
                    if payment_date_str:
                        try:
                            payment_date = datetime.fromisoformat(payment_date_str[:10]).date()
                            if inv_start <= payment_date <= inv_end:
                                filtered_bills.append(bill)
                        except:
                            pass

                if not filtered_bills:
                    st.warning(f'No bills found with paid_at between {inv_start} and {inv_end}.')
                    st.info(f'Total bills returned by API: {len(all_bills)}')
                    st.stop()

                st.success(f"Found {len(filtered_bills)} paid bills in date range")

                # Enrich with vendor external IDs
                try:
                    filtered_bills = enrich_bills_with_vendor_external_ids(filtered_bills, client)
                except Exception as e:
                    st.warning(f"Could not enrich vendor data: {e}")

                # Transform to Business Central format
                pi_df = ramp_bills_to_purchase_invoice_lines(filtered_bills, cfg)
                gj_df = ramp_bills_to_general_journal(filtered_bills, cfg)

                # Calculate totals
                if pi_df is not None and not pi_df.empty:
                    # Purchase Invoice format uses 'Vendor Invoice No.' not 'Document No.'
                    unique_bills = len(pi_df['Vendor Invoice No.'].unique())
                    total_rows = len(pi_df)
                    bill_total = pi_df['Amount'].sum()
                else:
                    unique_bills = 0
                    total_rows = 0
                    bill_total = 0.0

                # Cache results
                st.session_state['inv_bills'] = filtered_bills
                st.session_state['inv_pi_df'] = pi_df
                st.session_state['inv_gj_df'] = gj_df

                # Display summary
                st.write(f"**Unique Bills:** {unique_bills} | **Total Rows:** {total_rows} | **Total Amount:** ${bill_total:,.2f}")

                # Preview
                st.subheader("Purchase Invoice Preview (first 10)")
                if pi_df is None or pi_df.empty:
                    st.info("No purchase invoice rows generated.")
                else:
                    st.dataframe(pi_df.head(10), use_container_width=True)

                st.subheader("General Journal Preview (first 10)")
                if gj_df is None or gj_df.empty:
                    st.info("No general journal rows generated.")
                else:
                    st.dataframe(gj_df.head(10), use_container_width=True)

            except Exception as e:
                st.error(f"Error generating invoices: {e}")
                import traceback
                with st.expander("Error details"):
                    st.code(traceback.format_exc())

    # Download buttons (show if data cached)
    pi_df = st.session_state.get('inv_pi_df')
    gj_df = st.session_state.get('inv_gj_df')
    
    if pi_df is not None and not pi_df.empty:
        st.markdown("---")
        st.subheader("Downloads")
        
        # CSV
        csv_bytes = pi_df.to_csv(index=False).encode('utf-8')
        fname = f"purchase_invoices_{inv_start.strftime('%Y%m%d')}_{inv_end.strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
        st.download_button(
            "Download Purchase Invoices CSV", 
            data=csv_bytes, 
            file_name=fname, 
            mime='text/csv', 
            key='invoices_download_csv'
        )
        
        # Excel
        excel_buf = BytesIO()
        with pd.ExcelWriter(excel_buf, engine='openpyxl') as writer:
            pi_df.to_excel(writer, sheet_name='PurchaseInvoices', index=False)
        excel_buf.seek(0)
        fname_excel = fname.replace('.csv', '.xlsx')
        st.download_button(
            "Download Purchase Invoices Excel", 
            data=excel_buf, 
            file_name=fname_excel, 
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
            key='invoices_download_excel'
        )
        
        # General Journal
        if gj_df is not None and not gj_df.empty:
            gj_csv = gj_df.to_csv(index=False).encode('utf-8')
            gj_fname = f"purchase_invoices_journal_{inv_start.strftime('%Y%m%d')}_{inv_end.strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
            st.download_button(
                "Download General Journal CSV", 
                data=gj_csv, 
                file_name=gj_fname, 
                mime='text/csv', 
                key='invoices_download_gj'
            )

        # Post-export: Mark bills as synced
        st.markdown("---")
        with st.expander("⚠️ Post-export actions (Advanced)", expanded=False):
            st.write(f"{len(st.session_state.get('inv_bills', []))} bills prepared for potential sync.")
            mark_after_export = st.checkbox("Mark exported bills as synced", value=False, key='mark_bills_after_export')
            enable_live_sync = st.checkbox("Enable live Ramp sync (performs writes)", value=False, key='enable_live_bill_sync')

            if mark_after_export:
                st.warning("This action will update Ramp records when live sync is enabled.")
                confirm = st.checkbox("I confirm this action", key='confirm_mark_bills')
                if confirm and st.button("Mark bills as synced"):
                    bills = st.session_state.get('inv_bills') or []
                    if not bills:
                        st.error("No cached bills available to sync. Generate invoices first.")
                    else:
                        try:
                            client = RampClient(
                                base_url=cfg['ramp']['base_url'],
                                token_url=cfg['ramp']['token_url'],
                                client_id=env['RAMP_CLIENT_ID'],
                                client_secret=env['RAMP_CLIENT_SECRET'],
                                enable_sync=enable_live_sync
                            )
                            client.authenticate()

                            sync_ref = f"BC_EXPORT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            dry_run = not enable_live_sync

                            # Categorize bills per Ramp docs:
                            # sync_status=NOT_SYNCED, status=OPEN  → BILL_SYNC only
                            # sync_status=NOT_SYNCED, status=PAID  → BILL_SYNC then BILL_PAYMENT_SYNC
                            # sync_status=BILL_SYNCED, status=PAID → BILL_PAYMENT_SYNC only
                            bill_syncs = []
                            payment_syncs = []
                            skipped_bills = []

                            for bill in bills:
                                bid = bill.get('id')
                                if not bid:
                                    continue
                                entry = {'id': bid, 'reference_id': sync_ref}
                                bill_sync_status = bill.get('sync_status', 'NOT_SYNCED')
                                bill_status = bill.get('status', 'OPEN')

                                if bill_sync_status == 'NOT_SYNCED':
                                    bill_syncs.append(entry)
                                    if bill_status == 'PAID':
                                        payment_syncs.append(entry)
                                elif bill_sync_status == 'BILL_SYNCED' and bill_status == 'PAID':
                                    payment_syncs.append(entry)
                                else:
                                    skipped_bills.append(bid)

                            any_error = False

                            def _show_api_detail(label, ok, info, dry_run):
                                """Display request payload and response for a sync call."""
                                endpoint_used = info.get('endpoint', '') if isinstance(info, dict) else ''
                                payload_used = info.get('payload') if isinstance(info, dict) else None
                                response_body = info.get('response') if isinstance(info, dict) else info
                                http_status = info.get('status', '') if isinstance(info, dict) else ''
                                with st.expander(f"🔍 API detail: {label}", expanded=not ok):
                                    st.write(f"**Endpoint:** `POST {endpoint_used}`")
                                    if payload_used:
                                        st.write("**Request payload:**")
                                        st.json(payload_used)
                                    if not dry_run:
                                        st.write(f"**HTTP status:** `{http_status}`")
                                        st.write("**Response body:**")
                                        st.json(response_body) if isinstance(response_body, dict) else st.code(str(response_body))

                            # Step 1: BILL_SYNC
                            if bill_syncs:
                                ok, info = client.post_accounting_syncs(
                                    successful_syncs=bill_syncs,
                                    failed_syncs=[],
                                    sync_type='BILL_SYNC',
                                    dry_run=dry_run
                                )
                                if ok:
                                    if dry_run:
                                        st.info(f"[DRY RUN] Would send {len(bill_syncs)} bill(s) via BILL_SYNC.")
                                    else:
                                        st.success(f"✅ Marked {len(bill_syncs)} bill(s) as synced (BILL_SYNC) in Ramp.")
                                else:
                                    st.error(f"❌ BILL_SYNC failed")
                                    any_error = True
                                _show_api_detail('BILL_SYNC', ok, info, dry_run)

                            # Step 2: BILL_PAYMENT_SYNC (only after bill sync succeeds)
                            if payment_syncs and not any_error:
                                ok, info = client.post_accounting_syncs(
                                    successful_syncs=payment_syncs,
                                    failed_syncs=[],
                                    sync_type='BILL_PAYMENT_SYNC',
                                    dry_run=dry_run
                                )
                                if ok:
                                    if dry_run:
                                        st.info(f"[DRY RUN] Would send {len(payment_syncs)} payment(s) via BILL_PAYMENT_SYNC.")
                                    else:
                                        st.success(f"✅ Marked {len(payment_syncs)} payment(s) as synced (BILL_PAYMENT_SYNC) in Ramp.")
                                else:
                                    st.error(f"❌ BILL_PAYMENT_SYNC failed")
                                _show_api_detail('BILL_PAYMENT_SYNC', ok, info, dry_run)

                            if not bill_syncs and not payment_syncs:
                                st.warning("No bills required syncing based on their current sync_status/status.")

                            if skipped_bills:
                                st.info(f"Skipped {len(skipped_bills)} bill(s) with unexpected sync_status/status combination.")

                        except Exception as e:
                            st.error(f"❌ Error performing bill sync: {e}")
