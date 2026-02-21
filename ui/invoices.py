import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import os
import json

from ramp_client import RampClient
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
            with st.spinner("Fetching bills..."):
                try:
                    client = RampClient(
                        base_url=cfg['ramp']['base_url'],
                        token_url=cfg['ramp']['token_url'],
                        client_id=env['RAMP_CLIENT_ID'],
                        client_secret=env['RAMP_CLIENT_SECRET'],
                        enable_sync=False
                    )
                    client.authenticate()
                    
                    all_bills = client.get_bills(page_size=cfg['ramp'].get('page_size', 200), sync_ready=True) or []
                    st.info(f"Total bills fetched: {len(all_bills)}")
                    
                    filtered = []
                    for bill in all_bills:
                        payment_obj = bill.get('payment') or {}
                        payment_date_str = payment_obj.get('payment_date')
                        
                        if payment_date_str:
                            try:
                                payment_date = datetime.fromisoformat(payment_date_str[:10]).date()
                                if debug_start <= payment_date <= debug_end:
                                    filtered.append(bill)
                            except:
                                pass
                    
                    st.success(f"✅ Bills with payment_date between {debug_start} and {debug_end}: **{len(filtered)}**")
                    
                    if filtered and len(filtered) <= 20:
                        st.write("Sample bills:")
                        for bill in filtered[:20]:
                            payment_obj = bill.get('payment') or {}
                            payment_date_str = payment_obj.get('payment_date')
                            st.write(f"- Invoice #{bill.get('invoice_number', 'N/A')}: {payment_date_str[:10] if payment_date_str else 'N/A'} (Status: {bill.get('status', 'N/A')})")
                    
                except Exception as e:
                    st.error(f"Error: {e}")

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
        with st.spinner("Fetching bills from Ramp..."):
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

                # Fetch all bills (both OPEN with scheduled payments and PAID)
                # Bills can be OPEN but have payment.payment_date set (PAYMENT_SCHEDULED)
                all_bills = client.get_bills(
                    page_size=cfg['ramp'].get('page_size', 200), 
                    sync_ready=True
                ) or []
                
                # Filter by payment.payment_date (when payment was/will be sent to bank)
                filtered_bills = []
                for bill in all_bills:
                    # Payment date is nested: payment.payment_date
                    payment_obj = bill.get('payment') or {}
                    payment_date_str = payment_obj.get('payment_date')
                    
                    if payment_date_str:
                        try:
                            payment_date = datetime.fromisoformat(payment_date_str[:10]).date()
                            if inv_start <= payment_date <= inv_end:
                                filtered_bills.append(bill)
                        except:
                            continue
                
                if not filtered_bills:
                    st.warning(f'No bills found with payment dates between {inv_start} and {inv_end}.')
                    st.info(f'Total bills in system: {len(all_bills)}')
                    st.stop()
                
                st.success(f"Found {len(filtered_bills)} paid bills (from {len(all_bills)} total)")

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
                    bill_count = len(pi_df['Document No.'].unique())
                    bill_total = pi_df['Amount'].sum()
                else:
                    bill_count = 0
                    bill_total = 0.0

                # Cache results
                st.session_state['inv_bills'] = filtered_bills
                st.session_state['inv_pi_df'] = pi_df
                st.session_state['inv_gj_df'] = gj_df

                # Display summary
                st.write(f"**Bills:** {bill_count} | **Total Amount:** ${bill_total:,.2f}")

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
