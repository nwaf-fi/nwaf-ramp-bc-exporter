
import argparse
from datetime import datetime, timedelta
from typing import Dict
import pandas as pd
from utils import load_env, load_config
from ramp_client import RampClient
from transform import (ramp_to_bc_rows, ramp_bills_to_bc_rows, 
                      ramp_reimbursements_to_bc_rows, ramp_cashbacks_to_bc_rows,
                      ramp_statements_to_bc_rows)
from bc_export import export

def get_date_ranges(period_type: str) -> Dict[str, tuple]:
    """
    Calculate appropriate date ranges for each data type based on reconciliation period.
    
    Returns: {data_type: (start_date, end_date), ...}
    """
    today = datetime.now()
    
    if period_type == 'monthly':
        # All types: current month
        start_of_month = today.replace(day=1)
        end_of_month = (start_of_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        return {
            'transactions': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'bills': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'reimbursements': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'cashbacks': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'statements': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d'))
        }
    
    elif period_type == 'bi-weekly':
        # Bills: last 2 weeks, others: current month
        two_weeks_ago = today - timedelta(days=14)
        start_of_month = today.replace(day=1)
        end_of_month = (start_of_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        return {
            'transactions': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'bills': (two_weeks_ago.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')),
            'reimbursements': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'cashbacks': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'statements': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d'))
        }
    
    elif period_type == 'statement':
        # Transactions: last statement period (assume monthly statements)
        # Others: current month
        start_of_month = today.replace(day=1)
        end_of_month = (start_of_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        return {
            'transactions': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'bills': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'reimbursements': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'cashbacks': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')),
            'statements': (start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d'))
        }
    
    else:
        raise ValueError(f"Unknown period type: {period_type}")

def check_available_endpoints(client: RampClient, cfg: Dict) -> Dict[str, bool]:
    """
    Check which API endpoints are available based on OAuth scopes.
    Returns {endpoint_name: is_available}
    """
    endpoints_to_check = {
        'transactions': 'transactions:read',
        'bills': 'bills:read', 
        'reimbursements': 'reimbursements:read',
        'cashbacks': 'cashbacks:read',
        'statements': 'statements:read',
        'accounting': 'accounting:read'  # Check if accounting endpoints are available
    }
    
    available = {}
    
    for endpoint, required_scope in endpoints_to_check.items():
        try:
            # Try a simple request to see if the scope is available
            if endpoint == 'accounting':
                # For accounting, test a different endpoint or method
                url = f"{cfg['ramp']['base_url']}/transactions"
                resp = client.session.get(url, params={'limit': 1})
                # If we can read transactions, we might be able to write accounting data
                available[endpoint] = resp.status_code == 200
            else:
                url = f"{cfg['ramp']['base_url']}/{endpoint}"
                resp = client.session.get(url, params={'limit': 1})
                available[endpoint] = resp.status_code == 200
        except Exception:
            available[endpoint] = False
    
    return available

def fetch_data_for_type(client: RampClient, data_type: str, start_date: str, end_date: str, cfg: Dict) -> tuple:
    """Fetch data for a specific type and return (data, dataframe)"""
    if data_type == 'transactions':
        data = client.get_transactions(
            status=cfg['ramp'].get('status_filter'),
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        df = ramp_to_bc_rows(data, cfg)
    elif data_type == 'bills':
        data = client.get_bills(
            status='PAID',
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        df = ramp_bills_to_bc_rows(data, cfg)
    elif data_type == 'reimbursements':
        data = client.get_reimbursements(
            status='PAID',
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        df = ramp_reimbursements_to_bc_rows(data, cfg)
    elif data_type == 'cashbacks':
        data = client.get_cashbacks(
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        df = ramp_cashbacks_to_bc_rows(data, cfg)
    elif data_type == 'statements':
        data = client.get_statements(
            start_date=start_date,
            end_date=end_date,
            page_size=cfg['ramp'].get('page_size', 200)
        )
        df = ramp_statements_to_bc_rows(data, cfg)
    else:
        raise ValueError(f"Unknown data type: {data_type}")
    
    return data, df

def main():
    parser = argparse.ArgumentParser(description='Ramp → BC General Journal exporter')
    parser.add_argument('--type', choices=['transactions', 'bills', 'reimbursements', 'cashbacks', 'statements'], 
                       help='Type of Ramp data to export (use --all for all types)')
    parser.add_argument('--all', action='store_true', help='Export all data types with appropriate date ranges')
    parser.add_argument('--period', choices=['monthly', 'bi-weekly', 'statement'], default='monthly',
                       help='Reconciliation period (affects date ranges for different data types)')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD) - overrides period calculation')
    parser.add_argument('--end', help='End date (YYYY-MM-DD) - overrides period calculation')
    parser.add_argument('--mark-synced', action='store_true', 
                       help='Mark transactions as synced to Business Central after export (requires accounting:write scope)')
    args = parser.parse_args()

    # Validate arguments
    if not args.all and not args.type:
        parser.error("Either --all or --type must be specified")
    if args.all and args.type:
        parser.error("Cannot specify both --all and --type")

    env = load_env()
    cfg = load_config()

    client = RampClient(
        base_url=cfg['ramp']['base_url'],
        token_url=cfg['ramp']['token_url'],
        client_id=env['RAMP_CLIENT_ID'],
        client_secret=env['RAMP_CLIENT_SECRET']
    )
    client.authenticate()

    # Check which endpoints are available
    available_endpoints = check_available_endpoints(client, cfg)
    print("🔍 Checking API endpoint availability...")
    for endpoint, is_available in available_endpoints.items():
        status = "✅ Available" if is_available else "❌ Not available (scope not granted)"
        print(f"   {endpoint}: {status}")

    # Determine which types to process and their date ranges
    if args.all:
        # Process all available types with period-based date ranges
        date_ranges = get_date_ranges(args.period)
        types_to_process = [t for t in ['transactions', 'bills', 'reimbursements', 'cashbacks', 'statements'] 
                           if available_endpoints[t]]
        print(f"🔄 Processing available data types for {args.period} reconciliation period")
    else:
        # Process single type with manual or period-based dates
        if not available_endpoints[args.type]:
            print(f"❌ Error: {args.type} endpoint is not available (OAuth scope not granted)")
            return
        types_to_process = [args.type]
        if args.start and args.end:
            date_ranges = {args.type: (args.start, args.end)}
        else:
            period_ranges = get_date_ranges(args.period)
            date_ranges = {args.type: period_ranges[args.type]}

    # Fetch and combine data from all types
    combined_df = None
    total_records = 0
    
    for data_type in types_to_process:
        start_date, end_date = date_ranges[data_type]
        print(f"📊 Fetching {data_type} from {start_date} to {end_date}...")
        
        try:
            data, df = fetch_data_for_type(client, data_type, start_date, end_date, cfg)
            
            if data:
                print(f"✅ Found {len(data)} {data_type} records")
                total_records += len(data)
                
                # Combine dataframes
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
            else:
                print(f"⚠️ No {data_type} data found for the specified period")
                
        except Exception as e:
            print(f"❌ Error fetching {data_type}: {str(e)}")
            continue

    if combined_df is None or combined_df.empty:
        print("❌ No data found for any of the specified types and periods.")
        return

    # Export combined data
    period_suffix = args.period.replace('-', '_').upper()
    prefix = f"RAMP_ALL_{period_suffix}" if args.all else f"RAMP_{args.type.upper()}"
    
    xlsx_path, csv_path = export(combined_df, prefix=prefix)
    print(f"🎉 Exported {total_records} total records across {len(types_to_process)} data types")
    print(f"📁 Excel: {xlsx_path}")
    print(f"📄 CSV: {csv_path}")

    # Mark transactions as synced if requested and accounting scope is available
    if args.mark_synced and available_endpoints.get('accounting', False) and 'transactions' in types_to_process:
        print("🔄 Marking transactions as synced to Business Central...")
        print("⚠️  TESTING MODE: No actual changes will be made to Ramp data")
        synced_count = 0
        
        # Get transaction data that was processed
        for data_type in types_to_process:
            if data_type == 'transactions':
                # We need to fetch the transaction data again to get IDs
                start_date, end_date = date_ranges['transactions']
                try:
                    transaction_data = client.get_transactions(
                        status=cfg['ramp'].get('status_filter'),
                        start_date=start_date,
                        end_date=end_date,
                        page_size=cfg['ramp'].get('page_size', 200)
                    )
                    
                    for transaction in transaction_data:
                        transaction_id = transaction.get('id')
                        if transaction_id:
                            sync_ref = f"BC_EXPORT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            if client.mark_transaction_synced(transaction_id, sync_ref):
                                synced_count += 1
                            else:
                                print(f"⚠️ Failed to mark transaction {transaction_id} as synced")
                                
                except Exception as e:
                    print(f"❌ Error marking transactions as synced: {e}")
                    
        if synced_count > 0:
            print(f"✅ [TESTING] Would have marked {synced_count} transactions as synced to Business Central")

if __name__ == '__main__':
    main()
