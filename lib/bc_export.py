# bc_export.py

import pandas as pd
from datetime import datetime
from typing import Tuple
import os

def export(df: pd.DataFrame, output_dir: str = 'exports', prefix: str = 'RAMP_TRANSACTIONS') -> Tuple[str, str]:
    """
    Saves the Business Central journal DataFrame to CSV (for import) 
    and XLSX (for review).
    Returns the paths to the saved files.
    """
    if df.empty:
        print("No data to export.")
        return "", ""

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate timestamped filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename_base = f"BC_Journal_{prefix}_{timestamp}"
    
    # CSV file path (for actual BC upload)
    csv_path = os.path.join(output_dir, f"{filename_base}.csv")
    df.to_csv(csv_path, index=False)
    
    # XLSX file path (for human review)
    xlsx_path = os.path.join(output_dir, f"{filename_base}.xlsx")
    df.to_excel(xlsx_path, index=False)
    
    return xlsx_path, csv_path
