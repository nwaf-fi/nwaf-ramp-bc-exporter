# utils.py

import os
import tomllib
from dotenv import load_dotenv
from typing import Dict, Any
from datetime import datetime

def load_env() -> Dict[str, str]:
    """
    Loads environment variables from a .env file and verifies required credentials.
    Returns a dictionary of validated credentials.
    """
    load_dotenv()
    
    # Define required environment variables
    REQUIRED_VARS = ["RAMP_CLIENT_ID", "RAMP_CLIENT_SECRET"]
    
    env_vars = {}
    for var_name in REQUIRED_VARS:
        value = os.getenv(var_name, "").strip()
        if not value:
            raise ValueError(f"Environment variable '{var_name}' must be set in the .env file.")
        env_vars[var_name] = value
        
    return env_vars

def load_config(config_path: str = 'config.toml') -> Dict[str, Any]:
    """
    Loads configuration settings from a TOML file.
    """
    try:
        with open(config_path, 'r') as f:
            config = tomllib.loads(f.read())
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
    except Exception as e:
        raise IOError(f"Error loading configuration file: {e}")


# --- Amount parsing helpers (moved from streamlit_app) ---
def _extract_amount(amount_obj):
    """Extract a numeric major-unit amount from various Ramp amount representations.

    Accepts a dict with keys 'amount' and 'minor_unit_conversion_rate', or a numeric value.
    Returns a float.
    """
    if isinstance(amount_obj, dict):
        minor = amount_obj.get('amount', 0)
        conv = amount_obj.get('minor_unit_conversion_rate', 100)
        try:
            return float(minor) / float(conv) if conv else float(minor)
        except Exception:
            return 0.0
    try:
        return float(amount_obj or 0.0)
    except Exception:
        return 0.0


# --- Audit helpers (moved from streamlit_app) ---
def _write_sync_audit(results: list, sync_ref: str, user_email: str = '') -> str:
    """Write sync audit results (list of dicts) to a CSV file in the exports/ folder and return path."""
    import csv
    os.makedirs('exports', exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    fname = f"exports/sync_audit_{ts}.csv"
    headers = ['timestamp', 'transaction_id', 'status', 'sync_reference', 'user_email', 'message']
    try:
        with open(fname, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for r in results:
                writer.writerow({
                    'timestamp': r.get('timestamp', ''),
                    'transaction_id': r.get('transaction_id', ''),
                    'status': 'success' if r.get('ok') else 'failure',
                    'sync_reference': sync_ref,
                    'user_email': user_email,
                    'message': r.get('message', '')
                })
        return fname
    except Exception:
        return ''
