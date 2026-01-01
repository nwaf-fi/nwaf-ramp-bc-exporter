import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ramp_client import RampClient


def test_post_accounting_syncs_dry_run_preview_contains_required_keys():
    c = RampClient("https://api.ramp.com/developer/v1", "https://api.ramp.com/oauth/token", "id", "secret")
    ok, info = c.post_accounting_syncs(
        successful_syncs=[{"id": "T1", "reference_id": "ERP1"}],
        failed_syncs=[],
        sync_type='REIMBURSEMENT_SYNC',
        idempotency_key='testkey',
        dry_run=True,
    )
    assert ok is True
    assert isinstance(info, dict)
    payload_preview = info.get('payload_preview')
    assert payload_preview is not None
    # Ensure canonical fields are present in the preview JSON
    assert '"idempotency_key"' in payload_preview
    assert '"sync_type"' in payload_preview
    assert '"successful_syncs"' in payload_preview
