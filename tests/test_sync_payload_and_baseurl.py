import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ramp_client import RampClient


def test_base_url_normalization_collapses_duplicate_segment():
    c = RampClient('https://api.ramp.com/developer/v1/developer/v1', 'https://api.ramp.com/oauth/token', 'id', 'secret')
    assert c.base_url.endswith('/developer/v1')
    # Should not contain duplicate segment
    assert c.base_url.count('developer/v1') == 1


def test_post_accounting_syncs_omits_empty_failed_syncs():
    c = RampClient('https://api.ramp.com/developer/v1', 'https://api.ramp.com/oauth/token', 'id', 'secret')
    ok, info = c.post_accounting_syncs(successful_syncs=[{'id': 'T1'}], failed_syncs=[], sync_type='TRANSACTION_SYNC', dry_run=True)
    assert ok is True
    payload_preview = info.get('payload_preview')
    # failed_syncs should not appear in the preview when empty
    assert 'failed_syncs' not in payload_preview
