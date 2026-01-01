import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ramp_client import RampClient


def test_mark_transaction_synced_message_uses_normalized_endpoint(monkeypatch):
    c = RampClient('https://api.ramp.com/developer/v1', 'https://api.ramp.com/oauth/token', 'id', 'secret')
    c.enable_sync = True

    def fake_post(self, successful_syncs=None, failed_syncs=None, sync_type=None, idempotency_key=None, dry_run=True):
        return True, {'status': 200, 'response': 'ok'}

    monkeypatch.setattr(RampClient, 'post_accounting_syncs', fake_post)
    ok, msg = c.mark_transaction_synced_with_message('T1', sync_reference='REF')
    assert ok is True
    # msg should not contain duplicate 'developer/v1/developer/v1'
    assert 'developer/v1/developer/v1' not in msg
    assert 'accounting/syncs' in msg


def test_mark_transaction_synced_failure_message_includes_endpoint(monkeypatch):
    c = RampClient('https://api.ramp.com/developer/v1/developer/v1', 'https://api.ramp.com/oauth/token', 'id', 'secret')
    c.enable_sync = True

    def fake_post(self, successful_syncs=None, failed_syncs=None, sync_type=None, idempotency_key=None, dry_run=True):
        return False, {'status': 422, 'response': 'DEVELOPER_7001: Missing fields'}

    monkeypatch.setattr(RampClient, 'post_accounting_syncs', fake_post)
    ok, msg = c.mark_transaction_synced_with_message('T1', sync_reference='REF')
    assert ok is False
    assert 'developer/v1/developer/v1' not in msg
    assert 'accounting/syncs' in msg
    assert 'DEVELOPER_7001' in msg
