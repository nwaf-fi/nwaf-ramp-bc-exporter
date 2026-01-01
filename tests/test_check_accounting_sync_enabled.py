import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ramp_client import RampClient


def test_check_accounting_sync_enabled_uses_post_accounting_syncs_success(monkeypatch):
    c = RampClient("https://api.ramp.com/developer/v1", "https://api.ramp.com/oauth/token", "id", "secret")

    def fake_post(self, successful_syncs=None, failed_syncs=None, sync_type=None, idempotency_key=None, dry_run=True):
        return True, {'endpoint': 'https://api.ramp.com/developer/v1/accounting/syncs', 'payload_preview': '{"idempotency_key": "abc"}'}

    monkeypatch.setattr(RampClient, 'post_accounting_syncs', fake_post)
    assert c.check_accounting_sync_enabled() is True
    assert getattr(c, '_accounting_sync_enabled') is True
    assert 'dry-run preview' in getattr(c, '_accounting_sync_message')


def test_check_accounting_sync_enabled_handles_failure(monkeypatch):
    c = RampClient("https://api.ramp.com/developer/v1", "https://api.ramp.com/oauth/token", "id", "secret")

    def fake_post(self, successful_syncs=None, failed_syncs=None, sync_type=None, idempotency_key=None, dry_run=True):
        return False, {'status': 422, 'response': 'DEVELOPER_7001: Missing idempotency_key'}

    monkeypatch.setattr(RampClient, 'post_accounting_syncs', fake_post)
    assert c.check_accounting_sync_enabled() is False
    assert getattr(c, '_accounting_sync_enabled') is False
    assert 'DEVELOPER_7001' in getattr(c, '_accounting_sync_message')
