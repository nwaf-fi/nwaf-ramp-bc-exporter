import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ramp_client import RampClient


def test_mark_transaction_synced_with_message_dry_run():
    c = RampClient("https://api.ramp.com/developer/v1", "https://api.ramp.com/oauth/token", "id", "secret")
    c.enable_sync = False
    ok, msg = c.mark_transaction_synced_with_message('TID123', sync_reference='REF1')
    assert ok is True
    assert '[DRY RUN]' in msg


def test_mark_transaction_synced_with_message_delegates_to_post(monkeypatch):
    c = RampClient("https://api.ramp.com/developer/v1", "https://api.ramp.com/oauth/token", "id", "secret")
    c.enable_sync = True

    def fake_post(self, successful_syncs=None, failed_syncs=None, sync_type=None, idempotency_key=None, dry_run=True):
        assert successful_syncs == [{'id': 'TID123', 'reference_id': 'REF1'}]
        return True, {'status': 200, 'response': 'ok'}

    monkeypatch.setattr(RampClient, 'post_accounting_syncs', fake_post)
    ok, msg = c.mark_transaction_synced_with_message('TID123', sync_reference='REF1')
    assert ok is True
    assert '200' in msg
