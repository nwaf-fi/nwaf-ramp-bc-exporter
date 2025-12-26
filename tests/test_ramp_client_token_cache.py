from datetime import datetime, timedelta

from ramp_client import RampClient


def sample_client():
    return RampClient(base_url='https://api.ramp.com', token_url='https://api.ramp.com/developer/v1/token', client_id='x', client_secret='y')


def test_ensure_authenticated_reuses_token():
    client = sample_client()
    client._token = 'T1'
    client.token_expires_at = datetime.utcnow() + timedelta(seconds=3600)

    called = {'count': 0}

    def fake_auth():
        called['count'] += 1

    # Replace authenticate with fake that would increment counter if called
    client.authenticate = fake_auth

    client.ensure_authenticated()
    assert called['count'] == 0


def test_ensure_authenticated_reauths_when_expired():
    client = sample_client()
    client._token = 'T1'
    client.token_expires_at = datetime.utcnow() - timedelta(seconds=10)

    called = {'count': 0}

    def fake_auth():
        called['count'] += 1
        client._token = 'T2'
        client.token_expires_at = datetime.utcnow() + timedelta(seconds=3600)
        return client._token

    client.authenticate = fake_auth

    token = client.ensure_authenticated()
    assert called['count'] == 1
    assert token == 'T2'
