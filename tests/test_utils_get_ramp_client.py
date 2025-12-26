import sys
import types

from ramp_client import RampClient
from utils import get_ramp_client


def make_fake_streamlit_module():
    mod = types.SimpleNamespace()
    mod.session_state = {}
    return mod


def test_get_ramp_client_caches_in_session(monkeypatch):
    fake_st = make_fake_streamlit_module()
    monkeypatch.setitem(sys.modules, 'streamlit', fake_st)

    cfg = {'ramp': {'base_url': 'https://api.ramp.com', 'token_url': 'https://api.ramp.com/developer/v1/token'}}
    env = {'RAMP_CLIENT_ID': 'id', 'RAMP_CLIENT_SECRET': 'secret'}

    # Prevent network auth during test by patching RampClient.ensure_authenticated
    original_ensure = RampClient.ensure_authenticated
    RampClient.ensure_authenticated = lambda self: None
    try:
        client1 = get_ramp_client(cfg, env, enable_sync=False)
        assert isinstance(client1, RampClient)
        # Ensure it's stored in the fake session state
        assert 'ramp_client' in fake_st.session_state

        client2 = get_ramp_client(cfg, env, enable_sync=True)
        assert client2 is fake_st.session_state['ramp_client']
        # enable_sync updated
        assert client2.enable_sync is True
    finally:
        RampClient.ensure_authenticated = original_ensure
