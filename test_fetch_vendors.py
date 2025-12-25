import json
import os
import pytest

# This is an integration-style script that will attempt to contact the Ramp API.
# Skip the module when Ramp credentials are not set in the environment so that
# ordinary unit test runs don't fail during collection.
if not os.environ.get('RAMP_CLIENT_ID') or not os.environ.get('RAMP_CLIENT_SECRET'):
    pytest.skip("Skipping integration test that requires Ramp credentials", allow_module_level=True)

from ramp_client import RampClient
rc = RampClient(
    base_url="https://api.ramp.com",
    token_url="https://api.ramp.com/developer/v1/token",
    client_id=os.environ.get('RAMP_CLIENT_ID'),
    client_secret=os.environ.get('RAMP_CLIENT_SECRET')
)
rc.authenticate()               # obtains token via client_credentials
vendors = rc.get_vendors()      # returns list of vendor dicts
print(len(vendors))
print(json.dumps(vendors[:3], indent=2))
with open("sample_vendors.json", "w", encoding="utf-8") as f:
    json.dump(vendors, f, indent=2, ensure_ascii=False)