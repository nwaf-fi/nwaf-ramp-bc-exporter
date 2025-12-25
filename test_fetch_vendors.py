import json
from ramp_client import RampClient

import os
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