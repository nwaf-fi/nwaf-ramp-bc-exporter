from datetime import datetime
import json
import os

from ramp_client import RampClient


def main():
    import os
    rc = RampClient(
        base_url="https://api.ramp.com",
        token_url="https://auth.ramp.com/oauth/token",
        client_id=os.environ.get('RAMP_CLIENT_ID'),
        client_secret=os.environ.get('RAMP_CLIENT_SECRET')
    )

    # Allow using a pre-obtained access token via environment to bypass
    # the authenticate() call (useful if your environment cannot reach
    # the token endpoint but you already have a valid token).
    token = os.environ.get("RAMP_ACCESS_TOKEN")
    if token:
        rc.session.headers.update({"Authorization": f"Bearer {token}"})
        print("Using RAMP_ACCESS_TOKEN from environment; skipping authenticate()")
    else:
        print("Authenticating to Ramp...")
        rc.authenticate()

    print("Fetching all vendors...")
    vendors = rc.get_vendors()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = "exports"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"vendor_all_{ts}.ndjson")

    with open(out_path, "w", encoding="utf-8") as wf:
        for v in vendors:
            wf.write(json.dumps(v, ensure_ascii=False))
            wf.write("\n")

    print(f"Wrote {len(vendors)} vendors to {out_path}")


if __name__ == "__main__":
    main()
