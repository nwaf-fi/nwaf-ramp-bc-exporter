from datetime import datetime
import csv
import os

from ramp_client import RampClient


def extract_external_id(vendor: dict) -> str:
    """Try multiple possible keys to find the Ramp "External ID" value.

    Ramp vendor objects may store a labeled column in different keys depending
    on API shape or provider configuration. We'll check common names then
    fall back to remote_code/remote_id/name for diagnostics.
    """
    if not vendor or not isinstance(vendor, dict):
        return ""

    # Common candidate keys
    for k in ("external_id", "externalId", "external_code", "externalCode", "remote_code", "remoteCode", "remote_id", "remoteId"):
        val = vendor.get(k)
        if val:
            return str(val)

    # Some vendors may store custom fields under `attributes` or `metadata`
    for parent in ("attributes", "metadata", "custom_fields", "customFields"):
        sub = vendor.get(parent) or {}
        if isinstance(sub, dict):
            for k in ("external_id", "externalId", "external_code", "remote_code"):
                if sub.get(k):
                    return str(sub.get(k))

    # Fallbacks for diagnostic purposes
    if vendor.get("remote_id"):
        return str(vendor.get("remote_id"))
    if vendor.get("name"):
        return str(vendor.get("name"))
    return ""


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
        try:
            rc.authenticate()
        except Exception as ex:
            print(f"Authentication failed: {ex}")
            return

    print("Fetching vendors...")
    vendors = rc.get_vendors()
    print(f"Fetched {len(vendors)} vendors")\n*** End Patch

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = "exports"
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, f"vendor_external_ids_{ts}.csv")

    with open(out_csv, "w", newline='', encoding="utf-8") as wf:
        writer = csv.writer(wf)
        writer.writerow(["vendor_id", "name", "external_id_detected"])
        for v in vendors:
            vid = v.get("id") or ""
            name = v.get("name") or v.get("remote_name") or ""
            ext = extract_external_id(v)
            writer.writerow([vid, name, ext])

    print(f"Wrote vendor external IDs to {out_csv}")


if __name__ == "__main__":
    main()
