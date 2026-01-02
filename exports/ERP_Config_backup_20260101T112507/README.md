# ERP_Config (private)

This folder contains sensitive ERP configuration and payloads (chart of accounts, mapping files, generated payloads). These files are intentionally not tracked in Git and should be kept private.

Guidelines:
- Do not commit secrets (client IDs, client secrets, credentials) into this repository.
- Keep a local copy for deployments, or store in a secure secrets manager.
- Use `ERP_Config/coa_transform.py` to validate and generate Ramp payloads locally, then use the Ramp API to create/update accounting connections.
