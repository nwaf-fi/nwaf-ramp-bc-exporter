Streamlit App Refactor Plan (Non-Breaking)
Objective

Refactor the existing 1,300+ line Streamlit application into a modular, maintainable structure without changing behavior, UX, authentication flow, or API interactions.

This is a structural refactor only — no new features, no logic changes, no visual changes.

Constraints (Must Follow)

❌ Do not change authentication behavior (Azure AD / MSAL)

❌ Do not change Ramp API behavior or parameters

❌ Do not change exports, filenames, or formats

❌ Do not change Streamlit session behavior

❌ Do not change UI text or layout

❌ Do not introduce async or new frameworks

✅ Move code only (copy/paste refactor)

✅ Preserve variable names and logic

✅ App must run identically after each step

Target Architecture
app.py                     # Entry point (≤200 lines)
auth/
  azure_auth.py            # MSAL auth + token/session handling
  ui.py                    # Login / logout UI
ui/
  layout.py                # CSS, header, sidebar
  credit_cards.py          # Credit Card tab
  invoices.py              # Invoices tab
  reimbursements.py        # Reimbursements tab
utils/
  amounts.py               # Amount parsing helpers
  audit.py                 # Audit writers (CSV / NDJSON)
  state.py                 # Session helpers


Rule: If a section has a st.subheader() it should live in its own file.

Step-by-Step Refactor Plan
Phase 1 — Extract Pure Helpers (Zero Risk)

Create:

utils/
  amounts.py
  audit.py


Move (no edits):

_extract_amount

_write_sync_audit

Any repeated totals or amount parsing helpers

Requirements:

No Streamlit calls inside helpers

Same inputs → same outputs

Update imports only

Phase 2 — Extract Authentication Logic

Create:

auth/azure_auth.py


Move all Azure/MSAL code into:

def ensure_authenticated():
    """
    Handles:
    - token refresh
    - redirect flow
    - device code fallback
    - user claims
    Returns:
        user_name (str)
        user_email (str)
    """


This function must:

Handle redirects and st.stop()

Set st.session_state exactly as before

Return the same identity values currently used

In app.py:

from auth.azure_auth import ensure_authenticated
user_name, user_email = ensure_authenticated()


⚠️ No logic changes allowed here. Move only.

Phase 3 — Extract Layout & Sidebar

Create:

ui/layout.py


Move:

load_css

App header markup

Sidebar user profile

Logout button

Sidebar informational panels

Functions:

def load_css():
    ...

def render_header():
    ...

def render_sidebar(user_name, user_email):
    ...


In app.py:

load_css()
render_header()
render_sidebar(user_name, user_email)

Phase 4 — Extract Tabs (One File Per Tab)

Create:

ui/credit_cards.py
ui/invoices.py
ui/reimbursements.py


Each file must expose one function only:

def render_credit_cards_tab(cfg, env):
    ...

def render_invoices_tab(cfg, env):
    ...

def render_reimbursements_tab(cfg, env):
    ...


Move all tab logic verbatim:

UI elements

API calls

Previews

Downloads

Sync marking

In app.py:

cc_tab, inv_tab, reimb_tab = st.tabs(["Credit Cards", "Invoices", "Reimbursements"])

with cc_tab:
    render_credit_cards_tab(cfg, env)

with inv_tab:
    render_invoices_tab(cfg, env)

with reimb_tab:
    render_reimbursements_tab(cfg, env)

Final app.py Responsibilities

After refactor, app.py should:

Configure Streamlit page

Authenticate user

Load config/env

Render layout

Delegate tabs

Target size: ≤200 lines
If app.py feels “boring”, the refactor succeeded.

Validation Checklist (Required)

After each phase, verify:

 App starts successfully

 Authentication flow works

 Credit card export matches prior output

 Invoice export matches prior output

 Reimbursement export matches prior output

 Downloaded files are byte-identical

 Sync marking behavior unchanged

 No new warnings or errors in logs

Commit Strategy (Strongly Recommended)

One commit per phase

Commit messages:

refactor: extract utils helpers

refactor: isolate azure authentication

refactor: extract layout and sidebar

refactor: split export tabs

Explicit Non-Goals

❌ No performance optimization

❌ No UI redesign

❌ No class-based rewrite

❌ No test additions (optional later)

❌ No async conversion

Success Criteria

App behavior is unchanged

Files are smaller and purpose-driven

New features can be added without touching auth or unrelated tabs

A new developer can navigate the codebase confidently

Questions During Refactor

If unsure:

Move code, don’t rewrite it

Ask before modifying logic

Prefer duplication over cleverness (for now)