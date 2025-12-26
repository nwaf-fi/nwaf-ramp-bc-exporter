import streamlit as st
import msal
import base64
import hmac
import hashlib
import time
from uuid import uuid4
from urllib.parse import urlencode

# Session keys
SESSION_TOKEN_KEY = "msal_token"
SESSION_STATE_KEY = "msal_state"
TOKEN_ACQUIRED_TIME_KEY = "token_acquired_at"

# Read secrets from Streamlit secrets (the app must keep storing them there)
CLIENT_ID = st.secrets.get("AZURE_CLIENT_ID")
CLIENT_SECRET = st.secrets.get("AZURE_CLIENT_SECRET")
TENANT_ID = st.secrets.get("AZURE_TENANT_ID")
REDIRECT_URI = st.secrets.get("AZURE_REDIRECT_URI")
SCOPES = [s.strip() for s in st.secrets.get("AUTH_SCOPES", "User.Read").split(",")]
_reserved = {"openid", "profile", "offline_access", "email"}
SCOPES_SANITIZED = [s for s in SCOPES if s and s.lower() not in _reserved]
if not SCOPES_SANITIZED:
    SCOPES_SANITIZED = ["User.Read"]

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"


def _make_signed_state(raw_state: str, ttl: int = 600) -> str:
    ts = str(int(time.time()))
    payload = f"{raw_state}:{ts}".encode("utf-8")
    b64 = base64.urlsafe_b64encode(payload).decode("utf-8").rstrip("=")
    sig = hmac.new(CLIENT_SECRET.encode("utf-8"), b64.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"


def _verify_signed_state(signed_state: str, max_age: int = 600):
    try:
        if not signed_state or "." not in signed_state:
            return False, None
        b64, sig = signed_state.split('.', 1)
        expected_sig = hmac.new(CLIENT_SECRET.encode("utf-8"), b64.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected_sig, sig):
            return False, None
        pad = '=' * (-len(b64) % 4)
        payload = base64.urlsafe_b64decode(b64 + pad).decode("utf-8")
        raw_state, ts = payload.rsplit(':', 1)
        if int(time.time()) - int(ts) > max_age:
            return False, None
        return True, raw_state
    except Exception:
        return False, None


def get_valid_token():
    token = st.session_state.get(SESSION_TOKEN_KEY)
    if not token:
        return None
    token_acquired_at = st.session_state.get(TOKEN_ACQUIRED_TIME_KEY, 0)
    expires_in = token.get('expires_in', 3600)
    expires_at = token_acquired_at + expires_in
    if time.time() >= (expires_at - 300):
        # Try silent refresh
        cca = msal.ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
        accounts = cca.get_accounts()
        if accounts:
            result = cca.acquire_token_silent(SCOPES_SANITIZED, account=accounts[0])
            if result and result.get("access_token"):
                st.session_state[SESSION_TOKEN_KEY] = result
                st.session_state[TOKEN_ACQUIRED_TIME_KEY] = time.time()
                return result
        return None
    return token


def build_auth_url(state: str) -> str:
    cca = msal.ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
    return cca.get_authorization_request_url(scopes=SCOPES_SANITIZED, state=state, redirect_uri=REDIRECT_URI)


def ensure_authenticated():
    """Perform authentication flow and return (user_name, user_email).

    This reproduces the behavior moved verbatim from the monolithic app file and uses
    st.stop() when needed to render the login UX and halt further execution.
    """
    # Ensure the secrets are configured
    if not CLIENT_ID or not TENANT_ID or not REDIRECT_URI:
        st.error("Authentication is not configured. Add AZURE_CLIENT_ID, AZURE_TENANT_ID and AZURE_REDIRECT_URI to app secrets.")
        st.stop()

    token = get_valid_token()
    if token and token.get("access_token"):
        # token is valid
        pass
    else:
        qp = st.query_params
        if "code" in qp:
            code_list = qp.get("code")
            if isinstance(code_list, list):
                code = code_list[0] if code_list else ""
            else:
                code = str(code_list) if code_list else ""

            received_state = qp.get("state")
            if isinstance(received_state, list):
                received_state = received_state[0] if received_state else None
            expected_state = st.session_state.get(SESSION_STATE_KEY)

            valid_state = False
            if received_state and "." in str(received_state):
                ok, raw = _verify_signed_state(str(received_state))
                if ok:
                    valid_state = True
                    st.session_state[SESSION_STATE_KEY] = raw
            elif received_state and expected_state and str(received_state) == str(expected_state):
                valid_state = True

            if not received_state or not valid_state:
                st.error("Security Error: Invalid state parameter detected.")
                st.warning("This could indicate a Cross-Site Request Forgery (CSRF) attempt.")
                st.info("Please try signing in again. If this persists, contact your system administrator.")
                st.session_state.clear()
                st.stop()

            cca = msal.ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
            try:
                result = cca.acquire_token_by_authorization_code(code, scopes=SCOPES_SANITIZED, redirect_uri=REDIRECT_URI)
            except Exception as ex:
                st.error('Authentication failed. Please try again.')
                import logging
                logging.error(f'Token exchange exception: {str(ex)}')
                st.stop()

            if result and result.get("access_token"):
                st.session_state[SESSION_TOKEN_KEY] = result
                st.session_state[TOKEN_ACQUIRED_TIME_KEY] = time.time()
                st.query_params.clear()
                token = result
            else:
                err = {
                    "error": result.get("error") if isinstance(result, dict) else str(result),
                    "error_description": result.get("error_description") if isinstance(result, dict) else None,
                    "claims": result.get("claims") if isinstance(result, dict) else None,
                }
                st.error("Authentication failed during token exchange.")
                st.write(err)
                st.stop()
        else:
            raw_state = str(uuid4())
            signed_state = _make_signed_state(raw_state)
            st.session_state[SESSION_STATE_KEY] = raw_state
            auth_url = build_auth_url(signed_state)

            st.markdown("""
            <div class="auth-container">
                <div class="auth-icon">🔒</div>
                <h1 class="auth-title">Authentication Required</h1>
                <p class="auth-subtitle">Please authenticate with your Microsoft corporate account to access the financial data export platform.</p>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("**Sign in options:**")
            st.markdown(f"[Sign in with Microsoft]({auth_url})")
            st.write("If your browser prevents automatic navigation, copy the URL below and paste it into a new tab or the current tab's address bar.")
            st.code(auth_url, language=None)

            with st.expander("ℹ️ Security Information"):
                st.markdown("""
                **Why is this step required?**

                For security compliance, the authentication flow requires direct navigation to the Microsoft identity provider. 
                This ensures:
                - Secure session management
                - Proper authorization token delivery
                - CSRF protection
                - Compliance with enterprise security policies
                """)
            st.stop()

    # Show a friendly welcome using identity claims (if available)
    id_claims = st.session_state.get(SESSION_TOKEN_KEY, {}).get("id_token_claims", {})
    if not id_claims:
        id_token = st.session_state.get(SESSION_TOKEN_KEY, {}).get("id_token")
        if id_token:
            try:
                import jwt
                claims = jwt.decode(id_token, options={"verify_signature": False})
                id_claims = claims
            except Exception:
                id_claims = {}

    user_name = id_claims.get("name") or id_claims.get("preferred_username") or id_claims.get("email", "User")
    user_email = id_claims.get("email") or id_claims.get("preferred_username", "")

    return user_name, user_email
