import streamlit as st
import os
from auth.azure_auth import REDIRECT_URI, SESSION_TOKEN_KEY


def load_css():
    css_file = os.path.join(os.path.dirname(__file__), '..', 'assets', 'styles.css')
    css_file = os.path.normpath(css_file)
    if os.path.exists(css_file):
        with open(css_file) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    else:
        st.markdown("""
        <style>
        .app-header { background-color: #1a1f36; color: white; padding: 1.5rem 2rem; margin: -2rem -2rem 2rem -2rem; border-bottom: 3px solid #3498db; }
        .app-header h1 { font-size: 1.75rem; font-weight: 600; margin: 0; }
        .app-header p { margin: 0.5rem 0 0 0; color: #cbd5e0; font-size: 0.95rem; }
        </style>
        """, unsafe_allow_html=True)


def render_header():
    st.markdown("""
    <div class="app-header">
        <h1>Ramp → Business Central Export</h1>
        <p>Financial Data Integration Platform | Northwest Area Foundation</p>
    </div>
    """, unsafe_allow_html=True)


def render_sidebar(user_name: str, user_email: str):
    # Per-tab guidance
    st.sidebar.markdown("**Per-tab controls**")
    st.sidebar.info("Date ranges, export generation, and downloads are managed in each export tab (Credit Cards, Invoices, Reimbursements). Use the relevant tab to preview, generate, and download exports.")

    # Compact system overview
    st.sidebar.markdown("### System Overview")
    st.sidebar.markdown("- **Secure Microsoft Azure AD authentication**\n- **Real-time API integration with Ramp**\n- **Business Central-compatible exports (CSV, Excel)**\n- **Per-tab previews and dry-run-first export flows**")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 👤 User Profile")
    st.sidebar.success(f"**{user_name}**")
    if user_email and user_email != user_name:
        st.sidebar.caption(f"📧 {user_email}")
    st.sidebar.markdown("---")

    if st.sidebar.button("🚪 Log out", use_container_width=True):
        st.session_state.pop(SESSION_TOKEN_KEY, None)
        st.query_params.clear()
        logout_url = (
            f"https://login.microsoftonline.com/common/oauth2/v2.0/logout?post_logout_redirect_uri={REDIRECT_URI}"
        )
        st.success("✅ You have been logged out successfully.")
        st.markdown(f"[🔐 Sign in again]({logout_url})")
        st.stop()
