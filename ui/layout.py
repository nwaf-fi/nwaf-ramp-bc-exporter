import streamlit as st
import os
from auth.azure_auth import REDIRECT_URI, SESSION_TOKEN_KEY


def load_css():
    css_file = os.path.join(os.path.dirname(__file__), '..', 'assets', 'styles.css')
    css_file = os.path.normpath(css_file)

    if os.path.exists(css_file):
        with open(css_file) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    else:
        # Fallback styles only — no functional changes
        st.markdown("""
        <style>
        .app-header {
            background: linear-gradient(90deg, #1a1f36, #20264a);
            color: white;
            padding: 1.5rem 2.25rem;
            margin: -2rem -2rem 2rem -2rem;
            border-bottom: 3px solid #3498db;
        }

        .app-header h1 {
            font-size: 1.8rem;
            font-weight: 600;
            margin: 0;
            letter-spacing: 0.2px;
        }

        .app-header p {
            margin: 0.35rem 0 0 0;
            color: #cbd5e0;
            font-size: 0.95rem;
        }

        .sidebar-section {
            padding: 0.75rem 0;
        }

        .sidebar-title {
            font-size: 0.9rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.4rem;
            color: #6b7280;
        }
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
    # ---- Guidance ----
    with st.sidebar:
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.markdown('<div class="sidebar-title">Usage</div>', unsafe_allow_html=True)
        st.info(
            "Exports are managed per tab:\n\n"
            "• Credit Cards\n"
            "• Invoices\n"
            "• Reimbursements\n\n"
            "Each tab controls its own date range, preview, and downloads."
        )
        st.markdown('</div>', unsafe_allow_html=True)

        # ---- System Overview ----
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.markdown('<div class="sidebar-title">System Overview</div>', unsafe_allow_html=True)
        st.markdown(
            "- Secure Microsoft Azure AD authentication\n"
            "- Real-time Ramp API integration\n"
            "- Business Central–compatible exports\n"
            "- Preview-first, dry-run-safe workflows"
        )
        st.markdown('</div>', unsafe_allow_html=True)

        st.divider()

        # ---- User Profile ----
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.markdown('<div class="sidebar-title">User</div>', unsafe_allow_html=True)
        st.success(f"**{user_name}**")
        if user_email and user_email != user_name:
            st.caption(f"📧 {user_email}")
        st.markdown('</div>', unsafe_allow_html=True)

        st.divider()

        # ---- Logout ----
        if st.button("🚪 Log out", use_container_width=True, key="logout_btn"):
            st.session_state.pop(SESSION_TOKEN_KEY, None)
            st.query_params.clear()
            logout_url = (
                "https://login.microsoftonline.com/common/oauth2/v2.0/logout"
                f"?post_logout_redirect_uri={REDIRECT_URI}"
            )
            st.success("✅ You have been logged out successfully.")
            st.markdown(f"[🔐 Sign in again]({logout_url})")
            st.stop()
