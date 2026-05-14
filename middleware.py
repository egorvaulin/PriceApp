import logging
import streamlit as st
import streamlit_authenticator as stauth

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _get_authenticator():
    if "authenticator" not in st.session_state:
        credentials = st.secrets["credentials"].to_dict()
        cookie = st.secrets["cookie"]
        st.session_state["authenticator"] = stauth.Authenticate(
            credentials,
            cookie["name"],
            cookie["key"],
            cookie["expiry_days"],
            auto_hash=True,
        )
    return st.session_state["authenticator"]


def authenticate_user():
    authenticator = _get_authenticator()
    authenticator.login(max_login_attempts=5)

    status = st.session_state.get("authentication_status")

    if status:
        authenticator.logout(location="sidebar")
        logger.info("Authenticated: %s", st.session_state.get("username"))
        return True
    elif status is False:
        st.error("Username/password is incorrect")
        logger.warning("Failed login: %s", st.session_state.get("username"))
        return False
    else:
        return False
