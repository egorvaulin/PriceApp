import logging
import streamlit as st
import pandas as pd
import io
from utils import get_key, decrypt_data, data_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

key = get_key()

try:
    with open(data_path("Logs.parquet"), "rb") as f:
        encrypted_data = f.read()
        buffer = io.BytesIO(decrypt_data(encrypted_data, key))
        df = pd.read_parquet(buffer, engine='pyarrow')
except Exception as e:
    logger.error("Failed to load credentials file: %s", e)
    raise

def creds_entered():
    if st.session_state.user.strip() in df['usernames'].values:
        i = df['usernames'].tolist().index(st.session_state.user.strip())
        if st.session_state.password.strip() == df['password'][i]:
            st.session_state['authenticated'] = True
            logger.info("Successful login: %s", st.session_state.user.strip())
        else:
            st.session_state['authenticated'] = False
            logger.warning("Failed login attempt for user: %s", st.session_state.user.strip())
            if not st.session_state['password']:
                st.warning('Please enter a password')
            else:
                st.error('Username/password is incorrect')
    else:
        st.warning('Please enter a username')

def authenticate_user():
    if 'authenticated' not in st.session_state:
        st.text_input(label='Username', value='', key='user', on_change=creds_entered)
        st.text_input(label='Password', value='', key='password', 
                    type='password', on_change=creds_entered)
        return False
    else:
        if st.session_state.authenticated:
            return True
        else:
            st.text_input(label='Username', value='', key='user', on_change=creds_entered)
            st.text_input(label='Password', value='', key='password', 
                        type='password', on_change=creds_entered)
            return False
