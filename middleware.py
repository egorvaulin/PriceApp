import streamlit as st
import pandas as pd
import toml
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import pyarrow.parquet as pq
import io

config = toml.load("./.streamlit/secrets.toml")
key = config["secrets"]["data_key"].encode('utf-8')

def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    pt = unpad(cipher.decrypt(data[16:]), AES.block_size)
    return pt

with open('./data/Logs.parquet', 'rb') as f:
    encrypted_data = f.read()
    buffer = io.BytesIO(decrypt_data(encrypted_data, key))
    df = pd.read_parquet(buffer, engine='pyarrow')

def creds_entered():
    if st.session_state.user.strip() in df['usernames'].values:
        i = df['usernames'].tolist().index(st.session_state.user.strip())
        if st.session_state.password.strip() == df['password'][i]:
            st.session_state['authenticated'] = True
        else:
            st.session_state['authenticated'] = False
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
