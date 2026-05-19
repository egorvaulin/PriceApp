import logging
import io
from pathlib import Path
import dropbox
import streamlit as st
from Crypto.Cipher import AES
import polars as pl

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).parent


def get_key():
    return st.secrets["secrets"]["data_key"].encode("utf-8")


def data_path(filename):
    return _ROOT / "data" / filename


def decrypt_data(data, key):
    nonce, tag, ciphertext = data[:12], data[12:28], data[28:]
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    return cipher.decrypt_and_verify(ciphertext, tag)


@st.cache_data
def load_parquet(path, key):
    try:
        path = Path(path)
        if path.exists():
            with open(path, "rb") as f:
                encrypted_data = f.read()
        else:
            dbx = dropbox.Dropbox(
                oauth2_refresh_token=st.secrets["dropbox"]["refresh_token"],
                app_key=st.secrets["dropbox"]["app_key"],
                app_secret=st.secrets["dropbox"]["app_secret"],
            )
            _, response = dbx.files_download(f"/{path.name}")
            encrypted_data = response.content
        buffer = io.BytesIO(decrypt_data(encrypted_data, key))
        return pl.read_parquet(buffer)
    except Exception as e:
        logger.error("Failed to load %s: %s", path, e)
        raise


def apply_styling():
    st.markdown(
        """<style>
        body {font-family: Arial;}
        footer {visibility: hidden;}
        button[kind="header"] {display: none;}
        </style>""",
        unsafe_allow_html=True,
    )
