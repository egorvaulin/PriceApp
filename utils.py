import logging
import io
from pathlib import Path
import dropbox
import streamlit as st
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import polars as pl

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).parent


def get_key():
    return st.secrets["secrets"]["data_key"].encode("utf-8")


def data_path(filename):
    return _ROOT / "data" / filename


def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    return unpad(cipher.decrypt(data[16:]), AES.block_size)


@st.cache_data
def load_parquet(path, key):
    try:
        path = Path(path)
        if path.exists():
            with open(path, "rb") as f:
                encrypted_data = f.read()
        else:
            dbx = dropbox.Dropbox(st.secrets["dropbox"]["token"])
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
