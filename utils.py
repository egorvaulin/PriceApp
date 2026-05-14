import logging
import io
import toml
from pathlib import Path
import streamlit as st
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import polars as pl

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).parent


def get_key():
    config = toml.load(_ROOT / ".streamlit" / "secrets.toml")
    return config["secrets"]["data_key"].encode("utf-8")


def data_path(filename):
    return _ROOT / "data" / filename


def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    return unpad(cipher.decrypt(data[16:]), AES.block_size)


@st.cache_data
def load_parquet(path, key):
    try:
        with open(path, "rb") as f:
            encrypted_data = f.read()
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
