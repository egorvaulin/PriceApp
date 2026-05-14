"""
Encrypt a parquet file with AES-GCM and save it to data/.
Use this for weekly data updates before uploading to Dropbox.

Usage: python encrypt_file.py path\to\new_file.parquet
"""
import sys
import tomllib
from pathlib import Path
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

ROOT = Path(__file__).parent

with open(ROOT / ".streamlit" / "secrets.toml", "rb") as f:
    config = tomllib.load(f)
key = config["secrets"]["data_key"].encode("utf-8")


def encrypt_gcm(plaintext, key):
    nonce = get_random_bytes(12)
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    ciphertext, tag = cipher.encrypt_and_digest(plaintext)
    return nonce + tag + ciphertext


if len(sys.argv) != 2:
    print("Usage: python encrypt_file.py path\\to\\file.parquet")
    sys.exit(1)

src = Path(sys.argv[1])
if not src.exists():
    print(f"File not found: {src}")
    sys.exit(1)

with open(src, "rb") as f:
    plaintext = f.read()

out = ROOT / "data" / src.name
with open(out, "wb") as f:
    f.write(encrypt_gcm(plaintext, key))

print(f"Encrypted: {out}")
print("Upload this file to Dropbox to update the cloud app.")
