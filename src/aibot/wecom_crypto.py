"""企业微信通用消息加解密和 SHA1 签名工具。"""
import base64
import hashlib
import hmac
import os
import struct
from typing import Dict, Optional

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


def decode_encoding_aes_key(encoding_aes_key: str) -> bytes:
    value = encoding_aes_key.strip()
    key = base64.b64decode(value + "=" * (-len(value) % 4), validate=True)
    if len(key) != 32:
        raise ValueError("encoding_aes_key must decode to 32 bytes")
    return key


def pkcs7_pad(data: bytes, block_size: int = 32) -> bytes:
    if not 1 <= block_size <= 255:
        raise ValueError("Invalid block size")
    size = block_size - len(data) % block_size
    return data + bytes([size]) * size


def pkcs7_unpad(data: bytes, block_size: int = 32) -> bytes:
    if (not data or not 1 <= data[-1] <= block_size or data[-1] > len(data)
            or data[-data[-1]:] != bytes([data[-1]]) * data[-1]):
        raise ValueError("Invalid PKCS#7 padding")
    return data[:-data[-1]]


class WecomCrypto:
    def __init__(self, token: str, encoding_aes_key: str, receive_id: Optional[str] = None):
        if not token:
            raise ValueError("token is required")
        self._token = token
        self._key = decode_encoding_aes_key(encoding_aes_key)
        self._receive_id = receive_id or ""

    def compute_signature(self, timestamp: str, nonce: str, encrypt: str) -> str:
        parts = sorted([self._token, str(timestamp), str(nonce), encrypt])
        return hashlib.sha1("".join(parts).encode("utf-8")).hexdigest()

    def verify_signature(self, signature: str, timestamp: str, nonce: str, encrypt: str) -> bool:
        return hmac.compare_digest(signature, self.compute_signature(timestamp, nonce, encrypt))

    def encrypt(self, plain_text: str, timestamp: str, nonce: str) -> Dict[str, str]:
        message = plain_text.encode("utf-8")
        payload = os.urandom(16) + struct.pack(">I", len(message)) + message + self._receive_id.encode("utf-8")
        cipher = Cipher(algorithms.AES(self._key), modes.CBC(self._key[:16])).encryptor()
        encrypted = base64.b64encode(cipher.update(pkcs7_pad(payload)) + cipher.finalize()).decode("ascii")
        return {"encrypt": encrypted, "signature": self.compute_signature(timestamp, nonce, encrypted)}

    def decrypt(self, encrypt_text: str) -> str:
        cipher = Cipher(algorithms.AES(self._key), modes.CBC(self._key[:16])).decryptor()
        payload = pkcs7_unpad(cipher.update(base64.b64decode(encrypt_text, validate=True)) + cipher.finalize())
        if len(payload) < 20:
            raise ValueError("Invalid message payload length")
        end = 20 + struct.unpack(">I", payload[16:20])[0]
        if end > len(payload):
            raise ValueError("Invalid message length")
        if self._receive_id and payload[end:].decode("utf-8") != self._receive_id:
            raise ValueError("receive_id mismatch")
        return payload[20:end].decode("utf-8")
