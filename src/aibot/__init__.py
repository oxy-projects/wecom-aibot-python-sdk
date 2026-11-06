"""
企业微信智能机器人 Python SDK

基于 WebSocket 长连接通道，提供消息收发、流式回复、模板卡片、事件回调、文件下载解密等核心能力。
"""

__version__ = "2.0.0"

from .client import WSClient
from .api import WeComApiClient
from .ws import WsConnectionManager
from .message_handler import MessageHandler
from .crypto_utils import decrypt_file
from .wecom_crypto import WecomCrypto, decode_encoding_aes_key, pkcs7_pad, pkcs7_unpad
from .logger import DefaultLogger
from .utils import generate_req_id, generate_random_string
from .types import (
    WeComMediaType,
    UploadMediaOptions,
    UploadMediaFinishResult,
    WSAuthFailureError,
    WSReconnectExhaustedError,
    MessageType,
    MediaType,
    EventType,
    TemplateCardType,
    WsCmd,
    WSClientOptions,
    WsFrame,
    WsFrameHeaders,
    Logger,
)

__all__ = [
    # 版本
    "__version__",
    "WecomCrypto",
    "decode_encoding_aes_key",
    "pkcs7_pad",
    "pkcs7_unpad",
    "WeComMediaType",
    "UploadMediaOptions",
    "UploadMediaFinishResult",
    "WSAuthFailureError",
    "WSReconnectExhaustedError",
    # 类
    "WSClient",
    "WeComApiClient",
    "WsConnectionManager",
    "MessageHandler",
    "DefaultLogger",
    # 函数
    "decrypt_file",
    "generate_req_id",
    "generate_random_string",
    # 枚举/常量
    "MessageType",
    "MediaType",
    "EventType",
    "TemplateCardType",
    "WsCmd",
    # 类型
    "WSClientOptions",
    "WsFrame",
    "WsFrameHeaders",
    "Logger",
]
