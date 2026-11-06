"""
WSClient 核心客户端

对标 Node.js SDK src/client.ts
继承自 pyee.AsyncIOEventEmitter，组合 WsConnectionManager + MessageHandler + WeComApiClient。
"""

import asyncio
import base64
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from pyee.asyncio import AsyncIOEventEmitter

from .api import WeComApiClient
from .crypto_utils import decrypt_file
from .logger import DefaultLogger
from .message_handler import MessageHandler
from .types import (
    MediaType, WsCmd, WsFrame, WsFrameHeaders, WSClientOptions,
    UploadMediaOptions, UploadMediaFinishResult
)
from .utils import generate_req_id
from .ws import WsConnectionManager


class WSClient(AsyncIOEventEmitter):
    """
    企业微信智能机器人 Python SDK 核心客户端

    基于 asyncio + pyee 的事件驱动架构，提供 WebSocket 长连接消息收发能力。
    """

    def __init__(self, options: WSClientOptions) -> None:
        super().__init__()

        self._options = options
        self._logger = options.logger or DefaultLogger()
        self._started = False

        # 初始化 API 客户端（仅用于文件下载）
        self._api_client = WeComApiClient(
            self._logger,
            self._options.request_timeout,
        )

        # 初始化 WebSocket 管理器
        self._ws_manager = WsConnectionManager(
            self._logger,
            heartbeat_interval=self._options.heartbeat_interval,
            reconnect_base_delay=self._options.reconnect_interval,
            max_reconnect_attempts=self._options.max_reconnect_attempts,
            ws_url=self._options.ws_url or None,
            ws_options=self._options.ws_options,
            max_auth_failure_attempts=self._options.max_auth_failure_attempts,
            max_reply_queue_size=self._options.max_reply_queue_size,
        )

        # 设置认证凭证
        extra = {k: v for k, v in {"scene": options.scene, "plug_version": options.plug_version}.items() if v is not None}
        self._ws_manager.set_credentials(options.bot_id, options.secret, extra)

        # 初始化消息处理器
        self._message_handler = MessageHandler(self._logger)

        # 绑定 WebSocket 事件
        self._setup_ws_events()

    def _setup_ws_events(self) -> None:
        """设置 WebSocket 事件处理"""
        self._ws_manager.on_connected = lambda: self.emit("connected")

        def _on_authenticated() -> None:
            self._logger.info("Authenticated")
            self.emit("authenticated")

        self._ws_manager.on_authenticated = _on_authenticated

        def _on_server_disconnect(reason: str) -> None:
            self._started = False
            self.emit("disconnected", reason)

        self._ws_manager.on_server_disconnect = _on_server_disconnect

        self._ws_manager.on_disconnected = lambda reason: self.emit(
            "disconnected", reason
        )
        self._ws_manager.on_reconnecting = lambda attempt: self.emit(
            "reconnecting", attempt
        )
        self._ws_manager.on_error = lambda error: self.emit("error", error)
        self._ws_manager.on_message = lambda frame: self._message_handler.handle_frame(
            frame, self
        )

    async def connect(self) -> "WSClient":
        """
        建立 WebSocket 长连接

        SDK 使用内置默认地址建立连接，连接成功后自动发送认证帧（bot_id + secret）。

        :return: 返回 self，支持链式调用
        """
        if self._started:
            self._logger.warn("Client already connected")
            return self

        self._logger.info("Establishing WebSocket connection...")
        self._started = True

        await self._ws_manager.connect()

        return self

    def disconnect(self) -> None:
        """断开 WebSocket 连接"""
        if not self._started:
            self._logger.warn("Client not connected")
            return

        self._logger.info("Disconnecting...")
        self._started = False
        self._ws_manager.disconnect()
        self._logger.info("Disconnected")

    async def aclose(self) -> None:
        """断开连接并等待后台连接任务完成，关闭事件循环前应等待此方法。"""
        if self._started:
            self.disconnect()
        if self._ws_manager._disconnect_task is not None:
            await self._ws_manager._disconnect_task
        else:
            await self._ws_manager._async_disconnect()

    async def reply(
        self,
        frame: WsFrameHeaders,
        body: Dict[str, Any],
        cmd: Optional[str] = None,
    ) -> WsFrame:
        """
        通过 WebSocket 通道发送回复消息（通用方法）

        :param frame: 收到的原始 WebSocket 帧，透传 headers.req_id
        :param body: 回复消息体
        :param cmd: 发送的命令类型
        :return: 回执帧
        """
        headers = frame.get("headers", {})
        req_id = headers.get("req_id", "")
        return await self._ws_manager.send_reply(req_id, body, cmd or WsCmd.RESPONSE)

    async def reply_stream(
        self,
        frame: WsFrameHeaders,
        stream_id: str,
        content: str,
        finish: bool = False,
        msg_item: Optional[List[Dict[str, Any]]] = None,
        feedback: Optional[Dict[str, Any]] = None,
    ) -> WsFrame:
        """
        发送流式文本回复（便捷方法）

        :param frame: 收到的原始 WebSocket 帧，透传 headers.req_id
        :param stream_id: 流式消息 ID
        :param content: 回复内容（支持 Markdown）
        :param finish: 是否结束流式消息，默认 False
        :param msg_item: 图文混排项（仅在 finish=True 时有效）
        :param feedback: 反馈信息（仅在首次回复时设置）
        :return: 回执帧
        """
        stream: Dict[str, Any] = {
            "id": stream_id,
            "finish": finish,
            "content": content,
        }

        # msg_item 仅在 finish=True 时支持
        if finish and msg_item and len(msg_item) > 0:
            stream["msg_item"] = msg_item

        # feedback 仅在首次回复时设置
        if feedback:
            stream["feedback"] = feedback

        return await self.reply(
            frame,
            {
                "msgtype": "stream",
                "stream": stream,
            },
        )

    async def reply_welcome(
        self,
        frame: WsFrameHeaders,
        body: Dict[str, Any],
    ) -> WsFrame:
        """
        发送欢迎语回复

        注意：此方法需要使用对应事件（如 enter_chat）的 req_id 才能调用。
        收到事件回调后需在 5 秒内发送回复，超时将无法发送欢迎语。

        :param frame: 对应事件的 WebSocket 帧
        :param body: 欢迎语消息体（支持文本或模板卡片格式）
        :return: 回执帧
        """
        return await self.reply(frame, body, WsCmd.RESPONSE_WELCOME)

    async def reply_template_card(
        self,
        frame: WsFrameHeaders,
        template_card: Dict[str, Any],
        feedback: Optional[Dict[str, Any]] = None,
    ) -> WsFrame:
        """
        回复模板卡片消息

        :param frame: 收到的原始 WebSocket 帧
        :param template_card: 模板卡片内容
        :param feedback: 反馈信息
        :return: 回执帧
        """
        card = {**template_card, "feedback": feedback} if feedback else template_card
        body = {
            "msgtype": "template_card",
            "template_card": card,
        }
        return await self.reply(frame, body)

    async def reply_stream_with_card(
        self,
        frame: WsFrameHeaders,
        stream_id: str,
        content: str,
        finish: bool = False,
        msg_item: Optional[List[Dict[str, Any]]] = None,
        stream_feedback: Optional[Dict[str, Any]] = None,
        template_card: Optional[Dict[str, Any]] = None,
        card_feedback: Optional[Dict[str, Any]] = None,
    ) -> WsFrame:
        """
        发送流式消息 + 模板卡片组合回复

        :param frame: 收到的原始 WebSocket 帧
        :param stream_id: 流式消息 ID
        :param content: 回复内容（支持 Markdown）
        :param finish: 是否结束流式消息，默认 False
        :param msg_item: 图文混排项（仅在 finish=True 时有效）
        :param stream_feedback: 流式消息反馈信息（首次回复时设置）
        :param template_card: 模板卡片内容（同一消息只能回复一次）
        :param card_feedback: 模板卡片反馈信息
        :return: 回执帧
        """
        stream: Dict[str, Any] = {
            "id": stream_id,
            "finish": finish,
            "content": content,
        }

        if finish and msg_item and len(msg_item) > 0:
            stream["msg_item"] = msg_item

        if stream_feedback:
            stream["feedback"] = stream_feedback

        body: Dict[str, Any] = {
            "msgtype": "stream_with_template_card",
            "stream": stream,
        }

        if template_card:
            card = (
                {**template_card, "feedback": card_feedback}
                if card_feedback
                else template_card
            )
            body["template_card"] = card

        return await self.reply(frame, body)

    async def update_template_card(
        self,
        frame: WsFrameHeaders,
        template_card: Dict[str, Any],
        userids: Optional[List[str]] = None,
    ) -> WsFrame:
        """
        更新模板卡片

        注意：此方法需要使用对应事件（template_card_event）的 req_id 才能调用。
        收到事件回调后需在 5 秒内发送回复，超时将无法更新卡片。

        :param frame: 对应事件的 WebSocket 帧
        :param template_card: 模板卡片内容（task_id 需跟回调收到的 task_id 一致）
        :param userids: 要替换模版卡片消息的 userid 列表
        :return: 回执帧
        """
        body: Dict[str, Any] = {
            "response_type": "update_template_card",
            "template_card": template_card,
        }
        if userids and len(userids) > 0:
            body["userids"] = userids

        return await self.reply(frame, body, WsCmd.RESPONSE_UPDATE)

    async def send_message(
        self,
        chatid: str,
        body: Dict[str, Any],
    ) -> WsFrame:
        """
        主动发送消息

        向指定会话（单聊或群聊）主动推送消息，无需依赖收到的回调帧。

        :param chatid: 会话 ID，单聊填用户的 userid，群聊填对应群聊的 chatid
        :param body: 消息体（支持 markdown 或 template_card 格式）
        :return: 回执帧
        """
        req_id = generate_req_id(WsCmd.SEND_MSG)
        full_body = {"chatid": chatid, **body}
        return await self._ws_manager.send_reply(req_id, full_body, WsCmd.SEND_MSG)

    async def upload_media(self, file_buffer: bytes, options: UploadMediaOptions,
                           md5: Optional[str] = None) -> UploadMediaFinishResult:
        """上传临时素材；options 包含 type 和 filename，返回 media_id 等信息。

        每片 512 KiB，最多 100 片；每片失败后最多重试两次。
        md5 可传入预先计算的校验值；省略时自动计算。
        """
        media_type, filename = options["type"], options["filename"]
        if isinstance(media_type, MediaType):
            media_type = media_type.value
        self._validate_media_type(media_type)
        chunk_size = 512 * 1024
        total_chunks = (len(file_buffer) + chunk_size - 1) // chunk_size
        if not 1 <= total_chunks <= 100:
            raise ValueError("Media must be non-empty and at most 50 MiB")
        async def request(cmd: str, body: Dict[str, Any]) -> WsFrame:
            return await self._ws_manager.send_reply(generate_req_id(cmd), body, cmd)
        result = await request(WsCmd.UPLOAD_MEDIA_INIT, {
            "type": media_type, "filename": filename, "total_size": len(file_buffer),
            "total_chunks": total_chunks, "md5": md5 or hashlib.md5(file_buffer).hexdigest(),
        })
        upload_id = (result.get("body") or {}).get("upload_id")
        if not upload_id:
            raise RuntimeError("Upload init failed: no upload_id returned")
        concurrency = total_chunks if total_chunks <= 4 else 3 if total_chunks <= 10 else 2
        semaphore = asyncio.Semaphore(concurrency)
        async def upload_chunk(index: int) -> None:
            async with semaphore:
                data = base64.b64encode(file_buffer[index * chunk_size:(index + 1) * chunk_size]).decode("ascii")
                for attempt in range(3):
                    try:
                        await request(WsCmd.UPLOAD_MEDIA_CHUNK, {
                            "upload_id": upload_id, "chunk_index": index, "base64_data": data,
                        })
                        return
                    except Exception:
                        if attempt == 2:
                            raise
        tasks = [asyncio.create_task(upload_chunk(i)) for i in range(total_chunks)]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        result = await request(WsCmd.UPLOAD_MEDIA_FINISH, {"upload_id": upload_id})
        body = result.get("body") or {}
        if not body.get("media_id"):
            raise RuntimeError("Upload finish failed: no media_id returned")
        return {"type": body.get("type", media_type), "media_id": body["media_id"],
                "created_at": body.get("created_at", datetime.now(timezone.utc).isoformat())}

    @staticmethod
    def _validate_media_type(media_type: str) -> None:
        if media_type not in ("file", "image", "voice", "video"):
            raise ValueError("Unsupported media type: " + str(media_type))

    @classmethod
    def _media_body(cls, media_type: str, media_id: str,
                    video_options: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        if isinstance(media_type, MediaType):
            media_type = media_type.value
        cls._validate_media_type(media_type)
        media = {"media_id": media_id}
        if media_type == "video" and video_options:
            media.update({k: video_options[k] for k in ("title", "description") if video_options.get(k)})
        return {"msgtype": media_type, media_type: media}

    async def reply_media(self, frame: WsFrameHeaders, media_type: str, media_id: str,
                          video_options: Optional[Dict[str, str]] = None) -> WsFrame:
        """通过回调回复图片、文件、语音或视频素材。"""
        return await self.reply(frame, self._media_body(media_type, media_id, video_options))

    async def reply_image(self, frame: WsFrameHeaders, media_id: str) -> WsFrame:
        """使用已上传素材回复图片。"""
        return await self.reply_media(frame, "image", media_id)

    async def reply_file(self, frame: WsFrameHeaders, media_id: str) -> WsFrame:
        """使用已上传素材回复文件。"""
        return await self.reply_media(frame, "file", media_id)

    async def reply_voice(self, frame: WsFrameHeaders, media_id: str) -> WsFrame:
        """使用已上传素材回复语音。"""
        return await self.reply_media(frame, "voice", media_id)

    async def reply_video(self, frame: WsFrameHeaders, media_id: str,
                          title: Optional[str] = None,
                          description: Optional[str] = None) -> WsFrame:
        """使用已上传素材回复视频，可附带标题和描述。"""
        return await self.reply_media(
            frame, "video", media_id,
            {k: v for k, v in {"title": title, "description": description}.items() if v},
        )

    async def send_media_message(self, chatid: str, media_type: str, media_id: str,
                                 video_options: Optional[Dict[str, str]] = None) -> WsFrame:
        """向指定会话主动发送媒体素材。"""
        return await self.send_message(chatid, self._media_body(media_type, media_id, video_options))

    def has_pending_reply_ack(self, frame: WsFrameHeaders) -> bool:
        return self._ws_manager.has_pending_ack(frame.get("headers", {}).get("req_id", ""))

    async def reply_stream_non_blocking(
        self, frame: WsFrameHeaders, stream_id: str, content: str, finish: bool = False,
        msg_item: Optional[List[Dict[str, Any]]] = None,
        feedback: Optional[Dict[str, Any]] = None,
    ) -> Union[WsFrame, str]:
        """上一帧尚未确认时返回 'skipped'；最终帧始终进入发送队列。"""
        if not finish and self.has_pending_reply_ack(frame):
            return "skipped"
        return await self.reply_stream(frame, stream_id, content, finish, msg_item, feedback)

    async def download_file(
        self, url: str, aes_key: Optional[str] = None
    ) -> Tuple[bytes, Optional[str]]:
        """
        下载文件并使用 AES 密钥解密

        :param url: 文件下载地址
        :param aes_key: AES 解密密钥（Base64 编码），取自消息中 image.aeskey 或 file.aeskey
        :return: (解密后的文件数据, 文件名)
        """
        self._logger.info("Downloading and decrypting file...")

        try:
            # 下载加密的文件数据
            encrypted_data, filename = await self._api_client.download_file_raw(url)

            # 如果没有提供 aes_key，直接返回原始数据
            if not aes_key:
                self._logger.warn("No aes_key provided, returning raw file data")
                return encrypted_data, filename

            # 使用独立的解密模块进行 AES-256-CBC 解密
            decrypted_data = decrypt_file(encrypted_data, aes_key)

            self._logger.info("File downloaded and decrypted successfully")
            return decrypted_data, filename

        except Exception as e:
            self._logger.error(f"File download/decrypt failed: {e}")
            raise

    @property
    def is_connected(self) -> bool:
        """获取当前连接状态"""
        return self._ws_manager.is_connected

    @property
    def api(self) -> WeComApiClient:
        """获取 API 客户端实例（供高级用途使用）"""
        return self._api_client

    def run(self) -> None:
        """
        便捷方法：启动事件循环并连接

        等价于:
            asyncio.get_event_loop().run_until_complete(client.connect())
            asyncio.get_event_loop().run_forever()
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self.connect())
            loop.run_forever()
        finally:
            loop.run_until_complete(self.aclose())
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
