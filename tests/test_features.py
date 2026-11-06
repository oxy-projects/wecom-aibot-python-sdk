import asyncio
import base64
import hashlib
import json
import ssl
import unittest
from unittest.mock import AsyncMock, Mock, patch

from aibot import (WSClient, WSClientOptions, WecomCrypto, WsCmd,
                   WSAuthFailureError, WSReconnectExhaustedError, pkcs7_unpad)
from aibot.ws import WsConnectionManager


class FeatureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = WSClient(WSClientOptions("bot", "secret", logger=Mock()))

    async def test_upload_chunks_retries_and_content(self):
        data = b"a" * (512 * 1024 + 7)
        chunks = {}
        attempts = {}
        async def request(req_id, body, cmd):
            if cmd == WsCmd.UPLOAD_MEDIA_INIT:
                self.assertEqual(body["md5"], hashlib.md5(data).hexdigest())
                self.assertEqual(body["total_chunks"], 2)
                return {"body": {"upload_id": "upload"}}
            if cmd == WsCmd.UPLOAD_MEDIA_CHUNK:
                i = body["chunk_index"]
                attempts[i] = attempts.get(i, 0) + 1
                if i == 0 and attempts[i] == 1:
                    raise RuntimeError("transient")
                chunks[i] = base64.b64decode(body["base64_data"])
                return {"errcode": 0}
            self.assertEqual(len(chunks), 2)
            return {"body": {"media_id": "media", "created_at": "now"}}
        self.client._ws_manager.send_reply = request
        result = await self.client.upload_media(data, {"type": "file", "filename": "a.bin"})
        self.assertEqual(result["media_id"], "media")
        self.assertEqual(chunks[0] + chunks[1], data)
        self.assertEqual(attempts[0], 2)

    async def test_upload_concurrency_and_terminal_failure(self):
        active = peak = 0
        finish_called = False
        async def request(req_id, body, cmd):
            nonlocal active, peak, finish_called
            if cmd == WsCmd.UPLOAD_MEDIA_INIT:
                return {"body": {"upload_id": "upload"}}
            if cmd == WsCmd.UPLOAD_MEDIA_FINISH:
                finish_called = True
                return {"body": {"media_id": "media"}}
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(.001)
            finally:
                active -= 1
            return {}
        self.client._ws_manager.send_reply = request
        await self.client.upload_media(b"a" * (512 * 1024 * 6), {"type": "file", "filename": "a"})
        self.assertEqual(peak, 3)
        self.assertTrue(finish_called)
        attempts = 0
        async def failure(req_id, body, cmd):
            nonlocal attempts
            if cmd == WsCmd.UPLOAD_MEDIA_INIT:
                return {"body": {"upload_id": "upload"}}
            self.assertEqual(cmd, WsCmd.UPLOAD_MEDIA_CHUNK)
            attempts += 1
            raise RuntimeError("permanent failure")
        self.client._ws_manager.send_reply = failure
        with self.assertRaisesRegex(RuntimeError, "permanent"):
            await self.client.upload_media(b"a", {"type": "file", "filename": "a"})
        self.assertEqual(attempts, 3)

    async def test_heartbeat_timeout_stops_worker(self):
        manager = self.client._ws_manager
        manager._heartbeat_interval = 1
        manager._ws = Mock(close=AsyncMock())
        manager.send = AsyncMock()
        manager._start_heartbeat()
        task = manager._heartbeat_task
        await asyncio.wait_for(task, 1)
        self.assertEqual(manager.send.await_count, 2)
        manager._ws.close.assert_awaited_once()
        self.assertIsNone(manager._heartbeat_task)

    async def test_aclose_waits_for_background_tasks(self):
        manager = self.client._ws_manager
        ws = Mock(open=True, close=AsyncMock())
        manager._ws = ws
        self.client._started = True
        manager._receive_task = asyncio.create_task(asyncio.sleep(3600))
        manager._start_heartbeat()
        receive = manager._receive_task
        heartbeat = manager._heartbeat_task

        await self.client.aclose()
        await self.client.aclose()

        self.assertTrue(receive.done())
        self.assertTrue(heartbeat.done())
        self.assertTrue(manager._disconnect_task.done())
        self.assertFalse(self.client.is_connected)
        ws.close.assert_awaited_once()
        self.client._logger.warn.assert_not_called()

    async def test_disconnect_during_connection_open(self):
        manager = self.client._ws_manager
        ws = Mock(open=True, close=AsyncMock())
        opening = asyncio.Event()
        release = asyncio.Event()
        async def connect(*args, **kwargs):
            opening.set()
            await release.wait()
            return ws
        with patch("aibot.ws.websockets.connect", new=connect):
            task = asyncio.create_task(manager.connect())
            await opening.wait()
            manager.disconnect()
            release.set()
            await task
        self.assertFalse(manager.is_connected)
        ws.close.assert_awaited_once()

    async def test_queue_limit_and_cancel_reconnect(self):
        manager = self.client._ws_manager
        manager._max_reply_queue_size = 1
        manager.send = AsyncMock()
        task = asyncio.create_task(manager.send_reply("r", {}))
        await asyncio.sleep(0)
        with self.assertRaisesRegex(RuntimeError, "max size"):
            await manager.send_reply("r", {})
        manager._start_reconnect()
        reconnect = manager._reconnect_task
        manager.disconnect()
        with self.assertRaises(RuntimeError):
            await task
        await asyncio.sleep(0)
        self.assertTrue(reconnect.cancelled())

    async def test_upload_errors(self):
        for data in (b"", b"a" * (50 * 1024 * 1024 + 1)):
            with self.assertRaises(ValueError):
                await self.client.upload_media(data, {"type": "file", "filename": "a"})
        self.client._ws_manager.send_reply = AsyncMock(return_value={"body": {}})
        with self.assertRaisesRegex(RuntimeError, "upload_id"):
            await self.client.upload_media(b"a", {"type": "file", "filename": "a"})

    async def test_media_and_stream(self):
        reply = self.client._ws_manager.send_reply = AsyncMock(return_value={"errcode": 0})
        frame = {"headers": {"req_id": "request"}}
        await self.client.reply_media(frame, "video", "media", {"title": "标题"})
        self.assertEqual(reply.call_args.args[1], {"msgtype": "video", "video": {"media_id": "media", "title": "标题"}})
        await self.client.send_media_message("user", "image", "media")
        self.assertEqual(reply.call_args.args[1]["chatid"], "user")
        self.client._ws_manager._pending_acks["request"] = (None, None)
        self.assertEqual(await self.client.reply_stream_non_blocking(frame, "s", "x"), "skipped")
        await self.client.reply_stream_non_blocking(frame, "s", "done", True)
        self.assertTrue(reply.call_args.args[1]["stream"]["finish"])
        self.client._ws_manager._pending_acks.clear()

    async def test_video_event(self):
        handler = Mock()
        self.client.on("message.video", handler)
        frame = {"cmd": WsCmd.CALLBACK, "body": {"msgtype": "video"}}
        self.client._message_handler.handle_frame(frame, self.client)
        handler.assert_called_once_with(frame)

    async def test_fast_ack_and_serial_queue(self):
        manager = self.client._ws_manager
        sent = []
        async def send(frame):
            sent.append(frame["body"]["n"])
            manager._handle_frame({"headers": frame["headers"], "errcode": 0})
        manager.send = send
        results = await asyncio.gather(*(manager.send_reply("r", {"n": n}) for n in range(8)))
        self.assertEqual(sent, list(range(8)))
        self.assertEqual(len(results), 8)
        self.assertFalse(manager._pending_acks)

    async def test_ack_timeout_and_disconnect_cleanup(self):
        manager = self.client._ws_manager
        manager.send = AsyncMock()
        manager._reply_ack_timeout = .01
        with self.assertRaises(TimeoutError):
            await manager.send_reply("r", {})
        task = asyncio.create_task(manager.send_reply("r", {}))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        manager.disconnect()
        with self.assertRaises(RuntimeError):
            await task
        await asyncio.sleep(0)
        self.assertFalse(manager._pending_acks)

    async def test_auth_failure_limits_and_success_reset(self):
        manager = self.client._ws_manager
        manager._ws = Mock(close=AsyncMock())
        errors = []
        manager.on_error = errors.append
        manager._handle_frame({"headers": {"req_id": WsCmd.SUBSCRIBE + "_1"}, "errcode": 1})
        self.assertTrue(manager._last_close_was_auth_failure)
        await asyncio.sleep(0)
        manager._auth_failure_attempts = manager._max_auth_failure_attempts
        await manager._schedule_reconnect()
        self.assertIsInstance(errors[-1], WSAuthFailureError)
        manager._handle_frame({"headers": {"req_id": WsCmd.SUBSCRIBE + "_2"}, "errcode": 0})
        self.assertEqual(manager._auth_failure_attempts, 0)
        manager._stop_heartbeat()
        manager._reconnect_attempts = manager._max_reconnect_attempts
        await manager._schedule_reconnect()
        self.assertIsInstance(errors[-1], WSReconnectExhaustedError)

    async def test_kicked_does_not_reconnect(self):
        manager = self.client._ws_manager
        manager._ws = Mock(close=AsyncMock())
        self.client._started = True
        handler = Mock()
        self.client.on("event.disconnected_event", handler)
        manager._handle_frame({"cmd": WsCmd.EVENT_CALLBACK, "body": {
            "msgtype": "event", "event": {"eventtype": "disconnected_event"}}})
        await asyncio.sleep(0)
        self.assertTrue(manager._is_manual_close)
        self.assertFalse(self.client._started)
        manager._start_reconnect()
        self.assertIsNone(manager._reconnect_task)
        handler.assert_called_once()

    async def test_connection_options_and_auth(self):
        context = ssl.create_default_context()
        client = WSClient(WSClientOptions("bot", "secret", scene=0, plug_version="v1",
                          ws_options={"ssl": context}, max_reply_queue_size=7, logger=Mock()))
        ws = Mock(open=True, send=AsyncMock(), close=AsyncMock())
        ws.__aiter__ = Mock(return_value=iter(()))
        with patch("aibot.ws.websockets.connect", new=AsyncMock(return_value=ws)) as connect:
            with patch.object(client._ws_manager, "_receive_loop", new=AsyncMock()):
                await client.connect()
                self.assertIs(connect.call_args.kwargs["ssl"], context)
                body = json.loads(ws.send.call_args.args[0])["body"]
                self.assertEqual(body, {"bot_id": "bot", "secret": "secret", "scene": 0, "plug_version": "v1"})
                self.assertEqual(client._ws_manager._max_reply_queue_size, 7)
                await asyncio.sleep(0)
                client.disconnect()
                await asyncio.sleep(0)


class CryptoTests(unittest.TestCase):
    def test_roundtrip_signature_and_receive_id(self):
        key = base64.b64encode(bytes(range(32))).decode().rstrip("=")
        crypto = WecomCrypto("token", key, "bot")
        result = crypto.encrypt("你好 🌍", "123", "nonce")
        self.assertEqual(crypto.decrypt(result["encrypt"]), "你好 🌍")
        self.assertTrue(crypto.verify_signature(result["signature"], "123", "nonce", result["encrypt"]))
        self.assertFalse(crypto.verify_signature("bad", "123", "nonce", result["encrypt"]))
        with self.assertRaisesRegex(ValueError, "receive_id"):
            WecomCrypto("token", key, "other").decrypt(result["encrypt"])
        with self.assertRaises(ValueError):
            pkcs7_unpad(b"invalid\x02")


class LocalWebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_auth_retry_exhaustion_on_real_transport(self):
        import websockets
        count = 0
        exhausted = asyncio.Event()
        async def server(ws):
            nonlocal count
            count += 1
            auth = json.loads(await ws.recv())
            await ws.send(json.dumps({"headers": auth["headers"], "errcode": 400, "errmsg": "bad credentials"}))
            await ws.wait_closed()
        async with websockets.serve(server, "127.0.0.1", 0) as service:
            port = service.sockets[0].getsockname()[1]
            client = WSClient(WSClientOptions("bot", "wrong", ws_url=f"ws://127.0.0.1:{port}",
                              reconnect_interval=1, max_auth_failure_attempts=2, logger=Mock()))
            client.on("error", lambda error: exhausted.set() if isinstance(error, WSAuthFailureError) else None)
            await client.connect()
            await asyncio.wait_for(exhausted.wait(), 2)
            self.assertEqual(count, 3)
            self.assertFalse(client.is_connected)
            client.disconnect()
            await client._ws_manager._async_disconnect()

    async def test_real_transport_auth_ack_normal_close_reconnect_and_kick(self):
        import websockets
        connections = []
        second_auth = asyncio.Event()
        kicked = asyncio.Event()
        async def server(ws):
            connections.append(ws)
            auth = json.loads(await ws.recv())
            await ws.send(json.dumps({"headers": auth["headers"], "errcode": 0}))
            if len(connections) == 1:
                frame = json.loads(await ws.recv())
                await ws.send(json.dumps({"headers": frame["headers"], "errcode": 0}))
                await ws.close()
            else:
                second_auth.set()
                await ws.send(json.dumps({"cmd": WsCmd.EVENT_CALLBACK, "headers": {},
                    "body": {"msgtype": "event", "event": {"eventtype": "disconnected_event"}}}))
                await ws.wait_closed()
        async with websockets.serve(server, "127.0.0.1", 0) as service:
            port = service.sockets[0].getsockname()[1]
            client = WSClient(WSClientOptions("bot", "secret", ws_url=f"ws://127.0.0.1:{port}",
                                             reconnect_interval=1, logger=Mock()))
            client.on("error", lambda error: None)
            first_auth = asyncio.Event()
            client.on("authenticated", first_auth.set)
            client.on("event.disconnected_event", lambda frame: kicked.set())
            await client.connect()
            await asyncio.wait_for(first_auth.wait(), 2)
            await client.send_message("user", {"msgtype": "markdown", "markdown": {"content": "hi"}})
            await asyncio.wait_for(second_auth.wait(), 2)
            await asyncio.wait_for(kicked.wait(), 2)
            await asyncio.sleep(.02)
            self.assertEqual(len(connections), 2)
            self.assertFalse(client.is_connected)
            await client._ws_manager._async_disconnect()


if __name__ == "__main__":
    unittest.main()
