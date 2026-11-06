"""A 的媒体便捷能力及心跳恢复回归验证。"""
import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from aibot import MediaType, WSClient, WSClientOptions, WsCmd


class SyncedMediaTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = WSClient(WSClientOptions("bot", "secret", logger=Mock()))

    async def test_convenience_reply_payloads(self):
        send = self.client._ws_manager.send_reply = AsyncMock(return_value={"errcode": 0})
        frame = {"headers": {"req_id": "r"}}
        for kind in ("image", "file", "voice", "video"):
            result = await getattr(self.client, "reply_" + kind)(frame, "media")
            self.assertEqual(result, {"errcode": 0})
            self.assertEqual(send.call_args.args[1],
                             {"msgtype": kind, kind: {"media_id": "media"}})
        await self.client.reply_video(frame, "media", title="标题", description="说明")
        self.assertEqual(send.call_args.args[1]["video"],
                         {"media_id": "media", "title": "标题", "description": "说明"})
        await self.client.reply_video(frame, "media", title="标题")
        self.assertEqual(send.call_args.args[1]["video"],
                         {"media_id": "media", "title": "标题"})

    async def test_enum_and_custom_md5_preserve_upload_contract(self):
        async def request(req_id, body, cmd):
            if cmd == WsCmd.UPLOAD_MEDIA_INIT:
                self.assertEqual(body["type"], "image")
                self.assertIs(type(body["type"]), str)
                self.assertEqual(body["md5"], "custom-checksum")
                return {"body": {"upload_id": "upload"}}
            if cmd == WsCmd.UPLOAD_MEDIA_FINISH:
                return {"body": {"media_id": "media", "created_at": "now"}}
            return {}
        self.client._ws_manager.send_reply = request
        result = await self.client.upload_media(
            b"image", {"type": MediaType.Image, "filename": "image.png"}, md5="custom-checksum")
        self.assertEqual(result, {"type": "image", "media_id": "media", "created_at": "now"})
        self.assertEqual({v.value for v in MediaType}, {"file", "image", "voice", "video"})

    async def test_heartbeat_schedules_reconnect_even_when_close_blocks(self):
        manager = self.client._ws_manager
        manager._missed_pong_count = manager._max_missed_pong
        closing = asyncio.Event()
        release = asyncio.Event()
        reconnecting = asyncio.Event()
        async def close():
            closing.set()
            await release.wait()
        async def reconnect():
            reconnecting.set()
            await asyncio.Event().wait()
        manager._ws = Mock(close=close)
        manager._schedule_reconnect = reconnect
        heartbeat = asyncio.create_task(manager._send_heartbeat())
        manager._heartbeat_task = heartbeat
        try:
            await asyncio.wait_for(closing.wait(), 1)
            await asyncio.wait_for(reconnecting.wait(), 1)
            reconnect_task = manager._reconnect_task
            manager._start_reconnect()
            self.assertIs(manager._reconnect_task, reconnect_task)
            self.assertFalse(heartbeat.cancelled())
            release.set()
            await heartbeat
            manager.disconnect()
            await asyncio.sleep(0)
            self.assertTrue(reconnect_task.cancelled())
        finally:
            release.set()
            manager._cancel_reconnect()
            await asyncio.gather(heartbeat, return_exceptions=True)
