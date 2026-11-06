# Node.js 与 Python SDK 功能对照

以本目录中的 Node.js 1.0.6 为对齐基准；Python 已补齐此前缺失的运行时功能。

| 能力 | Node.js | Python |
| --- | --- | --- |
| WebSocket、认证、心跳、指数退避重连 | 支持 | 支持 |
| 文本／图片／混排／语音／文件／视频事件 | 支持 | 支持 |
| 流式回复、欢迎语、模板卡片、卡片更新 | 支持 | 支持 |
| 主动发送 Markdown 和卡片 | 支持 | 支持 |
| 素材分片上传、动态并发、每片最多重试两次 | `uploadMedia` | `upload_media` |
| 被动／主动发送媒体 | `replyMedia` / `sendMediaMessage` | `reply_media` / `send_media_message` |
| 流式中间帧防积压 | `replyStreamNonBlocking` | `reply_stream_non_blocking` |
| 检查未确认回执 | `hasPendingReplyAck` | `has_pending_reply_ack` |
| 回复队列上限（默认 500） | `maxReplyQueueSize` | `max_reply_queue_size` |
| 独立认证失败重试上限（默认 5） | `maxAuthFailureAttempts` | `max_auth_failure_attempts` |
| 被踢下线时清理并停止重连 | 支持 | 支持 |
| 认证附加字段 | `scene`, `plug_version` | `scene`, `plug_version` |
| 自定义连接／TLS 参数 | `wsOptions`（ws 参数） | `ws_options`（websockets 参数） |
| 下载文件并 AES 解密 | `downloadFile` | `download_file` |
| 通用消息加解密、签名和验签 | `WecomCrypto` | `WecomCrypto` |

## 语言与接口差异

- Node.js 使用 Promise；Python 使用 asyncio，异步接口必须 `await`，另有 `run()` 启动方法。
- Python 方法和配置使用 snake_case；协议中的 JSON 字段仍保留原名称。
- Node.js 有细粒度消息与卡片接口；Python 继续使用字典表示协议体，素材上传提供 TypedDict。类型注解不执行运行时类型校验。
- Node.js 下载返回 `{buffer, filename}`；Python 返回 `(bytes, filename)`。
- `reply_stream_non_blocking` 在前一帧等待 ACK 时返回 `"skipped"`；最终帧总是走正常队列，不因防积压逻辑被跳过，仍可能因连接、队列上限或服务端错误而失败。
- `ws_options` 直接传给 Python `websockets.connect`，不能照搬 Node.js 的 `ca`、`cert` 等键。自定义证书使用 `ssl.SSLContext`。
- Python 拒绝空素材和超过 50 MiB 的素材；企业微信服务端还可能按媒体类型施加更严格限制。

## 验证

`tests/test_features.py` 覆盖上传、媒体、流式、视频、回执、重试、连接配置和加解密，并包含基于本机临时端口的 WebSocket 集成测试。真实企业微信服务端验收需使用有效机器人凭证。

## Python 媒体便捷扩展

提供 `MediaType` 枚举及 `reply_image`、`reply_file`、`reply_voice`、`reply_video`，后者支持 `title` 和 `description`。`upload_media` 增加可选 `md5` 参数，继续使用 options 字典与结构化返回值，保留并发及重试。`examples/media_echo.py` 演示图片回显和文本转文件。心跳失联通过独立且去重的任务触发重连，不依赖底层关闭先完成。
