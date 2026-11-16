"""对外错误体统一 + 上游模型名隐藏（只作用于 `/v1/*`）。

## 为什么在中间件里做，而不是逐个 return 改

`/v1/*` 的错误出口有几十处（各平台网关、协议转换、dispatch 的
`PlatformRejected`、上游异常翻译…），逐个改一遍既漏又容易改出新行为。
这里在 ASGI 层统一收口：

* **状态码 >= 400 且响应体是 JSON** -> 整段替换成
  `{"error": {"message": "Upstream bad gateway", "type": "upstream_error",
  "code": "502", "status_code": 502}}`
  —— 只留状态码和通用英文，上游原文（含它的模型名、内部提示、账号信息）
  一律不外泄。
* **SSE 流式** -> 不改状态码，只把分块里出现的**上游模型 id** 换成下游名
  （`admin.model_map.redactions()`）。错误事件在流里已发出，改写会破坏帧边界，
  所以流内不做错误体替换。
* **非流式成功响应** -> 同样做模型名替换，客户端只能看到自己起的名字。

## 开关

* `MIRAIHUB_UNIFIED_ERRORS=0`  关掉错误体统一，恢复各出口原样返回；
* `MIRAIHUB_HIDE_UPSTREAM_MODEL=0`  关掉模型名替换。

两个都默认开启。改配置不改代码，线上排查需要原文时才有退路。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re

logger = logging.getLogger("admin.unified_errors")

#: 状态码 -> 通用英文短句。**只用这些**，不从上游抄文案。
STATUS_MESSAGE = {
    400: "Bad request",
    401: "Unauthorized",
    402: "Payment required",
    403: "Forbidden",
    404: "Model not found",
    405: "Method not allowed",
    408: "Request timeout",
    409: "Conflict",
    413: "Payload too large",
    415: "Unsupported media type",
    422: "Unprocessable entity",
    429: "Too many requests",
    500: "Internal server error",
    501: "Not implemented",
    502: "Upstream bad gateway",
    503: "Service unavailable",
    504: "Upstream timeout",
}

#: 状态码 -> OpenAI 兼容的 error.type
KIND_BY_STATUS = {
    400: "invalid_request_error",
    401: "authentication_error",
    402: "insufficient_quota",
    403: "permission_error",
    404: "not_found_error",
    422: "invalid_request_error",
    429: "rate_limit_error",
}


def _flag(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() not in ("0", "false", "off", "no", "n")


def errors_enabled() -> bool:
    return _flag("MIRAIHUB_UNIFIED_ERRORS", True)


def hide_model_enabled() -> bool:
    return _flag("MIRAIHUB_HIDE_UPSTREAM_MODEL", True)


def normalized_status(status) -> int:
    try:
        value = int(status)
    except (TypeError, ValueError):
        return 502
    if value < 400:
        return 502
    if value > 599:
        return 502
    return value


def generic_message(status) -> str:
    return STATUS_MESSAGE.get(normalized_status(status), "Upstream error")


def error_payload(status, *, message: str = "") -> dict:
    """统一错误体：状态码 + 通用英文，不带任何上游原文。"""
    status = normalized_status(status)
    return {
        "error": {
            "message": (message or "").strip() or generic_message(status),
            "type": KIND_BY_STATUS.get(status, "upstream_error"),
            "code": str(status),
            "status_code": status,
        }
    }


def error_body(status, *, message: str = "") -> bytes:
    return json.dumps(error_payload(status, message=message),
                      ensure_ascii=False).encode("utf-8")


def stream_error_event(status, *, message: str = "") -> bytes:
    """SSE 场景下的错误帧（各平台网关可直接复用）。"""
    return b"data: " + error_body(status, message=message) + b"\n\n"


# ---------------------------------------------------------------------------
# 响应体改写
# ---------------------------------------------------------------------------
def _is_json(headers) -> bool:
    for name, value in headers:
        if name.lower() == b"content-type":
            text = value.decode("latin-1").lower()
            return "json" in text
    return False


def _is_stream(headers) -> bool:
    for name, value in headers:
        if name.lower() == b"content-type":
            return "event-stream" in value.decode("latin-1").lower()
    return False


def _drop_header(headers, target: bytes) -> list:
    return [(n, v) for n, v in headers if n.lower() != target]


def _replace_models(body: bytes, pairs: dict[str, str]) -> bytes:
    """把 body 里的上游模型 id 换成下游名（大小写不敏感）。

    `json.dumps` 之后上游 id 会原样出现（`"model": "dfmodel"`），直接做文本
    替换即可，不需要解析 JSON —— 流式分块也能逐块处理。

    匹配必须**大小写不敏感**：上游回的是 `DeepSeek-V4.1-Flash`，而脱敏表
    登记的是小写 `deepseek-v4.1-flash`，逐字节比大小写就会漏替，
    结果「隐藏上游模型名」在最常见的那条路径上失效。长名优先，避免短名
    先吃掉长名的片段。
    """
    if not body or not pairs:
        return body
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:  # 二进制分块（图片等）不碰
        return body
    for upstream in sorted(pairs, key=len, reverse=True):
        name = str(upstream or "").strip()
        downstream = str(pairs.get(upstream) or "")
        if not name or not downstream:
            continue
        text = re.sub(re.escape(name), lambda _m, _d=downstream: _d, text,
                      flags=re.IGNORECASE)
    return text.encode("utf-8")


class UnifiedGatewayMiddleware:
    """`/v1/*` 的统一错误体 + 上游模型名隐藏。

    注册位置要在 `PlatformDispatchMiddleware` **之后**（`add_middleware` 是前插，
    后注册者在更外层），这样才能连 dispatch 自己产生的错误响应一起改。

    ## 缓冲策略

    `http.response.start` 一旦发出，`content-length` 就锁死了。要在错误时整段
    换 body，必须**先不发 start**：非流式响应本来就是一个完整 JSON，等 body
    收齐再一次发出，语义与直连上游一致；只有 `text/event-stream` 才边收边发，
    那条路只做模型名替换（不改变长度就必须改头，改头又不可能 —— 所以长度变化
    在这种情况下宁可放弃脱敏也不能把帧切坏）。
    """

    def __init__(self, app, *, prefix: str = "/v1/") -> None:
        self.app = app
        self.prefix = prefix

    async def __call__(self, scope, receive, send):
        path = str(scope.get("path") or "")
        if scope.get("type") != "http" or not path.startswith(self.prefix):
            return await self.app(scope, receive, send)

        unify = errors_enabled()
        hide = hide_model_enabled()
        if not (unify or hide):
            return await self.app(scope, receive, send)

        pairs: dict[str, str] = {}
        if hide:
            try:
                from admin import model_map

                pairs = await asyncio.to_thread(model_map.redactions)
            except Exception:  # noqa: BLE001 - 脱敏失败不影响转发
                pairs = {}

        state: dict = {"start": None, "stream": False, "chunks": []}

        async def flush(send_body: bool = True):
            start = state["start"]
            if start is None:
                return
            payload = b"".join(state["chunks"])
            status = int(start.get("status") or 200)
            headers = list(start.get("headers") or [])

            if unify and status >= 400 and _is_json(headers):
                payload = error_body(status)
            elif pairs:
                payload = _replace_models(payload, pairs)

            headers = _drop_header(headers, b"content-length")
            headers.append((b"content-length",
                            str(len(payload)).encode("latin-1")))
            await send({"type": "http.response.start", "status": status,
                        "headers": headers})
            if send_body:
                await send({"type": "http.response.body", "body": payload,
                            "more_body": False})

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                state["start"] = message
                state["chunks"] = []
                state["stream"] = _is_stream(message.get("headers") or [])
                if state["stream"]:
                    # 流式：start 必须立刻发，否则客户端拿不到首字节
                    await send(message)
                return

            if message["type"] != "http.response.body":
                await send(message)
                return

            body = message.get("body") or b""

            if state["stream"]:
                await send({**message,
                            "body": _replace_models(body, pairs)})
                return

            if body:
                state["chunks"].append(body)
            if message.get("more_body"):
                return
            await flush()

        await self.app(scope, receive, send_wrapper)
