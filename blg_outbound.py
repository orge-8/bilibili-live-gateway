"""发弹幕回直播间（P3 出站）。纯发送层，不依赖 maibot_sdk。

设计要点（全部来自真机事实）：

1. **smart_segmentation 会把一条回复预切成多段**（真机实录 2026-09-13：
   一次 Host 派发里 raw_message 已是分段数组），出站处理器会被连续调用多次——
   所以令牌桶按「每段一张票」计费，不能按"一次回复"计。
2. 普通账号弹幕长度上限约 20 字（大航海更高），超长必须截断而不是整条丢弃，
   截断后追加可配置后缀（如 `…`）让听者知道话没说完。
3. `msg/send` 的错误码是终态信号，重试只会加重风控：-101（未登录）、
   -111（csrf 校验失败=bili_jct 过期）、10031（发送频率过快）、
   1003212（内容触发风控）。这里只解释、不重试——频控问题应由令牌桶兜底。
"""

import asyncio
import time
from typing import Any, Awaitable, Callable, Optional

import httpx

from blg_auth import BiliApiError, BiliHttpClient

#: 普通账号实测可用的安全长度上限（B 站按"非空字符数"计，中英文同权）
DEFAULT_MAX_CHARS = 20

#: 发送错误码 → 人类可读解释（不再重试，重试只会加重风控）
SEND_ERROR_HINTS: dict[int, str] = {
    -101: "未登录或 Cookie 失效：重新导出含 SESSDATA 的 Cookie 后完整重启 MaiBot",
    -111: "csrf 校验失败：Cookie 缺 bili_jct 或已过期（发弹幕必需）",
    10031: "发送频率过快：已被服务端限频，请调大 outbound.min_interval_sec",
    1003212: "内容触发风控：弹幕被拒收，换措辞或稍后再试",
    -400: "请求参数错误：检查房间号与弹幕内容",
    10030: "弹幕内容为空或超长",
}


class TokenBucket:
    """令牌桶限流：容量 bucket_capacity，每 min_interval_sec 滴答回一个令牌。

    出站按「每段一张票」计费（smart_segmentation 一条回复多段 → 连续多次调用）。
    """

    def __init__(self, capacity: int, refill_interval: float):
        self._capacity = max(1, int(capacity))
        self._refill = max(0.5, float(refill_interval))
        self._tokens = float(self._capacity)
        self._last = time.monotonic()

    def _sync(self) -> None:
        now = time.monotonic()
        if now > self._last:
            self._tokens = min(self._capacity,
                               self._tokens + (now - self._last) / self._refill)
            self._last = now

    def try_acquire(self) -> bool:
        """立即取一张票；桶空返回 False（不等待）。"""
        self._sync()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    async def acquire(self, timeout: float) -> bool:
        """取票，最多等 timeout 秒；到点还没票返回 False。

        轮询间隔与回填节奏对齐（每 0.25s 查一次即可，回填粒度是秒级）。
        """
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            if self.try_acquire():
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.25, remaining))

    @property
    def tokens(self) -> float:
        self._sync()
        return self._tokens


def truncate_danmaku(text: str, max_chars: int, suffix: str = "") -> tuple[str, bool]:
    """截断到 max_chars（含后缀）。返回 (生效文本, 是否发生了截断)。

    B 站按字符数计长（中英文同权），空内容原样返回——空弹幕由调用方拒绝。
    """
    if len(text) <= max_chars:
        return text, False
    keep = max(0, max_chars - len(suffix))
    return text[:keep] + suffix, True


class DanmakuSender:
    """直播间弹幕发送器：令牌桶 + 截断 + 错误码解释，只发不重试。"""

    def __init__(
        self,
        auth: BiliHttpClient,
        *,
        max_chars: int = DEFAULT_MAX_CHARS,
        min_interval_sec: float = 3.0,
        bucket_capacity: int = 3,
        bucket_wait_sec: float = 0.0,
        fallback_suffix: str = "",
        logger: Any = None,
    ):
        self._auth = auth
        self._max_chars = max(1, int(max_chars))
        self._bucket = TokenBucket(bucket_capacity, min_interval_sec)
        self._wait = max(0.0, float(bucket_wait_sec))
        self._suffix = str(fallback_suffix or "")
        self._log = logger

    def _info(self, msg: str, *args: Any) -> None:
        if self._log is not None:
            self._log.info("[bili-live][send] " + msg, *args)

    @property
    def max_chars(self) -> int:
        return self._max_chars

    @property
    def tokens(self) -> float:
        return self._bucket.tokens

    async def send(self, room_id: int, text: str) -> dict[str, Any]:
        """发送一条弹幕。返回 {"success": bool, "error"?: str, "truncated"?: bool}。

        令牌桶等待超时是**软失败**：宿主语义上算 success=False（这条没送达），
        但不抛异常——限频丢一段比阻塞回复链路更安全。
        """
        text = (text or "").strip()
        if not text:
            return {"success": False, "error": "空弹幕不发送"}

        final, truncated = truncate_danmaku(text, self._max_chars, self._suffix)
        if truncated:
            self._info("弹幕超长已截断：%d 字 → %d 字", len(text), len(final))

        if not await self._bucket.acquire(self._wait):
            return {"success": False, "error": "发送限频：令牌桶等待超时，本段丢弃",
                    "truncated": truncated}

        try:
            await self._auth.send_danmaku(int(room_id), final)
        except BiliApiError as exc:
            hint = SEND_ERROR_HINTS.get(exc.code, f"B 站接口错误 code={exc.code}")
            return {"success": False, "error": f"{hint}", "truncated": truncated,
                    "bili_code": exc.code}
        except (httpx.HTTPError, ValueError) as exc:
            # 网络层异常（超时/连接失败/重定向耗尽等）不能穿透宿主 RPC 契约：
            # DanmakuSender.send 的返回契约是 dict，抛异常会炸掉整个出站链路。
            self._info("发送网络异常：%s: %s", type(exc).__name__, exc)
            return {"success": False, "error": f"网络异常：{type(exc).__name__}",
                    "truncated": truncated}

        self._info("已发送 room=%s（%d 字%s）", room_id, len(final),
                   "，截断" if truncated else "")
        return {"success": True, "truncated": truncated}
