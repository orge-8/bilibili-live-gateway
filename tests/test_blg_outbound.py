"""blg_outbound 单测：截断、令牌桶、错误码映射（全桩，不联网）。"""

import asyncio

import httpx
import pytest

from blg_auth import BiliApiError
from blg_outbound import (
    DEFAULT_MAX_CHARS,
    DanmakuSender,
    TokenBucket,
    truncate_danmaku,
)


class FakeSendAuth:
    """替身 BiliHttpClient：记录 send_danmaku 调用，可编程失败。"""

    def __init__(self, fail_code: int | None = None):
        self.calls: list[tuple[int, str]] = []
        self.fail_code = fail_code

    async def send_danmaku(self, room_id: int, message: str) -> None:
        self.calls.append((room_id, message))
        if self.fail_code is not None:
            raise BiliApiError(self.fail_code, "假失败")


def _sender(auth=None, **kwargs) -> tuple[DanmakuSender, FakeSendAuth]:
    auth = auth or FakeSendAuth()
    sender = DanmakuSender(auth, logger=None, **kwargs)
    return sender, auth


# ---------------------------------------------------------------- 截断

def test_truncate_short_text_untouched():
    out, truncated = truncate_danmaku("你好", 20)
    assert out == "你好" and truncated is False


def test_truncate_exact_limit_untouched():
    text = "字" * DEFAULT_MAX_CHARS
    out, truncated = truncate_danmaku(text, DEFAULT_MAX_CHARS)
    assert out == text and truncated is False


def test_truncate_appends_suffix_within_limit():
    text = "字" * 30
    out, truncated = truncate_danmaku(text, 20, "…")
    assert truncated is True
    assert len(out) == 20          # 含后缀总长不超限
    assert out.startswith("字" * 19) and out.endswith("…")


def test_truncate_empty_suffix():
    out, truncated = truncate_danmaku("abcdefgh", 5, "")
    assert out == "abcde" and truncated is True


# ---------------------------------------------------------------- 令牌桶

def test_token_bucket_immediate_burst_up_to_capacity():
    bucket = TokenBucket(capacity=3, refill_interval=10.0)
    assert bucket.try_acquire() and bucket.try_acquire() and bucket.try_acquire()
    assert bucket.try_acquire() is False  # 第 4 张立即取不到


def test_token_bucket_refills_over_time():
    async def run():
        bucket = TokenBucket(capacity=1, refill_interval=0.2)
        assert await bucket.acquire(timeout=0.0) is True
        assert await bucket.acquire(timeout=0.0) is False   # 桶空立即失败
        assert await bucket.acquire(timeout=1.0) is True    # 等待回填成功
    asyncio.run(run())


def test_token_bucket_acquire_times_out():
    async def run():
        bucket = TokenBucket(capacity=1, refill_interval=60.0)
        await bucket.acquire(timeout=0.0)
        assert await bucket.acquire(timeout=0.3) is False   # 60s 回填，0.3s 必超时
    asyncio.run(run())


# ---------------------------------------------------------------- 发送

def test_send_empty_text_rejected():
    async def run():
        sender, auth = _sender()
        result = await sender.send(1, "   ")
        assert result["success"] is False
        assert auth.calls == []
    asyncio.run(run())


def test_send_ok_and_recorded():
    async def run():
        sender, auth = _sender()
        result = await sender.send(2233, "你好呀")
        assert result["success"] is True
        assert auth.calls == [(2233, "你好呀")]
    asyncio.run(run())


def test_send_truncates_long_text():
    async def run():
        sender, auth = _sender(max_chars=20, fallback_suffix="…")
        result = await sender.send(1, "长" * 30)
        assert result["success"] is True and result["truncated"] is True
        assert len(auth.calls[0][1]) == 20
    asyncio.run(run())


def test_send_bucket_charges_one_token_per_segment():
    """smart_segmentation 一条回复多段 → 出站被连续调用多次，按段计票。"""
    async def run():
        sender, auth = _sender(bucket_capacity=2, min_interval_sec=60.0,
                               bucket_wait_sec=0.0)
        r1 = await sender.send(1, "第一段")
        r2 = await sender.send(1, "第二段")
        r3 = await sender.send(1, "第三段")  # 桶空 + 不等待 → 软失败丢弃
        assert r1["success"] and r2["success"]
        assert r3["success"] is False and "限频" in r3["error"]
        assert len(auth.calls) == 2
    asyncio.run(run())


@pytest.mark.parametrize("code,frag", [
    (-101, "Cookie"),
    (-111, "bili_jct"),
    (10031, "频率"),
    (1003212, "风控"),
])
def test_send_error_codes_map_to_hints(code, frag):
    async def run():
        sender, _ = _sender(auth=FakeSendAuth(fail_code=code))
        result = await sender.send(1, "测试")
        assert result["success"] is False
        assert result["bili_code"] == code
        assert frag in result["error"]
    asyncio.run(run())


class FakeNetErrorAuth:
    """替身：抛 httpx 网络层异常（超时/连接失败），不抛 BiliApiError。"""

    def __init__(self, exc: Exception):
        self.exc = exc
        self.calls: list[tuple[int, str]] = []

    async def send_danmaku(self, room_id: int, message: str) -> None:
        self.calls.append((room_id, message))
        raise self.exc


@pytest.mark.parametrize("exc", [
    httpx.ConnectTimeout("连接超时"),
    httpx.ReadError("读失败"),
    httpx.ConnectError("拒绝连接"),
])
def test_send_network_errors_do_not_escape(exc):
    """全检修复 #3：网络层异常必须转成软失败 dict，不能穿透宿主 RPC 契约。"""
    async def run():
        sender, _ = _sender(auth=FakeNetErrorAuth(exc))
        result = await sender.send(1, "测试")
        assert result["success"] is False
        assert "网络异常" in result["error"]
        assert "bili_code" not in result
    asyncio.run(run())

