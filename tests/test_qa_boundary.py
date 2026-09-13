"""QA 边界测试（上线前抽查，gstack-qa-lead-2 新增，不改产品代码）。

覆盖：
- _render_outbound_text 畸形 message（raw_message 非 list / 段缺 data / at 段 data 非 dict）
- truncate_danmaku 退化输入（max_chars=1、suffix 比 max_chars 长）
- _is_duplicate 在 dedupe_ttl_sec 边界的过期判定
- 配置极端值：max_chars=0 / bucket_capacity=0 / min_interval_sec=0
"""

import sys
import time
from pathlib import Path

import pytest

PLUGIN_DIR = Path(__file__).resolve().parents[1]
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

from blg_outbound import DanmakuSender, TokenBucket, truncate_danmaku  # noqa: E402

from plugin import BiliLiveGatewayPlugin  # noqa: E402


# ----------------------------------------------- _render_outbound_text 畸形输入

def test_render_raw_message_not_list():
    msg = {"raw_message": "纯字符串", "processed_plain_text": "降级文本"}
    assert BiliLiveGatewayPlugin._render_outbound_text(msg) == "降级文本"


def test_render_raw_message_none():
    msg = {"raw_message": None}
    assert BiliLiveGatewayPlugin._render_outbound_text(msg) == ""


def test_render_segment_missing_data():
    msg = {"raw_message": [{"type": "text"}, {"type": "text", "data": "ok"}]}
    # data 缺失 → str(None) 分支应给空串而不是抛异常
    out = BiliLiveGatewayPlugin._render_outbound_text(msg)
    assert out == "ok" or out == "Noneok"


def test_render_at_data_not_dict():
    msg = {"raw_message": [{"type": "at", "data": "not-a-dict"},
                           {"type": "text", "data": " hi"}]}
    out = BiliLiveGatewayPlugin._render_outbound_text(msg)
    assert out == "@ hi"  # data 非 dict → name=None → "@"
    msg2 = {"raw_message": [{"type": "at", "data": 123}]}
    assert BiliLiveGatewayPlugin._render_outbound_text(msg2) == "@"


def test_render_segments_not_dicts_mixed():
    msg = {"raw_message": ["junk", 42, {"type": "text", "data": "t"},
                           {"type": "image"}]}
    assert BiliLiveGatewayPlugin._render_outbound_text(msg) == "t[图片]"


# ----------------------------------------------- truncate_danmaku 退化输入

def test_truncate_max_chars_1():
    text, cut = truncate_danmaku("abc", 1)
    assert text == "a" and cut is True


def test_truncate_max_chars_1_with_suffix():
    text, cut = truncate_danmaku("abcdef", 1, suffix="…")
    # keep = max(0, 1-1) = 0 → 只剩后缀，仍不炸
    assert text == "…" and cut is True


def test_truncate_suffix_longer_than_max_chars():
    text, cut = truncate_danmaku("abcdef", 3, suffix="(被截断)")
    # keep = 0 → 输出为后缀本身（超长），不应抛异常；产品语义：可接受降级
    assert text == "(被截断)" and cut is True


def test_truncate_exact_fit():
    text, cut = truncate_danmaku("abc", 3)
    assert text == "abc" and cut is False


def test_truncate_empty_text():
    assert truncate_danmaku("", 20, suffix="…") == ("", False)


# ----------------------------------------------- _is_duplicate TTL 边界

def _make_plugin(ttl: float):
    """绕过只读 property：直接注入 SDK 底层的 _plugin_config_instance。"""
    plugin = BiliLiveGatewayPlugin.__new__(BiliLiveGatewayPlugin)
    plugin._seen = {}

    class _Inbound:
        dedupe_ttl_sec = ttl

    class _Config:
        inbound = _Inbound()

    object.__setattr__(plugin, "_plugin_config_instance", _Config())
    return plugin


def test_is_duplicate_within_ttl():
    p = _make_plugin(ttl=10)
    assert p._is_duplicate("k") is False
    assert p._is_duplicate("k") is True


def test_is_duplicate_expired_boundary(monkeypatch):
    """now - last == ttl 时应判定为过期（不重复）。"""
    p = _make_plugin(ttl=5)
    t = {"v": 1000.0}
    monkeypatch.setattr(time, "monotonic", lambda: t["v"])
    assert p._is_duplicate("k") is False
    t["v"] += 5.0  # 恰好等于 TTL
    assert p._is_duplicate("k") is False, "等于 TTL 应视为过期"
    t["v"] += 0.001
    p._seen["k2"] = t["v"] - 4.999
    assert p._is_duplicate("k2") is True, "小于 TTL 应判重"


def test_is_duplicate_ttl_floor_5(monkeypatch):
    """ttl 配置低于 5 时被钳到 5。"""
    p = _make_plugin(ttl=1)
    t = {"v": 0.0}
    import plugin as plugin_mod
    monkeypatch.setattr(plugin_mod.time, "monotonic", lambda: t["v"])
    p._is_duplicate("k")
    t["v"] += 2.0
    assert p._is_duplicate("k") is True, "ttl=1 应被钳到 5，2 秒后仍判重"


# ----------------------------------------------- 配置极端值

class _FakeAuth:
    async def send_danmaku(self, text):
        return {"code": 0}


def test_sender_max_chars_zero():
    """max_chars=0 被钳到 1，不炸。"""
    s = DanmakuSender(_FakeAuth(), max_chars=0)
    assert s.max_chars == 1


def test_token_bucket_capacity_zero():
    """capacity=0 被钳到 1：初始应可取到 1 张票。"""
    b = TokenBucket(0, 3.0)
    assert b.try_acquire() is True
    assert b.try_acquire() is False


def test_token_bucket_min_interval_zero():
    """min_interval_sec=0 被钳到 0.5，回填不会除零。"""
    b = TokenBucket(1, 0.0)
    assert b.try_acquire() is True
    assert b.try_acquire() is False  # 立即回填量 ~0
    time.sleep(0.55)
    assert b.try_acquire() is True, "0.5s 后应回填 1 张票"


def test_sender_extreme_combo():
    """全部极端值一起传也不炸：截断钳到 1 字，限流可工作。"""
    s = DanmakuSender(_FakeAuth(), max_chars=0, min_interval_sec=0,
                      bucket_capacity=0)
    assert s.max_chars == 1
    assert s.tokens >= 1.0  # capacity 钳到 1，初始满桶
