"""blg_proto 协议编解码单测。

覆盖 ver 0/1/2/3、多包拼接、大包不被截断、畸形包报错——
这几项正是社区实现最容易静默丢消息的地方。
"""

import json
import struct
import zlib

import brotli
import pytest

from blg_proto import (
    HEADER_LEN,
    OP_AUTH_REPLY,
    OP_HEARTBEAT,
    OP_HEARTBEAT_REPLY,
    OP_MESSAGE,
    VER_BROTLI,
    VER_HEARTBEAT,
    VER_PLAIN,
    VER_ZLIB,
    Packet,
    ProtocolError,
    iter_events,
    pack,
    pack_auth,
    pack_heartbeat,
    parse_auth_reply,
    parse_heartbeat_reply,
    unpack,
)

_HEADER = struct.Struct(">ihhii")


def _wrap(body: bytes, ver: int, op: int = OP_MESSAGE, seq: int = 1) -> bytes:
    """手工构造一个协议包（不借用被测代码，保证测试独立）。"""
    return _HEADER.pack(HEADER_LEN + len(body), HEADER_LEN, ver, op, seq) + body


def _danmaku_body(text: str = "你好") -> bytes:
    return json.dumps({"cmd": "DANMU_MSG", "info": [[0, 25, 16777215, 1, 2], text]}).encode()


# ---------------------------------------------------------------- 基础往返

def test_pack_unpack_roundtrip():
    raw = pack(OP_MESSAGE, "hello", ver=VER_HEARTBEAT, seq=7)
    packets = unpack(raw)
    assert len(packets) == 1
    assert packets[0].op == OP_MESSAGE
    assert packets[0].seq == 7
    assert packets[0].body == b"hello"


def test_heartbeat_empty_body():
    packets = unpack(pack_heartbeat(seq=3))
    assert len(packets) == 1
    assert packets[0].op == OP_HEARTBEAT
    assert packets[0].body == b""


def test_plain_version_zero_kept_as_is():
    packets = unpack(_wrap(b'{"a":1}', VER_PLAIN))
    assert packets[0].ver == VER_PLAIN
    assert packets[0].json() == {"a": 1}


# ---------------------------------------------------------------- 多包 / 大包

def test_zlib_packet_expands_multiple_sub_packets():
    """ver=2 的正文里套两个完整子包，必须都被拆出来。"""
    inner = _wrap(_danmaku_body("第一条"), VER_PLAIN) + _wrap(_danmaku_body("第二条"), VER_PLAIN)
    packets = unpack(_wrap(zlib.compress(inner), VER_ZLIB))
    assert len(packets) == 2
    assert [json.loads(p.body)["info"][1] for p in packets] == ["第一条", "第二条"]


def test_brotli_packet_expands_multiple_sub_packets():
    inner = _wrap(_danmaku_body("甲"), VER_PLAIN) + _wrap(_danmaku_body("乙"), VER_PLAIN)
    packets = unpack(_wrap(brotli.compress(inner), VER_BROTLI))
    assert len(packets) == 2
    assert packets[0].op == OP_MESSAGE


def test_large_packet_is_not_truncated():
    """包长超过 2048（社区实现常见的固定上限）时必须完整保留。"""
    big = "字" * 5000
    packets = unpack(_wrap(json.dumps({"cmd": "DANMU_MSG", "text": big}).encode(), VER_PLAIN))
    assert len(packets) == 1
    assert len(json.loads(packets[0].body)["text"]) == 5000


def test_coalesced_uncompressed_packets_are_split():
    packets = unpack(_wrap(b"a", VER_PLAIN) + _wrap(b"bb", VER_PLAIN) + _wrap(b"ccc", VER_PLAIN))
    assert [p.body for p in packets] == [b"a", b"bb", b"ccc"]


# ---------------------------------------------------------------- 畸形包

def test_truncated_buffer_raises():
    raw = _wrap(_danmaku_body(), VER_PLAIN)
    with pytest.raises(ProtocolError, match="截断"):
        unpack(raw[:-5])


def test_bad_header_len_raises():
    malformed = struct.pack(">ihhii", HEADER_LEN + 1, 12, VER_PLAIN, OP_MESSAGE, 1) + b"x"
    with pytest.raises(ProtocolError, match="header_len"):
        unpack(malformed)


def test_oversized_packet_len_raises():
    malformed = struct.pack(">ihhii", (1 << 21), HEADER_LEN, VER_PLAIN, OP_MESSAGE, 1)
    with pytest.raises(ProtocolError, match="安全上限"):
        unpack(malformed)


def test_bad_zlib_body_raises_protocol_error():
    with pytest.raises(ProtocolError, match="zlib"):
        unpack(_wrap(b"not-zlib-data", VER_ZLIB))


def test_two_compression_layers_are_accepted():
    """协议允许一层压缩；多套一层也应能正常展开。"""
    inner = _wrap(_danmaku_body("嵌套内的弹幕"), VER_PLAIN)
    packets = unpack(_wrap(zlib.compress(_wrap(zlib.compress(inner), VER_ZLIB)), VER_ZLIB))
    assert len(packets) == 1
    assert json.loads(packets[0].body)["info"][1] == "嵌套内的弹幕"


def test_deep_nesting_rejected():
    """三层以上嵌套压缩应被拒绝，避免解压炸弹。"""
    level1 = _wrap(_danmaku_body(), VER_PLAIN)
    level2 = _wrap(zlib.compress(level1), VER_ZLIB)
    level3 = _wrap(zlib.compress(level2), VER_ZLIB)
    level4 = _wrap(zlib.compress(level3), VER_ZLIB)
    with pytest.raises(ProtocolError, match="嵌套"):
        unpack(level4)


# ---------------------------------------------------------------- 事件抽取

def test_iter_events_yields_only_message_packets():
    packets = [
        Packet(op=OP_HEARTBEAT_REPLY, body=struct.pack(">i", 100)),
        Packet(op=OP_MESSAGE, body=_danmaku_body("真弹幕")),
        Packet(op=OP_MESSAGE, body=b"not json"),
    ]
    events = list(iter_events(packets))
    assert len(events) == 1
    assert events[0]["cmd"] == "DANMU_MSG"


def test_iter_events_accepts_list_body():
    body = json.dumps([{"cmd": "SEND_GIFT"}, {"cmd": "GUARD_BUY"}]).encode()
    events = list(iter_events([Packet(op=OP_MESSAGE, body=body)]))
    assert [e["cmd"] for e in events] == ["SEND_GIFT", "GUARD_BUY"]


def test_parse_auth_reply_code():
    assert parse_auth_reply(Packet(op=OP_AUTH_REPLY, body=b'{"code":0}')) == 0
    assert parse_auth_reply(Packet(op=OP_AUTH_REPLY, body=b'{"code":-1}')) == -1
    assert parse_auth_reply(Packet(op=OP_MESSAGE, body=b'{"code":0}')) is None
    assert parse_auth_reply(Packet(op=OP_AUTH_REPLY, body=b"garbage")) is None


def test_parse_heartbeat_reply_popularity():
    pkt = Packet(op=OP_HEARTBEAT_REPLY, body=struct.pack(">i", 4321))
    assert parse_heartbeat_reply(pkt) == 4321
    assert parse_heartbeat_reply(Packet(op=OP_MESSAGE, body=b"")) is None


def test_pack_auth_payload_fields():
    raw = pack_auth(uid=0, room_id=2233, token="tok", buvid="buv")
    body = json.loads(unpack(raw)[0].body)
    assert body["roomid"] == 2233
    assert body["protover"] == 3
    assert body["platform"] == "web"
    assert body["key"] == "tok"
    assert body["buvid"] == "buv"
