"""B 站直播弹幕 WebSocket 协议编解码（纯函数，零网络依赖，可直接单测）。

协议：16 字节定长头 + 变长包体，所有数值字段大端（Big Endian）对齐。

    offset  len  type  字段          说明
    0       4    i32   packet_len    整包长度（含头）
    4       2    i16   header_len    固定 16
    6       2    i16   ver           0=原文 JSON 1=心跳/鉴权 2=zlib 压缩 3=brotli 压缩
    8       4    i32   op            2 心跳 3 心跳回复 5 消息推送 7 鉴权 8 鉴权回复
    12      4    i32   seq           保留字段

两个容易踩的坑（社区实现常见缺陷）：

1. **ver=2/3 的正文里可能再套若干完整子包**（每个子包自带 16 字节头），
   必须循环按 packet_len 切分，而不是只解一层。
2. **不要用固定小上限（如 2048）卡包长**——礼物连击、长弹幕的包会超过它，
   超限即静默丢消息。这里改为按头部声明的长度解析 + 1 MiB 安全上限。
"""

import json
import struct
import zlib
from dataclasses import dataclass
from typing import Any, Iterator

try:  # brotli 是可选依赖：缺失时 ver=3 包无法解析，但模块仍可导入
    import brotli
except ImportError:  # pragma: no cover
    brotli = None


HEADER_LEN = 16
#: 单包长度安全上限（防止畸形包把内存吃干）
MAX_PACKET_LEN = 1 << 20
#: 解压后长度安全上限
MAX_DECOMPRESSED_LEN = 4 << 20
#: 解压嵌套深度上限（正常只有一层）
MAX_NEST_DEPTH = 2

OP_HEARTBEAT = 2
OP_HEARTBEAT_REPLY = 3
OP_MESSAGE = 5
OP_AUTH = 7
OP_AUTH_REPLY = 8

VER_PLAIN = 0
VER_HEARTBEAT = 1
VER_ZLIB = 2
VER_BROTLI = 3

_HEADER_STRUCT = struct.Struct(">ihhii")  # 4+2+2+4+4 = 16

OP_NAMES = {
    OP_HEARTBEAT: "heartbeat",
    OP_HEARTBEAT_REPLY: "heartbeat_reply",
    OP_MESSAGE: "message",
    OP_AUTH: "auth",
    OP_AUTH_REPLY: "auth_reply",
}


class ProtocolError(Exception):
    """包结构非法（长度越界、头长异常、缓冲区被截断等）。"""


@dataclass(frozen=True)
class Packet:
    """一个已解压的协议包。ver 已被归一化为 0/1（压缩层已剥掉）。"""

    op: int
    body: bytes
    ver: int = VER_PLAIN
    seq: int = 0

    @property
    def op_name(self) -> str:
        return OP_NAMES.get(self.op, f"op_{self.op}")

    def json(self) -> Any:
        """把 body 当 UTF-8 JSON 解析。body 为空返回 None。"""
        if not self.body:
            return None
        return json.loads(self.body.decode("utf-8", errors="replace"))


def pack(op: int, body: "str | bytes" = "", ver: int = VER_HEARTBEAT, seq: int = 1) -> bytes:
    """按协议打包一个包。默认 ver=1（心跳/鉴权包用的未压缩版本）。

    body 接受 str（按 UTF-8 编码）或 bytes（如 op=3 心跳回复的 4 字节人气值）。
    """
    raw = body.encode("utf-8") if isinstance(body, str) else bytes(body)
    packet_len = HEADER_LEN + len(raw)
    return _HEADER_STRUCT.pack(packet_len, HEADER_LEN, ver, op, seq) + raw


def pack_auth(uid: int, room_id: int, token: str, buvid: str = "",
              platform: str = "web", protover: int = 3, seq: int = 1) -> bytes:
    """打包 op=7 鉴权包。

    uid=0 是游客身份；带真实 uid 时必须配合该账号的 token，否则服务端直接断连。
    """
    body = json.dumps({
        "uid": int(uid),
        "roomid": int(room_id),
        "protover": int(protover),
        "platform": platform,
        "type": 2,
        "key": token or "",
        "buvid": buvid or "",
    }, ensure_ascii=False)
    return pack(OP_AUTH, body, ver=VER_HEARTBEAT, seq=seq)


def pack_heartbeat(seq: int = 1) -> bytes:
    """打包 op=2 心跳包（空体）。"""
    return pack(OP_HEARTBEAT, "", ver=VER_HEARTBEAT, seq=seq)


def unpack(buf: bytes, *, _depth: int = 0) -> list[Packet]:
    """把一段缓冲区解析成若干 Packet，自动解压并展开嵌套子包。

    一次性返回列表方便单测；流式消费可用 iter_unpack。
    """
    return list(iter_unpack(buf, _depth=_depth))


def iter_unpack(buf: bytes, *, _depth: int = 0) -> Iterator[Packet]:
    """流式解析：按 packet_len 逐包切分，压缩包递归展开。"""
    offset = 0
    total = len(buf)
    while offset + HEADER_LEN <= total:
        packet_len, header_len, ver, op, seq = _HEADER_STRUCT.unpack_from(buf, offset)
        if header_len != HEADER_LEN:
            raise ProtocolError(f"header_len 异常: {header_len}（期望 {HEADER_LEN}）")
        if packet_len < HEADER_LEN:
            raise ProtocolError(f"packet_len 过小: {packet_len}")
        if packet_len > MAX_PACKET_LEN:
            raise ProtocolError(f"packet_len 超过安全上限: {packet_len}")
        end = offset + packet_len
        if end > total:
            raise ProtocolError(f"缓冲区被截断：需要 {packet_len} 字节，只剩 {total - offset}")
        body = buf[offset + HEADER_LEN:end]
        offset = end

        if ver in (VER_ZLIB, VER_BROTLI):
            if _depth >= MAX_NEST_DEPTH:
                raise ProtocolError(f"压缩嵌套过深（depth={_depth}）")
            for sub in iter_unpack(_decompress(ver, body), _depth=_depth + 1):
                yield sub
        else:
            yield Packet(op=op, body=body, ver=ver, seq=seq)


def _decompress(ver: int, body: bytes) -> bytes:
    """按 ver 解压包体。解压失败统一抛 ProtocolError。"""
    if ver == VER_ZLIB:
        try:
            data = zlib.decompress(body)
        except zlib.error as exc:
            raise ProtocolError(f"zlib 解压失败: {exc}") from exc
    else:
        if brotli is None:
            raise ProtocolError("收到 brotli 压缩包（ver=3），但未安装 brotli 依赖")
        try:
            data = brotli.decompress(body)
        except Exception as exc:
            raise ProtocolError(f"brotli 解压失败: {exc}") from exc
    if len(data) > MAX_DECOMPRESSED_LEN:
        raise ProtocolError(f"解压后长度超过安全上限: {len(data)}")
    return data


def iter_events(packets: Any) -> Iterator[dict]:
    """从 Packet 序列里抽 op=5 的业务事件。

    兼容两种正文形态：单个 JSON 对象，或按行分隔的多个 JSON 对象。
    非 JSON 内容静默跳过——弹幕流里混着心跳回复、在线人数等非弹幕包是常态，
    任何一条解析失败都不能中断整条流。
    """
    for pkt in packets:
        if pkt.op != OP_MESSAGE or not pkt.body:
            continue
        try:
            raw = pkt.json()
        except (ValueError, UnicodeDecodeError):
            continue
        for item in _as_event_list(raw):
            if isinstance(item, dict) and item.get("cmd"):
                yield item


def _as_event_list(raw: Any) -> list:
    """把解析结果统一成事件列表，容忍行分隔的多事件正文。"""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return [raw]
    return []


def parse_heartbeat_reply(packet: Packet) -> int | None:
    """op=3 的正文是 4 字节大端人气值，可用来判断房间是否活着。"""
    if packet.op != OP_HEARTBEAT_REPLY or len(packet.body) < 4:
        return None
    return struct.unpack(">i", packet.body[:4])[0]


def parse_auth_reply(packet: Packet) -> int | None:
    """op=8 的正文是 JSON，返回其中的 code（0 = 鉴权成功）。"""
    if packet.op != OP_AUTH_REPLY:
        return None
    try:
        data = packet.json()
    except (ValueError, UnicodeDecodeError):
        return None
    if isinstance(data, dict):
        code = data.get("code")
        return int(code) if isinstance(code, (int, float, str)) and str(code).lstrip("-").isdigit() else None
    return None
