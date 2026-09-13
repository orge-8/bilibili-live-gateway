"""B 站直播间事件 → MaiBot 标准 MessageDict（纯函数，零网络依赖）。

字段契约来自 MaiBot 1.2.3 的 `src/plugin_runtime/host/message_utils.py`：

- `message_id` 非空字符串；`timestamp` 必须是**float 字符串**（解析失败会回退 now）；
- `platform` 非空；`message_info.user_info.user_id` / `user_nickname` 非空；
- 群聊语义要求 `message_info.group_info.group_id` / `group_name` 均非空；
- `raw_message` 是消息段列表，文本段形如 `{"type": "text", "data": "你好"}`；
- `is_notify=True` 表示通知语义：**入库但不触发 LLM 回复**（进场/关注/开播下播走这条）；
- `session_id` 由 Host 重算，插件不要设置。

直播间按「群」建模：`group_id = 真实房间号`。这样出站时 Host 回传的 route 里
能稳定反解出 room_id，同时让直播间天然复用 MaiBot 的群聊回复策略与记忆分区。
"""

import re
import time
from typing import Any, Iterable, Sequence

PLATFORM = "bilibili"

KIND_DANMAKU = "danmaku"
KIND_GIFT = "gift"
KIND_GUARD = "guard"
KIND_SUPER_CHAT = "super_chat"
KIND_ENTER = "enter"
KIND_FOLLOW = "follow"
KIND_SHARE = "share"
KIND_LIVE_START = "live_start"
KIND_LIVE_END = "live_end"

#: 高价值事件：默认绕过限流与热度闸门，必触发回复
HIGH_VALUE_KINDS = frozenset({KIND_GIFT, KIND_GUARD, KIND_SUPER_CHAT})
#: 通知类事件：只入库，不触发回复
NOTIFY_KINDS = frozenset({KIND_ENTER, KIND_FOLLOW, KIND_SHARE, KIND_LIVE_START, KIND_LIVE_END})

GUARD_LEVEL_NAMES = {1: "总督", 2: "提督", 3: "舰长"}
DEFAULT_GUARD_NAME = "大航海"

_INTERACT_ACTIONS = {1: "进入了直播间", 2: "关注了直播间", 3: "分享了直播间"}

# 弹幕里 @ 是纯文本，且 B 站客户端会掺入零宽字符 / 全角空格，必须先归一化
_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")

_CMD_SUFFIX_RE = re.compile(r":.*$")


# ---------------------------------------------------------------- 基础工具

def normalize_cmd(cmd: Any) -> str:
    """归一化 cmd：B 站会把参数拼在冒号后（如 `DANMU_MSG:4:0:2:2:2:0`）。"""
    if not isinstance(cmd, str):
        return ""
    return _CMD_SUFFIX_RE.sub("", cmd.strip()).upper()


def normalize_text(text: Any) -> str:
    """去掉零宽字符、把全角空格转半角，便于 @ / 关键词匹配。"""
    if not isinstance(text, str):
        return ""
    return _ZERO_WIDTH_RE.sub("", text).replace("\u3000", " ")


def detect_at(text: str, names: Iterable[str]) -> bool:
    """判断弹幕是否 @ 了机器人。B 站弹幕的 @ 是纯文本，没有结构化 at 段。"""
    normalized = normalize_text(text)
    if "@" not in normalized:
        return False
    for name in names:
        clean = normalize_text(name).strip().lstrip("@")
        if clean and f"@{clean}" in normalized:
            return True
    return False


def _payload(event: dict) -> dict:
    """取事件的业务载荷：绝大多数事件在 `data` 子对象里。"""
    data = event.get("data")
    return data if isinstance(data, dict) else {}


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _safe_index(seq: Any, idx: int, default: Any = None) -> Any:
    if isinstance(seq, (list, tuple)) and 0 <= idx < len(seq):
        return seq[idx]
    return default


# ---------------------------------------------------------------- 各事件抽取

def extract_danmaku(event: dict) -> dict:
    """抽取 DANMU_MSG：正文在顶层 `info` 数组里（不在 data 下）。"""
    info = event.get("info")
    if not isinstance(info, list):
        info = _safe_index(_payload(event).get("info"), 0, None) or []
        if not isinstance(info, list):
            info = []
    meta = info[0] if isinstance(_safe_index(info, 0), list) else []
    user = _safe_index(info, 2) if isinstance(_safe_index(info, 2), list) else []
    medal = _safe_index(info, 3) if isinstance(_safe_index(info, 3), list) else []
    extra = _safe_index(info, 7) if isinstance(_safe_index(info, 7), dict) else {}

    ts_ms = _as_int(_safe_index(meta, 4), 0)
    if not ts_ms:
        ts_ms = _as_int(_safe_index(meta, 3), 0) * 1000
    if not ts_ms:
        # 兜底在 info[9]["ts"]（秒级），部分版本没有 info[0][4]
        tail = _safe_index(info, 9)
        if isinstance(tail, dict):
            ts_ms = _as_int(tail.get("ts"), 0) * 1000

    return {
        "uid": _as_str(_safe_index(user, 0)),
        "uname": _as_str(_safe_index(user, 1)) or "未知用户",
        "text": _as_str(_safe_index(info, 1)),
        "ts_ms": ts_ms,
        "medal_level": _as_int(_safe_index(medal, 0), 0),
        "medal_name": _as_str(_safe_index(medal, 1)),
        "guard_level": _as_int(extra.get("guard_level"), 0),
    }


def extract_gift(event: dict) -> dict:
    """抽取 SEND_GIFT / GIFT。"""
    data = _payload(event)
    # 连击时 num 常为 1，真实累计数在 combo_num / batch_combo_num 里
    num = max(
        _as_int(data.get("num"), 0),
        _as_int(data.get("combo_num"), 0),
        _as_int(data.get("batch_combo_num"), 0),
        1,
    )
    return {
        "uid": _as_str(data.get("uid")),
        "uname": _as_str(data.get("uname")) or "未知用户",
        "gift_name": _as_str(data.get("giftName") or data.get("gift_name")) or "礼物",
        "num": num,
        "price": _as_int(data.get("price"), 0),
        "coin_type": _as_str(data.get("coin_type")),
        "ts_ms": _as_int(data.get("timestamp"), 0) * 1000,
    }


def extract_guard(event: dict) -> dict:
    """抽取 GUARD_BUY（大航海）。"""
    data = _payload(event)
    return {
        "uid": _as_str(data.get("uid")),
        "uname": _as_str(data.get("uname")) or "未知用户",
        "guard_level": _as_int(data.get("guard_level"), 0),
        "num": max(_as_int(data.get("num"), 0), 1),
        "price": _as_int(data.get("price"), 0),
        "ts_ms": _as_int(data.get("start_time") or data.get("timestamp"), 0) * 1000,
    }


def extract_super_chat(event: dict) -> dict:
    """抽取 SUPER_CHAT_MESSAGE（醒目留言）。"""
    data = _payload(event)
    user = data.get("user_info") if isinstance(data.get("user_info"), dict) else {}
    price = _as_int(data.get("price"), 0)
    return {
        "uid": _as_str(data.get("uid") or user.get("uid")),
        "uname": _as_str(user.get("uname") or data.get("uname")) or "未知用户",
        "message": _as_str(data.get("message")),
        "price": price,
        # SC 的 price 单位是元；部分版本给的是金瓜子
        "rmb": _as_float(data.get("rmb"), price if price < 10000 else price / 1000.0),
        "ts_ms": _as_int(data.get("ts") or data.get("start_time"), 0) * 1000,
    }


def extract_interact(event: dict) -> dict:
    """抽取 INTERACT_WORD（进场 / 关注 / 分享）。"""
    data = _payload(event)
    return {
        "uid": _as_str(data.get("uid")),
        "uname": _as_str(data.get("uname")) or "未知用户",
        "msg_type": _as_int(data.get("msg_type"), 1),
        "ts_ms": _as_int(data.get("timestamp") or data.get("ts"), 0) * 1000,
    }


def classify(cmd: str) -> str:
    """归一化后的 cmd → 内部事件类别。未识别返回空串。"""
    if cmd == "DANMU_MSG":
        return KIND_DANMAKU
    if cmd in ("SEND_GIFT", "GIFT"):
        return KIND_GIFT
    if cmd == "GUARD_BUY":
        return KIND_GUARD
    if cmd in ("SUPER_CHAT_MESSAGE", "SUPER_CHAT_MESSAGE_JPN"):
        return KIND_SUPER_CHAT
    if cmd == "INTERACT_WORD":
        return KIND_ENTER
    if cmd in ("LIVE", "ROOM_LIVE"):
        return KIND_LIVE_START
    if cmd in ("PREPARING",):
        # 注意：ROOM_REAL_TIME_MESSAGE_UPDATE 是开播期间的实时统计推送（人气/点赞数），
        # 高频出现，绝不能归类为 live_end——否则每次开播都会高频注入"主播下播了"污染记忆。
        return KIND_LIVE_END
    return ""


# ---------------------------------------------------------------- 文案渲染

def render(cmd: str, event: dict) -> str:
    """把事件渲染成给 LLM 看的一行中文文案。未识别事件返回空串。"""
    kind = classify(cmd)
    if kind == KIND_DANMAKU:
        return extract_danmaku(event)["text"]
    if kind == KIND_GIFT:
        g = extract_gift(event)
        return f"🎁 {g['uname']} 送出了 {g['num']} 个 {g['gift_name']}"
    if kind == KIND_GUARD:
        g = extract_guard(event)
        name = GUARD_LEVEL_NAMES.get(g["guard_level"], DEFAULT_GUARD_NAME)
        return f"🛡️ {g['uname']} 开通了 {name}"
    if kind == KIND_SUPER_CHAT:
        s = extract_super_chat(event)
        price = _fmt_amount(s["rmb"])
        if s["message"].strip():
            return f"💰 SC {price}元 {s['uname']}：{s['message']}"
        return f"💰 SC {price}元 {s['uname']} 发送了醒目留言"
    if kind == KIND_ENTER:
        return f"👋 {extract_interact(event)['uname']} {_INTERACT_ACTIONS.get(extract_interact(event)['msg_type'], '进入了直播间')}"
    if kind == KIND_LIVE_START:
        title = _as_str(_payload(event).get("title"))
        return f"🔴 主播开播了：{title}" if title else "🔴 主播开播了"
    if kind == KIND_LIVE_END:
        return "⚫ 主播下播了"
    return ""


def _fmt_amount(value: float) -> str:
    """金额去掉多余的小数尾巴：50.0 -> 50，9.9 -> 9.9。"""
    return f"{value:.0f}" if abs(value - round(value)) < 1e-6 else f"{value:g}"


def score_importance(cmd: str, event: dict) -> float:
    """事件重要性 [0,1]，用于排序与兜底阈值（算法对齐 Amaidesu 的官方采集器）。"""
    kind = classify(cmd)
    if kind == KIND_DANMAKU:
        d = extract_danmaku(event)
        medal_bonus = min(d["medal_level"] / 40.0, 0.2)
        guard_bonus = {1: 0.3, 2: 0.2, 3: 0.1}.get(d["guard_level"], 0.0)
        return min(0.5 + medal_bonus + guard_bonus, 1.0)
    if kind == KIND_GIFT:
        g = extract_gift(event)
        base = min(g["price"] / 10000.0, 0.5)
        quantity_bonus = min(g["num"] / 10.0, 0.3)
        paid_bonus = 0.1 if g["coin_type"] == "gold" else 0.0
        return min(base + quantity_bonus + paid_bonus, 1.0)
    if kind == KIND_GUARD:
        return {1: 1.0, 2: 0.9, 3: 0.8}.get(extract_guard(event)["guard_level"], 0.7)
    if kind == KIND_SUPER_CHAT:
        return min(0.5 + extract_super_chat(event)["rmb"] / 100.0, 1.0)
    if kind == KIND_LIVE_START:
        return 0.6
    if kind in NOTIFY_KINDS:
        return 0.1
    return 0.1


# ---------------------------------------------------------------- 构造 MessageDict

def to_message(
    event: dict,
    *,
    room_id: str,
    room_name: str = "",
    account_id: str = "bilibili",
    at_names: Sequence[str] = (),
    seq: int = 0,
    now: float | None = None,
) -> dict | None:
    """把一条 B 站事件转成 MaiBot MessageDict。未识别或正文为空时返回 None。"""
    cmd = normalize_cmd(event.get("cmd"))
    kind = classify(cmd)
    if not kind:
        return None

    text = normalize_text(render(cmd, event)).strip()
    if not text:
        return None

    detail = _extract_detail(kind, event)
    ts_ms = _as_int(detail.get("ts_ms"), 0)
    now = time.time() if now is None else now
    if ts_ms <= 0:
        ts_ms = int(now * 1000)

    uid = _as_str(detail.get("uid")) or "unknown"
    uname = _as_str(detail.get("uname")) or "未知用户"
    room = str(room_id)
    is_at = kind == KIND_DANMAKU and detect_at(text, at_names)

    return {
        "message_id": f"blg-{room}-{cmd}-{uid}-{ts_ms}-{seq}",
        # Host 要求 float 时间戳字符串（解析失败会回退 now）
        "timestamp": f"{ts_ms / 1000.0:.3f}",
        "platform": PLATFORM,
        "message_info": {
            "user_info": {
                "user_id": uid,
                "user_nickname": uname,
                "user_cardname": detail.get("medal_name") or None,
            },
            "group_info": {
                "group_id": room,
                "group_name": room_name or f"直播间 {room}",
            },
            "additional_config": {
                "self_id": str(account_id),
                "bili_room_id": room,
                "bili_event_kind": kind,
                "bili_event_cmd": cmd,
                "bili_importance": score_importance(cmd, event),
                "platform_io_target_group_id": room,
            },
        },
        "raw_message": [{"type": "text", "data": text}],
        "processed_plain_text": text,
        "is_at": is_at,
        "is_mentioned": is_at,
        "is_notify": kind in NOTIFY_KINDS,
    }


def _extract_detail(kind: str, event: dict) -> dict:
    if kind == KIND_DANMAKU:
        return extract_danmaku(event)
    if kind == KIND_GIFT:
        return extract_gift(event)
    if kind == KIND_GUARD:
        return extract_guard(event)
    if kind == KIND_SUPER_CHAT:
        return extract_super_chat(event)
    if kind == KIND_ENTER:
        return extract_interact(event)
    return {}


def synthetic_danmaku(text: str, uid: str = "0", uname: str = "探针观众",
                      ts_ms: int = 0, medal_level: int = 0,
                      guard_level: int = 0) -> dict:
    """造一条 DANMU_MSG 假事件。

    探针与单测共用：走完整 `to_message` 链路，而不是手工拼 MessageDict，
    这样连映射层的 bug 也一起验证到。
    """
    return {
        "cmd": "DANMU_MSG",
        "info": [
            [0, 25, 16777215, int((ts_ms or time.time() * 1000) / 1000), ts_ms or int(time.time() * 1000)],
            text,
            [int(uid) if str(uid).isdigit() else 0, uname, 0, 0, 0],
            [medal_level, "探针粉丝牌", "", 0] if medal_level else [],
            [0, 0, 0, 0],
            ["", ""],
            0,
            {"guard_level": guard_level},
            {},
            {"ts": int((ts_ms or time.time() * 1000) / 1000)},
        ],
    }
