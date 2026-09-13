"""blg_events 事件映射单测。

断言的重点是 MessageDict 契约（Host 会因为字段格式不对而拒收或回退）：
timestamp 必须是 float 字符串、user_info 两个字段非空、群聊语义要求 group_info 非空。
"""

import pytest

from blg_events import (
    HIGH_VALUE_KINDS,
    KIND_DANMAKU,
    KIND_GIFT,
    KIND_GUARD,
    KIND_ENTER,
    KIND_LIVE_START,
    KIND_SUPER_CHAT,
    NOTIFY_KINDS,
    classify,
    detect_at,
    normalize_cmd,
    normalize_text,
    score_importance,
    synthetic_danmaku,
    to_message,
)

ROOM = "2233"
ROOM_NAME = "测试直播间"


def _danmaku(text="你好", uid="1001", uname="观众甲", ts_ms=1700000000000, **kw):
    """走完整映射链路造一条弹幕 MessageDict（顺带一起验证映射层）。"""
    event = synthetic_danmaku(text, uid=uid, uname=uname, ts_ms=ts_ms, **kw)
    return to_message(event, room_id=ROOM, room_name=ROOM_NAME, at_names=["狸猫"])


# ---------------------------------------------------------------- 文本工具

def test_normalize_cmd_strips_parameter_suffix():
    assert normalize_cmd("DANMU_MSG:4:0:2:2:2:0") == "DANMU_MSG"
    assert normalize_cmd(" send_gift ") == "SEND_GIFT"
    assert normalize_cmd(None) == ""


def test_normalize_text_strips_zero_width_and_fullwidth_space():
    # 零宽字符直接删除，全角空格归一成半角
    assert normalize_text("你\u200b好\u3000吗") == "你好 吗"


def test_detect_at_handles_zero_width_between_at_and_name():
    assert detect_at("@\u200b狸猫 你好", ["狸猫"]) is True
    assert detect_at("狸猫你好", ["狸猫"]) is False
    assert detect_at("@其他人", ["狸猫"]) is False
    assert detect_at("@狸猫", []) is False


def test_detect_at_accepts_leading_at_in_config_name():
    assert detect_at("@狸猫 在吗", ["@狸猫"]) is True


# ---------------------------------------------------------------- 分类

def test_classify_covers_known_commands():
    assert classify("DANMU_MSG") == KIND_DANMAKU
    assert classify("SEND_GIFT") == KIND_GIFT
    assert classify("GIFT") == KIND_GIFT
    assert classify("GUARD_BUY") == KIND_GUARD
    assert classify("SUPER_CHAT_MESSAGE") == KIND_SUPER_CHAT
    assert classify("INTERACT_WORD") == KIND_ENTER
    assert classify("LIVE") == KIND_LIVE_START
    assert classify("SOME_NEW_EVENT") == ""


def test_classify_live_end_excludes_real_time_update():
    """全检修复 #2：ROOM_REAL_TIME_MESSAGE_UPDATE 是开播期间的实时统计推送，
    高频出现，不能归类为 live_end（否则开播时高频注入"主播下播了"污染记忆）。"""
    assert classify("PREPARING") == "live_end"
    assert classify("ROOM_REAL_TIME_MESSAGE_UPDATE") == ""
    # 该事件走完整映射链路必须被丢弃（返回 None）
    assert to_message({"cmd": "ROOM_REAL_TIME_MESSAGE_UPDATE"},
                      room_id=ROOM, room_name=ROOM_NAME) is None


def test_high_value_and_notify_sets_are_disjoint():
    assert not (HIGH_VALUE_KINDS & NOTIFY_KINDS)


# ---------------------------------------------------------------- 弹幕

def test_danmaku_message_dict_contract():
    msg = _danmaku("@狸猫 帮我看下这首歌", uid="1001", uname="观众甲")
    assert msg is not None
    assert msg["platform"] == "bilibili"
    assert msg["message_id"]
    # timestamp 必须是 float 字符串，Host 解析失败会回退 now
    float(msg["timestamp"])
    assert msg["timestamp"] == "1700000000.000"
    info = msg["message_info"]
    assert info["user_info"]["user_id"] == "1001"
    assert info["user_info"]["user_nickname"] == "观众甲"
    # 直播间按群建模：group_info 必须非空
    assert info["group_info"]["group_id"] == ROOM
    assert info["group_info"]["group_name"] == ROOM_NAME
    assert info["additional_config"]["bili_room_id"] == ROOM
    assert info["additional_config"]["platform_io_target_group_id"] == ROOM
    assert msg["raw_message"] == [{"type": "text", "data": "@狸猫 帮我看下这首歌"}]
    assert msg["is_at"] is True
    assert msg["is_mentioned"] is True
    assert msg["is_notify"] is False


def test_danmaku_without_at_is_not_triggered_flag():
    msg = _danmaku("普通弹幕")
    assert msg["is_at"] is False
    assert msg["is_notify"] is False


def test_danmaku_empty_text_returns_none():
    assert _danmaku("") is None
    assert _danmaku("   ") is None


def test_unknown_command_returns_none():
    msg = to_message({"cmd": "WATCHED_CHANGE", "data": {"num": 5}}, room_id=ROOM)
    assert msg is None


def test_message_id_is_unique_across_sequence():
    event = synthetic_danmaku("同一毫秒", ts_ms=1700000000000)
    first = to_message(event, room_id=ROOM, seq=1)
    second = to_message(event, room_id=ROOM, seq=2)
    assert first["message_id"] != second["message_id"]


def test_message_id_falls_back_to_now_when_ts_missing():
    event = synthetic_danmaku("无时间戳", ts_ms=0)
    event["info"][0][4] = 0
    event["info"][0][3] = 0
    event["info"][9] = {}
    msg = to_message(event, room_id=ROOM, now=1700000000.0)
    assert msg["timestamp"] == "1700000000.000"


# ---------------------------------------------------------------- 礼物 / 上舰 / SC

def test_gift_uses_combo_count_not_num():
    """连击时 num 恒为 1，必须取 combo_num 才是真实累计数量。"""
    event = {
        "cmd": "SEND_GIFT",
        "data": {"uid": 2002, "uname": "送礼人", "giftName": "辣条", "num": 1,
                 "combo_num": 5, "price": 100, "coin_type": "gold", "timestamp": 1700000000},
    }
    msg = to_message(event, room_id=ROOM, room_name=ROOM_NAME)
    assert msg["raw_message"][0]["data"] == "🎁 送礼人 送出了 5 个 辣条"
    assert msg["is_notify"] is False
    assert msg["message_info"]["additional_config"]["bili_event_kind"] == KIND_GIFT
    assert score_importance(event["cmd"], event) == pytest.approx(0.41)


def test_gift_batch_combo_num_also_counted():
    event = {"cmd": "SEND_GIFT", "data": {"uname": "甲", "giftName": "花花", "num": 1,
                                         "batch_combo_num": 9, "price": 0}}
    assert to_message(event, room_id=ROOM)["raw_message"][0]["data"] == "🎁 甲 送出了 9 个 花花"


def test_guard_level_maps_to_chinese_name():
    event = {"cmd": "GUARD_BUY", "data": {"uid": 3, "uname": "提督哥", "guard_level": 2,
                                         "num": 1, "price": 19998000, "start_time": 1700000000}}
    msg = to_message(event, room_id=ROOM)
    assert msg["raw_message"][0]["data"] == "🛡️ 提督哥 开通了 提督"
    assert score_importance("GUARD_BUY", event) == 0.9


def test_guard_unknown_level_falls_back():
    event = {"cmd": "GUARD_BUY", "data": {"uname": "甲", "guard_level": 9}}
    assert to_message(event, room_id=ROOM)["raw_message"][0]["data"] == "🛡️ 甲 开通了 大航海"


def test_super_chat_renders_amount_and_message():
    event = {"cmd": "SUPER_CHAT_MESSAGE",
             "data": {"uid": 9, "message": "加油", "price": 30,
                      "user_info": {"uname": "SC哥"}, "ts": 1700000000}}
    msg = to_message(event, room_id=ROOM)
    assert msg["raw_message"][0]["data"] == "💰 SC 30元 SC哥：加油"
    assert msg["is_notify"] is False
    assert score_importance("SUPER_CHAT_MESSAGE", event) == pytest.approx(0.8)


def test_super_chat_without_message_still_rendered():
    event = {"cmd": "SUPER_CHAT_MESSAGE",
             "data": {"uid": 9, "message": "  ", "price": 50, "user_info": {"uname": "SC哥"}}}
    assert to_message(event, room_id=ROOM)["raw_message"][0]["data"] == "💰 SC 50元 SC哥 发送了醒目留言"


# ---------------------------------------------------------------- 通知类

def test_enter_event_is_notify_only():
    event = {"cmd": "INTERACT_WORD",
             "data": {"uid": 5, "uname": "路人", "msg_type": 1, "timestamp": 1700000000}}
    msg = to_message(event, room_id=ROOM, room_name=ROOM_NAME)
    assert msg["is_notify"] is True
    assert msg["raw_message"][0]["data"] == "👋 路人 进入了直播间"
    assert score_importance("INTERACT_WORD", event) == 0.1


def test_follow_and_share_actions_rendered():
    for msg_type, expect in ((2, "关注了直播间"), (3, "分享了直播间")):
        event = {"cmd": "INTERACT_WORD", "data": {"uname": "甲", "msg_type": msg_type}}
        assert expect in to_message(event, room_id=ROOM)["raw_message"][0]["data"]


def test_live_start_event_is_notify_only():
    event = {"cmd": "LIVE", "data": {"title": "今晚聊歌"}}
    msg = to_message(event, room_id=ROOM)
    assert msg["is_notify"] is True
    assert msg["raw_message"][0]["data"] == "🔴 主播开播了：今晚聊歌"


# ---------------------------------------------------------------- 重要性评分

def test_danmaku_importance_reflects_medal_and_guard():
    plain = synthetic_danmaku("普通")
    assert score_importance("DANMU_MSG", plain) == pytest.approx(0.5)

    fan = synthetic_danmaku("粉丝牌40级", medal_level=40)
    assert score_importance("DANMU_MSG", fan) == pytest.approx(0.7)

    admiral = synthetic_danmaku("提督发言", guard_level=2)
    assert score_importance("DANMU_MSG", admiral) == pytest.approx(0.7)


def test_importance_never_exceeds_one():
    event = {"cmd": "SUPER_CHAT_MESSAGE", "data": {"price": 100000, "user_info": {"uname": "甲"}}}
    assert score_importance("SUPER_CHAT_MESSAGE", event) == 1.0
    # 礼物评分上限是 0.5(价格) + 0.3(数量) + 0.1(金瓜子) = 0.9，到不了 1.0
    gift = {"cmd": "SEND_GIFT", "data": {"num": 999, "price": 999999, "coin_type": "gold"}}
    assert score_importance("SEND_GIFT", gift) == pytest.approx(0.9)


def test_room_name_defaults_when_blank():
    event = synthetic_danmaku("你好")
    msg = to_message(event, room_id=ROOM)
    assert msg["message_info"]["group_info"]["group_name"] == f"直播间 {ROOM}"
