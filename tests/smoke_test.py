"""bilibili-live-gateway 冒烟测试：不启动 MaiBot、不连网络，验证网关双工链路。

    python tests/smoke_test.py

覆盖：
- 生命周期三件套可跑通（on_load / on_config_update / on_unload）
- 网关就绪上报（update_state ready=True → False）
- P1 探针：合成弹幕经完整映射层注入，报文满足 Host 契约
- 出站处理器：Host 回传的回复被正确降级成纯文本弹幕
- 卸载后不留后台协程

FakeHost 的 rpc_call 会把 `host.route_message` / `host.update_message_gateway_state`
原样记进 calls，所以能直接断言插件真正发出去的报文。
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PLUGIN_DIR = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))
sys.path.insert(0, str(PLUGIN_DIR))

import fakehost  # noqa: E402
from maibot_sdk.components import collect_components  # noqa: E402
from blg_events import synthetic_danmaku  # noqa: E402

PLUGIN_ID = "org.mai-mai.bilibili-live-gateway"
_ACCEPT = {"accepted": True}


class RecordingHost(fakehost.FakeHost):
    """devkit 的 FakeHost 只记 `(能力名, args)`，而网关调用走的是
    `call_host_method("host.route_message", payload=...)`——payload 里没有
    `capability`/`args` 两个键，会被记成空 dict，报文内容就丢了。
    这里继承一层，把原始 payload 完整留档。
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.payloads: list[tuple[str, dict]] = []

    async def rpc_call(self, method, plugin_id="", payload=None, **kwargs):
        self.payloads.append((method, dict(payload or {})))
        return await super().rpc_call(method, plugin_id, payload, **kwargs)

    def payloads_of(self, method: str) -> list[dict]:
        """取某个 Host 方法的全部调用载荷。"""
        return [payload for name, payload in self.payloads if name == method]


_RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    """记一条断言结果并打印。"""
    ok = bool(ok)
    _RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  | {detail}" if detail else ""))
    return ok


def section(title: str) -> None:
    print(f"\n--- {title} ---")


def _build(plugin_module, *, probe_delay: float = 0.05, enabled: bool = True, **overrides):
    """造一个绑好 FakeHost 的插件实例，返回 (plugin, host, config)。"""
    plugin = plugin_module.create_plugin()
    host = RecordingHost(
        plugin_id=PLUGIN_ID,
        returns={"host.route_message": _ACCEPT, "host.update_message_gateway_state": _ACCEPT},
    )
    ctx = fakehost.build_context(PLUGIN_ID, rpc_call=host.rpc_call)
    config = fakehost.get_default_config(plugin_module.BiliLiveGatewayConfig)
    config["plugin"]["enabled"] = enabled
    config["live"]["room_id"] = "2233"
    config["live"]["room_display_name"] = "冒烟直播间"
    # fake cookie：绕过 _start_live 的空 cookie 早退（场景 7 桩测试要走到
    # BiliHttpClient 实例化），桩掉网络后不会发任何真实请求
    config["auth"]["cookie"] = "SESSDATA=fake; bili_jct=fake; buvid3=fake"
    config["probe"]["delay_sec"] = probe_delay
    config["probe"]["enabled"] = True
    # 默认关掉自测与超时等待，让场景 1 的断言不被额外日志/等待干扰
    config["probe"]["self_test_outbound"] = False
    config["probe"]["outbound_wait_sec"] = 0.0
    # 默认不建长连接（涉及网络）；长连接行为在场景 7 里用桩验证
    config["inbound"]["enabled"] = False
    config["trigger"]["at_names"] = ["狸猫"]
    for section_name, values in overrides.items():
        config[section_name].update(values)
    fakehost.bind_context(plugin, ctx, config)
    return plugin, host, config


# ============================================================ 场景 1：探针链路

async def scenario_probe_roundtrip(plugin_module) -> None:
    section("场景 1：P1 探针 —— 入站注入 + 出站回派")
    plugin, host, _config = _build(plugin_module)
    await plugin.on_load()

    # 等探针跑完（probe_delay 之后注入；再留一点余量）
    await asyncio.sleep(0.4)

    state_calls = host.payloads_of("host.update_message_gateway_state")
    # P2 起 ready 跟随长连接状态：inbound 关闭 = 没有连接 = 不能上报就绪，
    # 否则宿主会把消息路由给一个实际收不到弹幕的网关
    check("inbound 关闭时不上报网关就绪", not state_calls,
          f"{len(state_calls)} 次 update_state")

    routes = host.payloads_of("host.route_message")
    check("探针调用了一次 route_message", len(routes) == 1, f"实际 {len(routes)} 次")

    if not routes:
        await plugin.on_unload()
        return

    payload = routes[0]
    check("route_message 使用正确的网关名", payload.get("gateway_name") == "bili_live")
    check("route_message 带 dedupe_key 防重", bool(payload.get("dedupe_key")))
    check("route_message 带 external_message_id", bool(payload.get("external_message_id")))

    msg = payload.get("message") or {}
    info = msg.get("message_info") or {}
    user = info.get("user_info") or {}
    group = info.get("group_info") or {}
    extra = info.get("additional_config") or {}

    check("报文 platform=bilibili", msg.get("platform") == "bilibili")
    check("报文 message_id 非空", bool(msg.get("message_id")))
    check("报文 user_id 非空", bool(user.get("user_id")))
    check("报文 user_nickname 非空", bool(user.get("user_nickname")))
    check("报文 group_id 非空（群聊语义）", bool(group.get("group_id")), str(group.get("group_id")))
    check("报文 group_name 非空", bool(group.get("group_name")), str(group.get("group_name")))
    check("group_id 即直播间号", group.get("group_id") == "2233")

    ts = str(msg.get("timestamp", ""))
    ts_is_float_str = False
    try:
        float(ts)
        ts_is_float_str = True
    except ValueError:
        pass
    check("timestamp 是 float 字符串", ts_is_float_str, ts)

    raw = msg.get("raw_message")
    check("raw_message 是文本段列表",
          isinstance(raw, list) and bool(raw) and raw[0].get("type") == "text",
          json.dumps(raw, ensure_ascii=False)[:80])
    check("探针弹幕判定为 @ 机器人", msg.get("is_at") is True)
    check("普通弹幕不是通知语义（会触发回复）", msg.get("is_notify") is False)
    check("additional_config 带 bili_room_id", extra.get("bili_room_id") == "2233")

    # ---- 出站：模拟 Host 把 MaiBot 的回复派发回网关
    outbound = await plugin.gateway_bili_live(
        {
            "raw_message": [
                {"type": "text", "data": "这首歌叫"},
                {"type": "image", "data": "base64..."},
                {"type": "text", "data": "《测试曲》"},
            ],
            "message_info": {"group_info": {"group_id": "2233"}},
        },
        route={"group_id": "2233"},
        metadata=None,
    )
    check("出站返回 success=True（dry-run）", outbound.get("success") is True)
    check("出站反解出 room_id", (plugin.last_outbound or {}).get("room_id") == "2233")
    check("图片段降级为可读占位符",
          (plugin.last_outbound or {}).get("text") == "这首歌叫[图片]《测试曲》",
          str((plugin.last_outbound or {}).get("text")))

    # 纯文本但无 raw_message 时回落到 processed_plain_text
    await plugin.gateway_bili_live(
        {"processed_plain_text": "只有纯文本", "message_info": {"group_info": {"group_id": "2233"}}},
        route=None, metadata=None,
    )
    check("无 raw_message 时回落到 processed_plain_text",
          (plugin.last_outbound or {}).get("text") == "只有纯文本")
    check("route 缺失时仍能从 group_info 反解 room_id",
          (plugin.last_outbound or {}).get("room_id") == "2233")

    await plugin.on_unload()

    state_calls = host.payloads_of("host.update_message_gateway_state")
    check("卸载过程不产生多余的状态上报", not state_calls,
          f"{len(state_calls)} 次 update_state")
    check("卸载后探针任务已清空", plugin._probe_task is None)


# ============================================================ 场景 5：自测与超时诊断

async def scenario_self_test_and_timeout(plugin_module) -> None:
    """自测出站不能冒充「Host 路由已通」，等不到回派时要给诊断而不是崩。

    真机教训（2026-09-13）：探针注入成功、accepted=True，但 Planner 判定
    「@的不是我」而拒绝回复 → 出站没被调用。此时必须能区分
    「handler 坏了」「LLM 没回」「路由没命中」三种情况。
    """
    section("场景 5：出站自测 + 等待回派超时的诊断")
    plugin, host, _config = _build(
        plugin_module,
        probe_delay=0.02,
        probe={"self_test_outbound": True, "outbound_wait_sec": 0.3},
    )
    await plugin.on_load()
    await asyncio.sleep(0.9)

    check("自测确实调用过出站 handler", (plugin.last_outbound or {}).get("from_host") is False)
    check("自测不计入 Host 出站计数（否则会骗过等待逻辑）",
          plugin._host_outbound_count == 0, f"计数={plugin._host_outbound_count}")
    check("等不到回派时探针正常结束（打印诊断而非抛错）",
          plugin._probe_task is not None and plugin._probe_task.done())

    await plugin.on_unload()
    check("超时诊断后 on_unload 仍正常", plugin._probe_task is None)

    # 场景 6：宿主真实派发时才计数
    section("场景 6：Host 真实派发才计入出站计数")
    plugin2, _host2, _cfg2 = _build(plugin_module, probe_delay=99.0)
    await plugin2.gateway_bili_live(
        {"raw_message": [{"type": "text", "data": "来自宿主"}],
         "message_info": {"group_info": {"group_id": "2233"}}},
        route={"group_id": "2233"}, metadata=None,
    )
    check("Host 派发会计数", plugin2._host_outbound_count == 1, f"计数={plugin2._host_outbound_count}")


# ============================================================ 场景 7：长连接事件流（桩）

class _FakeAuth:
    """替身 BiliHttpClient：不发任何网络请求。"""

    instances: list = []
    uid = 0
    send_fail_code: int | None = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        self.sent: list[tuple[int, str]] = []
        _FakeAuth.instances.append(self)

    async def resolve_room(self, room_id):
        return {"room_id": 2233, "anchor_uid": 99}

    async def anchor_name(self, uid):
        return "冒烟主播"

    async def login_uid(self):
        return _FakeAuth.uid

    async def close(self):
        self.closed = True

    async def send_danmaku(self, room_id, message):
        self.sent.append((room_id, message))
        if _FakeAuth.send_fail_code is not None:
            from blg_auth import BiliApiError
            raise BiliApiError(_FakeAuth.send_fail_code, "假失败")

    @property
    def buvid3(self):
        return "fake-buvid3"


class _FakeLiveClient:
    started = 0
    stopped = 0

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.connected = False  # /bili状态 会读这个字段

    def start(self):
        _FakeLiveClient.started += 1

    async def stop(self):
        _FakeLiveClient.stopped += 1


async def scenario_live_pipeline(plugin_module) -> None:
    """P2 事件流：房间解析、触发分级、自过滤、内容去重、卸载清理。"""
    section("场景 7：长连接事件流（桩掉网络层）")
    if not getattr(plugin_module, "LIVE_DEPS_OK", True):
        check("跳过：运行环境缺 websockets/brotli 依赖", True)
        return

    saved = (plugin_module.BiliHttpClient, plugin_module.LiveClient)
    plugin_module.BiliHttpClient, plugin_module.LiveClient = _FakeAuth, _FakeLiveClient
    _FakeLiveClient.started = _FakeLiveClient.stopped = 0
    _FakeAuth.instances = []
    try:
        plugin, host, _cfg = _build(
            plugin_module,
            probe_delay=99.0,
            inbound={"enabled": True},
            probe={"enabled": False},
        )
        await plugin.on_load()

        check("长连接客户端被创建并启动", _FakeLiveClient.started == 1)
        check("房间号解析进 resolved_room",
              plugin.resolved_room.get("room_id") == 2233
              and plugin.resolved_room.get("anchor_uid") == 99)
        check("group_name 优先用 room_display_name（未配置才取主播昵称）",
              plugin.resolved_room.get("anchor_name") == "冒烟直播间")

        # ready 跟随连接状态（P2 新语义），scope 带真实房间号
        await plugin._on_live_state(True, "测试连接")
        states = host.payloads_of("host.update_message_gateway_state")
        check("连接成功后上报 ready=True 且 scope 带真实房间号",
              any(c.get("ready") is True and c.get("scope") == "room:2233" for c in states))
        await plugin._on_live_state(False, "测试断开")
        states = host.payloads_of("host.update_message_gateway_state")
        check("断开后上报 ready=False（撤销路由）",
              any(c.get("ready") is False for c in states))
        # 回到连接态，后续事件才能注入
        await plugin._on_live_state(True, "测试重连")

        def routed():
            return host.payloads_of("host.route_message")

        # ① @ 弹幕 → 注入
        await plugin._on_live_event(synthetic_danmaku("@狸猫 在吗", uid="1", uname="甲", ts_ms=1700000000100))
        check("@ 弹幕注入宿主", len(routed()) == 1)

        # ② 普通弹幕（无 @ 无关键词）默认丢弃
        await plugin._on_live_event(synthetic_danmaku("随便聊聊", uid="2", uname="乙", ts_ms=1700000000200))
        check("无 @ 无关键词的普通弹幕默认丢弃", len(routed()) == 1)

        # ③ record_all_danmaku=True 时以通知语义入库
        plugin.config.inbound.record_all_danmaku = True
        await plugin._on_live_event(synthetic_danmaku("再聊聊", uid="2", uname="乙", ts_ms=1700000000300))
        last = (routed()[-1] or {}).get("message") or {}
        check("未触发弹幕按 record_all_danmaku 以通知语义入库",
              len(routed()) == 2 and last.get("is_notify") is True and last.get("is_at") is False)
        plugin.config.inbound.record_all_danmaku = False

        # ④ 关键词命中
        plugin.config.trigger.keywords = ["点歌"]
        await plugin._on_live_event(synthetic_danmaku("帮我点歌", uid="3", uname="丙", ts_ms=1700000000400))
        check("关键词命中的弹幕注入", len(routed()) == 3)
        plugin.config.trigger.keywords = []

        # ⑤ 礼物（高价值）必注入
        await plugin._on_live_event({"cmd": "SEND_GIFT",
                                     "data": {"uid": 4, "uname": "丁", "giftName": "辣条", "num": 1}})
        check("礼物事件必注入", len(routed()) == 4)

        # ⑥ 进场（通知类）按白名单注入；清空白名单后不注入
        enter = {"cmd": "INTERACT_WORD", "data": {"uid": 5, "uname": "戊", "msg_type": 1}}
        await plugin._on_live_event(enter)
        check("进场事件按默认白名单注入", len(routed()) == 5)
        plugin.config.inbound.record_notify_events = []
        await plugin._on_live_event(enter)
        check("通知类事件可被 record_notify_events 关闭", len(routed()) == 5)
        plugin.config.inbound.record_notify_events = ["enter", "follow", "share", "live_start", "live_end"]

        # ⑦ 自己发的弹幕被过滤（防 P3 真发后自问自答）
        plugin._bili_uid = 42
        await plugin._on_live_event(synthetic_danmaku("@狸猫 自问自答", uid="42", uname="鸣澜", ts_ms=1700000000500))
        check("自己发出的弹幕被过滤", len(routed()) == 5)

        # ⑧ 内容去重：同一事件（同房间|事件|用户|时间）不重复注入
        plugin._bili_uid = 0
        dup = synthetic_danmaku("@狸猫 重复消息", uid="6", uname="己", ts_ms=1700000000000)
        await plugin._on_live_event(dup)
        await plugin._on_live_event(dup)
        check("同一事件（内容键相同）不重复注入", len(routed()) == 6)

        # ⑨ 宿主侧 dedupe_key 也是内容键
        payload_keys = [p.get("dedupe_key") for p in routed()]
        check("dedupe_key 用内容键（房间|事件|用户|时间）",
              all(k and k.count("|") == 3 for k in payload_keys), str(payload_keys[-1]))

        await plugin.on_unload()
        check("卸载会停止长连接客户端", _FakeLiveClient.stopped == 1)
        check("卸载会关闭 HTTP 客户端", bool(_FakeAuth.instances)
              and all(a.closed for a in _FakeAuth.instances))
    finally:
        plugin_module.BiliHttpClient, plugin_module.LiveClient = saved


# ============================================================ 场景 8：真实出站（P3，桩）

async def scenario_real_outbound(plugin_module) -> None:
    """P3 真实发弹幕链路：dry_run=false 后出站 → DanmakuSender → auth.send_danmaku。

    全程桩掉网络，验证：发送调用、截断、限频软失败、错误码透传、房间号来源。
    """
    section("场景 8：真实出站链路（dry_run=false，桩掉网络）")
    if not getattr(plugin_module, "LIVE_DEPS_OK", True):
        check("跳过：运行环境缺 websockets/brotli 依赖", True)
        return

    saved = (plugin_module.BiliHttpClient, plugin_module.LiveClient, plugin_module.DanmakuSender)
    # DanmakuSender 用真实现（限流/截断逻辑要真实跑），只桩 auth 网络层
    plugin_module.BiliHttpClient, plugin_module.LiveClient = _FakeAuth, _FakeLiveClient
    _FakeLiveClient.started = _FakeLiveClient.stopped = 0
    _FakeAuth.instances = []
    _FakeAuth.send_fail_code = None
    try:
        # bucket_wait_sec=0：桶空立即失败，测试不用等回填。
        # inbound 开启：真实场景 sender 在 _start_live 预建（与长连接共用 auth）
        plugin, _host, _cfg = _build(
            plugin_module,
            probe_delay=99.0,
            inbound={"enabled": True},
            probe={"enabled": False, "outbound_dry_run": False},
            outbound={"enabled": True, "max_chars": 20, "bucket_capacity": 2,
                      "min_interval_sec": 60.0, "bucket_wait_sec": 0.0,
                      "fallback_suffix": "…"},
        )
        await plugin.on_load()
        room = plugin.resolved_room["room_id"]

        def sent():
            return [m for a in _FakeAuth.instances for m in a.sent]

        # ① 正常发送：文本与房间号
        result = await plugin.gateway_bili_live(
            {"raw_message": [{"type": "text", "data": "大家好呀"}],
             "message_info": {"group_info": {"group_id": str(room)}}},
            route={"platform": "bilibili", "scope": f"room:{room}"},
        )
        check("真实出站发送成功且文本原样", result.get("success") is True
              and sent() == [(room, "大家好呀")])

        # ② 超长截断（含后缀 ≤ 上限）
        await plugin.gateway_bili_live(
            {"raw_message": [{"type": "text", "data": "字" * 30}],
             "message_info": {"group_info": {"group_id": str(room)}}},
            route={"platform": "bilibili", "scope": f"room:{room}"},
        )
        check("超长弹幕被截断到 max_chars（含后缀）",
              len(sent()[-1][1]) == 20 and sent()[-1][1].endswith("…"))

        # ③ 桶容量 2 已用完 → 第三段软失败且不触网
        before = len(sent())
        result = await plugin.gateway_bili_live(
            {"raw_message": [{"type": "text", "data": "第三段"}],
             "message_info": {"group_info": {"group_id": str(room)}}},
            route={"platform": "bilibili", "scope": f"room:{room}"},
        )
        check("令牌桶耗尽后软失败不触网", result.get("success") is False
              and "限频" in (result.get("metadata") or {}).get("error", "")
              and len(sent()) == before)

        # ④ B 站错误码透传（-111 csrf 失效）。先重置令牌桶——③ 已把它耗尽，
        #    不重置的话失败原因会是「限频」而非错误码（真实坑：冒烟第一次跑就栽在这）
        plugin._sender._bucket = type(plugin._sender._bucket)(capacity=2, refill_interval=60.0)
        _FakeAuth.send_fail_code = -111
        result = await plugin.gateway_bili_live(
            {"raw_message": [{"type": "text", "data": "你好"}],
             "message_info": {"group_info": {"group_id": str(room)}}},
            route={"platform": "bilibili", "scope": f"room:{room}"},
        )
        check("B 站错误码透传并带人类可读解释",
              result.get("success") is False
              and (result.get("metadata") or {}).get("bili_code") == -111
              and "bili_jct" in (result.get("metadata") or {}).get("error", ""))
        _FakeAuth.send_fail_code = None

        # ⑤ outbound.enabled=false → 跳过发送
        plugin.config.outbound.enabled = False
        before = len(sent())
        result = await plugin.gateway_bili_live(
            {"raw_message": [{"type": "text", "data": "不该发"}],
             "message_info": {"group_info": {"group_id": str(room)}}},
            route={"platform": "bilibili", "scope": f"room:{room}"},
        )
        check("outbound.enabled=false 时跳过发送",
              result.get("success") is True and len(sent()) == before)
        plugin.config.outbound.enabled = True

        await plugin.on_unload()
        check("出站后卸载正常清理", _FakeLiveClient.stopped == 1
              and all(a.closed for a in _FakeAuth.instances))
    finally:
        plugin_module.BiliHttpClient, plugin_module.LiveClient, plugin_module.DanmakuSender = saved


# ============================================================ 场景 2：禁用

async def scenario_disabled(plugin_module) -> None:
    section("场景 2：plugin.enabled=false 时不应有任何副作用")
    plugin, host, _config = _build(plugin_module, enabled=False)
    await plugin.on_load()
    await asyncio.sleep(0.15)

    check("未上报网关状态", not host.payloads_of("host.update_message_gateway_state"))
    check("未注入任何消息", not host.payloads_of("host.route_message"))
    check("未启动探针任务", plugin._probe_task is None)

    # 禁用状态下卸载也不能报错
    await plugin.on_unload()
    check("禁用状态下 on_unload 正常返回", True)


# ============================================================ 场景 9：管理员命令

async def scenario_admin_commands(plugin_module) -> None:
    """P3 配套：/bili发 直接发弹幕（单号自测通道）与 /bili状态。"""
    section("场景 9：管理员命令（/bili发 /bili状态）")
    if not getattr(plugin_module, "LIVE_DEPS_OK", True):
        check("跳过：运行环境缺 websockets/brotli 依赖", True)
        return

    saved = (plugin_module.BiliHttpClient, plugin_module.LiveClient, plugin_module.DanmakuSender)
    plugin_module.BiliHttpClient, plugin_module.LiveClient = _FakeAuth, _FakeLiveClient
    _FakeLiveClient.started = _FakeLiveClient.stopped = 0
    _FakeAuth.instances = []
    _FakeAuth.send_fail_code = None
    try:
        plugin, _host, _cfg = _build(
            plugin_module,
            probe_delay=99.0,
            inbound={"enabled": True},
            probe={"enabled": False},
            admin={"admin_ids": ["12345"]},
        )
        await plugin.on_load()

        # 组件声明：命令元数据可直接从方法上取
        import re as _re
        info_send = plugin.cmd_bili_send.__maibot_component_info__
        info_status = plugin.cmd_bili_status.__maibot_component_info__
        check("命令 pattern 匹配 /bili发 文本",
              bool(_re.fullmatch(info_send.command_pattern, "/bili发 你好直播间")))
        check("命令 pattern 匹配 /bili状态", bool(_re.fullmatch(info_status.command_pattern, "/bili状态")))

        def sent():
            return [m for a in _FakeAuth.instances for m in a.sent]

        # 非管理员拒绝（无 is_local_operator、user_id 不在名单）
        result = await plugin.cmd_bili_send(
            stream_id="fake-stream", matched_groups={"text": "不该发"},
            user_id="999", message={})
        check("非管理员 /bili发 被拒绝且不触网",
              "管理员" in result[1] and not sent())

        # 管理员（user_id 在名单）发送成功
        result = await plugin.cmd_bili_send(
            stream_id="fake-stream", matched_groups={"text": "单号自测弹幕"},
            user_id="12345", message={})
        check("管理员 /bili发 发送成功", result[0] is True
              and sent() == [(2233, "单号自测弹幕")], str(sent()))

        # 本地控制台放行
        result = await plugin.cmd_bili_send(
            stream_id="fake-stream", matched_groups={"text": "本地控制台"},
            is_local_operator=True, message={})
        check("本地控制台触发 /bili发 放行",
              len(sent()) == 2 and sent()[-1] == (2233, "本地控制台"))

        # /bili状态：非管理员被拒（此前缺鉴权，全检发现）
        result = await plugin.cmd_bili_status(stream_id="fake-stream", message={})
        check("非管理员 /bili状态 被拒绝", "管理员" in result[1])

        # 管理员查询返回状态文本
        result = await plugin.cmd_bili_status(
            stream_id="fake-stream", user_id="12345", message={})
        check("管理员 /bili状态 返回状态文本",
              result[0] is True and "房间" in result[1])

        # B 站平台触发（uid 撞管理员 QQ 号）也必须被拒
        result = await plugin.cmd_bili_status(
            stream_id="fake-stream", user_id="12345",
            message={"platform": "bilibili"}, platform="bilibili")
        check("B 站平台撞号触发 /bili状态 被拒绝（平台校验）", "管理员" in result[1])

        # 鉴权工具本身
        check("_is_admin 兼容 qq: 前缀",
              plugin._normalize_admin_id("qq:12345") == "12345"
              and plugin._normalize_admin_id(" 12345 ") == "12345")

        await plugin.on_unload()
    finally:
        plugin_module.BiliHttpClient, plugin_module.LiveClient, plugin_module.DanmakuSender = saved


# ============================================================ 场景 10：黑名单（P4）

async def scenario_blocklist(plugin_module) -> None:
    """P4 黑名单：用户黑名单任何事件直接丢；关键词黑名单只拦普通弹幕。"""
    section("场景 10：黑名单（用户 / 关键词）")
    if not getattr(plugin_module, "LIVE_DEPS_OK", True):
        check("跳过：运行环境缺 websockets/brotli 依赖", True)
        return

    saved = (plugin_module.BiliHttpClient, plugin_module.LiveClient, plugin_module.DanmakuSender)
    plugin_module.BiliHttpClient, plugin_module.LiveClient = _FakeAuth, _FakeLiveClient
    _FakeLiveClient.started = _FakeLiveClient.stopped = 0
    _FakeAuth.instances = []
    _FakeAuth.send_fail_code = None
    try:
        plugin, host, _cfg = _build(
            plugin_module,
            probe_delay=99.0,
            inbound={"enabled": True},
            probe={"enabled": False},
            trigger={"at_names": ["狸猫"], "blocked_users": ["666"],
                     "blocked_keywords": ["广告"]},
        )
        await plugin.on_load()
        await plugin._on_live_state(True, "测试连接")

        def routed():
            return host.payloads_of("host.route_message")

        # ① 用户黑名单：普通弹幕丢
        await plugin._on_live_event(synthetic_danmaku("@狸猫 黑名单用户", uid="666", uname="坏人", ts_ms=1700000001000))
        check("用户黑名单的 @ 弹幕也被丢弃", len(routed()) == 0)

        # ② 用户黑名单：高价值事件（礼物）同样丢——黑名单先于一切触发判定
        await plugin._on_live_event({"cmd": "SEND_GIFT",
                                     "data": {"uid": 666, "uname": "坏人", "giftName": "辣条", "num": 1}})
        check("用户黑名单的礼物事件也丢弃", len(routed()) == 0)

        # ③ 正常用户 @ 弹幕放行
        await plugin._on_live_event(synthetic_danmaku("@狸猫 正常人", uid="7", uname="好", ts_ms=1700000001100))
        check("非黑名单用户正常注入", len(routed()) == 1)

        # ④ 关键词黑名单：普通弹幕命中即丢
        await plugin._on_live_event(synthetic_danmaku("牛牛广告牛牛", uid="8", uname="丙", ts_ms=1700000001200))
        check("关键词黑名单命中即丢", len(routed()) == 1)

        # ⑤ 关键词黑名单不拦通知类事件（进场事件不带可读文本）
        enter = {"cmd": "INTERACT_WORD", "data": {"uid": 8, "uname": "广告", "msg_type": 1}}
        await plugin._on_live_event(enter)
        check("关键词黑名单不拦通知类事件", len(routed()) == 2)

        # ⑥ @ 弹幕命中黑名单关键词：仍丢弃（黑名单先于触发判定）
        await plugin._on_live_event(synthetic_danmaku("@狸猫 发广告啦", uid="9", uname="丁", ts_ms=1700000001300))
        check("@ 弹幕命中关键词黑名单同样丢弃", len(routed()) == 2)

        # ⑦ 名单为空时零拦截（默认行为回归）
        plugin.config.trigger.blocked_users = []
        plugin.config.trigger.blocked_keywords = []
        await plugin._on_live_event(synthetic_danmaku("@狸猫 黑名单用户", uid="666", uname="坏人", ts_ms=1700000001400))
        check("清空黑名单后恢复放行", len(routed()) == 3)

        await plugin.on_unload()
    finally:
        plugin_module.BiliHttpClient, plugin_module.LiveClient, plugin_module.DanmakuSender = saved


# ============================================================ 场景 3：生命周期

async def scenario_lifecycle(plugin_module) -> None:
    section("场景 3：生命周期与配置热重载")
    plugin, _host, _config = _build(plugin_module)
    await plugin.on_load()

    for scope in ("self", "bot", "model"):
        try:
            await plugin.on_config_update(scope, {}, "1.0.0")
            check(f"on_config_update(scope={scope}) 不抛异常", True)
        except Exception as exc:
            check(f"on_config_update(scope={scope}) 不抛异常", False, repr(exc))

    await plugin.on_unload()
    # 重复卸载必须幂等（Host 可能重复调用）
    try:
        await plugin.on_unload()
        check("重复 on_unload 幂等", True)
    except Exception as exc:
        check("重复 on_unload 幂等", False, repr(exc))

    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    check("卸载后无遗留后台任务", not pending, f"{len(pending)} 个")


async def scenario_component_declaration(plugin_module) -> None:
    """校验 Runner 实际会收集到的组件声明。

    装饰器与 def 之间插入辅助方法时，组件会被静默注册到辅助方法上——
    这里用 SDK 的 collect_components 拿到 Runner 视角的真相，而不是靠静态推断。
    """
    section("场景 4：Runner 收集到的组件声明")
    plugin = plugin_module.create_plugin()
    gateways = [c for c in collect_components(plugin) if c.get("type") == "MESSAGE_GATEWAY"]

    check("恰好声明一个消息网关组件", len(gateways) == 1, f"{len(gateways)} 个")
    if not gateways:
        return

    gateway = gateways[0]
    meta = gateway.get("metadata") or {}
    check("网关名与 GATEWAY_NAME 常量一致",
          gateway.get("name") == plugin_module.GATEWAY_NAME, str(gateway.get("name")))
    check("route_type=duplex（收发双向）", meta.get("route_type") == "duplex")
    check("platform=bilibili", meta.get("platform") == "bilibili")
    check("handler 绑在 gateway_bili_live 上（未被辅助方法插队）",
          meta.get("handler_name") == "gateway_bili_live", str(meta.get("handler_name")))


async def main() -> int:
    # 插件日志默认走 stderr（lastResort），会让门禁摘要取到最后一行日志而不是结论。
    # 统一到 stdout，既保留插件真实日志又让收尾那行「冒烟全绿」落在最后。
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="  | %(message)s", force=True)

    module = fakehost.load_plugin_module(PLUGIN_DIR)
    check("插件入口 create_plugin() 可用", callable(getattr(module, "create_plugin", None)))

    await scenario_component_declaration(module)
    await scenario_probe_roundtrip(module)
    await scenario_self_test_and_timeout(module)
    await scenario_live_pipeline(module)
    await scenario_real_outbound(module)
    await scenario_admin_commands(module)
    await scenario_blocklist(module)
    await scenario_disabled(module)
    await scenario_lifecycle(module)

    failed = [name for name, ok, _ in _RESULTS if not ok]
    print("\n" + "=" * 62)
    print(f"冒烟结果：{len(_RESULTS) - len(failed)}/{len(_RESULTS)} 通过")
    if failed:
        print("失败项：")
        for name in failed:
            print(f"  - {name}")
        return 1
    print("冒烟全绿。")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
