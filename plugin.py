"""bilibili-live-gateway —— 把 B 站直播间接入 MaiBot（双工消息网关）。

架构（`@MessageGateway(route_type="duplex")`）：

    入站  B站弹幕/礼物/上舰/SC/进场 → blg_events.to_message() → MessageDict
          → ctx.gateway.route_message() → Host 去重后进 ChatBot.receive_message()
          → MaiBot 走完整人格 / 记忆 / 决策 / 频率控制链路

    出站  MaiBot 决定回复 → Host 反向 RPC 调本插件的网关方法（本文件下方的
          gateway_bili_live）→ 把消息段降级成纯文本 → POST api.live.bilibili.com/msg/send

插件本身**不调用** `ctx.send.*`（回复由 Host 反向派发），因此 manifest 里不需要
`send.text` 系列能力，只需要 `gateway.route_message` / `gateway.update_state`。

当前阶段 P1：**链路探针**
本阶段刻意不连 B 站 WebSocket、不做真实出站，只做一件事：验证宿主是否接纳
`platform="bilibili"` 这种非 QQ 平台入站，以及出站路由能否按 platform/account_id/scope
把回复派发回本网关。这是整个方案最大的未知点——若宿主拒绝，架构需重做，
所以必须先打通再投入协议实现（P2–P4）。

探针行为：on_load 后延迟若干秒，把一条 @ 机器人的**合成弹幕**经完整映射层注入 Host；
若 MaiBot 决定回复，出站处理器会被调用并打印 `[PROBE] 出站被调用`。两条日志都出现
即代表网关双工链路成立。
"""

import asyncio
import contextlib
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

from maibot_sdk import Command, Field, MaiBotPlugin, MessageGateway, PluginConfigBase

# 同目录兄弟模块按扁平名导入（与 bilibili-dynamic-push 一致）。
# 模块名统一加 blg_ 前缀，避免与其它插件（如 bilibili-dynamic-push 的
# buvid_activation.py）在同一 sys.path 上重名互相覆盖。
_PLUGIN_DIR = Path(__file__).resolve().parent
if str(_PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_DIR))

from blg_events import (  # noqa: E402
    HIGH_VALUE_KINDS,
    NOTIFY_KINDS,
    classify,
    normalize_cmd,
    normalize_text,
    synthetic_danmaku,
    to_message,
)

try:  # websockets/brotli 是 manifest 声明的依赖，但缺了也应优雅降级而不是拒载
    from blg_auth import BiliApiError, BiliHttpClient  # noqa: E402
    from blg_client import LiveClient  # noqa: E402
    from blg_outbound import DanmakuSender  # noqa: E402
    LIVE_DEPS_OK = True
    LIVE_DEPS_ERROR = ""
except ImportError as _exc:  # pragma: no cover
    BiliHttpClient = None  # type: ignore[assignment,misc]
    LiveClient = None  # type: ignore[assignment,misc]
    DanmakuSender = None  # type: ignore[assignment,misc]
    BiliApiError = Exception  # type: ignore[assignment,misc]
    LIVE_DEPS_OK = False
    LIVE_DEPS_ERROR = str(_exc)

#: 网关组件名，必须与 @MessageGateway(name=...) 及 update_state 首参一致
GATEWAY_NAME = "bili_live"
PLATFORM_NAME = "bilibili"

#: 出站日志里截断消息文本，避免刷屏
_LOG_TEXT_LIMIT = 120
#: 入站去重表容量上限（超过即全量清理过期项）
_DEDUP_MAX = 2000


# ============================================================ 配置模型

class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用 B 站直播间网关")
    # SDK 的 extract_plugin_config_version 强制要求此字段（缺失会抛
    # PluginConfigVersionError 拒绝加载），不是死代码。
    config_version: str = Field(default="0.1.0", description="配置版本")


class LiveSectionConfig(PluginConfigBase):
    """直播间配置。"""

    __ui_label__ = "直播间"
    __ui_icon__ = "video"
    __ui_order__ = 1

    room_id: str = Field(default="", description="直播间真实房间号（长号，不是短号）")
    room_display_name: str = Field(default="", description="直播间显示名，留空则用「直播间 <房间号>」")
    room_short_id: str = Field(default="", description="直播间短号，仅用于展示")


class AuthSectionConfig(PluginConfigBase):
    """凭证与网络。"""

    __ui_label__ = "凭证与网络"
    __ui_icon__ = "key"
    __ui_order__ = 2

    cookie: str = Field(default="", description="整行 Cookie，需含 SESSDATA（收）+ bili_jct（发弹幕）")
    verify_ssl: bool = Field(default=True, description="校验 TLS 证书；被中间人代理拦时改 false 或配 ca_bundle")
    ca_bundle: str = Field(default="", description="自定义根证书路径（优先于 verify_ssl=false），如 proxy-root-ca.cer")
    proxy: str = Field(default="", description="HTTP 代理，形如 http://127.0.0.1:7890，留空直连")


class InboundSectionConfig(PluginConfigBase):
    """入站（弹幕长连接）。P2 生效。"""

    __ui_label__ = "入站"
    __ui_icon__ = "download"
    __ui_order__ = 3

    enabled: bool = Field(default=True, description="是否建立弹幕长连接")
    record_all_danmaku: bool = Field(default=False, description="未被触发的普通弹幕也以通知语义入库（供 LLM 取上下文）")
    record_notify_events: list[str] = Field(
        default_factory=lambda: ["enter", "follow", "share", "live_start", "live_end"],
        description="要入库的通知类事件类别",
    )
    ws_heartbeat_sec: int = Field(default=15, description="应用层心跳间隔（秒）。真机实测服务端 ~20s 无数据就断开，别调大")
    ping_interval_sec: int = Field(default=20, description="已废弃：B 站弹幕服务器不回 WS 协议层 pong，库层 ping 必须禁用（保留字段仅为兼容旧配置）")
    reconnect_base_sec: float = Field(default=2.0, description="重连退避起始秒数")
    reconnect_max_sec: float = Field(default=60.0, description="重连退避上限秒数")
    dedupe_ttl_sec: int = Field(default=120, description="入站去重窗口（秒）")
    max_inbound_per_sec: int = Field(default=15, description="入站令牌桶速率，超限只丢普通弹幕，高价值事件恒放行")


class TriggerSectionConfig(PluginConfigBase):
    """分级触发策略。P4 生效，探针已用它的 at_names。"""

    __ui_label__ = "触发策略"
    __ui_icon__ = "filter"
    __ui_order__ = 4

    at_names: list[str] = Field(
        default_factory=lambda: ["鸣澜"],
        description=(
            "机器人在直播间的人格名/昵称，弹幕 @ 是纯文本、只能靠这个名单识别。"
            "必须与真机上的人格名一致（狸猫/鸣澜 等），否则 is_at 判错、消息进不了强制触发"
        ),
    )
    keywords: list[str] = Field(default_factory=list, description="命中即触发回复的关键词")
    keyword_is_regex: bool = Field(default=False, description="关键词按正则匹配")
    heat_window_sec: int = Field(default=10, description="同用户热度统计窗口（秒）")
    heat_max_per_user: int = Field(default=3, description="窗口内超过该条数只放行第一条")
    high_value_always: bool = Field(default=True, description="礼物/上舰/SC 必触发，绕过热度与限流")
    trigger_plain_danmaku: bool = Field(default=False, description="普通弹幕无需 @ 或关键词也进 LLM 决策")
    blocked_users: list[str] = Field(
        default_factory=list,
        description="用户 uid 黑名单：这些人的任何事件都直接丢弃（比 LLM 决策更早拦截，省 token）",
    )
    blocked_keywords: list[str] = Field(
        default_factory=list,
        description="关键词黑名单：弹幕命中即丢弃（正则同 keyword_is_regex）。黑名单先于一切触发判定",
    )


class OutboundSectionConfig(PluginConfigBase):
    """出站（发弹幕回直播间）。P3 生效。"""

    __ui_label__ = "出站"
    __ui_icon__ = "send"
    __ui_order__ = 5

    enabled: bool = Field(default=True, description="是否允许把 MaiBot 的回复发回直播间弹幕")
    max_chars: int = Field(default=20, description="弹幕长度上限（普通账号约 20 字，大航海更高，需实测）")
    min_interval_sec: float = Field(default=3.0, description="两条弹幕最小间隔（防错误码 10031）")
    bucket_capacity: int = Field(default=3, description="令牌桶容量")
    bucket_wait_sec: float = Field(default=2.0, description="桶空时最多等几秒再放弃本段（0=立即丢）")
    fallback_suffix: str = Field(default="…", description="被截断时追加的后缀")


class ProbeSectionConfig(PluginConfigBase):
    """P1 链路探针。默认关闭——P1 已真机验证通过，仅排查链路问题时临时开启。"""

    __ui_label__ = "链路探针"
    __ui_icon__ = "flask"
    __ui_order__ = 6

    enabled: bool = Field(default=False, description="启动后注入一条合成弹幕，验证宿主是否接纳 B 站平台入站（P1 调试用，默认关闭）")
    delay_sec: float = Field(default=8.0, description="启动后延迟几秒注入（给 MaiBot 留出初始化时间）")
    text: str = Field(default="", description="探针弹幕内容，留空则自动 @ 触发策略里的第一个昵称")
    outbound_dry_run: bool = Field(default=True, description="出站只打印不真发（P1 必须为 true，真实发文在 P3）")
    self_test_outbound: bool = Field(
        default=True,
        description="注入前先本地直调一次出站 handler。只证明 handler 可用，不代表 Host 路由已通",
    )
    outbound_wait_sec: float = Field(
        default=45.0,
        description="注入后等待出站回派的秒数；超时会打印诊断（含 bot 可能不回复的原因）。设 0 关闭等待",
    )


class AdminSectionConfig(PluginConfigBase):
    """管理与日志。"""

    __ui_label__ = "管理与日志"
    __ui_icon__ = "settings"
    __ui_order__ = 7

    admin_ids: list[str] = Field(default_factory=list, description="管理员 QQ 号，支持 'qq:123' 或 '123'；/bili发 与 /bili状态 仅管理员可用")
    log_level: str = Field(default="INFO", description="日志级别 DEBUG/INFO/WARNING/ERROR")


class BiliLiveGatewayConfig(PluginConfigBase):
    """B 站直播间网关配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    live: LiveSectionConfig = Field(default_factory=LiveSectionConfig)
    auth: AuthSectionConfig = Field(default_factory=AuthSectionConfig)
    inbound: InboundSectionConfig = Field(default_factory=InboundSectionConfig)
    trigger: TriggerSectionConfig = Field(default_factory=TriggerSectionConfig)
    outbound: OutboundSectionConfig = Field(default_factory=OutboundSectionConfig)
    probe: ProbeSectionConfig = Field(default_factory=ProbeSectionConfig)
    admin: AdminSectionConfig = Field(default_factory=AdminSectionConfig)


# ============================================================ 插件主体

class BiliLiveGatewayPlugin(MaiBotPlugin):
    """B 站直播间双工网关。"""

    config_model = BiliLiveGatewayConfig

    def __init__(self) -> None:
        """初始化运行时状态（不建立任何网络连接）。"""
        super().__init__()
        self._probe_task: asyncio.Task | None = None
        self._running = False
        self._gateway_ready = False
        self._event_seq = 0
        #: 宿主真实派发到本网关的出站次数（不含本地自测），用于判定出站路由是否打通
        self._host_outbound_count = 0
        #: 出站收到的最后一条消息载荷，供冒烟测试与真机排查断言
        self.last_outbound: dict[str, Any] | None = None
        #: 长连接客户端与 HTTP 客户端（P2）
        self._auth: Any = None
        self._live: Any = None
        #: 弹幕发送器（P3）：出站 dry_run=false 时才真正需要；懒建可让
        #  "只收不发"（无 bili_jct）的场景不受影响
        self._sender: Any = None
        self._bili_uid = 0
        #: 解析后的真实房间号与主播名
        self.resolved_room: dict[str, Any] = {}
        #: 入站去重表：message_id -> monotonic 时间
        self._seen: dict[str, float] = {}

    # -------------------------------------------------------- 生命周期

    async def on_load(self) -> None:
        """建立长连接并按配置启动链路探针。

        网关就绪不再无条件上报：ready 跟随连接状态（鉴权成功 True，断线 False），
        否则宿主会把消息路由给一个实际收不到弹幕的网关。
        """
        cfg = self.config
        if not cfg.plugin.enabled:
            self.ctx.logger.info("[bili-live] 插件未启用（plugin.enabled=false），跳过启动")
            return

        self._running = True
        self._apply_log_level(cfg.admin.log_level)
        self._log_startup_summary(cfg)

        if cfg.inbound.enabled:
            # 长连接失败（网络/风控/未配置）只降级为"不收弹幕"，绝不能炸掉 on_load——
            # 真机实录：login_uid 的 -101 未登录异常曾让插件整个注册失败
            try:
                await self._start_live()
            except Exception as exc:
                self.ctx.logger.error(
                    "[bili-live] 建立长连接失败（插件继续以降级模式运行，探针仍可用）：%s",
                    exc, exc_info=True,
                )
        else:
            self.ctx.logger.info("[bili-live] inbound.enabled=false，不建立长连接，也不上报网关就绪")

        if cfg.probe.enabled:
            self._start_probe()

    async def on_unload(self) -> None:
        """取消后台任务、断开长连接并撤销网关路由。"""
        self._running = False
        await self._stop_probe()
        await self._stop_live()
        if self._gateway_ready:
            with contextlib.suppress(Exception):
                await self.ctx.gateway.update_state(GATEWAY_NAME, ready=False)
            self._gateway_ready = False
            self.ctx.logger.info("[bili-live] 已撤销网关路由（ready=false）")
        self.ctx.logger.info("[bili-live] 插件已卸载")

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        """配置热重载：仅记录。改 manifest / capabilities 仍需完整重启 MaiBot。"""
        del config_data
        self.ctx.logger.info(
            "[bili-live] 配置已热重载 scope=%s version=%s（房间号/开关变更建议完整重启以重建长连接）",
            scope, version,
        )

    # -------------------------------------------------------- 双工网关

    # name 这里写字符串字面量而不是 GATEWAY_NAME：静态自检脚本只能识别字面量，
    # 用常量会让「装饰器与紧随的 def 是否配错」这项检查降级成人工确认。
    # 两处必须一致，由 tests/smoke_test.py 用 collect_components 自动断言兜住。
    @MessageGateway(
        "duplex",
        name="bili_live",
        platform=PLATFORM_NAME,
        protocol="bilibili-live-ws",
        description="B 站直播间弹幕双工网关：入站弹幕注入 MaiBot，出站回复发回直播间",
    )
    async def gateway_bili_live(
        self,
        message: dict[str, Any],
        route: Any = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """出站：Host 把 MaiBot 对该直播间的回复派发到这里。

        P1 只做 dry-run 打印并原样返回成功，用来证明「出站路由能命中本网关」。
        真实发弹幕在 P3 接上 outbound 模块。
        本地自测也走这个装饰器入口（顺带验证装饰器绑定与签名），但靠
        `bili_self_test` 标记区分，**不计入 Host 派发计数**。
        """
        del kwargs
        from_host = not (metadata or {}).get("bili_self_test")
        return await self._handle_outbound(message, route, metadata, from_host=from_host)

    async def _handle_outbound(
        self,
        message: dict[str, Any],
        route: Any,
        metadata: dict[str, Any] | None,
        *,
        from_host: bool,
    ) -> dict[str, Any]:
        """出站处理逻辑。`from_host=True` 表示这是宿主真实派发的回复。

        本地自测也走这里，但**不计数**——否则「等待出站回派」的探针会被自己的
        自测结果骗过去，把「handler 能跑」误读成「Host 路由已通」。
        """
        room_id = self._resolve_room_id(message, route)
        text = self._render_outbound_text(message)
        self.last_outbound = {
            "room_id": room_id,
            "text": text,
            "route": self._safe_repr(route),
            "metadata": metadata or {},
            "from_host": from_host,
        }

        if from_host:
            self._host_outbound_count += 1
            self.ctx.logger.info(
                "[PROBE] ★★★ 出站被调用 —— Host 已把回复派发回本网关（第 %d 次）★★★\n"
                "[PROBE]   room_id=%s\n"
                "[PROBE]   文本=%s\n"
                "[PROBE]   route=%s",
                self._host_outbound_count,
                room_id or "(未反解出)",
                text[:_LOG_TEXT_LIMIT] or "(空)",
                self._safe_repr(route),
            )
        else:
            self.ctx.logger.info(
                "[PROBE] 出站处理器本地自测通过：room_id=%s 文本=%s"
                "（仅证明 handler 可用，**不代表** Host 路由已通）",
                room_id or "(未反解出)", text[:_LOG_TEXT_LIMIT] or "(空)",
            )

        if self.config.probe.outbound_dry_run:
            self.ctx.logger.info("[PROBE] probe.outbound_dry_run=true，本条不真发到直播间")
            return {"success": True, "external_message_id": "probe-dryrun", "metadata": {"dry_run": True}}

        # ---- P3：真实发弹幕 ----
        if not self.config.outbound.enabled:
            self.ctx.logger.info("[bili-live] outbound.enabled=false，本条不发送")
            return {"success": True, "metadata": {"skipped": "outbound_disabled"}}

        if not room_id:
            self.ctx.logger.warning("[bili-live] 出站未反解出房间号，无法发送（route=%s）",
                                    self._safe_repr(route))
            return {"success": False, "external_message_id": "", "metadata": {"error": "no_room"}}

        sender = self._ensure_sender()
        if sender is None:
            # 缺依赖/无 HTTP 客户端且懒建失败（如 cookie 为空）
            return {"success": False, "external_message_id": "",
                    "metadata": {"error": "sender_unavailable"}}

        result = await sender.send(int(room_id), text)
        if result.get("success"):
            return {"success": True, "external_message_id": f"bili-{int(time.time())}",
                    "metadata": {"truncated": result.get("truncated", False)}}
        self.ctx.logger.warning("[bili-live] 弹幕发送失败（room=%s）：%s", room_id,
                                result.get("error"))
        return {"success": False, "external_message_id": "",
                "metadata": {"error": result.get("error"),
                             "bili_code": result.get("bili_code")}}

    def _ensure_sender(self) -> Any:
        """懒建弹幕发送器（依赖 outbound 配置与 HTTP 客户端）。

        懒建原因：长连接场景下 _auth 一定存在，但"只收不发"或 inbound 关闭时
        _auth 可能为 None——出站要用时再建，失败如实返回 None 而不是炸 handler。
        """
        if self._sender is not None:
            return self._sender
        if DanmakuSender is None:  # 缺依赖
            self.ctx.logger.error("[bili-live] 缺少依赖，无法发弹幕：%s", LIVE_DEPS_ERROR)
            return None
        auth = self._auth
        if auth is None:
            cookie = self.config.auth.cookie.strip()
            if not cookie:
                self.ctx.logger.error(
                    "[bili-live] auth.cookie 为空且长连接未建立，无法发弹幕；"
                    "Cookie 需含 bili_jct（发弹幕凭证）")
                return None
            self._auth = auth = BiliHttpClient(
                cookie=cookie,
                verify_ssl=self.config.auth.verify_ssl,
                ca_bundle=self.config.auth.ca_bundle,
                proxy=self.config.auth.proxy,
            )
        self._sender = DanmakuSender(
            auth,
            max_chars=self.config.outbound.max_chars,
            min_interval_sec=self.config.outbound.min_interval_sec,
            bucket_capacity=self.config.outbound.bucket_capacity,
            bucket_wait_sec=self.config.outbound.bucket_wait_sec,
            fallback_suffix=self.config.outbound.fallback_suffix,
            logger=self.ctx.logger,
        )
        return self._sender

    # -------------------------------------------------------- 管理员命令

    @staticmethod
    def _normalize_admin_id(entry: Any) -> str:
        """'qq:123' / '123' → '123'（小写比较）。"""
        s = str(entry or "").strip()
        if ":" in s:
            s = s.split(":", 1)[1].strip()
        return s.lower()

    def _is_admin(self, kwargs: dict[str, Any]) -> bool:
        """命令管理员鉴权（插件自管，参考 repeater-recall 实战）。

        本地控制台操作员天然放行；其余触发者须满足：
        1. 平台为 QQ（B 站弹幕消息带 platform="bilibili"，其 uid 可能与
           admin_ids 里的 QQ 号撞号，不校验平台等于给直播间观众提权）；
        2. 触发者 user_id 在 admin_ids 内。
        """
        if bool(kwargs.get("is_local_operator")):
            return True
        message = kwargs.get("message") if isinstance(kwargs.get("message"), dict) else {}
        platform = str(kwargs.get("platform") or message.get("platform") or "").strip().lower()
        if platform and platform != "qq":
            self.ctx.logger.warning(
                "[bili-live] 管理员命令被拒绝：触发平台为 %s（仅限 QQ 侧）", platform)
            return False
        admins = {self._normalize_admin_id(a) for a in (self.config.admin.admin_ids or [])}
        if not admins:
            return False
        user_id = str(kwargs.get("user_id") or "")
        if not user_id and isinstance(kwargs.get("message"), dict):
            info = kwargs["message"].get("message_info") or {}
            user_id = str((info.get("user_info") or {}).get("user_id") or "")
        return bool(user_id) and user_id.lower() in admins

    @Command(
        "bili_status",
        description="查看 B 站直播间网关状态（管理员）",
        pattern=r"^\s*[/／]\s*(?:bili状态|bili\s*status)\s*$",
    )
    async def cmd_bili_status(self, stream_id: str = "", **kwargs: Any) -> tuple[bool, str, int]:
        if not self._is_admin(kwargs):
            text = "该命令仅管理员可用"
            with contextlib.suppress(Exception):
                await self.ctx.send.text(text, stream_id)
            return True, text, 0
        if not stream_id:
            return True, "无法识别当前聊天（缺 stream_id）", 0
        cfg = self.config
        room = self.resolved_room or {}
        lines = [
            "B站直播间网关状态：",
            f"- 插件：{'启用' if cfg.plugin.enabled else '停用'}（v0.3.0）",
            f"- 房间：{room.get('room_id') or cfg.live.room_id or '(未解析)'}"
            f" 主播：{room.get('anchor_name') or '?'}",
            f"- 长连接：{'已连接' if self._live is not None and self._live.connected else '未连接'}"
            f" 网关ready：{'是' if self._gateway_ready else '否'}",
            f"- 登录身份：uid={self._bili_uid or '(游客，不能发弹幕)'}",
            f"- 出站：{'开' if cfg.outbound.enabled else '关'}"
            f"（{cfg.outbound.max_chars}字/条，间隔{cfg.outbound.min_interval_sec}s，"
            f"桶{cfg.outbound.bucket_capacity}）",
            f"- 令牌余量：{self._sender.tokens:.1f}/{cfg.outbound.bucket_capacity}"
            if self._sender is not None else "- 令牌桶：未就绪",
        ]
        text = "\n".join(lines)
        with contextlib.suppress(Exception):
            await self.ctx.send.text(text, stream_id)
        return True, text, 2

    @Command(
        "bili_send",
        description="直接往直播间发一条弹幕（管理员，绕过触发链路，用于单号自测）",
        pattern=r"^\s*[/／]\s*(?:bili发|bili\s*send)\s+(?P<text>.+?)\s*$",
    )
    async def cmd_bili_send(self, stream_id: str = "", matched_groups: dict | None = None,
                            **kwargs: Any) -> tuple[bool, str, int]:
        if not self._is_admin(kwargs):
            self.ctx.logger.warning("[bili-live] /bili发 被拒绝：触发者不在管理员列表（本地=%s）",
                                    bool(kwargs.get("is_local_operator")))
            text = "该命令仅管理员可用"
            with contextlib.suppress(Exception):
                await self.ctx.send.text(text, stream_id)
            return True, text, 0

        room = str(self.resolved_room.get("room_id") or "")
        if not room:
            text = "直播间房间号未解析，无法发送"
        else:
            body = str((matched_groups or {}).get("text") or "").strip()
            sender = self._ensure_sender()
            if not body or sender is None:
                text = "发送器未就绪（检查 outbound 配置与 auth.cookie）"
            else:
                result = await sender.send(int(room), body)
                if result.get("success"):
                    note = "（已截断）" if result.get("truncated") else ""
                    text = f"已发送到直播间 {room}{note}：{body}"
                else:
                    text = f"发送失败：{result.get('error')}"
        with contextlib.suppress(Exception):
            await self.ctx.send.text(text, stream_id)
        return True, text, 2

    # -------------------------------------------------------- 探针

    def _start_probe(self) -> None:
        """启动探针后台任务。"""
        if self._probe_task is not None and not self._probe_task.done():
            return
        self._probe_task = asyncio.create_task(self._run_probe())

    async def _stop_probe(self) -> None:
        """取消探针任务并等待其退出（卸载后不留协程残骸）。"""
        task = self._probe_task
        self._probe_task = None
        if task is None or task.done():
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    async def _run_probe(self) -> None:
        """P1 探针：注入一条合成弹幕，验证入站准入与出站回派。"""
        cfg = self.config
        delay = max(0.0, float(cfg.probe.delay_sec))
        self.ctx.logger.info(
            "[PROBE] ===== B站直播间网关 链路探针已排程：%.1fs 后动作 =====", delay,
        )
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            self.ctx.logger.info("[PROBE] 探针在等待期间被取消（插件卸载）")
            raise

        if not self._running:
            return

        room = str(cfg.live.room_id or "").strip()
        if not room:
            room = "0"
            self.ctx.logger.warning(
                "[PROBE] live.room_id 为空 —— 本次探针用占位群号 0（日志里出现「直播间 0」是正常的，"
                "出站反解也会拿到 0）。正式使用前必须填真实直播间长号。"
            )

        # ① 先本地直调一次出站 handler，把「handler 自身报错」这个可能性排除掉。
        #    这样后面「没有出站日志」就只剩「Planner 没回」和「路由没命中」两种解释。
        if cfg.probe.self_test_outbound:
            await self._self_test_outbound(room)

        # ② 注入合成弹幕
        text = cfg.probe.text.strip()
        if not text:
            target = (cfg.trigger.at_names or ["机器人"])[0]
            text = f"@{target} 你在吗？收到请回一句话"

        room_name = cfg.live.room_display_name or f"直播间 {room}"
        self._event_seq += 1
        event = synthetic_danmaku(text, uid="0", uname="链路探针", ts_ms=int(time.time() * 1000))
        message = to_message(
            event,
            room_id=room,
            room_name=room_name,
            account_id="bilibili",
            at_names=cfg.trigger.at_names,
            seq=self._event_seq,
        )
        if message is None:
            self.ctx.logger.error("[PROBE] 合成弹幕未通过映射层（to_message 返回 None），探针中止")
            return

        self.ctx.logger.info(
            "[PROBE] 注入合成弹幕 → message_id=%s is_at=%s group_id=%s",
            message["message_id"], message["is_at"], message["message_info"]["group_info"]["group_id"],
        )
        before = self._host_outbound_count
        try:
            accepted = await self.ctx.gateway.route_message(
                GATEWAY_NAME,
                message,
                route_metadata={"self_id": "bilibili", "room_id": room},
                external_message_id=message["message_id"],
                dedupe_key=message["message_id"],
            )
        except Exception as exc:  # 探针失败不能连累插件生命周期
            self.ctx.logger.error("[PROBE] route_message 抛异常：%s", exc, exc_info=True)
            return

        self.ctx.logger.info(
            "[PROBE] route_message 返回 accepted=%s → %s",
            accepted,
            "入站成立，宿主接纳了 platform=bilibili 的群聊消息" if accepted
            else "宿主拒绝了非 QQ 平台入站，架构需重做",
        )
        if not accepted:
            return

        # ③ 等宿主把回复派发回来
        wait = float(cfg.probe.outbound_wait_sec or 0.0)
        if wait > 0:
            await self._await_host_outbound(wait, before, room)

    async def _self_test_outbound(self, room: str) -> None:
        """本地直调出站 handler，排除「出站代码本身坏了」这一可能。"""
        probe_message = {
            "raw_message": [{"type": "text", "data": "[本地自测] 出站处理器连通"}],
            "message_info": {"group_info": {"group_id": room, "group_name": "自测"}},
        }
        try:
            result = await self.gateway_bili_live(
                probe_message,
                route={"group_id": room},
                metadata={"bili_self_test": True},
            )
        except Exception as exc:
            self.ctx.logger.error("[PROBE] 出站 handler 本地自测抛异常：%s", exc, exc_info=True)
            return
        self.ctx.logger.info("[PROBE] 出站 handler 本地自测返回 %s", result)

    async def _await_host_outbound(self, wait_sec: float, before: int, room: str) -> None:
        """等宿主把回复派发回来；超时则打印可操作的诊断。

        轮询间隔取「剩余时间」与 0.5s 的较小值——固定间隔会让实际等待
        明显超出 wait_sec（第一版用固定 1s，0.3s 的等待实际睡了 1s 才收尾）。
        """
        deadline = time.monotonic() + wait_sec
        while time.monotonic() < deadline:
            if self._host_outbound_count > before:
                self.ctx.logger.info(
                    "[PROBE] ★ 出站路由已打通：宿主把回复派发回本网关了。"
                    "P1 三项（入站准入 / 群聊建模 / 出站回派）全部通过，可以进入 P2。"
                )
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(0.5, remaining))
        self._log_outbound_timeout_diagnosis(room, wait_sec)

    def _log_outbound_timeout_diagnosis(self, room: str, wait_sec: float) -> None:
        """超时未收到出站回派时的排查清单（2026-09-13 真机实证版）。"""
        names = "、".join(self.config.trigger.at_names) or "(空)"
        self.ctx.logger.warning(
            "[PROBE] 等待 %.0fs 未收到出站回派。按可能性从高到低排查：\n"
            "[PROBE]  1) trigger.at_names 与真机人格名不一致（当前配置：%s）\n"
            "[PROBE]     弹幕里 @ 的是别人时，Planner 会判定「不是@我」而不回复。\n"
            "[PROBE]     取人格名：看主程序日志的「XXX 已成功唤醒」。\n"
            "[PROBE]  2) 人格设定偏安静，LLM 主动决定不回（日志能看到 Planner 结论文本）——\n"
            "[PROBE]     这仍是有效结论：入站链路正常，可把 probe.text 换成它感兴趣的话题再试。\n"
            "[PROBE]  3) Planner 已决定回复、却仍没有「出站被调用」——这才是真正的出站路由问题，\n"
            "[PROBE]     请把 planner 结论 + 本条日志一起回传。\n"
            "[PROBE]  群标识：直播间 %s（live.room_id 为空时显示 0）",
            wait_sec, names, room,
        )

    # -------------------------------------------------------- 长连接（P2）

    async def _start_live(self) -> None:
        """解析房间 → 建 HTTP 客户端 → 起长连接。任何一步失败都只降级，不拒载。"""
        cfg = self.config
        if not LIVE_DEPS_OK:
            self.ctx.logger.error(
                "[bili-live] 缺少依赖，无法建立长连接：%s（安装 websockets/Brotli/httpx 后完整重启）",
                LIVE_DEPS_ERROR,
            )
            return
        raw_room = str(cfg.live.room_id or "").strip()
        if not raw_room.isdigit():
            self.ctx.logger.error("[bili-live] live.room_id 未配置或不是数字，无法建立长连接")
            return
        if not cfg.auth.cookie.strip():
            # getDanmuInfo（2025-06-27 起）必须 WBI 签名 + SESSDATA + buvid3，
            # 没有 Cookie 连弹幕都收不到——早退并说清楚，而不是连上后反复撞风控
            self.ctx.logger.error(
                "[bili-live] auth.cookie 为空：收弹幕必须有登录 Cookie（整行，含 SESSDATA 与 buvid3）。"
                "浏览器登录 B 站后 F12 → Network → 任意请求 → 复制 Cookie 请求头整行填入 config.toml"
            )
            return

        self._auth = BiliHttpClient(
            cookie=cfg.auth.cookie,
            verify_ssl=cfg.auth.verify_ssl,
            ca_bundle=cfg.auth.ca_bundle,
            proxy=cfg.auth.proxy,
        )
        try:
            resolved = await self._auth.resolve_room(int(raw_room))
        except BiliApiError as exc:
            self.ctx.logger.error("[bili-live] 房间号解析失败：%s", exc)
            return
        room_id = int(resolved["room_id"])
        anchor_uid = int(resolved["anchor_uid"])
        try:
            anchor = (cfg.live.room_display_name.strip()
                      or await self._auth.anchor_name(anchor_uid)
                      or f"直播间 {room_id}")
        except Exception as exc:  # 主播名只是展示字段，拿不到就降级
            self.ctx.logger.warning("[bili-live] 取主播昵称失败（用兜底名）：%s", exc)
            anchor = cfg.live.room_display_name.strip() or f"直播间 {room_id}"
        self.resolved_room = {"room_id": room_id, "anchor_uid": anchor_uid, "anchor_name": anchor}
        self.ctx.logger.info(
            "[bili-live] 房间已解析：配置=%s → 真实=%s 主播=%s（uid=%s）",
            raw_room, room_id, anchor, anchor_uid or "?",
        )

        try:
            self._bili_uid = await self._auth.login_uid()
        except BiliApiError as exc:
            # 未登录已在 login_uid 内部降级为 0；走到这说明是别的风控/网络问题，
            # 同样不能炸 on_load——按游客继续，getDanmuInfo 那一步会给出真正的结论
            self.ctx.logger.warning("[bili-live] 获取登录 uid 失败，按游客继续：%s", exc)
            self._bili_uid = 0
        self.ctx.logger.info(
            "[bili-live] 登录身份 uid=%s%s",
            self._bili_uid, "" if self._bili_uid else "（游客：能收弹幕，不能发）",
        )

        # 发送器与长连接共用同一个 HTTP 客户端（cookie jar / buvid 指纹同源，
        # 别建第二套——两套指纹并发请求更容易触发风控）
        if cfg.outbound.enabled:
            self._sender = DanmakuSender(
                self._auth,
                max_chars=cfg.outbound.max_chars,
                min_interval_sec=cfg.outbound.min_interval_sec,
                bucket_capacity=cfg.outbound.bucket_capacity,
                bucket_wait_sec=cfg.outbound.bucket_wait_sec,
                fallback_suffix=cfg.outbound.fallback_suffix,
                logger=self.ctx.logger,
            )
            self.ctx.logger.info(
                "[bili-live] 出站发送器就绪：上限=%d 字 间隔=%.1fs 桶=%d",
                cfg.outbound.max_chars, cfg.outbound.min_interval_sec,
                cfg.outbound.bucket_capacity,
            )

        self._live = LiveClient(
            room_id=room_id,
            auth=self._auth,
            on_event=self._on_live_event,
            on_state=self._on_live_state,
            ws_heartbeat_sec=cfg.inbound.ws_heartbeat_sec,
            ping_interval_sec=cfg.inbound.ping_interval_sec,
            reconnect_base_sec=cfg.inbound.reconnect_base_sec,
            reconnect_max_sec=cfg.inbound.reconnect_max_sec,
            logger=self.ctx.logger,
        )
        self._live.start()

    async def _stop_live(self) -> None:
        client, self._live = self._live, None
        if client is not None:
            with contextlib.suppress(Exception):
                await client.stop()
        self._sender = None
        auth, self._auth = self._auth, None
        if auth is not None:
            with contextlib.suppress(Exception):
                await auth.close()

    async def _on_live_state(self, connected: bool, detail: str) -> None:
        """连接状态变化 → 网关 ready 跟随（宿主只给 ready 的网关路由消息）。"""
        await self._set_gateway_ready(
            connected,
            room_id=str(self.resolved_room.get("room_id") or self.config.live.room_id or ""),
            note=detail,
        )

    async def _on_live_event(self, event: dict[str, Any]) -> None:
        """B 站事件 → MessageDict → 宿主。"""
        kind = classify(normalize_cmd(event.get("cmd")))
        if not kind:
            return

        room = str(self.resolved_room.get("room_id") or "")
        self._event_seq += 1
        message = to_message(
            event,
            room_id=room,
            room_name=str(self.resolved_room.get("anchor_name") or ""),
            account_id="bilibili",
            at_names=self.config.trigger.at_names,
            seq=self._event_seq,
        )
        if message is None:
            return
        # 去重必须用内容键而不是 message_id：message_id 带 seq，每条都不同，
        # 断线重连后 B 站重放的同一事件会绕过去重被注入两次。
        dedupe_key = self._content_key(message)
        if self._is_duplicate(dedupe_key):
            return
        if self._is_self(message):
            self.ctx.logger.debug("[bili-live] 忽略自己发出的弹幕：%s", message["message_id"])
            return
        if not self._apply_trigger(kind, message):
            return

        try:
            accepted = await self.ctx.gateway.route_message(
                GATEWAY_NAME,
                message,
                route_metadata={"self_id": "bilibili", "room_id": room},
                external_message_id=message["message_id"],
                dedupe_key=dedupe_key,
            )
        except Exception as exc:
            self.ctx.logger.error("[bili-live] route_message 异常：%s", exc, exc_info=True)
            return
        if accepted:
            self.ctx.logger.info(
                "[bili-live] 已注入 %s：%s",
                kind, str(message.get("processed_plain_text") or "")[:_LOG_TEXT_LIMIT],
            )
        else:
            self.ctx.logger.warning(
                "[bili-live] 宿主拒收（accepted=False）：%s", message["message_id"],
            )

    # -------------------------------------------------------- 触发判定（P2 精简版）

    def _apply_trigger(self, kind: str, message: dict[str, Any]) -> bool:
        """决定一条事件要不要注入宿主。

        分级判定顺序（P4 全量）：
        1. 用户黑名单（任何事件直接丢，比 LLM 决策更早拦截省 token）
        2. 关键词黑名单（弹幕命中即丢；高价值/通知事件不带可读文本，不适用）
        3. 高价值必过（礼物/上舰/SC，绕过热度与限流）
        4. 通知按白名单
        5. 普通弹幕要 @ / 关键词，或全部入库语义
        """
        if self._is_blocked_user(message):
            self.ctx.logger.debug("[bili-live] 命中用户黑名单，丢弃：%s",
                                  (message.get("message_info") or {}).get("user_info", {}).get("user_id"))
            return False

        text = str(message.get("processed_plain_text") or "")
        if kind not in HIGH_VALUE_KINDS and kind not in NOTIFY_KINDS \
                and self._match_blocked_keywords(text):
            self.ctx.logger.debug("[bili-live] 命中关键词黑名单，丢弃弹幕：%s", text[:_LOG_TEXT_LIMIT])
            return False

        if kind in HIGH_VALUE_KINDS:
            return True

        if kind in NOTIFY_KINDS:
            return kind in set(self.config.inbound.record_notify_events)

        is_at = bool(message.get("is_at"))
        if is_at or self._match_keywords(text) or self.config.trigger.trigger_plain_danmaku:
            return True

        if self.config.inbound.record_all_danmaku:
            # 未触发的普通弹幕以通知语义入库：进记忆与上下文，但不触发回复
            message["is_notify"] = True
            message["is_at"] = False
            message["is_mentioned"] = False
            return True
        return False

    def _is_blocked_user(self, message: dict[str, Any]) -> bool:
        """用户 uid 黑名单。名单为空时零开销。"""
        blocked = [str(u).strip() for u in (self.config.trigger.blocked_users or []) if str(u).strip()]
        if not blocked:
            return False
        info = message.get("message_info") if isinstance(message.get("message_info"), dict) else {}
        user = info.get("user_info") if isinstance(info.get("user_info"), dict) else {}
        uid = str(user.get("user_id") or "")
        return bool(uid) and uid in blocked

    def _match_blocked_keywords(self, text: str) -> bool:
        """关键词黑名单命中判定，匹配语义与触发关键词一致（归一化/正则）。"""
        blocked = [k for k in (self.config.trigger.blocked_keywords or []) if str(k).strip()]
        if not blocked:
            return False
        if self.config.trigger.keyword_is_regex:
            return any(self._safe_search(str(k), text) for k in blocked)
        normalized = normalize_text(text)
        return any(normalize_text(str(k)) in normalized for k in blocked)

    def _match_keywords(self, text: str) -> bool:
        """关键词命中。非正则时做归一化（去零宽字符/全角空格）后的子串匹配。"""
        keywords = [k for k in (self.config.trigger.keywords or []) if str(k).strip()]
        if not keywords:
            return False
        if self.config.trigger.keyword_is_regex:
            return any(self._safe_search(str(k), text) for k in keywords)
        normalized = normalize_text(text)
        return any(normalize_text(str(k)) in normalized for k in keywords)

    @staticmethod
    def _safe_search(pattern: str, text: str) -> bool:
        """坏正则（用户配置）不能炸掉事件流。"""
        try:
            return bool(re.search(pattern, text))
        except re.error:
            return False

    @staticmethod
    def _content_key(message: dict[str, Any]) -> str:
        """内容去重键：房间|事件|用户|时间。同一事件的 message_id 每次都不同（带 seq），
        重连重放只能靠内容键识别。"""
        info = message.get("message_info") if isinstance(message.get("message_info"), dict) else {}
        extra = info.get("additional_config") if isinstance(info.get("additional_config"), dict) else {}
        user = info.get("user_info") if isinstance(info.get("user_info"), dict) else {}
        return "|".join(str(x) for x in (
            extra.get("bili_room_id"), extra.get("bili_event_cmd"),
            user.get("user_id"), message.get("timestamp"),
        ))

    def _is_duplicate(self, key: str) -> bool:
        """本地 TTL 去重。宿主侧还有 dedupe_key 兜底，这里是省 RPC。"""
        now = time.monotonic()
        ttl = max(5, int(self.config.inbound.dedupe_ttl_sec))
        if len(self._seen) > _DEDUP_MAX:
            self._seen = {k: t for k, t in self._seen.items() if now - t < ttl}
        last = self._seen.get(key)
        self._seen[key] = now
        return last is not None and now - last < ttl

    def _is_self(self, message: dict[str, Any]) -> bool:
        """过滤自己发出的弹幕（P3 真发后，弹幕会从流里绕回来，不过滤会自问自答）。"""
        own = str(self._bili_uid or "")
        if not own:
            return False
        info = message.get("message_info") if isinstance(message.get("message_info"), dict) else {}
        user = info.get("user_info") if isinstance(info.get("user_info"), dict) else {}
        return str(user.get("user_id") or "") == own

    # -------------------------------------------------------- 内部工具

    async def _set_gateway_ready(self, ready: bool, *, room_id: str, note: str = "") -> None:
        """上报网关运行时状态。只有 ready=True 的网关才参与消息路由。"""
        try:
            ok = await self.ctx.gateway.update_state(
                GATEWAY_NAME,
                ready=ready,
                platform=PLATFORM_NAME,
                account_id="bilibili",
                scope=f"room:{room_id}" if room_id else "",
                metadata={"room_id": room_id, "note": note},
            )
        except Exception as exc:
            self.ctx.logger.error("[bili-live] update_state 抛异常：%s", exc, exc_info=True)
            return
        self._gateway_ready = ready
        self.ctx.logger.info(
            "[bili-live] 上报网关状态 ready=%s platform=%s scope=room:%s → 宿主接受=%s（%s）",
            ready, PLATFORM_NAME, room_id or "-", ok, note or "无备注",
        )

    def _resolve_room_id(self, message: dict[str, Any], route: Any) -> str:
        """从出站报文里反解直播间号，四级兜底。"""
        # 1) Host 回传的 route（字段名随版本可能变化，逐个体检）
        for attr in ("group_id", "target_group_id", "stream_id", "scope"):
            value = getattr(route, attr, None) if not isinstance(route, dict) else route.get(attr)
            if value:
                return str(value).replace("room:", "")
        # 2) message_info.group_info.group_id（入站时我们写的就是房间号）
        info = message.get("message_info") if isinstance(message.get("message_info"), dict) else {}
        group = info.get("group_info") if isinstance(info.get("group_info"), dict) else {}
        if group.get("group_id"):
            return str(group["group_id"])
        # 3) additional_config.bili_room_id
        extra = info.get("additional_config") if isinstance(info.get("additional_config"), dict) else {}
        if extra.get("bili_room_id"):
            return str(extra["bili_room_id"])
        # 4) 配置兜底
        return str(self.config.live.room_id or "")

    @staticmethod
    def _render_outbound_text(message: dict[str, Any]) -> str:
        """把 Host 传来的消息段降级成纯文本（弹幕无法承载图片/语音/表情）。

        P3 会在此基础上加长度截断。文本段之外一律给可读占位符，
        而不是直接丢弃——否则机器人说了「图片」但日志里什么都看不到。
        """
        placeholders = {
            "image": "[图片]", "emoji": "[表情]", "voice": "[语音]",
            "file": "[文件]", "video": "[视频]", "music": "[音乐]",
        }
        segments = message.get("raw_message")
        if not isinstance(segments, list) or not segments:
            return str(message.get("processed_plain_text") or "").strip()

        parts: list[str] = []
        for seg in segments:
            if not isinstance(seg, dict):
                continue
            seg_type = str(seg.get("type") or "")
            if seg_type == "text":
                parts.append(str(seg.get("data") or ""))
            elif seg_type == "at":
                data = seg.get("data")
                name = data.get("target_user_nickname") if isinstance(data, dict) else None
                parts.append(f"@{name or ''}")
            elif seg_type in placeholders:
                parts.append(placeholders[seg_type])
        return "".join(parts).strip()

    @staticmethod
    def _safe_repr(value: Any, limit: int = 300) -> str:
        """把 route 之类的对象安全转成短字符串（可能是 dict/dataclass/None）。"""
        if value is None:
            return "None"
        try:
            if isinstance(value, (dict, list, str, int, float, bool)):
                text = json.dumps(value, ensure_ascii=False, default=str)
            else:
                text = repr(value)
        except Exception:
            text = f"<{type(value).__name__}>"
        return text[:limit]

    def _apply_log_level(self, level: str) -> None:
        """按配置调整插件 logger 级别（不触碰全局 logging 配置）。"""
        import logging

        normalized = str(level or "INFO").strip().upper()
        resolved = getattr(logging, normalized, None)
        if isinstance(resolved, int):
            self.ctx.logger.setLevel(resolved)

    def _log_startup_summary(self, cfg: BiliLiveGatewayConfig) -> None:
        """启动摘要：一眼看出关键开关与缺失项。"""
        warnings: list[str] = []
        if not cfg.live.room_id:
            warnings.append("live.room_id 为空（日志会显示「直播间 0」，正式使用前必须填真实长号）")
        if not cfg.trigger.at_names:
            warnings.append("trigger.at_names 为空 → 任何弹幕都不会被判为 @机器人，永远不触发")
        if not cfg.auth.cookie:
            warnings.append("auth.cookie 为空（P2 起收弹幕、P3 起发弹幕都需要）")
        elif "bili_jct" not in cfg.auth.cookie:
            warnings.append("auth.cookie 缺 bili_jct（只能收不能发，P3 出站会失败）")
        self.ctx.logger.info(
            "[bili-live] 启动摘要：房间=%s @名单=%s 入站=%s 出站=%s 探针=%s（dry_run=%s）长连接依赖=%s%s",
            cfg.live.room_id or "(未配置)",
            "、".join(cfg.trigger.at_names) or "(空)",
            "开" if cfg.inbound.enabled else "关",
            "开" if cfg.outbound.enabled else "关",
            "开" if cfg.probe.enabled else "关",
            cfg.probe.outbound_dry_run,
            "可用" if LIVE_DEPS_OK else f"缺失（{LIVE_DEPS_ERROR}）",
            ("；注意：" + "；".join(warnings)) if warnings else "",
        )
        if warnings:
            self.ctx.logger.warning("[bili-live] 配置待补：%s", "；".join(warnings))


def create_plugin() -> BiliLiveGatewayPlugin:
    """创建插件实例（Runner 与本地冒烟测试共用入口）。"""
    return BiliLiveGatewayPlugin()
