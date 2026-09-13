"""B 站直播间弹幕长连接客户端（Web 端协议，不依赖 maibot_sdk）。

流程：`getDanmuInfo` 拿 token/host_list → 连 `wss://{host}:{wss_port}/sub`
→ 发 op=7 鉴权包（uid/roomid/protover=3/key/buvid）→ 等 op=8 → op=2 心跳保活
→ 循环 recv → `blg_proto` 解包（含 brotli 多包拆分）→ 事件回调。

生命周期约定：
- `start()` 幂等；`stop()` 必须 cancel 并 await 连接任务，卸载后不留协程残骸。
- 断线按指数退避重连（带 jitter，连稳 60s 后归零）；连接状态变化通过
  `on_state(connected, detail)` 上报——**插件要用它驱动网关 ready=True/False**，
  而不是像 P1 那样无条件上报就绪。
- 重连时重新取 token（旧 token 已随连接失效），uid 只取一次并缓存。
"""

import asyncio
import contextlib
import random
import ssl
import time
from typing import Any, Awaitable, Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

import blg_proto
from blg_auth import BiliApiError, BiliHttpClient

#: 单帧上限。brotli 解压前的原始帧通常不大，但礼物连击的压缩帧可能到几百 KB；
#: websockets 默认 1 MiB，放宽到 4 MiB 防止偶发大帧直接断连
WS_MAX_SIZE = 4 << 20
#: 鉴权回复等待时间
AUTH_TIMEOUT_SEC = 10.0
#: 连接稳定多久之后把重连退避归零
STABLE_RESET_SEC = 60.0
#: 应用层心跳包（op=2）的间隔下限。官方文档说 30s，但真机实测（2026-09-13）服务端
#: 疑似 ~20s 无数据就断开（与库层 ping timeout 同时出现，后者已禁用），取 15s 留足余量
MIN_HEARTBEAT_SEC = 15.0
#: 心跳间隔上限。超过它服务端会在两次心跳之间因无数据断连（真机实测）
HEARTBEAT_MAX_SEC = 20.0


class LiveClient:
    """B 站直播间弹幕长连接。"""

    def __init__(
        self,
        *,
        room_id: int,
        auth: BiliHttpClient,
        on_event: Callable[[dict], Awaitable[None]],
        on_state: Callable[[bool, str], Awaitable[None]],
        ws_heartbeat_sec: int = 15,
        ping_interval_sec: int = 20,  # 已废弃：库层 ping 必须禁用，参数仅为兼容保留
        reconnect_base_sec: float = 2.0,
        reconnect_max_sec: float = 60.0,
        logger: Any = None,
    ):
        self._room_id = int(room_id)
        self._auth = auth
        self._on_event = on_event
        self._on_state = on_state
        # 钳制到 [15, 20]：真机实测服务端 ~20s 无数据就断开。存量 config.toml 里
        # 可能还写着 30，改代码默认值救不了已生成的配置文件，所以在这里硬钳
        self._heartbeat_sec = min(max(MIN_HEARTBEAT_SEC, float(ws_heartbeat_sec)),
                                  HEARTBEAT_MAX_SEC)
        self._ping_interval = max(0.0, float(ping_interval_sec))  # 已废弃，不再使用
        self._base_delay = max(0.5, float(reconnect_base_sec))
        self._max_delay = max(self._base_delay, float(reconnect_max_sec))
        self._log = logger

        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._connected = False
        self._uid: Optional[int] = None

    # ---- 状态

    @property
    def connected(self) -> bool:
        return self._connected

    def _info(self, message: str, *args: Any) -> None:
        if self._log is not None:
            self._log.info("[bili-live][ws] " + message, *args)

    def _warn(self, message: str, *args: Any) -> None:
        if self._log is not None:
            self._log.warning("[bili-live][ws] " + message, *args)

    async def _report_state(self, connected: bool, detail: str) -> None:
        if self._connected == connected:
            return
        self._connected = connected
        try:
            await self._on_state(connected, detail)
        except Exception as exc:  # 状态回调异常不能拖垮连接循环
            self._warn("on_state 回调异常：%r", exc)

    # ---- 生命周期

    def start(self) -> None:
        """启动连接任务（幂等）。"""
        if self._task is not None and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="bili-live-ws")

    async def stop(self) -> None:
        """停止连接并等待任务退出。"""
        self._running = False
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    # ---- 主循环

    async def _run(self) -> None:
        attempt = 0
        try:
            while self._running:
                connected_at = time.monotonic()
                ok = await self._run_once()
                if not self._running:
                    break
                # 连稳一段时间后把退避归零；否则按次数指数增长
                if ok and time.monotonic() - connected_at > STABLE_RESET_SEC:
                    attempt = 0
                else:
                    attempt += 1
                delay = min(self._base_delay * (2 ** (attempt - 1)), self._max_delay)
                delay *= 0.8 + random.random() * 0.4  # jitter，避免多实例同步重连
                self._info("连接断开，%.1fs 后第 %d 次重连", delay, attempt)
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            self._info("连接任务被取消")
            raise
        finally:
            await self._report_state(False, "连接任务退出")

    async def _run_once(self) -> bool:
        """单次「取凭据 → 连接 → 鉴权 → 心跳 + 收包」，返回是否正常退出。"""
        ws = None
        heartbeat: Optional[asyncio.Task] = None
        try:
            info = await self._auth.danmu_info(self._room_id)
            if self._uid is None:
                self._uid = await self._auth.login_uid()
            url, token = self._pick_host(info["host_list"]), info["token"]
            self._info("连接 %s（room=%s uid=%s）", url, self._room_id, self._uid)

            ws = await websockets.connect(
                url,
                ssl=self._ssl_context(),
                # ★ 必须禁用库层 keepalive：B 站弹幕服务器不回 WebSocket 协议层
                #   pong 帧，开了 ping_interval 必然在 ~50s 时被
                #   "keepalive ping timeout" 踢掉（真机实录 2026-09-13：
                #   12:18:28 建连 → 12:19:18 断开，第二连同样 50s）。
                #   保活只靠应用层 op=2 心跳（见 _heartbeat_loop）。
                ping_interval=None,
                max_size=WS_MAX_SIZE,
                open_timeout=15.0,
            )
            await self._auth_ws(ws, token)
            self._info("鉴权成功，开始接收弹幕")
            await self._report_state(True, f"room={self._room_id}")

            heartbeat = asyncio.create_task(self._heartbeat_loop(ws))
            await self._recv_loop(ws)
            return True
        except BiliApiError as exc:
            self._warn("取弹幕凭据失败：%s", exc)
            await self._report_state(False, f"凭据失败: {exc.code}")
            if exc.code == -101:
                # 未登录重试没有意义（Cookie 不换结果不会变），停掉重连循环，
                # 等"填好 Cookie + 完整重启"再试。无限重试只会每分钟撞一次风控。
                self._warn("账号未登录（-101）：收弹幕必须配置登录 Cookie，停止重连。"
                           "填好 auth.cookie 后完整重启 MaiBot")
                self._running = False
            return False
        except ConnectionClosed as exc:
            self._warn("连接被服务端关闭：%s", exc)
            await self._report_state(False, "server closed")
            return False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._warn("连接异常：%s", exc)
            await self._report_state(False, f"error: {exc}")
            return False
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await heartbeat
            if ws is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await ws.close()

    @staticmethod
    def _pick_host(host_list: Any) -> str:
        """优先 wss_port，其次 ws_port（TLS 仍走 wss 端口语义，不降级明文）。"""
        for item in host_list:
            port = item.get("wss_port")
            if port:
                return f"wss://{item.get('host')}:{port}/sub"
        for item in host_list:
            if item.get("port"):
                return f"wss://{item.get('host')}:{item['port']}/sub"
        return "wss://broadcastlv.chat.bilibili.com/sub"

    @staticmethod
    def _ssl_context() -> ssl.SSLContext:
        """默认系统证书校验；代理 CA 的处理在 HTTP 层，wss 域名与 REST 不同源，
        需要 MITM 时优先让系统信任代理根证书，而不是在这里关校验。"""
        return ssl.create_default_context()

    async def _auth_ws(self, ws: Any, token: str) -> None:
        """发 op=7 鉴权包并等待 op=8 回复。失败抛异常触发重连。"""
        uid = self._uid or 0
        await ws.send(blg_proto.pack_auth(
            uid=uid, room_id=self._room_id, token=token,
            buvid=self._auth.buvid3, protover=3,
        ))
        deadline = time.monotonic() + AUTH_TIMEOUT_SEC
        while time.monotonic() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.monotonic())
            for pkt in blg_proto.unpack(raw):
                if pkt.op == blg_proto.OP_AUTH_REPLY:
                    code = blg_proto.parse_auth_reply(pkt)
                    if code in (None, 0):
                        return
                    raise ConnectionError(f"B 站鉴权被拒（code={code}）")
        raise ConnectionError("等待鉴权回复超时")

    async def _heartbeat_loop(self, ws: Any) -> None:
        """应用层心跳（op=2 空包）——这是唯一的保活手段（库层 ping 已禁用）。

        间隔取 15s：真机实测服务端疑似 ~20s 无数据就断开，30s 会踩线。
        """
        while True:
            await asyncio.sleep(self._heartbeat_sec)
            await ws.send(blg_proto.pack_heartbeat())
            self._info("心跳已发送")

    async def _recv_loop(self, ws: Any) -> None:
        """收包循环：解包（含 brotli 多包）→ 事件回调。"""
        while True:
            raw = await ws.recv()
            packets = blg_proto.unpack(raw)
            for pkt in packets:
                if pkt.op == blg_proto.OP_HEARTBEAT_REPLY:
                    popularity = blg_proto.parse_heartbeat_reply(pkt)
                    if popularity is not None:
                        self._info("人气值 %s", popularity)
                    continue
                async for event in self._events_of(pkt):
                    try:
                        await self._on_event(event)
                    except Exception as exc:  # 单条事件处理失败不影响整条流
                        self._warn("事件回调异常（cmd=%s）：%r",
                                   event.get("cmd"), exc)

    @staticmethod
    async def _events_of(pkt: blg_proto.Packet):
        """把单个包变成事件流。iter_events 是同步生成器，这里只是转成异步迭代，
        让 recv 循环保持统一的 async for 写法。"""
        for event in blg_proto.iter_events([pkt]):
            yield event
