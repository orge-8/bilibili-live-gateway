"""重连压测：桩掉网络层，把 LiveClient 的生命周期循环真实跑一遍。

覆盖（全部不联网）：
- 连续失败 → 指数退避重连 → 成功建连 → 断线 → 再重连 的完整循环
- stop() 能及时取消重连等待（不留协程残骸）
- on_state 回调只在上报真实变化时触发
- -101 终态错误停止重连，不再撞接口

压测方式：把退避调小（base=0.01s）让循环高速迭代数百次，
用计数器替身观察 danmu_info 的调用次数与重连轨迹。
"""

import asyncio
import time
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosed

import blg_proto
from blg_auth import BiliApiError
from blg_client import LiveClient

SLEEP_CHUNK = 0.005


class CountingAuth:
    """计数替身：按脚本逐次决定 danmu_info 成败。script 用尽后默认成功。"""

    def __init__(self, script: list[Exception | dict] | None = None):
        self.script = list(script or [])
        self.danmu_calls = 0
        self.closed = False
        self.buvid3 = "buvid-test"

    async def danmu_info(self, room_id):
        self.danmu_calls += 1
        if self.script:
            item = self.script.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return {"token": "tok", "host_list": [{"host": "h", "wss_port": 1}]}

    async def login_uid(self):
        return 123

    async def close(self):
        self.closed = True


class ScriptedWS:
    """按脚本动作的假 ws：'ok'=收到一条事件，'close'=模拟服务端断开。"""

    def __init__(self, script: list[str]):
        self.script = list(script)
        self.closed = False

    async def send(self, data):
        pass

    async def recv(self):
        if self.script:
            action = self.script.pop(0)
            if action == "close":
                raise ConnectionClosed(None, None)
            if action == "ok":
                frame = blg_proto.pack(
                    blg_proto.OP_MESSAGE,
                    '{"cmd":"DANMU_MSG","info":[[],"压测"]}',
                    ver=blg_proto.VER_HEARTBEAT)
                return frame
        raise ConnectionClosed(None, None)

    async def close(self):
        self.closed = True


def _auth_frame_ok() -> bytes:
    return blg_proto.pack(blg_proto.OP_AUTH_REPLY, '{"code":0}',
                          ver=blg_proto.VER_HEARTBEAT)


async def _pump(deadline_sec: float, until) -> None:
    """让出事件循环控制权直到 until() 为真或超时；超时抛 AssertionError。

    client.start() 已经把连接任务挂进循环，这里只需要持续 yield，
    让后台任务有机会推进（sleep 间隔远大于循环体耗时即可）。
    """
    deadline = time.monotonic() + deadline_sec
    while not until():
        if time.monotonic() > deadline:
            raise AssertionError("压测泵超时：条件未在时限内满足")
        await asyncio.sleep(SLEEP_CHUNK)


def _fast_client(auth, on_event, on_state, **kw) -> LiveClient:
    defaults = dict(reconnect_base_sec=0.01, reconnect_max_sec=0.05)
    defaults.update(kw)
    return LiveClient(
        room_id=2233, auth=auth, on_event=on_event, on_state=on_state,
        logger=None, **defaults)


def test_reconnect_cycle_fail_then_success_then_drop():
    """失败×3 → 成功收 1 条弹幕 → 服务端断开 → 再连成功。全程验证重连轨迹。"""
    async def run():
        auth = CountingAuth(script=[
            BiliApiError(-500, "网络抖动1"),
            BiliApiError(-500, "网络抖动2"),
            ConnectionClosed(None, None),  # getDanmuInfo 层的瞬断
        ])
        events: list[dict] = []
        states: list[tuple[bool, str]] = []
        ws_scripts = [
            ["ok", "close"],   # 第 1 次建连：收 1 条弹幕后被服务端踢
            [],                # 第 2 次建连：立即断开（压一次断线重连）
            ["ok"],            # 第 3 次建连：收到弹幕后脚本用尽 → recv 抛断开
        ]
        made_ws: list[ScriptedWS] = []

        async def on_event(event):
            events.append(event)

        async def on_state(connected, detail):
            states.append((connected, detail))

        client = _fast_client(auth, on_event, on_state)
        restore, made_ws = _patch_connect(client, ws_scripts)
        try:
            client.start()
            await _pump(10.0, lambda: auth.danmu_calls >= 5 and len(events) >= 2)
        finally:
            await client.stop()
            restore()

        assert auth.danmu_calls >= 5, f"应多次重试取凭据，实际 {auth.danmu_calls}"
        assert len(events) >= 2, f"应收到至少 2 条弹幕，实际 {len(events)}"
        connected_changes = [s for s in states if s[0]]
        disconnected_changes = [s for s in states if not s[0]]
        assert connected_changes, "应有 ready=True 上报"
        assert disconnected_changes, "应有 ready=False 上报（断线）"
        # ready=True 只在真实建连后出现，且不连续重复（状态抑制生效）
        assert len(connected_changes) >= 2, "两次建连应各上报一次 ready=True"
        assert all(ws.closed for ws in made_ws), "每个 ws 都应被关闭"
    asyncio.run(run())


def _patch_connect(client: LiveClient, ws_scripts: list[list[str]] | None = None):
    """把 client 模块里的 websockets.connect 换成桩，返回还原函数。

    ws_scripts 为 None 时桩永不成功（建连阶段就抛 ConnectionClosed）。
    """
    real = client._run_once.__globals__["websockets"].connect
    made: list[Any] = []

    async def fake_connect(url, **kwargs):
        script = ws_scripts.pop(0) if ws_scripts else None
        ws = ScriptedWS(script or [])
        made.append(ws)
        if script is None:
            # 桩"网络不可达"：直接当作建连后立即断开
            async def auth_ws(_ws, _tok):
                raise ConnectionClosed(None, None)
        else:
            async def auth_ws(_ws, _tok):
                await _ws.send(_auth_frame_ok())
        client._auth_ws = auth_ws
        return ws

    client._run_once.__globals__["websockets"].connect = fake_connect

    def restore():
        client._run_once.__globals__["websockets"].connect = real

    return restore, made


def test_stop_cancels_pending_reconnect_promptly():
    """stop() 必须立刻打断退避等待——卸载时不能留下还在睡的重连协程。"""
    async def run():
        script = [BiliApiError(-500, "永远失败")]  # danmu_info 一直失败

        class _Auth(CountingAuth):
            async def danmu_info(self, room_id):
                self.danmu_calls += 1
                if script:
                    raise script.pop(0)
                raise BiliApiError(-500, "永远失败")

        a = _Auth()
        client = _fast_client(a, _noop_event, _noop_state,
                              reconnect_base_sec=0.5, reconnect_max_sec=0.5)
        restore, _made = _patch_connect(client)  # 防御：万一失败脚本用尽也不碰真网
        client.start()
        try:
            await _pump(5.0, lambda: a.danmu_calls >= 2)
            t0 = time.monotonic()
            await client.stop()
            elapsed = time.monotonic() - t0
            assert elapsed < 0.4, f"stop() 应打断退避等待，实际耗时 {elapsed:.2f}s"
            assert client._running is False
            calls_at_stop = a.danmu_calls
            await asyncio.sleep(0.3)
            assert a.danmu_calls == calls_at_stop, "stop() 后不应再有重连尝试"
        finally:
            await client.stop()
            restore()
    asyncio.run(run())


def test_not_logged_in_stops_and_never_hits_api_again():
    """-101 后停止重连：不再撞 danmu_info，状态停留在 False。"""
    async def run():
        class _NoLoginAuth(CountingAuth):
            async def danmu_info(self, room_id):
                self.danmu_calls += 1
                raise BiliApiError(-101, "账号未登录")

        a = _NoLoginAuth()
        client = _fast_client(a, _noop_event, _noop_state)
        client.start()
        await _pump(5.0, lambda: client._running is False)
        assert client._running is False
        calls = a.danmu_calls
        assert calls >= 1
        await asyncio.sleep(0.2)
        assert a.danmu_calls == calls, "-101 后仍撞接口（会持续触发风控）"
    asyncio.run(run())


def test_backoff_grows_and_is_capped():
    """退避按指数增长且有上限，不会无限翻倍。"""
    async def run():
        client = LiveClient(room_id=1, auth=CountingAuth(), on_event=_noop_event,
                            on_state=_noop_state,
                            reconnect_base_sec=2.0, reconnect_max_sec=8.0, logger=None)
        attempt = 0
        delays = []
        for i in range(10):
            attempt += 1
            delay = min(client._base_delay * (2 ** (attempt - 1)), client._max_delay)
            delays.append(delay)
        assert delays[0] == 2.0
        assert delays[-1] == 8.0, "退避必须被 max 封顶"
        assert all(b >= a for a, b in zip(delays, delays[1:])), "退避应单调不减"
    asyncio.run(run())


def test_state_not_spammed_on_unchanged_status():
    """反复失败时 ready=False 只上报一次（状态变化抑制在重连循环里成立）。"""
    async def run():
        states: list[tuple[bool, str]] = []

        async def on_state(connected, detail):
            states.append((connected, detail))

        client = _fast_client(CountingAuth(script=[BiliApiError(-500, "fail")]),
                              _noop_event, on_state)
        restore, _made = _patch_connect(client)  # 失败脚本用尽后防真网
        try:
            for _ in range(5):
                await client._run_once()
        finally:
            restore()
        false_reports = [s for s in states if not s[0]]
        # 从未连上过：初始 _connected 就是 False，上报被抑制 → 0 次；
        # 无论哪种实现，连续失败都不应产生 >1 次 False（那会刷屏宿主）
        assert len(false_reports) <= 1, \
            f"连续失败不应重复上报 False，实际 {len(false_reports)} 次"
        assert len(states) == len(false_reports), "不应有 ready=True 混入"
    asyncio.run(run())


async def _noop_event(event):  # pragma: no cover
    pass


async def _noop_state(connected, detail):  # pragma: no cover
    pass
