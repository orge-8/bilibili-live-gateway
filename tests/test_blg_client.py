"""blg_client 单测：host 选择、鉴权应答判定、收包循环（全部用假 ws，不联网）。"""

import asyncio
import json

import pytest
from websockets.exceptions import ConnectionClosed

import blg_proto
from blg_auth import BiliApiError
from blg_client import LiveClient


class FakeAuth:
    """替身 BiliHttpClient：只提供鉴权包需要的字段。"""

    def __init__(self, buvid: str = "buvid-x"):
        self._buvid = buvid

    @property
    def buvid3(self) -> str:
        return self._buvid

    async def danmu_info(self, room_id):  # pragma: no cover - 测试里不走到
        return {"token": "tok", "host_list": []}

    async def login_uid(self):  # pragma: no cover
        return 0

    async def close(self):  # pragma: no cover
        pass


class FakeWS:
    """假 websocket：按顺序回放帧，记录所有 send。"""

    def __init__(self, frames: list[bytes]):
        self.frames = list(frames)
        self.sent: list[bytes] = []
        self.closed = False

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def recv(self) -> bytes:
        if self.frames:
            return self.frames.pop(0)
        raise ConnectionClosed(None, None)

    async def close(self) -> None:
        self.closed = True


def _make_client(**kwargs) -> LiveClient:
    events: list[dict] = []
    states: list[tuple[bool, str]] = []

    async def on_event(event):
        events.append(event)

    async def on_state(connected, detail):
        states.append((connected, detail))

    client = LiveClient(
        room_id=2233,
        auth=FakeAuth(),
        on_event=on_event,
        on_state=on_state,
        logger=None,
        **kwargs,
    )
    return client


def _frame(obj: dict) -> bytes:
    return blg_proto.pack(blg_proto.OP_MESSAGE, json.dumps(obj, ensure_ascii=False),
                          ver=blg_proto.VER_HEARTBEAT)


def _auth_frame(code: int) -> bytes:
    """鉴权回复必须用 op=8——parse_auth_reply 只认这个 op。"""
    return blg_proto.pack(blg_proto.OP_AUTH_REPLY, json.dumps({"code": code}),
                          ver=blg_proto.VER_HEARTBEAT)


# ---------------------------------------------------------------- host 选择

def test_pick_host_prefers_wss_port():
    hosts = [{"host": "a.com", "ws_port": 2244, "wss_port": 2245}]
    assert LiveClient._pick_host(hosts) == "wss://a.com:2245/sub"


def test_pick_host_falls_back_to_port():
    hosts = [{"host": "b.com", "port": 2243}]
    assert LiveClient._pick_host(hosts) == "wss://b.com:2243/sub"


def test_pick_host_ultimate_fallback():
    assert LiveClient._pick_host([]) == "wss://broadcastlv.chat.bilibili.com/sub"
    assert LiveClient._pick_host([{"host": "c.com"}]) == \
        "wss://broadcastlv.chat.bilibili.com/sub"


def test_pick_host_skips_zero_port():
    """wss_port=0 的节点不可用，要跳到下一个。"""
    hosts = [{"host": "bad.com", "wss_port": 0, "ws_port": 0},
             {"host": "good.com", "wss_port": 2245}]
    assert LiveClient._pick_host(hosts) == "wss://good.com:2245/sub"


# ---------------------------------------------------------------- 鉴权

def test_auth_ws_sends_auth_packet_and_accepts_code_zero():
    async def run():
        client = _make_client()
        client._uid = 123
        ws = FakeWS([_auth_frame(0)])
        await client._auth_ws(ws, "tok")
        assert len(ws.sent) == 1
        body = json.loads(blg_proto.unpack(ws.sent[0])[0].body)
        assert body["uid"] == 123
        assert body["roomid"] == 2233
        assert body["key"] == "tok"
        assert body["buvid"] == "buvid-x"
        assert body["protover"] == 3

    asyncio.run(run())


def test_auth_ws_rejects_nonzero_code():
    async def run():
        client = _make_client()
        ws = FakeWS([_auth_frame(-1)])
        with pytest.raises(ConnectionError, match="鉴权被拒"):
            await client._auth_ws(ws, "tok")

    asyncio.run(run())


def test_auth_ws_propagates_closed_connection():
    async def run():
        client = _make_client()
        ws = FakeWS([])  # recv 立刻抛 ConnectionClosed（服务端直接断开）
        with pytest.raises(ConnectionClosed):
            await client._auth_ws(ws, "tok")

    asyncio.run(run())


# ---------------------------------------------------------------- 收包循环

def test_recv_loop_delivers_events_and_skips_heartbeat():
    async def run():
        events: list[dict] = []

        async def on_event(event):
            events.append(event)

        async def on_state(connected, detail):
            pass

        client = LiveClient(room_id=2233, auth=FakeAuth(), on_event=on_event,
                            on_state=on_state, logger=None)
        frames = [
            blg_proto.pack(blg_proto.OP_HEARTBEAT_REPLY,
                           (12345).to_bytes(4, "big"), ver=blg_proto.VER_HEARTBEAT),
            _frame({"cmd": "DANMU_MSG", "info": [[0, 25, 16777215, 1, 2], "你好"]}),
            # 合法协议包但正文不是 JSON：不能中断整条流
            blg_proto.pack(blg_proto.OP_MESSAGE, "garbage-not-json",
                           ver=blg_proto.VER_HEARTBEAT),
        ]
        ws = FakeWS(frames)
        with pytest.raises(ConnectionClosed):
            await client._recv_loop(ws)
        assert len(events) == 1
        assert events[0]["cmd"] == "DANMU_MSG"

    asyncio.run(run())


def test_recv_loop_event_callback_exception_does_not_kill_stream():
    """单条事件处理炸掉不能中断收包——否则一条坏弹幕就打死整个连接。"""
    async def run():
        calls = {"n": 0}

        async def on_event(event):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("故意的")

        async def on_state(connected, detail):
            pass

        client = LiveClient(room_id=2233, auth=FakeAuth(), on_event=on_event,
                            on_state=on_state, logger=None)
        frames = [_frame({"cmd": "DANMU_MSG", "info": [[], "第一条"]}),
                  _frame({"cmd": "DANMU_MSG", "info": [[], "第二条"]})]
        with pytest.raises(ConnectionClosed):
            await client._recv_loop(FakeWS(frames))
        assert calls["n"] == 2

    asyncio.run(run())


# ---------------------------------------------------------------- 生命周期

def test_stop_is_idempotent_without_start():
    async def run():
        client = _make_client()
        await client.stop()  # 未 start 也能安全调用
        assert client.connected is False

    asyncio.run(run())


class _NotLoggedInAuth:
    """替身：getDanmuInfo 直接抛 -101（cookie 为空的真机场景）。"""

    buvid3 = ""

    async def danmu_info(self, room_id):
        raise BiliApiError(-101, "账号未登录")

    async def login_uid(self):
        return 0

    async def close(self):
        pass


def test_run_once_stops_retrying_when_not_logged_in():
    """-101 重试没有意义（Cookie 不换结果不会变），必须停掉重连循环而不是每分钟撞风控。"""
    async def run():
        states: list[tuple[bool, str]] = []

        async def on_state(connected, detail):
            states.append((connected, detail))

        client = LiveClient(room_id=1, auth=_NotLoggedInAuth(), on_event=_noop,
                            on_state=on_state, logger=None)
        client._running = True
        ok = await client._run_once()
        assert ok is False
        assert client._running is False, "未登录时应停止重连"
        # 注意：-101 时 _connected 本来就是 False，_report_state 对未变化状态
        # 是 no-op（设计如此），所以 states 为空是正确行为，不能断言它有值

    asyncio.run(run())


def test_run_once_keeps_retrying_on_transient_errors():
    """非 -101 的失败（如网络抖动、临时风控）仍应保持重连意愿。"""

    class _FlakyAuth(_NotLoggedInAuth):
        async def danmu_info(self, room_id):
            raise BiliApiError(-500, "系统繁忙")

    async def run():
        client = LiveClient(room_id=1, auth=_FlakyAuth(), on_event=_noop,
                            on_state=_noop_state, logger=None)
        client._running = True
        ok = await client._run_once()
        assert ok is False
        assert client._running is True, "可重试错误不应停掉重连循环"

    asyncio.run(run())


def test_heartbeat_interval_is_clamped():
    """真机实测服务端 ~20s 无数据就断连：间隔必须钳在 [15, 20]。

    尤其要防存量 config.toml 里写死的 30——改代码默认值对已生成的配置文件无效。
    """
    assert _make_client(ws_heartbeat_sec=30)._heartbeat_sec == 20.0
    assert _make_client(ws_heartbeat_sec=15)._heartbeat_sec == 15.0
    assert _make_client(ws_heartbeat_sec=18)._heartbeat_sec == 18.0
    assert _make_client(ws_heartbeat_sec=5)._heartbeat_sec == 15.0


def test_state_callback_only_fires_on_change():
    """重复上报同一状态要被抑制——否则每次心跳回复都会打一次 update_state。"""
    async def run():
        calls: list[tuple[bool, str]] = []

        async def on_state(connected, detail):
            calls.append((connected, detail))

        client = LiveClient(room_id=2233, auth=FakeAuth(), on_event=_noop,
                            on_state=on_state, logger=None)
        await client._report_state(True, "a")
        await client._report_state(True, "b")
        await client._report_state(False, "c")
        await client._report_state(False, "d")
        assert calls == [(True, "a"), (False, "c")]

    asyncio.run(run())


def test_report_state_swallows_callback_errors():
    async def run():
        async def bad_state(connected, detail):
            raise RuntimeError("回调炸了")

        client = LiveClient(room_id=2233, auth=FakeAuth(), on_event=_noop,
                            on_state=bad_state, logger=None)
        await client._report_state(True, "x")  # 不应抛出
        assert client.connected is True

    asyncio.run(run())


async def _noop(event):  # pragma: no cover
    pass


async def _noop_state(connected, detail):  # pragma: no cover
    pass
