"""blg_auth 单测：WBI 签名组装、Cookie 解析、风控判定相关的纯逻辑。

不联网——网络层行为靠 MockTransport 的既有模式在真机前验证即可，
这里锁定的是"签名怎么算、什么情况该重试"这些易错逻辑。
"""

import asyncio
import hashlib
import urllib.parse

import pytest

from blg_auth import (
    MIXIN_KEY_ENC_TAB,
    WAF_STATUS,
    BiliApiError,
    BiliHttpClient,
    _build_mixin_key,
    _sign_params,
)


IMG_KEY = "7cd084941338484aae1ad9425b84077c"
SUB_KEY = "4932caff0ff74606abaa8d0c6b6a1a7b"


def test_build_mixin_key_reorders_and_truncates():
    """mixin key = (img_key + sub_key) 按 MIXIN_KEY_ENC_TAB 重排后取前 32 位。"""
    raw = IMG_KEY + SUB_KEY
    expected = "".join(raw[i] for i in MIXIN_KEY_ENC_TAB)[:32]
    key = _build_mixin_key(
        f"https://i0.hdslb.com/bfs/wbi/{IMG_KEY}.png",
        f"https://i0.hdslb.com/bfs/wbi/{SUB_KEY}.png",
    )
    assert key == expected
    assert len(key) == 32
    # 重排确实发生（不是简单截断）
    assert key != raw[:32]


def test_build_mixin_key_is_deterministic():
    a = _build_mixin_key(f"https://x/{IMG_KEY}.png", f"https://x/{SUB_KEY}.png")
    b = _build_mixin_key(f"https://x/{IMG_KEY}.png", f"https://x/{SUB_KEY}.png")
    assert a == b


def test_sign_params_appends_wts_and_w_rid():
    mixin = "a" * 32
    signed = _sign_params({"foo": "bar"}, mixin)
    assert signed["foo"] == "bar"
    assert isinstance(signed["wts"], int)
    # w_rid 独立复算：参与哈希的是【加 wts 之前、不含 w_rid】的参数集
    without_rid = {k: v for k, v in signed.items() if k != "w_rid"}
    query = urllib.parse.urlencode(sorted(without_rid.items()))
    query = "".join(ch for ch in query if ch not in "!'()*")
    assert signed["w_rid"] == hashlib.md5((query + mixin).encode()).hexdigest()


def test_sign_params_excludes_special_chars_from_query():
    """w_rid 参与哈希的 query 要剔除 !'()*，这是 B 站的怪规则。"""
    mixin = "b" * 32
    signed = _sign_params({"text": "a(b)c!d'e*f"}, mixin)
    params_wo_rid = {k: v for k, v in signed.items() if k != "w_rid"}
    query = urllib.parse.urlencode(sorted(params_wo_rid.items()))
    query = "".join(ch for ch in query if ch not in "!'()*")
    assert signed["w_rid"] == hashlib.md5((query + mixin).encode()).hexdigest()


def test_parse_cookie_handles_spaces_and_missing_pairs():
    parsed = BiliHttpClient.parse_cookie(" SESSDATA=abc;  bili_jct = xyz ; ; broken ; uid=1 ")
    assert parsed == {"SESSDATA": "abc", "bili_jct": "xyz", "uid": "1"}


def test_parse_cookie_empty():
    assert BiliHttpClient.parse_cookie("") == {}
    assert BiliHttpClient.parse_cookie(None) == {}  # type: ignore[arg-type]


def test_error_carries_code_and_message():
    err = BiliApiError(-412, "request was banned")
    assert err.code == -412
    assert "-412" in str(err)


def test_waf_status_is_http_level():
    """WAF 是 HTTP 状态码，-412 是业务码，两者不可混淆。"""
    assert 412 in WAF_STATUS
    assert -412 not in WAF_STATUS


# ---------------------------------------------------------------- login_uid（MockTransport，不联网）

def _mock_nav_client(monkeypatch, payload: dict):
    """把 BiliHttpClient 的 HTTP 层替换成返回固定 nav 响应的假客户端。"""
    import httpx

    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json=payload)

    client = BiliHttpClient()
    fake = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def _ensure():
        return fake

    monkeypatch.setattr(client, "_ensure_client", _ensure)
    return client, fake, calls


def test_login_uid_tolerates_not_logged_in(monkeypatch):
    """真机实录：cookie 为空时 nav 返回 -101，login_uid 若上抛会把整个 on_load 炸掉。"""
    client, fake, calls = _mock_nav_client(
        monkeypatch, {"code": -101, "message": "账号未登录", "ttl": 1})
    try:
        assert asyncio.run(client.login_uid()) == 0
    finally:
        asyncio.run(fake.aclose())
    assert calls


def test_login_uid_returns_mid_when_logged_in(monkeypatch):
    client, fake, _calls = _mock_nav_client(
        monkeypatch, {"code": 0, "message": "0", "ttl": 1, "data": {"mid": 2472005478, "wbi_img": {}}})
    try:
        assert asyncio.run(client.login_uid()) == 2472005478
    finally:
        asyncio.run(fake.aclose())


def test_login_uid_still_raises_on_risk_control(monkeypatch):
    """-101 之外的业务错误不吞：那是真的风控/网络故障，要让上层看到。"""
    client, fake, _calls = _mock_nav_client(
        monkeypatch, {"code": -412, "message": "request was banned", "ttl": 1})
    try:
        with pytest.raises(BiliApiError) as excinfo:
            asyncio.run(client.login_uid())
        assert excinfo.value.code == -412
    finally:
        asyncio.run(fake.aclose())


def test_verify_prefers_ca_bundle_over_verify_ssl():
    """中间人代理环境：给了 CA 路径就不应该退化成关校验。"""
    client = BiliHttpClient(ca_bundle="/tmp/proxy-root-ca.cer", verify_ssl=False)
    assert client._verify == "/tmp/proxy-root-ca.cer"
    plain = BiliHttpClient(verify_ssl=False)
    assert plain._verify is False
