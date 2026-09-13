"""B 站 web 端 HTTP 客户端（直播间接入所需的最小集，纯网络层，不依赖 maibot_sdk）。

从 bilibili-dynamic-push 的 bili_client.py 移植（2026-09-13），裁掉动态推送专属部分，
保留 WBI 签名 / buvid 指纹 / 风控重试这些经过真机验证的骨架，并新增直播所需接口：

- `resolve_room()`     短号 → 真实房间号 + 主播 uid（`room/v1/Room/room_init`）
- `anchor_name()`      主播昵称（作 MessageDict 的 group_name）
- `danmu_info()`       弹幕长连接凭据（`getDanmuInfo`，**必须 WBI 签名** + SESSDATA + buvid3）

移植时保留的关键经验（都是真机踩出来的，别简化掉）：

1. Cookie 必须播种进 cookie jar 而不是塞 headers——否则后续申请的 buvid3
   永远不会随请求发出，风控概率反而上升。
2. B 站会在 HTTP 412 的同时返回合法 JSON（业务码 -412），这与 WAF 的 HTML
   风控页对策完全不同，必须先尝试解析 JSON 再看状态码。
3. 412（WBI 签名失效）与 -412（request was banned）是两回事：前者重建签名重试有效，
   后者重试只会加重风控，要换指纹重新激活。
"""

import asyncio
import hashlib
import time
import urllib.parse
from typing import Any, Optional

import httpx

from blg_buvid import activate_buvid

NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
SPI_URL = "https://api.bilibili.com/x/frontend/finger/spi"
#: 短号 → 真实房间号 + 主播 uid（无需 WBI，但走统一风控重试）
ROOM_INIT_URL = "https://api.live.bilibili.com/room/v1/Room/room_init"
#: 用户信息（WBI 签名），取主播昵称
USER_INFO_URL = "https://api.bilibili.com/x/space/wbi/acc/info"
#: 弹幕长连接凭据（WBI 签名 + SESSDATA + buvid3，2025-06-27 起强制）
DANMU_INFO_URL = "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo"
#: 发弹幕（POST，csrf=bili_jct；WBI 签名不需要，但要登录 Cookie + 直播间页 Referer）
SEND_MSG_URL = "https://api.live.bilibili.com/msg/send"

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# 一整套自洽的浏览器请求头。B 站 WAF 会校验 UA / sec-ch-ua / sec-fetch-* 的一致性，
# 只发 UA + Referer 的"半成品"请求在机房 IP 上会被判为脚本直接 412 + HTML 风控页。
DEFAULT_HEADERS = {
    "User-Agent": DEFAULT_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Origin": "https://www.bilibili.com",
    "Referer": "https://www.bilibili.com/",
    "sec-ch-ua": '"Chromium";v="126", "Google Chrome";v="126", "Not A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "Connection": "keep-alive",
}

#: live 域名接口的 Referer/Origin 要换成 live.bilibili.com，与浏览器行为一致
LIVE_HEADERS = dict(DEFAULT_HEADERS, Origin="https://live.bilibili.com",
                    Referer="https://live.bilibili.com/")

WAF_STATUS = (403, 412, 429, 503)
RISK_CODES = (-412, -352)
_RETRY_BACKOFF = (3.0, 8.0)

# WBI mixin key 重排表（B 站固定常量）
MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
]
#: WBI key 有效期（B 站每日轮换，本地缓存 1 小时足够）
WBI_CACHE_TTL = 3600


class BiliApiError(Exception):
    """B 站接口返回异常。"""

    def __init__(self, code: int, message: str):
        super().__init__(f"接口错误 code={code} msg={message}")
        self.code = code
        self.message = message


def _build_mixin_key(img_url: str, sub_url: str) -> str:
    """由 wbi_img 的 img_url / sub_url 生成 mixin key。"""
    def _raw(url: str) -> str:
        return url.rsplit("/", 1)[-1].split(".")[0]

    raw = _raw(img_url) + _raw(sub_url)
    return "".join(raw[i] for i in MIXIN_KEY_ENC_TAB)[:32]


def _sign_params(params: dict[str, Any], mixin_key: str) -> dict[str, Any]:
    """给参数加上 wts 与 w_rid（WBI 签名）。"""
    signed = dict(params)
    signed["wts"] = int(time.time())
    query = urllib.parse.urlencode(sorted(signed.items()))
    query = "".join(ch for ch in query if ch not in "!'()*")
    signed["w_rid"] = hashlib.md5((query + mixin_key).encode("utf-8")).hexdigest()
    return signed


class BiliHttpClient:
    """轻量 B 站 web 客户端（自带 buvid 指纹与 WBI 签名）。"""

    def __init__(
        self,
        cookie: str = "",
        timeout: float = 15.0,
        verify_ssl: bool = True,
        ca_bundle: str = "",
        proxy: str = "",
    ):
        self._cookie = cookie.strip()
        self._timeout = timeout
        # 中间人代理环境优先"指定 CA"而不是关校验
        self._verify: Any = ca_bundle.strip() or verify_ssl
        self._proxy = proxy.strip()
        self._client: Optional[httpx.AsyncClient] = None
        self._client_stale = False
        self._lock = asyncio.Lock()
        self._mixin_key = ""
        self._mixin_ts = 0.0
        self._bootstrapped = False
        self._activated = False
        self._activation_failed = False

    # ---------- Cookie / 连接管理

    @property
    def cookie(self) -> str:
        return self._cookie

    @property
    def buvid3(self) -> str:
        """当前会话使用的 buvid3（鉴权包要用；没有则空串，服务端按游客处理）。"""
        if self._client is None:
            return self.parse_cookie(self._cookie).get("buvid3", "")
        return self._client.cookies.get("buvid3", domain=".bilibili.com") or ""

    def set_cookie(self, cookie: str) -> None:
        """更新用户 Cookie；变更后重建连接并重新握手。"""
        new_cookie = cookie.strip()
        if new_cookie != self._cookie:
            self._cookie = new_cookie
            self._mixin_key = ""
            self._mixin_ts = 0.0
            self._bootstrapped = False
            self._activated = False
            self._activation_failed = False
            self._client_stale = True

    @staticmethod
    def parse_cookie(cookie_str: str) -> dict[str, str]:
        """把整行 Cookie 字符串解析成键值字典。"""
        out: dict[str, str] = {}
        for part in (cookie_str or "").split(";"):
            name, sep, value = part.partition("=")
            if not sep:
                continue
            name, value = name.strip(), value.strip()
            if name and value:
                out[name] = value
        return out

    def _seed_cookies(self, client: httpx.AsyncClient) -> None:
        """把用户 Cookie 播种进 cookie jar，而不是塞进 headers。

        关键：http.cookiejar 在请求已自带 Cookie 头时不会再追加 jar 内容。
        若用 headers["Cookie"] 传用户 Cookie，后续 _bootstrap 写入 jar 的
        buvid3/buvid4 指纹将永远不会随请求发出，风控概率反而上升。
        """
        for name, value in self.parse_cookie(self._cookie).items():
            client.cookies.set(name, value, domain=".bilibili.com")

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client_stale and self._client is not None:
            await self.close()
        self._client_stale = False
        if self._client is None or self._client.is_closed:
            client = httpx.AsyncClient(
                timeout=httpx.Timeout(self._timeout),
                headers=dict(DEFAULT_HEADERS),
                verify=self._verify,
                proxy=self._proxy or None,
                follow_redirects=True,
            )
            self._seed_cookies(client)
            self._client = client
        return self._client

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
        self._client = None

    # ---------- 握手：buvid 指纹 + 激活 + WBI key

    async def _bootstrap(self) -> None:
        """拉取 buvid3/4 指纹、激活指纹、计算 WBI mixin key（幂等，带缓存）。"""
        if self._bootstrapped and self._mixin_key and (
            time.time() - self._mixin_ts < WBI_CACHE_TTL
        ):
            return
        async with self._lock:
            if self._bootstrapped and self._mixin_key and (
                time.time() - self._mixin_ts < WBI_CACHE_TTL
            ):
                return
            client = await self._ensure_client()

            # 1) 浏览器指纹 buvid3 / buvid4（用户 Cookie 自带的老指纹可信度更高，不覆盖）
            try:
                resp = await client.get(SPI_URL)
                data = resp.json()
                b3 = ((data.get("data") or {}).get("b_3") or "")
                b4 = ((data.get("data") or {}).get("b_4") or "")
                if b3 and not client.cookies.get("buvid3", domain=".bilibili.com"):
                    client.cookies.set("buvid3", b3, domain=".bilibili.com")
                if b4 and not client.cookies.get("buvid4", domain=".bilibili.com"):
                    client.cookies.set("buvid4", b4, domain=".bilibili.com")
            except Exception:
                pass  # 指纹失败不致命，继续取 WBI key

            # 1.5) 激活指纹：未激活的死指纹会被风控敏感接口以 -412 拒收
            await self._maybe_activate(client)

            # 2) WBI key（nav 未登录也会返回 wbi_img，只需该字段）
            resp = await client.get(NAV_URL)
            if resp.status_code in WAF_STATUS:
                raise BiliApiError(
                    resp.status_code,
                    f"nav 接口被风控拦截（HTTP {resp.status_code}），无法获取 WBI 签名素材",
                )
            try:
                nav = resp.json()
            except Exception:
                raise BiliApiError(resp.status_code, "nav 接口返回非 JSON（疑似风控拦截）")
            wbi = ((nav.get("data") or {}).get("wbi_img") or {})
            img_url = wbi.get("img_url") or ""
            sub_url = wbi.get("sub_url") or ""
            if not img_url or not sub_url:
                raise BiliApiError(
                    int(nav.get("code") or -1),
                    f"未能获取 WBI 签名素材（nav msg={nav.get('message')}）",
                )
            self._mixin_key = _build_mixin_key(img_url, sub_url)
            self._mixin_ts = time.time()
            self._bootstrapped = True

    async def _maybe_activate(self, client: httpx.AsyncClient, force: bool = False) -> None:
        """激活 buvid 指纹。激活失败只标记不反复撞（每次请求都是风控计数）。"""
        if self._activated and not force:
            return

        b3 = client.cookies.get("buvid3", domain=".bilibili.com") or ""
        user_has_b3 = bool(self._cookie) and "buvid3" in self.parse_cookie(self._cookie)

        if user_has_b3 and not force:
            self._activated = True  # 登录态会话，指纹随账号走
            return

        if not b3 or force:
            if b3:
                client.cookies.delete("buvid3", domain=".bilibili.com")
                client.cookies.delete("buvid4", domain=".bilibili.com")
            try:
                resp = await client.get(SPI_URL)
                data = resp.json()
                b3 = ((data.get("data") or {}).get("b_3") or "")
                b4 = ((data.get("data") or {}).get("b_4") or "")
                if b3:
                    client.cookies.set("buvid3", b3, domain=".bilibili.com")
                if b4:
                    client.cookies.set("buvid4", b4, domain=".bilibili.com")
            except Exception:
                pass
            if not b3:
                return

        if self._activation_failed and not force:
            return

        ok = await activate_buvid(client, b3, "", DEFAULT_UA)
        self._activated = ok
        self._activation_failed = not ok

    def _invalidate_wbi(self) -> None:
        self._mixin_key = ""
        self._mixin_ts = 0.0
        self._bootstrapped = False

    # ---------- 请求主入口

    async def _get_json(
        self, url: str, params: dict[str, Any], *, signed: bool = True,
        headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """带 WBI 签名的 GET；签名失效（-403/412）或 WAF 拦截时退避重试一次。"""
        for attempt in (0, 1):
            if signed:
                await self._bootstrap()
            client = await self._ensure_client()
            request_params = _sign_params(params, self._mixin_key) if signed else dict(params)
            resp = await client.get(url, params=request_params,
                                    headers=dict(headers) if headers else None)

            # 判断顺序至关重要：先尝试解析 JSON，再谈状态码。
            # HTTP 412 + 合法 JSON 是业务层风控（-412）；WAF 拦截返回 HTML 风控页。
            try:
                data = resp.json()
            except Exception:
                data = None

            if data is None:
                if resp.status_code in WAF_STATUS and attempt == 0:
                    self._invalidate_wbi()
                    await asyncio.sleep(_RETRY_BACKOFF[attempt])
                    continue
                raise BiliApiError(
                    resp.status_code or -1,
                    f"接口返回非 JSON（HTTP {resp.status_code}）"
                    + ("，疑似 WAF 拦截：可尝试填写 cookie / 调大间隔" if resp.status_code in WAF_STATUS else ""),
                )

            code = int(data.get("code") or 0)
            if code == 0:
                return data.get("data") or {}
            msg = str(data.get("message") or "")
            if attempt == 0 and code in (-403, 412):
                self._invalidate_wbi()
                await asyncio.sleep(_RETRY_BACKOFF[attempt])
                continue
            if code in RISK_CODES:
                if code == -412 and attempt == 0 and not self._cookie:
                    # 自救：丢弃当前指纹，换新指纹重新激活后再试最后一次
                    client = await self._ensure_client()
                    await self._maybe_activate(client, force=True)
                    await asyncio.sleep(_RETRY_BACKOFF[attempt])
                    continue
                hint = "请求被 B 站风控拒绝，可尝试填写 cookie" if code == -412 else "风控校验失败"
                raise BiliApiError(code, f"{msg}。{hint}" if msg else hint)
            raise BiliApiError(code, msg or f"接口返回 code={code}")
        return {}

    # ---------- 直播间业务接口

    async def login_uid(self) -> int:
        """当前 Cookie 的用户 uid；未登录（-101）返回 0。

        未登录是**合法状态**不是故障——真机实录（2026-09-13）：cookie 为空时这里抛
        -101 会把整个 on_load 炸掉导致插件注册失败。鉴权包的 uid 与 token 必须同源，
        所以游客（uid=0）也必须能走到 getDanmuInfo，由那里决定是否真的连不上。
        """
        try:
            data = await self._get_json(NAV_URL, {}, signed=False)
        except BiliApiError as exc:
            if exc.code == -101:
                return 0
            raise
        try:
            return int(data.get("mid") or 0)
        except (TypeError, ValueError):
            return 0

    async def resolve_room(self, room_id: str | int) -> dict[str, int]:
        """短号/长号 → 真实房间号 + 主播 uid。"""
        data = await self._get_json(ROOM_INIT_URL, {"id": int(room_id)},
                                    signed=False, headers=LIVE_HEADERS)
        return {
            "room_id": int(data.get("room_id") or room_id),
            "anchor_uid": int(data.get("uid") or 0),
        }

    async def anchor_name(self, uid: int) -> str:
        """主播昵称（作 MessageDict 的 group_name）。失败返回空串，不阻断连接。"""
        if not uid:
            return ""
        try:
            data = await self._get_json(USER_INFO_URL, {"mid": int(uid)})
        except BiliApiError:
            return ""
        return str(data.get("name") or "")

    async def danmu_info(self, room_id: int) -> dict[str, Any]:
        """弹幕长连接凭据：token + host_list。必须带 WBI 签名与登录 Cookie。"""
        data = await self._get_json(DANMU_INFO_URL, {"id": int(room_id), "type": 0},
                                    signed=True, headers=LIVE_HEADERS)
        token = str(data.get("token") or "")
        host_list = data.get("host_list") or []
        if not token or not host_list:
            raise BiliApiError(-1, f"getDanmuInfo 未返回可用凭据（msg 含义见日志）")
        return {"token": token, "host_list": host_list}

    async def send_danmaku(self, room_id: int, message: str) -> None:
        """发一条弹幕到直播间（msg/send，POST）。失败抛 BiliApiError。

        注意：
        - 该接口**不走 WBI 签名**，但要登录 Cookie（bili_jct 作 csrf）；
        - Referer 必须是直播间页面（live.bilibili.com/<room_id>），与浏览器一致；
        - 不做自动重试：-101/-111/10031/1003212 都是终态或会加重风控的信号，
          频控由上层令牌桶兜底，这里只如实抛出。
        """
        csrf = self.parse_cookie(self._cookie).get("bili_jct", "")
        if not csrf:
            raise BiliApiError(-111, "Cookie 缺少 bili_jct，无法发弹幕（只能收）")
        room_id = int(room_id)
        headers = dict(LIVE_HEADERS, Referer=f"https://live.bilibili.com/{room_id}")
        client = await self._ensure_client()
        resp = await client.post(SEND_MSG_URL, headers=headers, data={
            "bubble": 0,
            "msg": message,
            "color": 16777215,
            "mode": 1,
            "fontsize": 25,
            "rnd": int(time.time()),
            "roomid": room_id,
            "csrf": csrf,
            "csrf_token": csrf,
        })
        # 先解析 JSON 再看状态码——B 站 412 + 合法 JSON 是业务层风控（同 _get_json 判序）
        try:
            data = resp.json()
        except Exception:
            data = None
        if data is None:
            raise BiliApiError(
                resp.status_code or -1,
                f"msg/send 返回非 JSON（HTTP {resp.status_code}，疑似 WAF 拦截）",
            )
        code = int(data.get("code") or 0)
        if code != 0:
            raise BiliApiError(code, str(data.get("message") or f"msg/send code={code}"))
