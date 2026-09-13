"""ExClimbWuzhi buvid 激活（上传设备指纹，纯函数 + 独立模块）。

本文件从 bilibili-dynamic-push 的 buvid_activation.py 原样移植（2026-09-13），
仅改模块名为 blg_ 前缀——扁平 sys.path 导入下，两个插件的同名模块会互相覆盖。
上游实现有更新时，重新拷贝并把本注释带上即可。

为什么需要这个：
    通过 finger/spi 拿到的 buvid3/buvid4 是"未激活"状态（死指纹）。
    B 站风控要求指纹必须与一台"真实设备"绑定过——即向
    x/internal/gaia-gateway/ExClimbWuzhi 上报一份浏览器环境指纹 payload，
    服务端校验通过后 buvid 才被标记为可信。
    未激活的 buvid 在访问 feed/space 等风控敏感接口时会被拒：
    JSON 业务码 -412 "request was banned"（注意区别于 HTTP 状态码 412）。

实现思路来源：bilibili-API-collect issue #933（社区共识方案）；
参考 bilibili-api-python 17.4.2 的 _active_buvid 移植，
payload 环境字段改为 Windows/Chrome 风格以匹配插件 UA。

不依赖 maibot_sdk，可单独导入测试。
"""

import io
import json
import random
import struct
import time
from typing import Any

import httpx

EX_URL = "https://api.bilibili.com/x/internal/gaia-gateway/ExClimbWuzhi"

# 该接口同样吃 WAF 的请求头一致性校验——只发 Content-Type+UA 会被拒，
# 必须带全套浏览器头（与 bili_client.DEFAULT_HEADERS 保持一致的风格）。
EX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Content-Type": "application/json",
    "Origin": "https://www.bilibili.com",
    "Referer": "https://www.bilibili.com/",
    "sec-ch-ua": '"Chromium";v="126", "Google Chrome";v="126", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

_MOD = 1 << 64


def _rotate_left(x: int, k: int) -> int:
    bin_str = bin(x)[2:].rjust(64, "0")
    return int(bin_str[k:] + bin_str[:k], base=2)


def _murmur3_x64_128(source: bytes, seed: int) -> int:
    """murmur3 128 位哈希（x64 变体）。返回 (h2 << 64) | h1。"""
    C1 = 0x87C3_7B91_1142_53D5
    C2 = 0x4CF5_AD43_2745_937F
    C3 = 0x52DC_E729
    C4 = 0x3849_5AB5
    R1, R2, R3, M = 27, 31, 33, 5
    h1, h2 = seed, seed
    processed = 0
    stream = io.BytesIO(source)
    while True:
        read = stream.read(16)
        processed += len(read)
        if len(read) == 16:
            k1 = struct.unpack("<q", read[:8])[0]
            k2 = struct.unpack("<q", read[8:])[0]
            h1 ^= _rotate_left(k1 * C1 % _MOD, R2) * C2 % _MOD
            h1 = ((_rotate_left(h1, R1) + h2) * M + C3) % _MOD
            h2 ^= _rotate_left(k2 * C2 % _MOD, R3) * C1 % _MOD
            h2 = ((_rotate_left(h2, R2) + h1) * M + C4) % _MOD
        elif len(read) == 0:
            h1 ^= processed
            h2 ^= processed
            h1 = (h1 + h2) % _MOD
            h2 = (h2 + h1) % _MOD
            return (h2 << 64) | h1
        else:
            k1 = k2 = 0
            if len(read) >= 15:
                k2 ^= int(read[14]) << 48
            if len(read) >= 14:
                k2 ^= int(read[13]) << 40
            if len(read) >= 13:
                k2 ^= int(read[12]) << 32
            if len(read) >= 12:
                k2 ^= int(read[11]) << 24
            if len(read) >= 11:
                k2 ^= int(read[10]) << 16
            if len(read) >= 10:
                k2 ^= int(read[9]) << 8
            if len(read) >= 9:
                k2 ^= int(read[8])
                k2 = _rotate_left(k2 * C2 % _MOD, R3) * C1 % _MOD
                h2 ^= k2
            if len(read) >= 8:
                k1 ^= int(read[7]) << 56
            if len(read) >= 7:
                k1 ^= int(read[6]) << 48
            if len(read) >= 6:
                k1 ^= int(read[5]) << 40
            if len(read) >= 5:
                k1 ^= int(read[4]) << 32
            if len(read) >= 4:
                k1 ^= int(read[3]) << 24
            if len(read) >= 3:
                k1 ^= int(read[2]) << 16
            if len(read) >= 2:
                k1 ^= int(read[1]) << 8
            if len(read) >= 1:
                k1 ^= int(read[0])
                k1 = _rotate_left(k1 * C1 % _MOD, R2) * C2 % _MOD
                h1 ^= k1


def _fmix64(k: int) -> int:
    C1 = 0xFF51_AFD7_ED55_8CCD
    C2 = 0xC4CE_B9FE_1A85_EC53
    R = 33
    tmp = k
    tmp ^= tmp >> R
    tmp = tmp * C1 % _MOD
    tmp ^= tmp >> R
    tmp = tmp * C2 % _MOD
    tmp ^= tmp >> R
    return tmp


def gen_uuid_infoc() -> str:
    """生成浏览器 _uuid 风格的标识（8-4-4-4-12 + 时间尾 + infoc）。"""
    t = int(time.time() * 1000) % 100000
    mp = list("123456789ABCDEF") + ["10"]
    pck = [8, 4, 4, 4, 12]
    return "-".join(
        "".join(random.choice(mp) for _ in range(length)) for length in pck
    ) + str(t).ljust(5, "0") + "infoc"


def gen_buvid_fp(payload: str) -> str:
    """buvid_fp = murmur3(payload, seed=31) 两段 hex 拼接。"""
    m = _murmur3_x64_128(payload.encode("ascii"), 31)
    return "{}{}".format(hex(m & (_MOD - 1))[2:], hex(m >> 64)[2:])


# PDF 插件列表（Chrome 全家桶标准值）
_PDF_PLUGINS = [
    ["PDF Viewer", "Portable Document Format",
     [["application/pdf", "pdf"], ["text/pdf", "pdf"]]],
    ["Chrome PDF Viewer", "Portable Document Format",
     [["application/pdf", "pdf"], ["text/pdf", "pdf"]]],
    ["Chromium PDF Viewer", "Portable Document Format",
     [["application/pdf", "pdf"], ["text/pdf", "pdf"]]],
    ["Microsoft Edge PDF Viewer", "Portable Document Format",
     [["application/pdf", "pdf"], ["text/pdf", "pdf"]]],
    ["WebKit built-in PDF", "Portable Document Format",
     [["application/pdf", "pdf"], ["text/pdf", "pdf"]]],
]

_WEBGL_PARAMS = [
    "extensions:ANGLE_instanced_arrays;EXT_blend_minmax;EXT_color_buffer_half_float;"
    "EXT_float_blend;EXT_frag_depth;EXT_shader_texture_lod;"
    "EXT_texture_compression_bptc;EXT_texture_compression_rgtc;"
    "EXT_texture_filter_anisotropic;EXT_sRGB;KHR_parallel_shader_compile;"
    "OES_element_index_uint;OES_fbo_render_mipmap;OES_standard_derivatives;"
    "OES_texture_float;OES_texture_float_linear;OES_texture_half_float;"
    "OES_texture_half_float_linear;OES_vertex_array_object;"
    "WEBGL_color_buffer_float;WEBGL_compressed_texture_astc;"
    "WEBGL_compressed_texture_etc;WEBGL_compressed_texture_etc1;"
    "WEBGL_compressed_texture_s3tc;WEBGL_compressed_texture_s3tc_srgb;"
    "WEBGL_debug_renderer_info;WEBGL_debug_shaders;WEBGL_depth_texture;"
    "WEBGL_draw_buffers;WEBGL_lose_context;WEBGL_multi_draw",
    "webgl aliased line width range:[1, 1]",
    "webgl aliased point size range:[1, 511]",
    "webgl blue bits:8",
    "webgl depth bits:24",
    "webgl green bits:8",
    "webgl max anisotropy:16",
    "webgl max combined texture image units:32",
    "webgl max cube map texture size:16384",
    "webgl max fragment uniform vectors:1024",
    "webgl max render buffer size:16384",
    "webgl max texture image units:16",
    "webgl max texture size:16384",
    "webgl max varying vectors:30",
    "webgl max vertex attribs:16",
    "webgl max vertex texture image units:16",
    "webgl max vertex uniform vectors:1024",
    "webgl max viewport dims:[16384, 16384]",
    "webgl red bits:8",
    "webgl renderer:WebKit WebGL",
    "webgl shading language version:WebGL GLSL ES 1.0 (1.0)",
    "webgl stencil bits:0",
    "webgl vendor:WebKit",
    "webgl version:WebGL 1.0",
    "webgl vertex shader high float precision:23",
    "webgl vertex shader high float precision rangeMin:127",
    "webgl vertex shader high float precision rangeMax:127",
    "webgl vertex shader medium float precision:23",
    "webgl vertex shader medium float precision rangeMin:127",
    "webgl vertex shader medium float precision rangeMax:127",
    "webgl vertex shader low float precision:23",
    "webgl vertex shader low float precision rangeMin:127",
    "webgl vertex shader low float precision rangeMax:127",
    "webgl fragment shader high float precision:23",
    "webgl fragment shader high float precision rangeMin:127",
    "webgl fragment shader high float precision rangeMax:127",
    "webgl fragment shader medium float precision:23",
    "webgl fragment shader medium float precision rangeMin:127",
    "webgl fragment shader medium float precision rangeMax:127",
    "webgl fragment shader low float precision:23",
    "webgl fragment shader low float precision rangeMin:127",
    "webgl fragment shader low float precision rangeMax:127",
    "webgl vertex shader high int precision:0",
    "webgl vertex shader high int precision rangeMin:31",
    "webgl vertex shader high int precision rangeMax:30",
    "webgl vertex shader medium int precision:0",
    "webgl vertex shader medium int precision rangeMin:31",
    "webgl vertex shader medium int precision rangeMax:30",
    "webgl vertex shader low int precision:0",
    "webgl vertex shader low int precision rangeMin:31",
    "webgl vertex shader low int precision rangeMax:30",
    "webgl fragment shader high int precision:0",
    "webgl fragment shader high int precision rangeMin:31",
    "webgl fragment shader high int precision rangeMax:30",
    "webgl fragment shader low int precision:0",
    "webgl fragment shader low int precision rangeMin:31",
    "webgl fragment shader low int precision rangeMax:30",
]

_FONT_LIST = [
    "Andale Mono", "Arial", "Arial Black", "Arial Hebrew", "Arial Narrow",
    "Arial Rounded MT Bold", "Arial Unicode MS", "Comic Sans MS", "Courier",
    "Courier New", "Geneva", "Georgia", "Helvetica", "Helvetica Neue",
    "Impact", "LUCIDA GRANDE", "Microsoft Sans Serif", "Monaco", "Palatino",
    "Tahoma", "Times", "Times New Roman", "Trebuchet MS", "Verdana",
    "Wingdings", "Wingdings 2", "Wingdings 3",
]


def build_payload(user_agent: str, uuid: str) -> str:
    """构造 ExClimbWuzhi 上报 payload（外层 {"payload": "<json 字符串>"}）。"""
    content: dict[str, Any] = {
        "3064": 1,
        "5062": int(time.time() * 1000),
        "03bf": "https%3A%2F%2Fwww.bilibili.com%2F",
        "39c8": "333.788.fp.risk",
        "34f1": "",
        "d402": "",
        "654a": "",
        "6e7c": "1920x1040",
        "3c43": {
            "2673": 1,
            "5766": 24,
            "6527": 0,
            "7003": 1,
            "807e": 1,
            "b8ce": user_agent,
            "641c": 0,
            "07a4": "zh-CN",
            "1c57": "not available",
            "0bd0": 8,
            "748e": [1040, 1920],
            "d61f": [969, 1920],
            "fc9d": -480,
            "6aa9": "Asia/Shanghai",
            "75b8": 1,
            "3b21": 1,
            "8a1c": 0,
            "d52f": "not available",
            "adca": "Win32",
            "80c9": _PDF_PLUGINS,
            "13ab": "0dAAAAAASUVORK5CYII=",
            "bfe9": "QgAAEIQAACEIAABCCQN4FXANGq7S8KTZayAAAAAElFTkSuQmCC",
            "a3c1": _WEBGL_PARAMS,
            "6bc5": "Google Inc. (NVIDIA)~ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0)",
            "ed31": 0,
            "72bd": 0,
            "097b": 0,
            "52cd": [0, 0, 0],
            "a658": _FONT_LIST,
            "d02f": "124.04345259929687",
        },
        "54ef": ('{"in_new_ab":true,"ab_version":{"remove_back_version":"REMOVE",'
                 '"login_dialog_version":"V_PLAYER_PLAY_TOAST",'
                 '"open_recommend_blank":"SELF","storage_back_btn":"HIDE",'
                 '"call_pc_app":"FORBID","clean_version_old":"GO_NEW",'
                 '"optimize_fmp_version":"LOADED_METADATA",'
                 '"for_ai_home_version":"V_OTHER","bmg_fallback_version":"DEFAULT",'
                 '"ai_summary_version":"SHOW","weixin_popup_block":"ENABLE",'
                 '"rcmd_tab_version":"DISABLE","in_new_ab":true},'
                 '"ab_split_num":{"remove_back_version":11,'
                 '"login_dialog_version":43,"open_recommend_blank":90,'
                 '"storage_back_btn":87,"call_pc_app":47,"clean_version_old":46,'
                 '"optimize_fmp_version":28,"for_ai_home_version":38,'
                 '"bmg_fallback_version":86,"ai_summary_version":466,'
                 '"weixin_popup_block":45,"rcmd_tab_version":90,"in_new_ab":0},'
                 '"pageVersion":"new_video","videoGoOldVersion":-1}'),
        "8b94": "https%3A%2F%2Fwww.bilibili.com%2F",
        "df35": uuid,
        "07a4": "zh-CN",
        "5f45": None,
        "db46": 0,
    }
    return json.dumps(
        {"payload": json.dumps(content, separators=(",", ":"))},
        separators=(",", ":"),
    )


async def activate_buvid(
    client: httpx.AsyncClient,
    buvid3: str,
    buvid4: str,
    user_agent: str,
    extra_cookies: dict[str, str] | None = None,
) -> bool:
    """激活 buvid（上报设备指纹）。返回是否成功。

    幂等：重复激活无害。失败不抛异常（打日志即可），调用方应继续尝试业务请求——
    激活失败 ≠ 一定被拒，只是风控概率更高。
    """
    if not buvid3:
        return False
    uuid = gen_uuid_infoc()
    payload = build_payload(user_agent, uuid)
    buvid_fp = gen_buvid_fp(payload)
    cookies = {
        "buvid3": buvid3,
        "buvid4": buvid4 or "",
        "buvid_fp": buvid_fp,
        "_uuid": uuid,
    }
    if extra_cookies:
        cookies.update(extra_cookies)
    headers = dict(EX_HEADERS)
    headers["User-Agent"] = user_agent  # 与调用方 UA 一致
    try:
        resp = await client.post(
            EX_URL,
            content=payload,
            headers=headers,
            cookies=cookies,
        )
        data = resp.json()
        # 注意：不能用 "code or -1" 兜底——成功码 0 也是 falsy，
        # 会被 or 吞掉变成 -1（真实踩过的坑）
        code = data.get("code")
        return code is not None and int(code) == 0
    except Exception:
        return False
