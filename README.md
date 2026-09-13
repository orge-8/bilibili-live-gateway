# bilibili-live-gateway

把 **B 站直播间**接入 MaiBot：弹幕 / 礼物 / 上舰 / SC / 进场事件入站 → MaiBot 走完整
人格·记忆·决策链路 → 回复经弹幕发回直播间。

参考实现：[Mai-with-u/Amaidesu](https://github.com/Mai-with-u/Amaidesu)（它做单向采集喂虚拟形象，
本插件把它升级成 MaiBot 的**双工平台网关**）。

> **当前版本 0.3.0 = P1–P5 全部完成（可长期运行）**
>
> **真机已实证（2026-09-13）**
> - ✅ 宿主接纳 `platform="bilibili"` 入站：`accepted=True`、伪群建立、`平台：bilibili` 用户注册、
>   完整人格/记忆/决策链路跑通
> - ✅ 出站回派：Planner 调 reply 工具 → `已通过 Platform IO 将消息发往平台 'bilibili'
>   (drivers: gateway:org.mai-mai.bilibili-live-gateway:bili_live)` → 出站处理器收到回复
> - ✅ P2 弹幕长连接：心跳 20s 一次持续稳定，无 50s 断连（keepalive 修复实证通过）
> - ✅ P3 真实发弹幕：`/bili发` 真发成功（6 字）、超长自动截断（25→20 字 + `…`）、
>   `/bili状态` 令牌余量速览正常
>
> **P4 新增**：分级触发全量——`trigger.blocked_users`（用户 uid 黑名单，任何事件直接丢，
> 先于一切触发判定）、`trigger.blocked_keywords`（关键词黑名单，只拦普通弹幕）。
> 管理员命令：`/bili发 <文本>`（绕过触发链路直发，单号自测）、`/bili状态`（网关速览）。

## 架构

```
入站  wss 长连接（getDanmuInfo 取 token → op=7 鉴权 → op=2 心跳）
      ──→ blg_proto 解包（brotli 多包拆分）
      ──→ blg_events.to_message() ──→ MessageDict
      ──→ 自过滤/去重/触发分级 ──→ ctx.gateway.route_message()
      ──→ Host 去重 → ChatBot.receive_message() ──→ 人格/记忆/决策/频率控制

出站  MaiBot 决定回复 ──→ Host 反向 RPC 调本插件的 @MessageGateway(duplex) 方法
      ──→ 消息段降级为纯文本（P3 起截断 + 限流）
      ──→ POST api.live.bilibili.com/msg/send（P3）
```

插件**不调用** `ctx.send.*`（回复由 Host 反向派发），因此 manifest 不需要 `send.text` 系列能力。

## 安装

```powershell
# 方式一：符号链接（开发调试推荐，需先开启 Windows 开发者模式）
New-Item -ItemType SymbolicLink `
  -Path "<MaiBot路径>\plugins\bilibili-live-gateway" `
  -Target "<本插件路径>"

# 方式二：整目录复制
Copy-Item -Recurse "<本插件路径>" "<MaiBot路径>\plugins\bilibili-live-gateway"
```

**改完必须完整重启 MaiBot**：manifest / capabilities 在插件加载前校验，热重载不生效。

依赖由 Host 按 `_manifest.json` 的 `dependencies` 安装：`httpx>=0.27`、`websockets>=12.0`、`Brotli>=1.1`
（`Brotli` 提供 `import brotli`，管它叫 `brotli` 的旧包也能导入但已不维护）。

## 配置

`config.toml` 由 Runner 首次加载时按默认值生成，**不要手工写 BOM**（PowerShell `Set-Content -Encoding UTF8` 会写 BOM，导致 TOML 解析失败）。

| 段 | 字段 | 默认 | 说明 |
|---|---|---|---|
| `plugin` | `enabled` | `true` | 插件总开关 |
| | `config_version` | `0.1.0` | Host 版本策略必填，勿删 |
| `live` | `room_id` | 空 | 直播间**真实长号**（短号会重定向，不能用） |
| | `room_display_name` | 空 | 直播间显示名，留空用「直播间 <房间号>」 |
| `auth` | `cookie` | 空 | 整行 Cookie，需含 `SESSDATA`（收）+ `bili_jct`（发弹幕） |
| | `verify_ssl` / `ca_bundle` / `proxy` | `true` / 空 / 空 | `ca_bundle` 优先于 `verify_ssl=false` |
| `inbound` | `enabled` | `true` | 是否建立弹幕长连接（P2 生效） |
| | `record_all_danmaku` | `false` | 未被触发的弹幕也以通知语义入库供 LLM 取上下文 |
| | `dedupe_ttl_sec` / `max_inbound_per_sec` | `120` / `15` | 去重窗口 / 入站令牌桶 |
| `trigger` | `at_names` | `["  "]` | **机器人在直播间的昵称**，弹幕 @ 靠它识别（改成你的 bot 名） |
| | `keywords` / `keyword_is_regex` | 空 / `false` | 命中即触发 |
| | `heat_window_sec` / `heat_max_per_user` | `10` / `3` | 同用户热度闸门 |
| | `high_value_always` | `true` | 礼物/上舰/SC 必触发，绕过热度与限流 |
| `outbound` | `enabled` / `max_chars` / `min_interval_sec` | `true` / `20` / `3.0` | 出站开关 / 弹幕长度上限 / 最小间隔 |
| `probe` | `enabled` | `true` | **P1 探针**，验证完请置 `false` |
| | `delay_sec` | `8.0` | 启动后延迟几秒注入 |
| | `text` | 空 | 留空则自动 `@<at_names 第一个>` |
| | `outbound_dry_run` | `true` | P1 必须为 `true`，真实发弹幕在 P3 |
| | `self_test_outbound` | `true` | 注入前先本地直调一次出站 handler（只证明代码可用，不代表路由已通） |
| | `outbound_wait_sec` | `45` | 注入后等出站回派的秒数；超时打印诊断。设 0 关闭 |

## 命令

**P1 阶段没有对外命令**，诊断全部走日志（避免额外声明 `send.*` 能力）。请在 MaiBot 日志里
检索 `bili-live` 与 `PROBE` 两个关键字。

## 真机探针验证（P1 的核心步骤）

### 前置：`trigger.at_names` 必须与真机人格名一致（最容易踩）

弹幕里的 `@` 是**纯文本**，插件只能靠 `at_names` 名单判断「这条在 @ 我」。
名字不对会连锁失败：`is_at` 判错 → 消息进不了强制触发 → 即使进了触发，LLM 也会判定
「@ 的不是我」而拒绝回复。

**2026-09-13 真机实证**：配置里是默认的「狸猫」，而真机人格名叫「鸣澜」，
Planner 的结论原文就是：

> 这条消息是@狸猫的，不是@鸣澜的 …… 鸣澜不需要回复这条消息

取人格名的办法：看主程序启动日志的 `[主程序] 全部系统初始化完成，XXX 已成功唤醒`，
那个 `XXX` 就是。**这不是探针专属问题——正式使用也必须配对，否则所有弹幕都不会触发。**

### 步骤

1. 把 `live.room_id` 填成真实直播间**长号**（探针只用它当群标识，不会真的连过去）。
   留空时探针会用占位群号 `0`，日志里会出现「直播间 0」——功能上能跑，但出站反解也拿到 0。
2. 把 `trigger.at_names` 改成真机人格名。
3. 保持 `probe.enabled = true`、`probe.outbound_dry_run = true`，**完整重启** MaiBot。
4. 等 8 秒左右，逐行核对：

```
[bili-live] 启动摘要：房间=xxx @名单=鸣澜 入站=开 出站=开 探针=开（dry_run=True）...
[bili-live] 上报网关状态 ready=True platform=bilibili scope=room:xxx → 宿主接受=True
[PROBE] 出站 handler 本地自测通过：...（仅证明 handler 可用，不代表 Host 路由已通）
[PROBE] 注入合成弹幕 → message_id=blg-... is_at=True group_id=xxx
[PROBE] route_message 返回 accepted=True → 入站成立，宿主接纳了 platform=bilibili 的群聊消息
[PROBE] ★★★ 出站被调用 —— Host 已把回复派发回本网关（第 1 次）★★★
```

### 判读

| 现象 | 结论 |
|---|---|
| `accepted=True` + 「出站被调用」 | **网关双工链路成立**，P1 通过，可以进入 P2 |
| `accepted=True`，`self_test` 通过，但没有「出站被调用」 | 入站成立、handler 可用；问题在「Planner 没回」或「出站路由没命中」——看超时诊断分两类**——见下** |
| `accepted=False` | 宿主拒绝了非 QQ 平台入站 → 架构需重做，先别投入 P2 |
| 完全看不到 `[PROBE]` | 插件没加载 / `plugin.enabled=false` / 日志级别过高 |

`accepted=True` 但没等到回派时，探针会在 `outbound_wait_sec` 后打印分类诊断，对照日志即可定位：

- **日志里有 Planner 结论且明确说「不回复」** → 属于 LLM 行为（名字没配对 / 人格偏安静）。
  入站链路本身是正常的，可把 `probe.text` 换成它感兴趣的话题再试。
- **Planner 已决定回复，却仍没有「出站被调用」** → 这才是真正的出站路由问题。
  请把 `logs/maisaka_prompt/planner/...` 里的 planner 结论和这段日志一起回传。

### 已验证结论（2026-09-13 真机）

| 项 | 结果 | 证据 |
|---|---|---|
| 宿主接纳 `platform="bilibili"` 入站 | ✅ | `route_message 返回 accepted=True` |
| 插件加载（含 `gateway.*` 能力声明） | ✅ | `已加载=29，失败=0`，插件正常起探针 |
| 群聊建模（伪群） | ✅ | 出现 `[直播间 0] Maisaka 运行时已启动`，聊天统计里有「直播间 0 消息数量 1」 |
| 非 QQ 平台用户注册 | ✅ | `成功注册新用户：…，平台：bilibili，昵称：链路探针` |
| 完整管线（防抖→hook→强制触发→Planner→记忆检索） | ✅ | file-reader hook 收到完整报文；`检测到@消息…强制触发`；`开始思考: 第 1 轮消息数=1` |
| session 命名 | ✅ | `logs/maisaka_prompt/planner/bilibili_group_0/` |
| 出站回派 | ⏳ 待验证 | 上一轮 Planner 因 `at_names` 不匹配而拒绝回复，未产生出站 |

> **`gateway.*` 能力声明的分歧已由真机裁决**：把 `gateway.route_message` /
> `gateway.update_state` 写在 manifest `capabilities` 里，Host 1.2.3 **不会**拒载。
> 因此无需删除，保留声明即可。

验证完成后把 `probe.enabled` 改为 `false`，避免每次重启都往聊天记录里插一条假消息。
探针会在 MaiBot 里创建一条以房间号为 group_id 的**伪群**记录，这是设计使然（直播间按「群」建模，
这样出站才能反解房间号，并复用群聊回复策略与记忆分区）。

## 故障排查

| 现象 | 原因 / 处理 |
|---|---|
| 注入了消息、`accepted=True`，但 bot 不回复 | **先查 `trigger.at_names` 是否等于真机人格名**（见上方前置说明）。日志里会有 Planner 的明确结论，例如「这条消息是@狸猫的，不是@鸣澜的」 |
| 日志显示「直播间 0」 | `live.room_id` 为空，填上真实长号 |
| 长连接建不起来：`取弹幕凭据失败 code=-412` | `getDanmuInfo` 被风控。优先填 `auth.cookie`（含 SESSDATA + buvid3）；仍不行多半是 IP 信誉问题，等一等或换网络 |
| `取弹幕凭据失败 code=-101` | Cookie 失效/未登录，重新导出 Cookie |
| 日志出现 `B 站鉴权被拒（code=…）` | token 与 uid 不同源：uid 来自 Cookie、token 来自 getDanmuInfo，两者必须用同一份 Cookie |
| 连上后收不到弹幕 | 看是否停在「取弹幕凭据失败」的重连循环；直播没开播时 `DANMU_MSG` 不会有，但 `人气值` 心跳日志应持续出现 |
| 每约 50 秒断线一次（`keepalive ping timeout`）| **已在 v0.2.0 修复**。根因：websockets 库的协议层 ping（20s ping + 20s timeout），而 B 站弹幕服务器不回 WS 协议层 pong → 必然超时被踢。修复：`ping_interval=None` 禁用库层 ping，保活只靠应用层 op=2 心跳，且心跳间隔硬钳在 [15, 20]s（真机实测服务端 ~20s 无数据就断开；存量 config.toml 里写 30 也会被钳到 20） |
| 日志出现「账号未登录（-101）：停止重连」 | 这是预期行为：Cookie 不换重试结果不会变，插件会停止重连等重启。填好 `auth.cookie` 后**完整重启 MaiBot** |
| wss 报 `CERTIFICATE_VERIFY_FAILED` | 中间人代理拦截了 `broadcastlv.chat.bilibili.com`。HTTP 层可用 `auth.ca_bundle`；wss 层用系统证书，需把代理根证书装入系统信任。万不得已才改代码关校验 |
| 出站一直没有「出站被调用」 | 追加上 `outbound_wait_sec` 的诊断输出分两类：Planner 说不回（LLM 行为）vs Planner 说回但没出站（真的是路由问题） |
| 改了 `capabilities` 没生效 | manifest 只在加载前校验，必须完整重启 MaiBot，热重载无效 |
| `config.toml` 报 TOML Invalid statement | 文件带了 UTF-8 BOM，重写为无 BOM |
| 出站 `success=false` | 看 metadata.error：`限频`=令牌桶耗尽（调大 `outbound.bucket_wait_sec` 或 `min_interval_sec`）；`bili_code=-101`/-111=Cookie 失效或缺 bili_jct；`bili_code=1003212`=内容触发风控；`no_room`=出站 route 没带房间号 |
| 出站没发出去（dry_run 提示还在） | `probe.outbound_dry_run` 只控制探针路径；真实回复经 `outbound.enabled` 控制（默认 true）。两者都开才会真发 |
| 出站日志没出现「已发送」 | 确认 Cookie 含 `bili_jct`（收弹幕只要 SESSDATA，发弹幕必须要 bili_jct）；普通账号 20 字上限，超长部分被截断 |
| 房间号填了短号 | 可以填，插件会用 `room_init` 自动换算成长号；但 `msg/send` 用的是换算后的长号 |

> 关于 `gateway.*` 能力声明：曾担心 Host registry 里没有这个能力名会拒载，
> 2026-09-13 真机实证 **不会**（`已加载=29，失败=0`），保留声明即可。

## 开发与门禁

```bash
# 全量门禁（check_plugin + 冒烟 + pytest），必须全绿
/c/Users/38160/Desktop/tools/maibot-devkit/gate.sh \
  "/c/Users/38160/WorkBuddy/2026-09-13-08-14-57/MaiBot插件开发/plugins/bilibili-live-gateway"

# 单独跑
python tests/smoke_test.py      # 不连网络、不启动 MaiBot 的网关双工冒烟
python -m pytest -q tests       # 协议编解码 + 事件映射单测
```

模块按 `blg_` 前缀命名并用扁平导入（`sys.path.insert`），是为了避免与
`bilibili-dynamic-push` 的 `buvid_activation.py` 之类的同名模块在同一 `sys.path` 上互相覆盖。

## 后续阶段

| 阶段 | 内容 | 状态 |
|---|---|---|
| P1 | 网关链路探针（入站准入 / 群聊建模 / 出站回派） | ✅ 真机实证 |
| P2 | WBI/buvid 基建 + `getDanmuInfo` + wss 长连接 + 事件注入 | ✅ 真机实证（keepalive 修复后稳定） |
| P3 | `outbound` 真实发弹幕：令牌桶限流、长度截断、错误码处理 + `/bili发` `/bili状态` | ✅ 真机实证（真发/截断/状态全过） |
| P4 | 分级触发全量：黑名单（用户/关键词）；热度闸门（同用户刷屏限速） | ✅ 完成（活跃度闸门暂缓：单房间场景意义有限） |
| P5 | 重连压测（失败循环/退避/stop 取消/-101 终态）+ 打包 | ✅ 完成 |

**P3 已知事实（来自真机出站日志）**：装了 smart_segmentation 插件时，一条回复会被预切分成
多段（实测「收到，链路通畅。」拆成 2 段），**出站处理器会被连续调用多次**——
限流令牌桶必须按"每段一张票"设计，且 B 站 `msg/send` 有最小间隔，多段回复会按间隔串行发出。

## 打包部署

```bash
# 部署 = 整目录替换真机插件目录 + 完整重启 MaiBot（热重载对此插件不可靠）
# 需要同步的文件（全部）：
#   _manifest.json plugin.py blg_auth.py blg_buvid.py blg_client.py
#   blg_events.py blg_outbound.py README.md tests/
# 真机依赖自检：python -c "import httpx, websockets, brotli"
# capabilities 变更过的版本（>=0.3.0 加了 send.text）必须完整重启才生效
```
