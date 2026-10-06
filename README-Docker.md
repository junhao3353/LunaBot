# QQ2 机器人 NAS(Docker) 部署指南

把「本子下载」QQ 机器人 + NapCat 用 Docker 跑在 NAS 上（飞牛OS / 群晖 / 任何 Docker 主机），
7x24 不关机。**前提：NAS CPU 为 x86/amd64**（Intel J4105 ✓）。

## 交付文件

| 文件 | 说明 |
|---|---|
| `QQ2.py` | 机器人主程序（已支持 3 种文件投递模式，NAS 用共享目录模式） |
| `Dockerfile` | 容器镜像（Python3.11 + 7z + 依赖） |
| `requirements.txt` | pip 依赖 |
| `docker-compose.yml` | NapCat + QQbot 一键编排（改里面的 key 和白名单） |
| 本文件 | 部署步骤 |

## 部署步骤

1. 把整个项目文件夹拷贝到 NAS（例如 `/vol1/docker/qqbot/`），进到该目录。

2. 编辑 **`qqbot-config.yml`**（所有配置都在这里，bot 启动时读取）：
   - **必改**：`deepseek_api_key`、`bocha_api_key` ← 你的两个 key
   - **必改**：`onebot_ws` 改为 `ws://napcat:6700?access_token=xxx`（token 与 NapCat 一致）
   - 白名单 `allowed_groups` / `allowed_users`、人设 `system_prompt`、下载格式/密码/通知开关等按需改
   - NAS 用共享目录投递：`napcat_container: ""`、`napcat_host_dir: "/deliver"`、`napcat_view_dir: "/app/data"`

3. 先起 NapCat：
   ```bash
   docker compose up -d napcat
   ```
   浏览器打开 `http://NAS_IP:6099`，**首次扫码登录 QQ**（之后登录态存 volume，不丢）。
   在 webui 里确认 WebSocket 服务开着、端口 6700、token 与上面一致。

4. 构建并启动 bot：
   ```bash
   docker compose up -d --build qqbot
   ```

5. 看日志确认上线：
   ```bash
   docker compose logs -f qqbot
   ```
   看到「✅ 成功连接 … 等待群消息」即成功。群里 @机器人发 `下载本子123` 验证。

## 常用命令

```bash
docker compose logs -f qqbot       # 看bot日志
docker compose logs -f napcat      # 看NapCat日志
docker compose restart qqbot       # 重启bot
docker compose down                # 全部停止（volume数据保留）
docker compose up -d --build qqbot # 改代码后重建
```

## WebUI（配置界面）

控制器（`qq-controller`）内置了一个 WebUI 服务，用于在浏览器里查看/修改配置（仪表盘 + 系统提示词/连接/模型配置）。

| 项 | 说明 |
|---|---|
| 地址 | `http://NAS_IP:6650/`（compose 已映射 `6650:6650`）→ 打开就是**登录页**，输入令牌即可 |
| 令牌 | 没有单独"关闭校验"的开关：配置为空就自动生成 32 位强令牌并写入 `qqbot-config.yml`；自定义令牌建议 **≥16 位**（短于 8 位会被自动替换，8–15 位会提示） |
| 查看令牌 | 项目目录下的 `webui_token.txt`（容器内 `/app/webui_token.txt`，权限 600，**已被 .gitignore 忽略**），或看 `qqbot-config.yml` 的 `controller.webui_token` |
| 登录方式 | 登录页提交 → 下发 `HttpOnly; SameSite=Strict` Cookie（30 天）；也支持请求头 `X-WebUI-Token`；旧式 `?token=` 仍可用（会 302 跳转并把令牌从地址栏去掉） |
| 防爆破 | 同一 IP 连续 5 次错误令牌 → 锁定 **300 秒**（返回 429），已有会话不受影响；失败尝试会写日志 |
| 仪表盘内容 | API调用次数、今日消耗余额（DeepSeek 实时余额）、容器状态、NapCat 反向连接状态、近30天消耗柱状图 |
| 数据来源 | `stats.json`（路径 `controller.stats_file`，默认 `/data/stats.json`，由主程序写入）+ DeepSeek 余额接口；**接口不可用时页面顶部会显示"演示数据"警示条**（不会静默显示假数据） |
| 密钥显示 | **接口不回显明文**（页面显示"已设置"提示）：输入框留空=保持不变，输入 `__CLEAR__`=清空 |
| 保存行为 | 原子写入 + 自动备份 `qqbot-config.yml.bak`；有 `ruamel.yaml` 时保留注释与格式 |
| 应用并重启 | 按钮会重启 `qq-bot` 容器（等同群里的"重启服务 ai"） |

安全设计：所有接口都要求令牌（登录页除外）；登录失败限速；POST 只接受同源 JSON（CSRF 防护）；
`/api/history/{days}` 上限 365 天；密钥类字段写入前有范围/格式校验；页面不引用任何外部 CDN（离线 NAS 也能秒开）。

安全提示：WebUI 端口**不要暴露到公网**；如需外网访问请走反向代理 + HTTPS，并把 `webui_token` 设成固定强值。
`webui_token.txt` 与 `qqbot-config.yml` 都在 `.gitignore` 里——**别手动 `git add -f` 它们**。

## 数据与磁盘

| 路径 | 用途 | 清理策略 |
|---|---|---|
| `./transfer` | bot→NapCat 传文件的共享目录 | 每次发送成功/失败后自动清；bot 启动时再兜底清一次 |
| volume `bot-data` | 下载缓存(`/data/downloads`) + 日志(`/data/logs`) | 每次发送成功后自动删下载缓存 |
| volume `napcat-data` | NapCat 配置+QQ登录态 | 长期保留，别删（删了要重新扫码） |

## 注意事项

- **QQ 账号**：7x24 在线 + 频繁传文件更容易触发 QQ 安全限制，新小号风险尤其高；
  建议登录一个常用/主号，并控制每日下载量。
- **端口**：6099(webui)、6700(OneBot) 如被占改映射即可；bot 内部走 `napcat:6700` 不需要暴露到外网。
- **jmcomic 用量**：作者呼吁勿批量抓取，本 bot 默认只下 1 个章节，够用就好。
- 升级 NapCat 或换镜像架构请先备份 `napcat-data`。

## 常见问题

- `rich media transfer failed` → 文件过大或该 QQ 号被临时限制：可把 `QQ2_IMG_SUFFIX` 改 `.jpg`，或等 30~60 分钟。
- 传文件报找不到路径 → 检查 `./transfer` 目录存在且两个容器都有读写权限（`ls -ld transfer`）。
- bot 连不上 → `docker compose exec qqbot python -c "import socket;print(socket.gethostbyname('napcat'))"` 测内网解析；并确认 NapCat webui 里 WS 服务已开。
