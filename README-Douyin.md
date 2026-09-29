# 抖音解析：主程序只管"提交链接 + 上传"，解析/下载全在独立容器

## 架构（现在）

```
QQ群里发抖音链接
   ↓
主程序(QQ_AI_Bot.py)：只做三件事
   ① POST {解析服务}/api/v1/downloads  提交链接（短链先过 /api/v1/parse）
   ② 轮询 {解析服务}/api/v1/downloads/{id} 直到下载完成
   ③ 把下好的文件取回 → 上传到QQ（视频直发；多图打包zip）
   ↓
解析容器(douyin_tiktok_download_api)：真正的解析 + 下载
   · 负责签名、Cookie、身份池、风控对抗
   · 文件落在它自己的 media-data 卷：<platform>/<作者>/<内容id>/
```

**主程序里已经没有任何抖音解析代码**（`douyin_parse / v2 / v3 / v5 / refresh_cookie / crawlers / Playwright` 全部删除，文件从 200KB 降到 168KB）。
抖音改版时：**只更新解析容器**，主程序不用动。

## 一、部署解析服务（一次性）

```bash
git clone https://github.com/Evil0ctal/Douyin_TikTok_Download_API.git
cd Douyin_TikTok_Download_API
cp .env.example .env          # 按 docker/README.md 生成随机密钥（DTK_SECRET_KEY 等）
# 关键：开启下载器侧车（负责把视频/图片真正存到磁盘）
echo 'DTK_DOWNLOADER_URL=http://downloader:9100' >> .env
docker compose -p dtk -f docker/compose.yml --profile downloader up -d --build

# 首次：从日志里取 setup token，进控制台创建管理员
docker compose -p dtk -f docker/compose.yml logs api | grep -i setup

# 然后在控制台创建 API Key，权限勾选 media:read + media:write
```

## 二、把 bot 接上解析服务（二选一）

**方式 A：让 bot 用解析服务的内网名（推荐，容器间直连）**
```bash
# bot 栈先起，再把 bot 接进解析服务的网络（解析栈项目名 dtk → 网络名 dtk_default）
docker network connect dtk_default qq-bot
```
`.env` 里写：
```dotenv
QQ2_DOUYIN_API_BASE=http://dtk-api:8000
QQ2_DOUYIN_API_KEY=在控制台创建的key
```

**方式 B：走宿主机端口（不接网络）**
```bash
# 解析服务暴露到宿主（默认只绑 127.0.0.1，改成 0.0.0.0 才能被容器访问）
DTK_BIND_HOST=0.0.0.0 docker compose -p dtk -f docker/compose.yml --profile downloader up -d
```
```dotenv
QQ2_DOUYIN_API_BASE=http://<NAS的IP>:8000
QQ2_DOUYIN_API_KEY=...
```

改完 `.env` 后：`docker compose -p qqbot up -d`（重启 bot 生效）。配置项也支持写在 `qqbot-config.yml`（见下）。

## 三、可选：直接读共享卷（省一次 HTTP 传输）

文件本来由解析容器写到 `media-data` 卷。把同一个卷挂进 bot，就能直接读文件：

1. 打开主 `docker-compose.yml`，取消这两处注释：
   ```yaml
   # qqbot.volumes 里：
   - dtk-media:/dtk_media:ro
   # 文件末尾 volumes 里：
   dtk-media:
     external: true
     name: dtk_media-data
   ```
2. `.env` 里加：`QQ2_DOUYIN_MEDIA_DIR=/dtk_media`
3. `docker compose -p qqbot up -d`

不挂也行——bot 会自动改用 API `GET /api/v1/downloads/{id}/files/{name}` 取文件。

## 四、配置项一览

| 位置 | 项 | 说明 |
|---|---|---|
| `.env` | `QQ2_DOUYIN_API_BASE` | 解析服务地址（优先级高于 yml） |
| `.env` | `QQ2_DOUYIN_API_KEY` | 解析服务 Key（需 media:read + media:write） |
| `.env` | `QQ2_DOUYIN_MEDIA_DIR` | 可选：共享媒体卷在本容器内的路径 |
| `qqbot-config.yml` | `enable_douyin_api` | 总开关 |
| `qqbot-config.yml` | `douyin_api_base` / `douyin_api_key` | 同上（Key 建议只放 .env） |
| `qqbot-config.yml` | `douyin_poll_interval` | 轮询间隔（默认 3 秒） |
| `qqbot-config.yml` | `douyin_job_timeout` | 单任务最长等待（默认 300 秒） |
| `qqbot-config.yml` | `douyin_media_dir` | 同上（共享卷路径） |

## 五、怎么验证

1. 群里发一条抖音链接，bot 会回「已提交解析服务，下载完成后自动发送…」
2. 看 bot 日志：
   ```
   🎬 收到抖音链接，提交解析服务：https://v.douyin.com/xxx/
   🎬 解析任务已创建：xxxxxxxx（state=pending）
   ✅ 抖音文件已发送
   ```
3. 看解析服务日志：`docker compose -p dtk logs -f api worker downloader`
4. 解析服务控制台里能看到 download 记录与文件状态

## 六、维护成本

| | 以前 | 现在 |
|---|---|---|
| 抖音改签名 | 改主程序 + crawlers，约 5 天一次 | **只更新解析容器**（`docker compose -p dtk pull && up -d`），主程序不动 |
| 多图/图文 | 主程序自己下载+转png+打包 | 解析容器下载，主程序只打包上传 |
| 换解析服务商 | 重写解析代码 | 只改 `.env` 两行 |

> 建议再给解析栈加自动更新（只更新解析容器，不动 bot）：
> ```yaml
> watchtower:
>   image: containrrr/watchtower
>   restart: unless-stopped
>   volumes: ["/var/run/docker.sock:/var/run/docker.sock"]
>   command: --interval 86400 --cleanup dtk-api dtk-worker dtk-downloader
> ```

## 七、注意事项

- 解析服务需要 API Key（v5 默认全部接口鉴权）；Key 权限至少要 `media:read` + `media:write`
- 短链（`v.douyin.com/xxx`）主程序会先送 `/api/v1/parse` 展开；若该接口不可用会退化为"跟随跳转"展开链接
- 解析容器的 media 卷由它自己的容量策略清理（`media.max_bytes`），bot 上传完只删自己取回的临时文件
- `crawlers/` 目录已不再被主程序使用，可以从仓库删除（历史里的 Cookie 也已清理）
