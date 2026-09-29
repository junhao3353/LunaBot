# -*- coding: utf-8 -*-
"""抖音解析服务（douyin_tiktok_download_api）的瘦客户端 —— 主程序只通过本模块与解析服务打交道。

职责划分：
  · 本模块：提交链接、轮询任务状态、把解析服务已下载好的文件取回本地
  · 解析服务容器：真正的解析 + 下载（抖音改版时只需更新那个容器）
主程序不再包含任何抖音签名/Cookie/Playwright 解析逻辑。
"""
from __future__ import annotations

import os
import re
import time
from typing import Optional

import requests

_URL_RE = re.compile(r"https?://[^\s'\"<>]+")


def _extract_url(text: str) -> str:
    """从分享文案里抠出链接（抖音分享常带一堆文字）"""
    m = _URL_RE.search(text or "")
    return m.group(0) if m else (text or "").strip()


def _headers(api_key: str) -> dict:
    h = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    if api_key:
        h["Authorization"] = f"Bearer {api_key}"
    return h


def _first(d: dict, keys) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, (str, int)) and str(v).strip():
            return str(v).strip()
    return None


def resolve_short_link(base: str, api_key: str, link: str, *, timeout: float = 15.0,
                       poll_interval: float = 1.5, poll_limit: int = 20) -> str:
    """短链(v.douyin.com/xxx)需要先展开：优先走解析服务的 /api/v1/parse；
    服务不支持时退化为"跟随 HTTP 跳转"（只是展开链接，不做任何解析）。"""
    link = _extract_url(link)
    if "v.douyin.com" not in link and "/share/" not in link:
        return link
    # 1) 走解析服务的 parse 接口
    try:
        r = requests.post(f"{base.rstrip('/')}/api/v1/parse", json={"url": link},
                          headers=_headers(api_key), timeout=timeout)
        if r.status_code in (200, 202):
            data = r.json() if r.text else {}
            payload = data.get("data", data) if isinstance(data, dict) else {}
            task_id = _first(payload, ("task_id", "id", "job_id"))
            direct = _first(payload, ("url", "content_id", "resolved_url"))
            if direct and direct.startswith("http"):
                return direct
            if task_id:
                for _ in range(poll_limit):
                    time.sleep(poll_interval)
                    pr = requests.get(f"{base.rstrip('/')}/api/v1/parse/{task_id}",
                                      headers=_headers(api_key), timeout=timeout)
                    if pr.status_code != 200:
                        continue
                    pd = pr.json() if pr.text else {}
                    pp = pd.get("data", pd) if isinstance(pd, dict) else {}
                    got = _first(pp, ("url", "resolved_url", "share_url"))
                    if got and got.startswith("http"):
                        return got
                    cid = _first(pp, ("content_id",))
                    plat = _first(pp, ("platform",)) or "douyin"
                    if cid:
                        return f"https://www.douyin.com/video/{cid}"
    except Exception:
        pass
    # 2) 退化：跟随跳转拿最终地址
    try:
        r = requests.get(link, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, allow_redirects=True)
        final = r.url or link
        if "douyin.com" in final and final != link:
            return final
    except Exception:
        pass
    return link


def start_download(base: str, api_key: str, link: str, *, timeout: float = 20.0) -> dict:
    """POST /api/v1/downloads 提交下载任务。
    返回 {"download_id":..., "state":..., "directory":..., "err":None|str}"""
    base = (base or "").rstrip("/")
    if not base:
        return {"err": "未配置 douyin_api_base"}
    link = resolve_short_link(base, api_key, link)
    try:
        r = requests.post(f"{base}/api/v1/downloads",
                          json={"url": link, "skip_existing": False},
                          headers=_headers(api_key), timeout=timeout)
    except Exception as e:
        return {"err": f"提交失败：{e}"}
    if r.status_code not in (200, 202):
        return {"err": f"提交失败 HTTP {r.status_code}：{(r.text or '')[:160]}"}
    try:
        data = r.json() or {}
    except Exception:
        return {"err": "解析服务返回不是JSON"}
    payload = data.get("data", data) if isinstance(data, dict) else {}
    download_id = _first(payload, ("download_id", "id"))
    if not download_id:
        return {"err": f"返回里没有 download_id：{str(payload)[:160]}"}
    return {
        "download_id": download_id,
        "state": _first(payload, ("state",)) or "pending",
        "directory": _first(payload, ("directory",)) or "",
        "archived": payload.get("archived"),
        "err": None,
    }


def wait_download(base: str, api_key: str, download_id: str, *, timeout: float = 300.0,
                  interval: float = 3.0) -> dict:
    """轮询 GET /api/v1/downloads/{id} 直到任务结束。
    返回 {"state":..., "files":[{name,state,size}], "directory":..., "err":None|str}"""
    base = (base or "").rstrip("/")
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        try:
            r = requests.get(f"{base}/api/v1/downloads/{download_id}",
                             headers=_headers(api_key), timeout=20)
        except Exception as e:
            time.sleep(interval)
            last = {"err": f"查询失败：{e}"}
            continue
        if r.status_code != 200:
            return {"err": f"查询任务失败 HTTP {r.status_code}：{(r.text or '')[:160]}"}
        try:
            data = r.json() or {}
        except Exception:
            return {"err": "查询返回不是JSON"}
        row = data.get("data", data) if isinstance(data, dict) else {}
        state = str(row.get("state") or "").lower()
        files = [f for f in (row.get("files") or []) if isinstance(f, dict)]
        last = {"state": state, "files": files, "directory": row.get("directory") or ""}
        if state in ("done", "completed", "success", "failed", "error", "cancelled", "partial"):
            return last
        time.sleep(interval)
    return {"err": f"等待超时（{timeout:.0f}秒），最后状态：{last.get('state') or '未知'}"}


def fetch_file(base: str, api_key: str, download_id: str, name: str, dest_path: str,
               *, media_dir: str = "", directory: str = "", timeout: float = 300.0) -> dict:
    """把一个已下载好的文件取回本地。
    优先直接读共享的 media 目录（快、不走网络）；读不到再走 API /downloads/{id}/files/{name}"""
    # 1) 共享目录
    if media_dir and directory:
        src = os.path.join(media_dir, directory, name)
        if os.path.isfile(src):
            try:
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                if os.path.abspath(src) != os.path.abspath(dest_path):
                    import shutil
                    shutil.copy2(src, dest_path)
                return {"path": dest_path, "err": None}
            except Exception as e:
                return {"err": f"读取共享目录失败：{e}"}
    # 2) HTTP 取回
    base = (base or "").rstrip("/")
    try:
        r = requests.get(f"{base}/api/v1/downloads/{download_id}/files/{requests.utils.quote(name)}",
                         headers=_headers(api_key), timeout=timeout, stream=True)
        if r.status_code != 200:
            return {"err": f"取回文件失败 HTTP {r.status_code}"}
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        if os.path.getsize(dest_path) < 1024:
            return {"err": "取回的文件过小"}
        return {"path": dest_path, "err": None}
    except Exception as e:
        return {"err": f"取回文件异常：{e}"}


def pick_media(files) -> dict:
    """从任务文件列表里判断是视频还是图文，返回 {"video":name, "images":[names]}"""
    video, images = None, []
    for f in files or []:
        name = str(f.get("name") or "")
        if not name or str(f.get("state") or "").lower() not in ("done", "completed", "success", ""):
            continue
        low = name.lower()
        if low.endswith((".mp4", ".mov", ".mkv", ".flv", ".webm")) and video is None:
            video = name
        elif low.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic")):
            images.append(name)
    return {"video": video, "images": images}
