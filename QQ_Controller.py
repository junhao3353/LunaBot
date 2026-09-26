#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QQ群远程Docker控制器（反向WebSocket模式）
- 控制器作为WS服务端监听端口，NapCat主动反向连接上来
- 避免NapCat正向WS只支持一个客户端连接的限制（qq-bot已占6700）
- 白名单QQ才能发指令
- 指令：关闭服务 / 重启服务 / 重启服务 ai / 重启服务 napcat
- 通过docker.sock控制宿主机容器
- 配置从 qqbot-config.yml 的 controller 段读取，环境变量可覆盖
"""
import asyncio
import json
import os
import re
import time
from datetime import datetime

import websockets
import docker
import yaml

RECONNECT_DELAY = 5
CONFIG_PATH = os.environ.get("CTRL_CONFIG_PATH", "/app/qqbot-config.yml")
WS_HOST = os.environ.get("CTRL_WS_HOST", "0.0.0.0")
WS_PORT = int(os.environ.get("CTRL_WS_PORT", "6701"))

# 待发消息队列：重启NapCat后WS断开，确认消息等重连成功后补发
_pending_msgs = []
# 当前活动的WS连接（NapCat反向连接上来的）
_active_ws = None
# 全局配置（main里加载，handle_message里用）
_cfg = None


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ===================== 配置加载（yml > 环境变量 > 默认） =====================
def load_config():
    cfg = {}
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            ctrl = data.get("controller", {})
            if isinstance(ctrl, dict) and ctrl:
                cfg.update(ctrl)
                log(f"📄 已从配置文件加载控制器设置：{CONFIG_PATH}")
        except Exception as e:
            log(f"⚠️ 读取配置文件失败: {e}")
    else:
        log(f"ℹ️ 未找到配置文件 {CONFIG_PATH}，使用环境变量/默认值")

    if os.environ.get("CTRL_WS_PORT"):
        cfg["ws_port"] = int(os.environ["CTRL_WS_PORT"])
    if os.environ.get("CTRL_ALLOWED_GROUPS"):
        cfg["allowed_groups"] = [int(x) for x in os.environ["CTRL_ALLOWED_GROUPS"].split(",") if x.strip()]
    if os.environ.get("CTRL_ALLOWED_USERS"):
        cfg["allowed_users"] = [int(x) for x in os.environ["CTRL_ALLOWED_USERS"].split(",") if x.strip()]
    if os.environ.get("CTRL_CONTAINER_AI"):
        cfg["container_ai"] = os.environ["CTRL_CONTAINER_AI"]
    if os.environ.get("CTRL_CONTAINER_NAPCAT"):
        cfg["container_napcat"] = os.environ["CTRL_CONTAINER_NAPCAT"]
    if os.environ.get("CTRL_CONTAINER_SELF"):
        cfg["container_self"] = os.environ["CTRL_CONTAINER_SELF"]

    cfg.setdefault("ws_port", WS_PORT)
    cfg.setdefault("allowed_groups", [996930225])
    cfg.setdefault("allowed_users", [3353936945])
    cfg.setdefault("container_ai", "qq-bot")
    cfg.setdefault("container_napcat", "qq-napcat")
    cfg.setdefault("container_self", "qq-controller")
    return cfg


# ===================== Docker操作 =====================
_docker_client = None


def get_docker_client():
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


def docker_restart(name):
    try:
        c = get_docker_client().containers.get(name)
        c.restart(timeout=15)
        log(f"✅ 已重启容器: {name}")
        return True, "已重启"
    except Exception as e:
        log(f"❌ 重启容器 {name} 失败: {e}")
        return False, f"失败: {e}"


def docker_stop(name):
    try:
        c = get_docker_client().containers.get(name)
        c.stop(timeout=15)
        log(f"✅ 已停止容器: {name}")
        return True, "已停止"
    except Exception as e:
        log(f"❌ 停止容器 {name} 失败: {e}")
        return False, f"失败: {e}"


# ===================== 待发消息队列 =====================
def enqueue_pending_msg(group_id, text):
    _pending_msgs.append((group_id, text))


async def flush_pending_msgs(ws):
    if not _pending_msgs:
        return
    log(f"📤 补发 {len(_pending_msgs)} 条待发消息...")
    while _pending_msgs:
        gid, text = _pending_msgs.pop(0)
        await send_group_msg(ws, gid, text)
        await asyncio.sleep(0.3)


# ===================== OneBot消息发送 =====================
async def send_group_msg(ws, group_id, text):
    payload = {
        "action": "send_group_msg",
        "params": {"group_id": group_id, "message": text},
        "echo": f"ctrl_{int(time.time())}"
    }
    try:
        await ws.send(json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        log(f"⚠️ 发消息失败: {e}")


# ===================== 指令解析 =====================
def parse_command(text):
    """只认四个指令：关闭服务 / 重启服务 / 重启服务 ai / 重启服务 napcat"""
    text = text.strip().lower()
    text = re.sub(r'^[的\s]+', '', text)
    if text == "关闭服务":
        return "shutdown", "all"
    if text == "重启服务":
        return "restart", "all"
    m = re.match(r'^重启服务\s*(ai|napcat)$', text)
    if m:
        return "restart", m.group(1)
    return None


# ===================== 执行指令 =====================
async def execute_command(ws, group_id, cmd_type, target):
    global _cfg
    container_ai = _cfg["container_ai"]
    container_napcat = _cfg["container_napcat"]
    container_self = _cfg["container_self"]

    if cmd_type == "shutdown":
        await send_group_msg(ws, group_id, "🔴 收到关闭指令，正在关闭所有服务...")
        await asyncio.sleep(0.5)
        docker_stop(container_ai)
        await asyncio.sleep(1)
        docker_stop(container_napcat)
        await asyncio.sleep(1)
        await send_group_msg(ws, group_id, "🔴 AI和NapCat已关闭。控制器即将退出，下次使用请手动启动 qq-controller。")
        await asyncio.sleep(1)
        docker_stop(container_self)
        return

    if cmd_type == "restart":
        if target == "all":
            await send_group_msg(ws, group_id, "🔄 收到重启指令，先重启AI，再重启NapCat...")
            await asyncio.sleep(0.5)
            ok1, msg1 = docker_restart(container_ai)
            await send_group_msg(ws, group_id, f"{'✅' if ok1 else '❌'} AI {msg1}")
            await asyncio.sleep(2)
            await send_group_msg(ws, group_id, "🔄 正在重启 NapCat...")
            ok2, msg2 = docker_restart(container_napcat)
            enqueue_pending_msg(group_id, f"{'✅' if ok2 else '❌'} NapCat {msg2}")
            enqueue_pending_msg(group_id, "🔄 全部重启完成")
        elif target == "napcat":
            await send_group_msg(ws, group_id, "🔄 正在重启 NapCat...")
            ok, msg = docker_restart(container_napcat)
            enqueue_pending_msg(group_id, f"{'✅' if ok else '❌'} NapCat {msg}")
        elif target == "ai":
            await send_group_msg(ws, group_id, "🔄 正在重启 AI 机器人...")
            ok, msg = docker_restart(container_ai)
            await send_group_msg(ws, group_id, f"{'✅' if ok else '❌'} AI {msg}")
        return


# ===================== 消息处理 =====================
async def handle_message(ws, raw):
    global _cfg
    try:
        data = json.loads(raw)
    except Exception:
        return

    # 忽略API响应（有echo且既无post_type也无message_type）
    if "echo" in data and "post_type" not in data and "message_type" not in data:
        return
    # 消息事件判断：有post_type=message，或有message_type字段（NapCat反向WS可能省略post_type）
    is_message = (data.get("post_type") == "message") or ("message_type" in data)
    if not is_message:
        return
    if data.get("message_type") != "group":
        return

    group_id = data.get("group_id")
    user_id = data.get("user_id")
    raw_msg = data.get("raw_message", "")
    self_id = data.get("self_id")

    allowed_groups = _cfg["allowed_groups"]
    allowed_users = _cfg["allowed_users"]

    if group_id not in allowed_groups:
        return
    if user_id not in allowed_users:
        return

    # 提取去掉CQ码后的纯文字（@判断要用）
    text = re.sub(r"\[CQ:[^]]*\]", "", raw_msg).strip()

    # 必须@了控制器（和AI共用同一个QQ号，@DeepSeek即可；兼容CQ码/@QQ号/@昵称三种格式）
    at_flag = (f"[CQ:at,qq={self_id}]" in raw_msg
               or f"@{self_id}" in raw_msg
               or text.startswith("@"))
    if not at_flag:
        return

    if not text:
        # 只@没说具体指令，控制器不插嘴，让AI正常回复
        return

    log(f"📩 群{group_id} | 用户{user_id}：{text}")

    # 去掉开头的 @昵称 前缀（@DeepSeek 重启服务 napcat → 重启服务 napcat）
    if text.startswith("@"):
        idx = text.find(" ")
        if idx > 0:
            text = text[idx:].strip()

    cmd = parse_command(text)
    if not cmd:
        # 不是控制器指令，直接闭嘴不回复（让AI正常处理）
        return

    cmd_type, target = cmd
    log(f"🎯 执行指令: {cmd_type} {target}")
    await execute_command(ws, group_id, cmd_type, target)


# ===================== 反向WS连接处理 =====================
async def handle_connection(ws):
    global _active_ws
    _active_ws = ws
    log("✅ NapCat 已反向连接，等待指令...")
    await flush_pending_msgs(ws)
    try:
        async for raw in ws:
            await handle_message(ws, raw)
    except websockets.exceptions.ConnectionClosed:
        log("⚠️ NapCat 反向连接断开，等待重连...")
    except Exception as e:
        log(f"❌ 连接处理异常: {e}")
    finally:
        _active_ws = None


# ===================== 主循环 =====================
async def main():
    global _cfg
    _cfg = load_config()

    log("🚀 QQ远程控制器启动（反向WS模式）")
    log(f"📋 群白名单: {_cfg['allowed_groups']}")
    log(f"👤 用户白名单: {_cfg['allowed_users']}")
    log(f"📦 控制目标: AI={_cfg['container_ai']}, NapCat={_cfg['container_napcat']}, 自身={_cfg['container_self']}")

    try:
        get_docker_client().ping()
        log("✅ Docker socket 连接正常")
    except Exception as e:
        log(f"❌ Docker socket 连接失败: {e}")

    ws_port = _cfg.get("ws_port", WS_PORT)
    log(f"🔊 反向WS服务端启动，监听 0.0.0.0:{ws_port}，等待NapCat反向连接...")
    log(f"   请在NapCat WebUI里添加反向WebSocket地址：ws://qq-controller:{ws_port}")

    async with websockets.serve(handle_connection, WS_HOST, ws_port):
        await asyncio.Future()  # 永久运行


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("\n👋 控制器已退出")
