#每次更改代码的时候请备份到backup
# 本次修改（备份见 backup\QQ2_20260904_182434.py）：
#  1. 新增单连接消息分发器：事件全部进队列，API响应按echo交给等待方，不再丢消息
#  2. 图片识别/DeepSeek调用改为 asyncio.to_thread，不再卡死事件循环
#  3. ENABLE_WEB_SEARCH 开关真正生效（关闭时不给AI下发搜索工具）
#  4. 修复多个tool_call时assistant消息被重复追加的问题
#  5. 兼容OneBot消息段数组格式（message为list时也能识别@/图片/回复）
#  6. 私聊空消息不再误填"群友@了你"
#  7. DSML解析改为完整正则，支持多条调用和CDATA/实体转义
#  8. 新增：群内 @机器人 发送「下载本子<ID>」→ jmcomic下载 → 打包zip → 上传群文件
#  9. 本子下载修复：目录规则改为 Bd_Aid_Pid（专辑id/章节id/图片），多章节本子不再互相覆盖；
#     下载前清理同名旧目录；打包改为递归统计图片；上传失败时输出Lagrange返回的具体错误
# 10. 上传适配 NapCat(Docker)：先 docker cp 复制进容器再传容器内路径+upload_file=True；
#     已下载过的本子(zip还在)重复发指令时直接重传，不再重复下载
# 11. 打包前把图片转成PNG/JPEG再压zip（QQ会扫描zip里的图片内容，疑似违规直接撤/拒收；
#     实测发现原始webp包被QQ安全扫描拦截、同账号传文本文件却正常）
# 12. 转PNG后仍被撤 → 新增加密打包：ZIP_PASSWORD非空时用 7z 打 AES-256 加密zip（QQ扫不开内容）
# 13. 章节选择「下载本子<id> pN」（不写默认只下第1章）；下载时让jmcomic直接存png；
#     任务完成后自动清理本地+容器缓存；QQ消息精简；文件名直接带解压密码(如 350234密码jm.zip)
# 14. 修复审查发现的坑：①配置校验改为无条件执行(缺key直接报错退出) ②AI调用异常兜底，不再打断WS连接
#     ③websockets max_size=64MB(防大消息触发1009断连) ④抖音/汽水下载改后台任务+同链接去重+每会话冷却
#     ⑤启动清理只删自己生成且超10分钟的临时文件，且只跑一次
# 15. 分段回复：AI回复按空行/长句拆成多条顺序发送（首条沿用随机延迟，段间隔0.8~5秒）；
#     拆出来超过 split_max_parts(默认3) 条则【整条一次发出】；提示词要求聊天1~3段、不固定条数
# 16. 修复 Windows GBK 控制台打印 emoji 抛 UnicodeEncodeError 把逻辑打断的问题
# 17. 好感度降权：提示词改成"内心印象标记/极淡参考"，新增【内部规则】禁止在回复里提好感度/档案/记忆；
#     update_member 工具要求克制（多数情况传0）；"查看记忆"不再显示好感度数值；新增 enable_favor 开关
# 18. 群聊旁听缓存：没@机器人的群消息也缓存（默认每群10条、每条100字，满了丢最旧，取走即清空），
#     被@时作为"背景闲聊"一并投给AI（不入对话历史、不重复塞），私聊不受影响
import asyncio
from typing import Optional
import jmcomic
import websockets
import json
import requests
import os
import sys
import re
import html
import zipfile
import subprocess
import time
import random
import yaml
from collections import defaultdict
from datetime import datetime

# Windows(GBK控制台)下打印 emoji/生僻字会抛 UnicodeEncodeError 把逻辑打断；这里统一改成交替字符输出
try:
    sys.stdout.reconfigure(errors="replace")
    sys.stderr.reconfigure(errors="replace")
except Exception:
    pass


def _run_subprocess(cmd, **kwargs):
    """封装subprocess.run，统一UTF-8编码+errors=replace，抑制PyCharm对encoding参数的类型误报。
    默认 capture_output=True, text=True, encoding='utf-8', errors='replace'；可通过kwargs覆盖。"""
    kwargs.setdefault("capture_output", True)
    kwargs.setdefault("text", True)
    kwargs.setdefault("encoding", "utf-8")
    kwargs.setdefault("errors", "replace")
    # noinspection PyTypeChecker
    return subprocess.run(cmd, **kwargs)


def _post_json(url, headers, payload, timeout, what="接口"):
    """POST并解析JSON：网络错误/非JSON响应统一抛 RuntimeError（调用方兜底），
    避免 requests 或 .json() 的异常冒泡到 main() 把整条WS连接打断。"""
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    except Exception as e:
        raise RuntimeError(f"{what}请求失败：{e}")
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"{what}返回非JSON（HTTP {getattr(resp, 'status_code', '?')}）：{str(getattr(resp, 'text', ''))[:200]}")


def _build_chat_data(model, messages, thinking_enabled):
    """构造DeepSeek chat/completions请求体，统一处理思考模式开关。
    开启：thinking.type=enabled + reasoning_effort，不传temperature（思考模式下temperature不生效）。
    关闭：thinking.type=disabled，保留temperature=0.7。"""
    data = {"model": model, "messages": messages}
    if thinking_enabled:
        data["thinking"] = {"type": "enabled"}
        if REASONING_EFFORT:
            data["reasoning_effort"] = REASONING_EFFORT
    else:
        data["thinking"] = {"type": "disabled"}
        data["temperature"] = 0.7
    return data


def _log_reasoning(message, label=""):
    """如果API响应里有reasoning_content（思考模式），打印思维链字节数，方便确认思考模式是否生效。
    没有reasoning_content说明思考模式没开或模型本轮没思考。"""
    rc = message.get("reasoning_content") if isinstance(message, dict) else None
    if rc:
        rc_bytes = len(rc.encode("utf-8", errors="replace"))
        prefix = f"{label} " if label else ""
        print(f"🧠 {prefix}思考模式生效，思维链 {rc_bytes} 字节（{len(rc)} 字符）")

# ===================== 日志功能：聊天记录和控制台日志分两个文件 =====================
_log_dir = os.environ.get("QQ2_LOG_DIR", os.path.dirname(os.path.abspath(__file__)))
os.makedirs(_log_dir, exist_ok=True)  # 容器里QQ2_LOG_DIR可能是新建volume子目录，先建好再写日志
_kzt_path = os.path.join(_log_dir, "QQKZT.txt")  # 控制台日志：搜索、视觉、系统信息
_lt_path = os.path.join(_log_dir, "QQLT.txt")    # 纯QQ聊天记录
_kzt_file = open(_kzt_path, "a", encoding="utf-8")
_lt_file = open(_lt_path, "a", encoding="utf-8")
MAX_LOG_SIZE = 5 * 1024 * 1024  # 日志文件最大5MB

def _check_size(path, fobj):
    """超过5MB就删除重建，返回文件对象"""
    try:
        if os.path.exists(path) and os.path.getsize(path) > MAX_LOG_SIZE:
            fobj.close()
            os.remove(path)
            return open(path, "a", encoding="utf-8")
    except:
        pass
    return fobj

def print(*args, **kwargs):
    """控制台日志：输出到控制台和QQKZT.txt，带时间戳，多行每行都带时间戳"""
    global _kzt_file
    msg = " ".join(str(a) for a in args)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = msg.split("\n")
    line = "\n".join(f"[{timestamp}] {p}" for p in parts)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    try:
        _kzt_file = _check_size(_kzt_path, _kzt_file)
        _kzt_file.write(line + "\n")
        _kzt_file.flush()
    except:
        pass

def chat_print(*args, **kwargs):
    """纯聊天记录：输出到控制台和QQLT.txt，带时间戳，多行每行都带时间戳"""
    global _lt_file
    msg = " ".join(str(a) for a in args)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = msg.split("\n")
    line = "\n".join(f"[{timestamp}] {p}" for p in parts)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    try:
        _lt_file = _check_size(_lt_path, _lt_file)
        _lt_file.write(line + "\n")
        _lt_file.flush()
    except:
        pass
# ================================================================================

# ===================== 配置区域（所有值从 qqbot-config.yml 读取；此处仅声明变量，不内置任何密钥/白名单/人设） =====================
ONEBOT_WS = None
DEEPSEEK_API_KEY = None
DEEPSEEK_MODEL = None
DEEPSEEK_VISION_MODEL = None
BOCHA_API_KEY = None
MAX_CONTEXT_LEN = None
COOLDOWN = None
ALLOWED_GROUPS = None
ALLOWED_USERS = None
ENABLE_WEB_SEARCH = None
ENABLE_NOTIFY = None
SEARCH_RESULT_NUM = None
CONTROLLER_ALLOWED_USERS = None  # 控制器白名单：只有这些QQ发的控制指令AI才跳过回复，非白名单正常回复
ENABLE_THINKING_CHAT = None    # 对话模型思考模式开关（从配置文件加载）
ENABLE_THINKING_VISION = None  # 视觉模型思考模式开关（从配置文件加载）
REASONING_EFFORT = None        # 思考强度 low/high/max（从配置文件加载，默认high）
ENABLE_DOWNLOAD = True         # 下载总开关（true=接受下载任务，false=所有下载任务拒绝不回复）
ENABLE_SEND_DELAY = True       # 发送消息随机延迟开关（防秒回被检测）
SEND_DELAY_MIN = 1             # 随机延迟最小秒数
SEND_DELAY_MAX = 5             # 随机延迟最大秒数
# ---- 分段回复（把AI一段话拆成多条发，降低人机感） ----
ENABLE_SPLIT_REPLY = True      # 总开关：true=按段落拆成多条消息发送
SPLIT_DELAY_MIN = 0.8          # 段与段之间的随机延迟下限（秒）
SPLIT_DELAY_MAX = 2.0          # 段与段之间的随机延迟上限（秒）
SPLIT_MAX_PARTS = 3            # 最多拆成几条：算出来超过这个数就【不拆】，整条一次发出（防太碎/太人机）
SPLIT_PART_MAX_LEN = 100       # 单段超过该字数时再按句号等标点细拆
USE_CHAT_MODEL_FOR_VISION = False  # true=图片识别也用语言模型（V4.1原生多模态），false=用单独的视觉模型（原逻辑）

# ---- AI回复转语音（edge-tts，免费在线合成；true=所有AI回复都发成语音条，false=原文本逻辑） ----
ENABLE_TTS = False             # 总开关（配置文件 enable_tts 覆盖）
TTS_VOICE = "zh-CN-XiaoyiNeural"  # 音色：晓伊女声（配置文件 tts_voice 覆盖）
TTS_ENGINE = "edge"            # TTS引擎：edge=微软edge-tts（免费），volc=火山引擎（豆包音色）
# 火山引擎TTS配置（TTS_ENGINE=volc时生效）
VOLC_API_KEY = ""            # 火山引擎API Key（新版大模型用）
# 旧版参数（已废弃，新版用API Key）
VOLC_CLUSTER = "volcano_mega"   # （废弃）
VOLC_VOICE = "zh_female_vv_uranus_bigtts"  # 音色ID
PEAK_PERIOD_NAME = "高峰"       # 高峰时段自定义显示名（配置文件可改，如"梁文峰"）
OFFPEAK_PERIOD_NAME = "空闲"    # 空闲时段自定义显示名（配置文件可改，如"梁文谷"）

# ---- 永久记忆存档（memory.json：对话上下文+成员档案印象值+全局备忘，重启不丢） ----
ENABLE_MEMORY = True           # 永久记忆总开关
MEMORY_CONTEXT_TURNS = 5       # 对话上下文持久化轮数（1轮=用户+AI各一条）
MEMORY_INJECT_PROMPT = False   # 是否把成员档案/印象值/备忘注入system提示词；false=只记录存档+恢复上下文，完全不碰人格提示词（避免记忆把AI性格带偏）
MEMORY_FILE = None             # memory.json 绝对路径（配置加载时确定）
memory_store = None            # MemoryStore 全局单例（配置加载后初始化）
# ---- 印象值：AI自主记录对群成员的印象/特点/称呼，可参考但不被数值绑架，绝不在群里透露 ----
ENABLE_FAVOR = True            # false=完全不注入印象值、也不允许AI修改印象值

# ---- 群聊旁听缓存：没@机器人的群消息也存起来当上下文 ----
# 每群最多 AMBIENT_MAX_MSGS 条，满了丢最旧的；每条截断到 AMBIENT_MSG_MAX_LEN 字；不设过期
ENABLE_AMBIENT_CACHE = True
AMBIENT_MAX_MSGS = 10
AMBIENT_MSG_MAX_LEN = 100
_ambient_msgs = defaultdict(list)   # chat_id -> [{"user_id":.., "text":..}]

# ---- AI表情包（扫描表情包目录，AI自主判断是否发送、选哪个，以图片消息发出） ----
ENABLE_EMOJI = True            # 表情包总开关
EMOJI_DIR = None               # 表情包目录绝对路径（配置加载时确定）
EMOJI_MAP = {}                 # 表情名(文件名去扩展名) -> 图片绝对路径
_EMOJI_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".webp")

SYSTEM_PROMPT = None  # 人设从 qqbot-config.yml 的 system_prompt 加载

# 回复风格硬限制（防人机感，自动追加在人设后面，不占用人设内容）
REPLY_STYLE_PROMPT = """

【回复风格硬性规则】
1. 像真实群友一样说话：短、松、随意，正常人聊天一次就说1-2句话，最多3句
2. 禁止客服腔、助手腔、小作文，别讲大道理，别科普，别列步骤
3. 标点只用：，？！ 这三个。禁止用句号、省略号、波浪号、冒号、分号
4. 禁止任何动作描写，比如（挠头）(叹气)【捂脸】这种
5. 禁止自称AI、助手、语言模型、机器人，你就是群里的一个人
6. 别人问的问题没那么复杂的时候，直接说重点，别铺垫
7. 不要每次都长篇大论，有时候一个"？"、"嗯？"、"行吧"就够了
8. 说话要有烟火气，像跟熟人聊天，不要像写作文
"""
# ==================================================================

# ===================== 环境变量覆盖（NAS/Docker部署用；不设则用上面的默认值） =====================
def _format_now():
    """返回当前时间戳字符串：MM-DD HH:MM，给AI看用"""
    import datetime
    now = datetime.datetime.now()
    return now.strftime("%m-%d %H:%M")

def _env_bool(name: str, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")

def _env_int_list(name: str, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return [int(p) for p in parts] if parts else default

ONEBOT_WS = os.environ.get("QQ2_ONEBOT_WS", ONEBOT_WS)
DEEPSEEK_API_KEY = os.environ.get("QQ2_DEEPSEEK_KEY", DEEPSEEK_API_KEY)
BOCHA_API_KEY = os.environ.get("QQ2_BOCHA_KEY", BOCHA_API_KEY)
ALLOWED_GROUPS = _env_int_list("QQ2_ALLOWED_GROUPS", ALLOWED_GROUPS)
ALLOWED_USERS = _env_int_list("QQ2_ALLOWED_USERS", ALLOWED_USERS)
ENABLE_WEB_SEARCH = _env_bool("QQ2_ENABLE_WEB_SEARCH", ENABLE_WEB_SEARCH)
ENABLE_NOTIFY = _env_bool("QQ2_ENABLE_NOTIFY", ENABLE_NOTIFY)
_raw_srn = os.environ.get("QQ2_SEARCH_RESULT_NUM")
SEARCH_RESULT_NUM = int(_raw_srn) if _raw_srn else SEARCH_RESULT_NUM

# 存储每个群的对话上下文
group_history = defaultdict(list)
last_reply_time = defaultdict(float)

# 挂起的API请求：echo -> asyncio.Future，由 dispatcher 负责回填
_pending_echo = {}
# 发送锁：所有OneBot action串行发送，避免并发写同一条WebSocket导致帧错乱
_send_lock = asyncio.Lock()
# 本子下载相关状态
_download_lock = asyncio.Lock()  # 同一时间只下载一个本子
_downloading_aids = set()        # 正在下载的本子id，防止重复下载
_bg_tasks = set()                # 保存后台任务强引用，防止被GC回收
# 链接下载（抖音/汽水）后台任务状态：同链接去重 + 每会话冷却，避免阻塞主循环被刷爆
_download_tasks = {}             # (kind, url) -> task 正在处理的任务
_last_link_download = {}         # chat_id -> 最近一次链接下载开始时间
LINK_DOWNLOAD_COOLDOWN = 10      # 同一会话链接下载冷却（秒）


def web_search(query: str, num_results: int = 3) -> str:
    """用博查AI联网搜索，返回搜索结果摘要"""
    try:
        url = "https://api.bochaai.com/v1/web-search"
        headers = {
            "Authorization": f"Bearer {BOCHA_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "query": query,
            "count": num_results,
            "summary": True
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        res_json = resp.json()

        # 解析搜索结果
        results = []
        data = res_json.get("data", {})
        # 博查返回格式：data.webPages.value
        web_pages = data.get("webPages", {})
        value_list = web_pages.get("value", [])

        for item in value_list[:num_results]:
            title = item.get("name", "")  # 博查用name字段
            snippet = item.get("snippet", "")  # 博查用snippet字段
            url = item.get("url", "")
            display_url = item.get("displayUrl", "")

            if title and snippet:
                result_str = f"标题：{title}"
                if display_url:
                    result_str += f"\n来源：{display_url}"
                result_str += f"\n摘要：{snippet}"
                results.append(result_str)

        if results:
            return "\n\n".join(results)
        return "未找到相关搜索结果"
    except Exception as e:
        return f"搜索出错：{str(e)}"


def github_search(query: str, num_results: int = 3) -> str:
    """用GitHub官方Search API搜开源仓库，找具体项目/repo/插件/源码时比通用网页搜索精准。无需token。"""
    try:
        resp = requests.get(
            "https://api.github.com/search/repositories",
            params={"q": query, "per_page": num_results, "sort": "stars", "order": "desc"},
            headers={"Accept": "application/vnd.github+json", "User-Agent": "qqbot-bot"},
            timeout=15
        )
        if resp.status_code != 200:
            return f"GitHub搜索失败 HTTP {resp.status_code}"
        items = resp.json().get("items", [])
        if not items:
            return "GitHub上未找到相关仓库"
        lines = []
        for it in items:
            lines.append(
                f"仓库名：{it.get('full_name','')}\n"
                f"地址：{it.get('html_url','')}\n"
                f"Star：{it.get('stargazers_count',0)}｜Fork：{it.get('forks_count',0)}｜语言：{it.get('language') or '未知'}\n"
                f"描述：{it.get('description') or '无描述'}\n"
                f"最近更新：{str(it.get('updated_at',''))[:10]}"
            )
        return "\n\n".join(lines)
    except Exception as e:
        return f"GitHub搜索出错：{str(e)}"


def recognize_image(image_url: str, prompt: str = "请详细描述这张图片的内容，包括图片中的文字、物体、人物、场景、图表等所有可见信息。") -> str:
    """调用DeepSeek视觉模型识别图片内容（先下载转base64，避免防盗链）"""
    try:
        import base64
        # 先下载图片
        print(f"📥 正在下载图片...")
        img_resp = requests.get(image_url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        if img_resp.status_code != 200:
            return f"图片下载失败：HTTP {img_resp.status_code}"

        # 转base64
        img_base64 = base64.b64encode(img_resp.content).decode("utf-8")
        # 判断图片格式
        content_type = img_resp.headers.get("Content-Type", "image/jpeg")
        if "png" in content_type:
            img_format = "png"
        elif "gif" in content_type:
            img_format = "gif"
        elif "webp" in content_type:
            img_format = "webp"
        else:
            img_format = "jpeg"

        data_url = f"data:image/{img_format};base64,{img_base64}"
        print(f"📦 图片已转base64，大小：{len(img_base64)}字符")

        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        messages = [
            {"role": "system", "content": "你是一个专业的图片识别助手，需要详细、准确地描述图片内容。"},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}}
                ]
            }
        ]
        data = {
            "model": DEEPSEEK_VISION_MODEL,
            "messages": messages,
            "temperature": 0.3
        }
        resp = requests.post("https://api.deepseek.com/v1/chat/completions", headers=headers, json=data, timeout=60)
        res_json = resp.json()
        if "choices" in res_json:
            return res_json["choices"][0]["message"]["content"].strip()
        return f"图片识别失败：{res_json.get('error', '未知错误')}"
    except Exception as e:
        return f"图片识别出错：{str(e)}"


def recognize_image_data_url(data_url, prompt="详细、准确地描述这张图片的全部内容，包括其中的文字、界面元素、人物、场景等。"):
    """用已转好的base64 data_url直接调视觉模型，把图片解析成文字描述（不再下载）。"""
    try:
        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        messages = [
            {"role": "system", "content": "你是一个专业的图片识别助手，需要详细、准确地描述图片内容。"},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}}
                ]
            }
        ]
        data = {"model": DEEPSEEK_VISION_MODEL, "messages": messages, "temperature": 0.3}
        resp = requests.post("https://api.deepseek.com/v1/chat/completions", headers=headers, json=data, timeout=60)
        res_json = resp.json()
        if "choices" in res_json:
            return res_json["choices"][0]["message"]["content"].strip()
        return f"图片识别失败：{res_json.get('error', '未知错误')}"
    except Exception as e:
        return f"图片识别出错：{str(e)}"


def extract_images_from_message(message: str) -> list:
    """从CQ码消息中提取图片URL列表"""
    image_urls = []
    # 匹配 [CQ:image,file=xxx,url=xxx,...] 格式
    pattern = r'\[CQ:image,[^\]]*url=([^,\]]+)'
    matches = re.findall(pattern, message)
    for url in matches:
        # URL可能被转义，需要处理
        clean_url = url.replace('&amp;', '&').strip()
        if clean_url.startswith('http'):
            image_urls.append(clean_url)
    return image_urls


def extract_videos_from_message(message: str) -> list:
    """从CQ码消息中提取视频URL列表。
    匹配 [CQ:video,url=...] 和 [CQ:file,file=xxx.mp4,url=...]（文件形式发的视频）"""
    video_urls = []
    # 1. 标准视频消息
    for url in re.findall(r'\[CQ:video,[^\]]*url=([^,\]]+)', message):
        clean_url = url.replace('&amp;', '&').strip()
        if clean_url.startswith('http'):
            video_urls.append(clean_url)
    # 2. 文件形式发的视频（.mp4/.mov/.avi/.mkv/.webm/.flv等）
    for m in re.finditer(r'\[CQ:file,[^\]]*?file=([^,\]]+\.(?:mp4|mov|avi|mkv|webm|flv|m4v))[^\]]*?url=([^,\]]+)', message, re.I):
        clean_url = m.group(2).replace('&amp;', '&').strip()
        if clean_url.startswith('http'):
            video_urls.append(clean_url)
    return video_urls


def extract_records_from_message(message: str) -> list:
    """从CQ码消息中提取语音信息列表。
    返回 [{"file": "xxx.silk", "url": "..."或缺省}, ...]（QQ语音是silk格式，file为文件名）"""
    records = []
    for m in re.finditer(r'\[CQ:record,([^\]]+)\]', message):
        params = m.group(1)
        file_m = re.search(r'file=([^,\]]+)', params)
        if not file_m:
            continue
        rec = {"file": file_m.group(1).replace('&amp;', '&').strip()}
        url_m = re.search(r'url=([^,\]]+)', params)
        if url_m:
            u = url_m.group(1).replace('&amp;', '&').strip()
            if u.startswith('http'):
                rec["url"] = u
        records.append(rec)
    return records


# ===================== 图片缓存：手机端先分开发图、再发文字，图片先base64缓存，发文字时一起投给视觉模型 =====================
# key=(chat_id, user_id)，value=[{"data_url": data_url, "ts": 时间戳}]
_image_cache = {}
_IMAGE_CACHE_MAX = 5    # 每个用户最多缓存5张，超过拒绝
_IMAGE_CACHE_TTL = 60   # 每张图片缓存60秒，过期丢弃


def _cache_add_image(chat_id, user_id, data_url):
    """添加图片到缓存。返回(当前有效数量, 是否因超量被拒绝)。自动清理该用户的过期图片。"""
    now = time.time()
    key = (chat_id, user_id)
    lst = [x for x in _image_cache.get(key, []) if now - x["ts"] < _IMAGE_CACHE_TTL]
    if len(lst) >= _IMAGE_CACHE_MAX:
        _image_cache[key] = lst
        return len(lst), True
    lst.append({"data_url": data_url, "ts": now})
    _image_cache[key] = lst
    return len(lst), False


def _cache_take_images(chat_id, user_id):
    """取出并清空该用户的缓存图片，返回有效data_url列表（自动过滤过期）。"""
    now = time.time()
    key = (chat_id, user_id)
    lst = _image_cache.pop(key, [])
    return [x["data_url"] for x in lst if now - x["ts"] < _IMAGE_CACHE_TTL]


# ===================== 视频识别缓存：群友发视频→下载→ffprobe查时长(>10分钟拒)→抽帧→@时连同图片投视觉模型 =====================
# key=(chat_id, user_id)，value={"frames":[data_url,...], "duration":秒, "ts":时间戳}
ENABLE_VIDEO = True             # 视频识别总开关：false=不处理群里发的视频
_video_cache = {}
_VIDEO_CACHE_TTL = 120          # 视频缓存120秒
_VIDEO_MAX_SECONDS = 600        # 最长10分钟，超过拒收


def _cache_add_video(chat_id, user_id, frames, duration, transcript=""):
    """每人只存一个视频，新的覆盖旧的。"""
    _video_cache[(chat_id, user_id)] = {"frames": frames, "duration": duration, "transcript": transcript, "ts": time.time()}
    print(f"📹 视频已缓存（{duration:.0f}秒，抽{len(frames)}帧{', 转写'+str(len(transcript))+'字' if transcript else ''}，{_VIDEO_CACHE_TTL}秒有效）")


def _cache_take_video(chat_id, user_id):
    """取出并清空该用户的缓存视频帧，返回(frames列表, transcript文本)。"""
    item = _video_cache.pop((chat_id, user_id), None)
    if item and time.time() - item["ts"] < _VIDEO_CACHE_TTL:
        return item["frames"], item.get("transcript", "")
    return [], ""


# ===================== 语音转文字：群友发语音→NapCat转wav→Whisper转写→@时把转写内容投给语言模型 =====================
# key=(chat_id, user_id)，value={"transcript": 转写文本, "ts": 时间戳}
ENABLE_VOICE = True             # 语音转文字总开关：false=不处理群里发的语音


# ===================== 消息合并（防抖）：同一会话连续消息合并成一次AI回复 =====================
ENABLE_MERGE = True                 # 总开关：true=收到消息先等MERGE_WAIT_SECONDS秒，期间有新消息就重置计时，攒成一次AI回复；false=每条立即单独回复
MERGE_WAIT_SECONDS = 8              # 等待窗口（秒）：N秒内无新消息才处理
MERGE_MAX_COUNT = 8                 # 超过多少条直接处理（防刷屏）
_merge_buf = {}                     # chat_id -> [msg dict] 待合并缓冲
_merge_workers = {}                 # chat_id -> asyncio.Task 每会话一个防抖worker
_merge_last_ts = {}                 # chat_id -> 最后一条消息入缓冲的时间戳
_record_cache = {}
_RECORD_CACHE_TTL = 120         # 语音转写缓存120秒
_WHISPER_MODEL = None           # Whisper模型全局复用（避免每条语音/视频重复加载）


def _get_whisper_model():
    """加载并复用 SenseVoice-Small 模型（funasr/CPU，阿里开源，中文+嘈杂场景更强）。"""
    global _WHISPER_MODEL
    if _WHISPER_MODEL is None:
        from funasr import AutoModel
        _WHISPER_MODEL = AutoModel(
            model="iic/SenseVoiceSmall",
            device="cpu",
            disable_update=True,
        )
        print("🎙️ SenseVoice-Small 模型加载完成")
    return _WHISPER_MODEL


def _sensevoice_transcribe(model, wav_path):
    """用 SenseVoice 转写，保留音频事件/情感标签，转换成AI能理解的描述。"""
    import re
    result = model.generate(input=wav_path, language="zh", use_itn=True)
    if not result:
        return ""
    text = result[0].get("text", "")
    # SenseVoice原始标签 -> 可读描述（让AI知道有BGM/笑声/情绪等）
    tag_map = {
        "<|BGM|>": "[含背景音乐]",
        "<|Laughter|>": "[笑声]",
        "<|Applause|>": "[掌声]",
        "<|Cheering|>": "[欢呼]",
        "<|Crying|>": "[哭声]",
        "<|HAPPY|>": "[开心]",
        "<|SAD|>": "[悲伤]",
        "<|ANGRY|>": "[生气]",
        "<|NEUTRAL|>": "[平静]",
        "<|FEARFUL|>": "[害怕]",
        "<|DISGUSTED|>": "[厌恶]",
        "<|SURPRISED|>": "[惊讶]",
    }
    for tag, desc in tag_map.items():
        text = text.replace(tag, desc)
    # 去掉剩余的语言标签/方向标签等（<|zh|><|woitn|>等）
    text = re.sub(r"<\|[^|]+\|>", "", text).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _cache_add_record(chat_id, user_id, transcript):
    """每人只存一条语音转写，新的覆盖旧的。"""
    _record_cache[(chat_id, user_id)] = {"transcript": transcript, "ts": time.time()}
    print(f"🎙️ 语音转写已缓存（{len(transcript)}字，{_RECORD_CACHE_TTL}秒有效）")


def _cache_take_record(chat_id, user_id):
    """取出并清空该用户的缓存语音转写，返回文本（自动过滤过期）。"""
    item = _record_cache.pop((chat_id, user_id), None)
    if item and time.time() - item["ts"] < _RECORD_CACHE_TTL:
        return item["transcript"]
    return ""


def _download_and_transcribe_audio(url_or_path: str):
    """下载音频→统一转16k单声道wav→Whisper转写→返回(transcript, err)。
    在to_thread里同步执行。"""
    import subprocess, tempfile, uuid
    tmp_dir = tempfile.gettempdir()
    is_http = url_or_path.startswith("http")
    audio_path = os.path.join(tmp_dir, f"qzvoice_{uuid.uuid4().hex[:8]}")
    try:
        # 1. 下载（http链接）或直接用本地路径
        if is_http:
            r = requests.get(url_or_path, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
            if r.status_code != 200:
                return "", f"HTTP {r.status_code}"
            with open(audio_path, "wb") as f:
                f.write(r.content)
        else:
            audio_path = url_or_path
        if not os.path.isfile(audio_path) or os.path.getsize(audio_path) < 100:
            return "", "音频文件不存在或过小"

        # 2. 统一转16k单声道wav（QQ语音经NapCat get_record已转码，这里兜底各种格式）
        wav_path = os.path.join(tmp_dir, f"qzvoice_{uuid.uuid4().hex[:8]}.wav")
        subprocess.run(
            ["ffmpeg", "-y", "-i", audio_path, "-ac", "1", "-ar", "16000", "-f", "wav", wav_path],
            capture_output=True, timeout=20)
        if not os.path.isfile(wav_path) or os.path.getsize(wav_path) < 1000:
            return "", "ffmpeg转wav失败"

        # 3. SenseVoice 转写
        model = _get_whisper_model()
        transcript = _sensevoice_transcribe(model, wav_path)
        try:
            os.remove(wav_path)
            if is_http:
                os.remove(audio_path)
        except Exception:
            pass
        return transcript, None
    except Exception as e:
        return "", str(e)


def _download_and_extract_frames(video_url: str):
    """下载视频→ffprobe查时长(>600秒拒)→ffmpeg抽3帧→返回(frames_list, duration, err)。
    在to_thread里同步执行。"""
    import subprocess, tempfile, uuid, base64
    tmp_dir = tempfile.gettempdir()
    video_path = os.path.join(tmp_dir, f"qzvideo_{uuid.uuid4().hex[:8]}.mp4")
    try:
        # 1. 下载视频（或直接用本地文件路径）
        if video_url.startswith("/app/") and os.path.isfile(video_url):
            # get_file返回的容器内本地路径，直接用
            video_path = video_url
            size_mb = os.path.getsize(video_path) / 1024 / 1024
        else:
            r = requests.get(video_url, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
            if r.status_code != 200:
                return None, 0, "", f"HTTP {r.status_code}"
            with open(video_path, "wb") as f:
                f.write(r.content)
            size_mb = os.path.getsize(video_path) / 1024 / 1024

        # 2. ffprobe查时长
        probe = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", video_path],
            capture_output=True, text=True, timeout=15)
        if probe.returncode != 0:
            return None, 0, "", "ffprobe失败"
        info = json.loads(probe.stdout)
        duration = float(info.get("format", {}).get("duration", 0))
        if duration > _VIDEO_MAX_SECONDS:
            return None, duration, "", f"视频{duration:.0f}秒超过{_VIDEO_MAX_SECONDS//60}分钟限制"
        if duration <= 0:
            return None, 0, "", "无法读取时长"

        # 3. 场景检测抽帧（画面明显变化才抽）+ 密度保底（至少每5秒一帧）+ 去重限帧
        frame_dir = os.path.join(tmp_dir, f"qzframes_{uuid.uuid4().hex[:8]}")
        os.makedirs(frame_dir, exist_ok=True)
        # 场景检测：select='gt(scene,0.3)' 在镜头切换处抽帧；fps=1/5 密度保底每5秒至少一帧
        subprocess.run(
            ["ffmpeg", "-y", "-i", video_path,
             "-vf", r"select='gt(scene,0.3)+not(mod(n\,5*30))',scale=480:-1",
             "-vsync", "vfr", "-q:v", "3",
             os.path.join(frame_dir, "f_%04d.jpg")],
            capture_output=True, timeout=30)
        # 开头保底一帧
        subprocess.run(
            ["ffmpeg", "-y", "-ss", "0.5", "-i", video_path,
             "-frames:v", "1", "-q:v", "3", "-vf", "scale=480:-1",
             os.path.join(frame_dir, "f_start.jpg")],
            capture_output=True, timeout=15)
        # 收集所有帧
        all_frames = sorted(
            [os.path.join(frame_dir, f) for f in os.listdir(frame_dir) if f.endswith(".jpg")])
        # 去重：简单按文件大小聚类（相同大小跳过），最多取8帧
        seen_sizes = set()
        picked = []
        for fp in all_frames:
            sz = os.path.getsize(fp)
            if sz in seen_sizes:
                continue
            seen_sizes.add(sz)
            picked.append(fp)
            if len(picked) >= 8:
                break
        # 转base64
        frames = []
        for fp in picked:
            with open(fp, "rb") as ff:
                b64 = base64.b64encode(ff.read()).decode()
            frames.append(f"data:image/jpeg;base64,{b64}")
        # 清理帧目录
        import shutil
        shutil.rmtree(frame_dir, ignore_errors=True)
        if not frames:
            os.remove(video_path)
            return None, 0, "", "抽帧失败"

        # 4. 提取音频→Whisper转文字（没字幕的视频靠这个知道在说什么）
        transcript = ""
        try:
            # 检查有没有音频轨
            audio_probe = subprocess.run(
                ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", video_path],
                capture_output=True, text=True, timeout=10)
            streams = json.loads(audio_probe.stdout).get("streams", [])
            has_audio = any(s.get("codec_type") == "audio" for s in streams)
            if has_audio:
                audio_path = os.path.join(tmp_dir, f"qzaudio_{uuid.uuid4().hex[:8]}.wav")
                subprocess.run(
                    ["ffmpeg", "-y", "-i", video_path, "-vn", "-ac", "1",
                     "-ar", "16000", "-f", "wav", audio_path],
                    capture_output=True, timeout=20)
                if os.path.isfile(audio_path) and os.path.getsize(audio_path) > 1000:
                    print(f"🎤 视频有音频，SenseVoice转写中...")
                    model = _get_whisper_model()
                    transcript = _sensevoice_transcribe(model, audio_path)
                    os.remove(audio_path)
                    if transcript:
                        print(f"🎤 视频转写完成（{len(transcript)}字）")
        except Exception as ae:
            print(f"⚠️ 音频转写跳过：{ae}")

        os.remove(video_path)
        return frames, duration, transcript, None
    except Exception as e:
        return None, 0, "", str(e)


def _is_pure_image_silent(image_urls, plain_text: str, is_group: bool, at_flag: bool) -> bool:
    """是否属于「纯图片且没在叫它」→ 静默缓存不回复。
    - 群里@了机器人：即使只有图/只引用了图，也必须回复（否则消息被吞）
    - 群里没@：纯图静默缓存（图片已缓存，等以后被@时一起用）
    - 私聊：纯图静默缓存，等用户接着发的文字一起处理（手机端先发图后发字的习惯）
    """
    if not image_urls:
        return False
    if plain_text:
        return False
    return not (is_group and at_flag)


# ===================== 群聊旁听缓存：没@机器人的消息也记下来当上下文 =====================
def _ambient_add(chat_id, user_id, text: str):
    """把一条没有@机器人的群消息存进旁听缓存（每条截断、每群限条数、满了丢最旧）"""
    if not ENABLE_AMBIENT_CACHE:
        return
    t = (text or "").strip().replace("\n", " ")
    if not t:
        return
    if AMBIENT_MSG_MAX_LEN > 0 and len(t) > AMBIENT_MSG_MAX_LEN:
        t = t[:AMBIENT_MSG_MAX_LEN] + "…"
    lst = _ambient_msgs[chat_id]
    lst.append({"user_id": user_id, "text": t})
    while AMBIENT_MAX_MSGS > 0 and len(lst) > AMBIENT_MAX_MSGS:
        lst.pop(0)


def _ambient_take_text(chat_id) -> str:
    """取出并清空该群的旁听缓存，拼成给AI的背景块（取走即清空，避免每条回复重复塞旧闲聊）"""
    lst = _ambient_msgs.pop(chat_id, None)
    if not lst:
        return ""
    import datetime
    lines = []
    for x in lst:
        _t = datetime.datetime.fromtimestamp(x.get("ts", time.time())).strftime("%m-%d %H:%M")
        lines.append(f"[{_t}] [QQ号:{x['user_id']}] {x['text']}")
    body = "\n".join(lines)
    return ("\n\n【群里的背景闲聊（以下消息都【没有】@你，只是让你了解上下文，"
            "不要专门回应或复述它们，也不要因此改变你的回复对象，注意每条前面的时间）】\n" + body)


def download_image_as_data_url(image_url):
    """下载图片转data_url（base64内联），返回(data_url, None)或(None, error)。"""
    import base64
    try:
        img_resp = requests.get(image_url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        if img_resp.status_code != 200:
            return None, f"HTTP {img_resp.status_code}"
        img_base64 = base64.b64encode(img_resp.content).decode("utf-8")
        content_type = img_resp.headers.get("Content-Type", "image/jpeg")
        if "png" in content_type:
            img_format = "png"
        elif "gif" in content_type:
            img_format = "gif"
        elif "webp" in content_type:
            img_format = "webp"
        else:
            img_format = "jpeg"
        return f"data:image/{img_format};base64,{img_base64}", None
    except Exception as e:
        return None, str(e)


def message_to_cq_str(message) -> str:
    """把OneBot消息段数组转成CQ码字符串；本来就是字符串则原样返回"""
    if isinstance(message, str):
        return message
    parts = []
    for seg in message or []:
        if not isinstance(seg, dict):
            continue
        seg_type = seg.get("type", "")
        data = seg.get("data", {}) or {}
        if seg_type == "text":
            parts.append(data.get("text", ""))
        elif seg_type == "at":
            parts.append(f"[CQ:at,qq={data.get('qq', '')}]")
        elif seg_type == "image":
            file_ = data.get("file", "")
            url = data.get("url", "")
            parts.append(f"[CQ:image,file={file_}" + (f",url={url}" if url else "") + "]")
        elif seg_type == "reply":
            parts.append(f"[CQ:reply,id={data.get('id', '')}]")
        else:
            params = ",".join(f"{k}={v}" for k, v in data.items())
            parts.append(f"[CQ:{seg_type}" + (f",{params}" if params else "") + "]")
    return "".join(parts)


def extract_user_text(cq_msg: str, self_id: int, raw_msg: str = "") -> str:
    """去掉@机器人的内容，返回纯文字（同时兼容CQ码和纯文本@两种格式）"""
    text = cq_msg.replace(f"[CQ:at,qq={self_id}]", "")
    if raw_msg:
        text = text.replace(f"@{self_id}", "")
    # 去掉其余CQ码（图片/表情/回复等）
    text = re.sub(r"\[CQ:[^]]*\]", "", text).strip()
    # 去掉开头的 @昵称 xxx
    if text.startswith("@"):
        idx = text.find(" ")
        text = text[idx:].strip() if idx > 0 else ""
    return text.strip()


def is_controller_command(text: str) -> bool:
    """检测是否为qq-controller的远程控制指令（这些指令由控制器处理，AI不回复）
    只认四个指令：关闭服务 / 重启服务 / 重启服务 ai / 重启服务 napcat"""
    t = text.strip().lower()
    t = re.sub(r'^[的\s]+', '', t)
    if t == "关闭服务":
        return True
    if t == "重启服务":
        return True
    if re.match(r'^重启服务\s*(ai|napcat)$', t):
        return True
    return False


# ===================== OneBot API 调用封装（基于echo匹配，不丢事件） =====================
# 每会话发送锁 + 上次发送时间：保证同一群/私聊的回复严格按顺序发出，不交错
_chat_send_locks = defaultdict(asyncio.Lock)
_last_send_ts = defaultdict(float)


async def _chat_delay(chat_id):
    """按会话计算发送前延迟（模拟真人节奏）：
    - 该会话 10 秒内刚发过消息（用户在连发/回复在跟上）→ 用短间隔，让后面的回复快速跟上
    - 否则（正常聊天节奏）→ 用标准随机延迟 send_delay，模拟"思考一会儿再回" """
    now = time.time()
    if now - _last_send_ts[chat_id] < SEND_DELAY_MAX + SPLIT_DELAY_MAX + 2:
        await asyncio.sleep(random.uniform(SPLIT_DELAY_MIN, SPLIT_DELAY_MAX))
    else:
        await asyncio.sleep(random.uniform(SEND_DELAY_MIN, SEND_DELAY_MAX))


async def api_call(websocket, action: str, params: dict, timeout: float = 8.0) -> dict:
    """发送OneBot action并等待echo匹配的响应。
    成功(retcode==0)或失败(retcode!=0)都会返回原始响应dict（调用方自行判断）；
    只有超时/连接异常/无响应时返回{}"""
    import uuid
    echo = f"{action}_{uuid.uuid4().hex[:10]}"
    fut = asyncio.get_running_loop().create_future()
    _pending_echo[echo] = fut
    try:
        # 只在实际发送时持锁，等待响应期间不占锁（大文件上传也不会卡住其他消息）
        async with _send_lock:
            await websocket.send(json.dumps({"action": action, "params": params, "echo": echo}))
        resp = await asyncio.wait_for(fut, timeout)
        return resp if isinstance(resp, dict) else {}
    except Exception:
        return {}
    finally:
        _pending_echo.pop(echo, None)


async def send_group_msg(websocket, group_id: int, msg: str, no_delay: bool = False):
    """发送群消息（会话锁保证顺序，开启随机延迟时按真人节奏等待再发，防秒回被检测）。
    no_delay=True 用于分段回复的后续分段（延迟由分段间隔控制）。"""
    async with _chat_send_locks[group_id]:
        if not no_delay and ENABLE_SEND_DELAY and SEND_DELAY_MAX > 0:
            await _chat_delay(group_id)
        await api_call(websocket, "send_group_msg", {"group_id": group_id, "message": msg}, timeout=5)
        _last_send_ts[group_id] = time.time()


async def send_private_msg(websocket, user_id: int, msg: str, no_delay: bool = False):
    """发送私聊消息（会话锁保证顺序，开启随机延迟时按真人节奏等待再发，防秒回被检测）。
    no_delay=True 用于分段回复的后续分段（延迟由分段间隔控制）。"""
    async with _chat_send_locks[user_id]:
        if not no_delay and ENABLE_SEND_DELAY and SEND_DELAY_MAX > 0:
            await _chat_delay(user_id)
        await api_call(websocket, "send_private_msg", {"user_id": user_id, "message": msg}, timeout=5)
        _last_send_ts[user_id] = time.time()



async def _tts_and_send(websocket, is_group: bool, chat_id: int, text: str) -> bool:
    """TTS 把文本转语音并发送语音条（record消息）。失败返回False，由调用方降级为文本。
    生成的mp3放NapCat能读的位置，发完延迟清理。"""
    import tempfile, uuid
    try:
        # 1. 合成mp3到临时目录
        tmp = os.path.join(tempfile.gettempdir(), f"tts_{uuid.uuid4().hex[:8]}.mp3")
        if TTS_ENGINE == "volc" and VOLC_API_KEY:
            # 火山引擎大模型TTS
            ok = await asyncio.to_thread(_volc_tts_synthesize, text, tmp)
            if not ok:
                print(f"⚠️ 火山引擎TTS失败，降级edge-tts")
                import edge_tts
                communicate = edge_tts.Communicate(text, TTS_VOICE)
                await communicate.save(tmp)
        else:
            # edge-tts
            import edge_tts
            communicate = edge_tts.Communicate(text, TTS_VOICE)
            await communicate.save(tmp)
        if not os.path.isfile(tmp) or os.path.getsize(tmp) <= 0:
            print(f"⚠️ TTS合成结果为空，降级文本")
            return False
        # 2. 放到NapCat能读的位置
        name = f"tts_{uuid.uuid4().hex[:8]}.mp3"
        file_param, mode = await asyncio.to_thread(put_file_where_napcat_reads, tmp, name)
        # 3. 发语音消息（QQ语音条 record）
        if is_group:
            resp = await api_call(websocket, "send_group_msg",
                                  {"group_id": chat_id,
                                   "message": [{"type": "record", "data": {"file": file_param}}]},
                                  timeout=600)
        else:
            resp = await api_call(websocket, "send_private_msg",
                                  {"user_id": chat_id,
                                   "message": [{"type": "record", "data": {"file": file_param}}]},
                                  timeout=600)
        ok = resp.get("retcode") == 0
        if not ok:
            print(f"⚠️ 语音消息发送失败：{json.dumps(resp, ensure_ascii=False) if resp else '无响应'}")
        # 4. 清理本地临时mp3
        try:
            os.remove(tmp)
        except Exception:
            pass
        # 5. 清理NapCat侧临时文件（成功延迟删，失败立即删）
        if mode != "native":
            if ok and UPLOAD_CLEANUP:
                task = asyncio.create_task(_delayed_remove(file_param, mode))
                _bg_tasks.add(task)
                task.add_done_callback(_bg_tasks.discard)
            else:
                await asyncio.to_thread(remove_napcat_tmp, file_param, mode)
        return ok
    except Exception as e:
        print(f"⚠️ TTS转语音异常（降级文本）：{e}")
        return False


async def send_reply_multi(websocket, is_group: bool, chat_id: int, reply: str) -> list:
    """按段把回复拆成多条消息顺序发出，返回实际发出的分段列表。
    每一条分段都间隔 send_delay（1~8秒）再发出：第一条是"思考时间"，
    后续分段等上一条发出后再停顿同样时长，一条一条像真人打字。
    ENABLE_TTS开启时，每段文本先转语音条发出（失败自动降级为文本）。"""
    parts = split_reply_to_parts(reply) if ENABLE_SPLIT_REPLY else [reply.strip() or ""]
    parts = [p for p in parts if p]
    if not parts:
        return []
    for idx, part in enumerate(parts):
        if idx > 0 and SEND_DELAY_MAX > 0:
            await asyncio.sleep(random.uniform(SEND_DELAY_MIN, SEND_DELAY_MAX))
        if ENABLE_TTS:
            # 语音模式：优先发语音条，失败降级为文本
            _ok = await _tts_and_send(websocket, is_group, chat_id, part)
            if _ok:
                print(f"🎙️ [TTS] 已发送语音段{idx + 1}/{len(parts)}：{part[:20]}…")
                continue
        if is_group:
            await send_group_msg(websocket, chat_id, part, no_delay=(idx > 0))
        else:
            await send_private_msg(websocket, chat_id, part, no_delay=(idx > 0))
    return parts


def split_reply_to_parts(text: str) -> list:
    """把AI回复拆成多条消息的文本列表：
    1) 先按空行分段（AI常用空行分隔两段话）
    2) 只有一段但含换行时，按换行拆
    3) 单段仍过长时按句末标点细拆
    4) 代码块(```)不拆，避免拆坏
    5) 拆分条数超过 SPLIT_MAX_PARTS（默认3）时【不拆】，整条一次发出
    """
    if not text:
        return []
    t = text.replace("\r\n", "\n").strip()
    if not t:
        return []
    if "```" in t:            # 含代码块，整条发，避免拆坏
        return [t]

    parts = [p.strip() for p in re.split(r"\n\s*\n", t) if p.strip()]
    if len(parts) <= 1 and "\n" in t:      # 没有空行但有多行 → 按行拆
        parts = [p.strip() for p in t.split("\n") if p.strip()]

    # 过长的段再按句末标点细拆
    fine = []
    for p in parts:
        if len(p) <= SPLIT_PART_MAX_LEN:
            fine.append(p)
            continue
        buf = ""
        for seg in re.split(r"(?<=[。！？!?；;…])", p):
            if not seg:
                continue
            # 单句本身就超长（整段没标点）→ 先按长度硬切
            while len(seg) > SPLIT_PART_MAX_LEN:
                if buf:
                    fine.append(buf.strip())
                    buf = ""
                fine.append(seg[:SPLIT_PART_MAX_LEN].strip())
                seg = seg[SPLIT_PART_MAX_LEN:]
            if not seg:
                continue
            if len(buf) + len(seg) > SPLIT_PART_MAX_LEN and buf:
                fine.append(buf.strip())
                buf = seg
            else:
                buf += seg
        if buf.strip():
            fine.append(buf.strip())

    # 条数上限：算出来超过上限就整条发，不拆（避免切太碎显得人机）
    if SPLIT_MAX_PARTS > 0 and len(fine) > SPLIT_MAX_PARTS:
        return [t]
    return [p for p in fine if p.strip()]


def _volc_tts_synthesize(text: str, output_path: str) -> bool:
    """新版火山引擎大模型TTS合成mp3到本地文件。返回True=成功，False=失败。"""
    import requests, uuid, base64, json
    try:
        url = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
        headers = {
            "X-Api-Key": VOLC_API_KEY,
            "X-Api-Resource-Id": "seed-tts-2.0",
            "X-Api-Request-Id": str(uuid.uuid4()),
            "Content-Type": "application/json",
            "Connection": "keep-alive"
        }
        payload = {
            "req_params": {
                "text": text[:1000],  # 限制长度
                "speaker": VOLC_VOICE,
                "audio_params": {
                    "format": "mp3",
                    "sample_rate": 24000
                }
            }
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=30, stream=True)
        if resp.status_code != 200:
            print(f"⚠️ 火山引擎TTS HTTP错误: {resp.status_code} {resp.text[:200]}")
            return False
        # 流式读取，拼接所有音频块
        audio_chunks = []
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                chunk = json.loads(line)
                if chunk.get("code") not in (0, 20000000):  # 0=正常数据，20000000=结束标记
                    _code = chunk.get("code", "")
                    _msg = chunk.get("message", "")
                    print(f"⚠️ 火山引擎TTS chunk错误: code={_code} msg={_msg}")
                    return False
                b64_data = chunk.get("data", "")
                if b64_data:
                    audio_chunks.append(base64.b64decode(b64_data))
            except json.JSONDecodeError:
                continue
        if not audio_chunks:
            print(f"⚠️ 火山引擎TTS返回空音频")
            return False
        # 拼接写入文件
        with open(output_path, "wb") as f:
            for chunk in audio_chunks:
                f.write(chunk)
        return True
    except Exception as e:
        print(f"⚠️ 火山引擎TTS异常：{e}")
        return False


def _qs_extract_json(html, marker):
    """括号匹配法提取 marker 后的第一个完整 JSON 对象"""
    idx = html.find(marker)
    if idx < 0:
        return None
    start = html.find("=", idx)
    if start < 0:
        return None
    start += 1
    while start < len(html) and html[start] in " \n\r\t":
        start += 1
    if start >= len(html) or html[start] != "{":
        return None
    depth = 0
    end = start
    in_string = False
    escape = False
    for i in range(start, len(html)):
        c = html[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    try:
        return json.loads(html[start:end])
    except Exception:
        return None


def qishui_parse(raw_link):
    """解析汽水音乐页面，返回 (song_name, artist, audio_url, cover_url, headers, None) 或 (None,None,None,None,None,error_msg)"""
    _ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    s = requests.Session()
    s.headers.update({"User-Agent": _ua})
    try:
        resp = s.get(raw_link, allow_redirects=True, timeout=20)
    except Exception as e:
        return None, None, None, None, None, f"页面访问失败：{e}"
    html = resp.text
    router = _qs_extract_json(html, "_ROUTER_DATA")
    if not router:
        return None, None, None, None, None, "页面未找到_ROUTER_DATA（可能被反爬或链接失效）"
    try:
        track_page = router.get("loaderData", {}).get("track_page", {})
        awlo = track_page.get("audioWithLyricsOption", {})
        audio_url = awlo.get("url")
        song_name = awlo.get("trackName") or awlo.get("track_name") or "未知歌曲"
        artist = awlo.get("artistName") or awlo.get("artist_name") or "未知歌手"
        cover_url = awlo.get("coverURL") or awlo.get("cover_url")
        if not audio_url or song_name == "未知歌曲":
            track_info = awlo.get("trackInfo", {})
            if track_info:
                if not song_name or song_name == "未知歌曲":
                    song_name = track_info.get("name") or song_name
                if not artist or artist == "未知歌手":
                    artists = track_info.get("artists", [])
                    if isinstance(artists, list) and artists:
                        artist = artists[0].get("simple_display_name") or artists[0].get("name") or artist
        if not audio_url:
            return None, None, None, None, None, "未在页面数据中找到音频链接"
        cookie_str = "; ".join([f"{c.name}={c.value}" for c in s.cookies])
        headers = {"User-Agent": _ua, "Referer": raw_link, "Cookie": cookie_str}
        return song_name, artist, audio_url, cover_url, headers, None
    except Exception as e:
        return None, None, None, None, None, f"解析页面数据失败：{e}"


# ===================== 本子下载功能：@机器人/私聊「下载本子123 [pN]」→ 打包加密zip → 发送 =====================
DOWNLOAD_ROOT = None  # 从 qqbot-config.yml 的 download_root 加载
# 下载本子350234 / 下载350234 / 下载本子350234 p2（pN=第N个章节，1开始，不写默认第1章）——不需要@
_ALBUM_CMD_RE = re.compile(r"^下载(?:本子)?\s*(\d{1,10})(?:\s*[pP]\s*(\d{1,6}))?\s*$")
DEFAULT_CHAPTER = None  # 从 qqbot-config.yml 的 default_chapter 加载
# 图片格式统一为 png（清晰）。如需群聊jpg/私聊png拆分，改成：
#   def image_suffix_for(is_group): return ".jpg" if is_group else ".png"
IMG_SUFFIX = None  # 从 qqbot-config.yml 的 img_suffix 加载
# QQ富媒体单文件过大会被拒（实测~130MB报rich media transfer failed，~40MB可过）。
# 残留zip超过该大小就不再复用，删掉重新下载小包。
MAX_SEND_MB = None  # 从 qqbot-config.yml 的 max_send_mb 加载

# 解压密码：非空则用 7z 打 AES-256 加密zip（QQ扫不开内容就不会撤文件）；留空""则不加密。
# 密码直接写在文件名里发给对方（如 350234密码jm.zip，QQ文件名不允许冒号），不再单独发提示文字。
# 本机需要安装 7-Zip（C:\Program Files\7-Zip\7z.exe）。
ZIP_PASSWORD = None  # 从 qqbot-config.yml 的 zip_password 加载
_CONVERT_DIR = None  # 由 _load_config_file 根据 download_root 设置
DOUYIN_TTWID = None  # 从 qqbot-config.yml 的 douyin_ttwid 加载（抖音视频/图文解析需要，留空则抖音功能不可用）


def _find_7z():
    """找7z可执行文件，找不到返回None"""
    for p in (r"C:\Program Files\7-Zip\7z.exe",
              r"C:\Program Files (x86)\7-Zip\7z.exe",
              "7z"):
        if p == "7z":
            return p  # 让subprocess去PATH里找
        if os.path.isfile(p):
            return p
    return None


def zip_output_path(aid: str, img_suffix: str) -> str:
    """本地zip保存路径。文件名带图片格式标签（png版/jpg版），
    不同聊天类型的缓存互不干扰、切换格式后旧缓存自动失效重建（QQ显示名另见 display_file_name）"""
    name = f"本子{aid}"
    if img_suffix in (".png", ".jpg"):
        name += f".{img_suffix[1:]}版"
    return os.path.join(DOWNLOAD_ROOT, f"{name}.zip")


def display_file_name(aid: str) -> str:
    """发到QQ的文件名：直接带上解压密码，对方不用再问密码。
    注意：QQ文件名不接受 ':' 等非法字符（实测带冒号会报 rich media transfer failed），
    所以密码部分只保留安全字符，形如 197224密码jm.zip。"""
    if ZIP_PASSWORD:
        pwd_part = "".join(c for c in ZIP_PASSWORD if c not in ':\\/*?"<>|')
        return f"{aid}密码{pwd_part}.zip"
    return f"{aid}.zip"


class _NoSuchChapter(Exception):
    """请求的章节序号超出该本子的章节数"""
    def __init__(self, total_chapters: int):
        self.total_chapters = total_chapters
        super().__init__(f"本子只有{total_chapters}个章节")


def build_jm_option(img_suffix: str) -> jmcomic.JmOption:
    """构造统一下载option：目录=专辑id/章节id/图片；下载时把图片直接存成 img_suffix 格式"""
    d = jmcomic.JmOption.default_dict()
    d["dir_rule"] = {"rule": "Bd_Aid_Pid", "base_dir": DOWNLOAD_ROOT}
    if img_suffix:
        d.setdefault("download", {}).setdefault("image", {})["suffix"] = img_suffix
    return jmcomic.JmOption.construct(d)


def download_jm_album(aid: str, chapter_index: int = DEFAULT_CHAPTER, img_suffix: Optional[str] = None):
    """只下载本子<aid>的第 chapter_index 个章节（1开始）到 DOWNLOAD_ROOT/<aid>/。
    返回 (章节文件夹路径, 该本子总章节数)。img_suffix: 图片输出格式 ".png"/".jpg"/None(原样)"""
    folder = os.path.join(DOWNLOAD_ROOT, aid)
    # 先清掉同名旧目录，避免残留文件混进新包
    import shutil
    shutil.rmtree(folder, ignore_errors=True)
    os.makedirs(DOWNLOAD_ROOT, exist_ok=True)

    option = build_jm_option(img_suffix)
    client = option.new_jm_client()
    # 拉专辑详情（含章节列表），只下载用户选的那一章
    album = client.get_album_detail(aid)
    total = len(album)
    idx = chapter_index - 1
    if not (0 <= idx < total):
        raise _NoSuchChapter(total)
    photo = album[idx]  # 带专辑上下文的章节实体，目录规则正常生效

    downloader = jmcomic.new_downloader(option)
    downloader.download_by_photo_detail(photo)
    if not os.path.isdir(folder):
        raise RuntimeError(f"下载完成但未找到目录: {folder}")
    return folder, total


def pack_album_to_zip(aid: str, folder: str, img_suffix: str):
    """把本子文件夹打包成zip（zip放 DOWNLOAD_ROOT 下），返回(zip路径, 图片张数)。
    图片在下载时已按 img_suffix 转好格式；ZIP_PASSWORD非空则用7z打AES-256加密zip。"""
    import shutil
    img_exts = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")
    pages = []
    for root, _dirs, files in os.walk(folder):
        for name in sorted(files):
            if name.lower().endswith(img_exts):
                pages.append(os.path.join(root, name))
    if not pages:
        raise RuntimeError("下载目录里没有找到图片文件")

    zip_path = zip_output_path(aid, img_suffix)
    shutil.rmtree(os.path.join(_CONVERT_DIR, aid), ignore_errors=True)
    try:
        if ZIP_PASSWORD:
            # 用 7z 打 AES-256 加密zip，QQ安全扫描打不开内容，不会撤文件
            seven = _find_7z()
            if seven is None:
                raise RuntimeError("已设置 ZIP_PASSWORD，但找不到 7z.exe（请安装7-Zip，或把 ZIP_PASSWORD 留空不加密）")
            cmd = [seven, "a", "-tzip", f"-p{ZIP_PASSWORD}", "-mem=AES256", "-y", zip_path, aid]
            import subprocess
            r = _run_subprocess(cmd, cwd=DOWNLOAD_ROOT, timeout=1800)
            if r.returncode != 0:
                raise RuntimeError(f"7z打包失败: {(r.stderr or r.stdout).strip()[-500:]}")
        else:
            # 普通zip
            base = os.path.join(DOWNLOAD_ROOT, aid)
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for root, _dirs, files in os.walk(base):
                    for name in sorted(files):
                        full = os.path.join(root, name)
                        rel = os.path.relpath(full, DOWNLOAD_ROOT)  # aid/章节id/文件名
                        zf.write(full, rel)
        return zip_path, len(pages)
    finally:
        shutil.rmtree(os.path.join(_CONVERT_DIR, aid), ignore_errors=True)


def cleanup_local_cache(aid: str, zip_path: str):
    """任务完成后删掉本地缓存（下载的图片目录+zip），防止把C/D盘堆满"""
    import shutil
    shutil.rmtree(os.path.join(DOWNLOAD_ROOT, aid), ignore_errors=True)
    shutil.rmtree(os.path.join(_CONVERT_DIR, aid), ignore_errors=True)
    try:
        os.remove(zip_path)
    except OSError:
        pass


# ===================== 发送文件给NapCat（3种部署模式，可用环境变量切换） =====================
# 模式1 docker-cp(win10现状)：QQ2与NapCat不同文件系统 → 宿主机 docker cp 进容器（默认，QQ2_NAPCAT_CONTAINER=napcat）
# 模式2 NAS共享目录：QQ2与NapCat两容器共享同一host目录 → bot把zip写进 NAPCAT_HOST_DIR，
#        NapCat 从 NAPCAT_VIEW_DIR（同一目录在NapCat容器内的路径）读取，无需挂docker socket
#        （用法：QQ2_NAPCAT_CONTAINER="" QQ2_NAPCAT_HOST_DIR=/deliver QQ2_NAPCAT_VIEW_DIR=/app/data）
# 模式3 本机原生：NapCat与QQ2同机 → 直接传本地路径（NAPCAT_CONTAINER与HOST_DIR都为空）
NAPCAT_CONTAINER = None  # 从 qqbot-config.yml / 环境变量加载
NAPCAT_DATA_DIR = None  # 从 qqbot-config.yml / 环境变量加载
NAPCAT_HOST_DIR = None  # 从 qqbot-config.yml / 环境变量加载
NAPCAT_VIEW_DIR = None  # 从 qqbot-config.yml / 环境变量加载
# 发送成功/失败后延迟清理待传文件（容器或共享目录里的），防占磁盘
UPLOAD_CLEANUP = None  # 从 qqbot-config.yml 的 upload_cleanup 加载
UPLOAD_CLEANUP_DELAY_SECONDS = None  # 从 qqbot-config.yml 的 upload_cleanup_delay_seconds 加载
# True = 把文件作为聊天文件消息发出（仅适合私聊；群里QQ会限制bot发文件消息，实测rich media transfer failed）；
# False = 群聊走 upload_group_file 传群文件（实测稳定），私聊仍自动走聊天文件消息（私聊没有群文件）
SEND_FILE_AS_CHAT_MSG = None  # 从 qqbot-config.yml 的 send_file_as_chat_msg 加载


def delivery_mode() -> str:
    """当前文件投递模式：'docker' | 'share' | 'native'"""
    if NAPCAT_CONTAINER:
        return "docker"
    if NAPCAT_HOST_DIR:
        return "share"
    return "native"


def docker_copy_into_container(local_path: str, container_name: Optional[str] = None) -> str:
    """用 docker cp 把本地文件复制进NapCat容器，返回容器内路径；失败抛异常。
    container_name 可指定容器内的文件名——聊天文件消息的显示名取自路径文件名，
    所以要显示「197224密码jm.zip」就必须让容器里的文件也叫这个名字。"""
    import subprocess
    if not container_name:
        container_name = os.path.basename(local_path)
    container_path = f"{NAPCAT_DATA_DIR}/{container_name}"
    result = _run_subprocess(
        ["docker", "cp", local_path, f"{NAPCAT_CONTAINER}:{container_path}"])
    if result.returncode != 0:
        raise RuntimeError(f"docker cp 失败: {(result.stderr or result.stdout).strip()}")
    return container_path


def docker_remove_file(container_path: str):
    """清理docker容器内的临时文件（失败只打日志，不影响主流程）"""
    if not UPLOAD_CLEANUP:
        return
    try:
        import subprocess
        _run_subprocess(["docker", "exec", NAPCAT_CONTAINER, "rm", "-f", container_path],
                        timeout=30)
    except Exception as e:
        print(f"⚠️ 清理容器内临时文件失败：{e}")


def put_file_where_napcat_reads(local_path: str, name_in_dir: Optional[str] = None):
    """把本地zip放到NapCat能读到的地方，返回 (NapCat看到的路径, 模式)。
    模式 docker：docker cp 进容器；share：复制进共享目录；native：NapCat与bot同机，直接用本地路径"""
    if not name_in_dir:
        name_in_dir = os.path.basename(local_path)
    mode = delivery_mode()
    if mode == "docker":
        return docker_copy_into_container(local_path, name_in_dir), mode
    if mode == "share":
        import shutil
        os.makedirs(NAPCAT_HOST_DIR, exist_ok=True)
        dst = os.path.join(NAPCAT_HOST_DIR, name_in_dir)
        shutil.copy2(local_path, dst)
        return f"{NAPCAT_VIEW_DIR.rstrip('/')}/{name_in_dir}", mode
    return local_path, mode


def remove_napcat_tmp(param: str, mode: str):
    """清理传给NapCat的临时文件（docker容器内 或 共享目录）；native无动作。失败只打日志"""
    if not UPLOAD_CLEANUP:
        return
    try:
        if mode == "docker":
            docker_remove_file(param)
        elif mode == "share":
            name = str(param).rsplit("/", 1)[-1]
            try:
                os.remove(os.path.join(NAPCAT_HOST_DIR, name))
            except OSError:
                pass
    except Exception as e:
        print(f"⚠️ 清理NapCat临时文件失败：{e}")


_startup_cleanup_done = False   # 启动清理只做一次（重连不重复执行）


def _is_our_tmp_file(name: str) -> bool:
    """判断是不是我们自己生成的待传临时文件（只清自己的，绝不碰NapCat数据/别人的文件）"""
    if name.startswith("upload_"):          # upload_group_file 用的 upload_时间戳_随机数.zip
        return True
    low = name.lower()
    if low.endswith(".zip") and ("密码" in name or name.startswith("本子")):
        return True                          # 聊天文件消息用的显示名 zip
    return False


def docker_clear_container_data():
    """启动清理：只删除【自己生成的、且超过10分钟的】待传临时文件，防止残留占磁盘。
    - 只跑一次（重连不重复执行）
    - 只匹配 upload_* / *密码*.zip / 本子*.zip，目录和其它文件一律不动
    - 失败只打日志，不影响启动
    """
    global _startup_cleanup_done
    if _startup_cleanup_done:
        return
    _startup_cleanup_done = True
    mode = delivery_mode()
    if mode == "native":
        return
    try:
        if mode == "docker":
            cmd = (f"find {NAPCAT_DATA_DIR} -maxdepth 1 -type f -mmin +10 "
                   f"\\( -name 'upload_*' -o -name '*密码*.zip' -o -name '本子*.zip' \\) -delete 2>/dev/null")
            _run_subprocess(["docker", "exec", NAPCAT_CONTAINER, "sh", "-c", cmd], timeout=30)
            print(f"🧹 已清理容器 {NAPCAT_DATA_DIR} 里遗留的自建临时文件")
        else:  # share
            if os.path.isdir(NAPCAT_HOST_DIR):
                now = time.time()
                removed = 0
                for name in os.listdir(NAPCAT_HOST_DIR):
                    p = os.path.join(NAPCAT_HOST_DIR, name)
                    if not os.path.isfile(p) or not _is_our_tmp_file(name):
                        continue
                    try:
                        if now - os.path.getmtime(p) < 600:   # 10分钟内的可能正在传，别动
                            continue
                        os.remove(p)
                        removed += 1
                    except OSError:
                        pass
                if removed:
                    print(f"🧹 已清理共享目录 {NAPCAT_HOST_DIR} 里 {removed} 个遗留临时文件")
    except Exception as e:
        print(f"⚠️ 启动时清理临时文件失败（可忽略）：{e}")


def _repack_video_with_ffmpeg(file_path: str) -> Optional[str]:
    """用ffmpeg重新封装视频文件（-c copy不转码，只修复容器格式和文件头，速度快）。
    解决v2下载的1080p视频文件头/元数据异常导致NTQQ报rich media transfer failed的问题。
    成功返回原路径（已替换为重新封装后的文件），失败返回None。"""
    _ext = os.path.splitext(file_path)[1].lower()
    if _ext not in {".mp4", ".mov", ".m4v"}:
        return None
    try:
        import shutil
        tmp_path = file_path + ".repack.mp4"
        # -c copy: 不转码直接复制流；-movflags +faststart: moov atom移到文件头，利于NTQQ解析
        cmd = ["ffmpeg", "-y", "-i", file_path, "-c", "copy", "-movflags", "+faststart", tmp_path]
        result = _run_subprocess(cmd, timeout=120)
        if result.returncode == 0 and os.path.isfile(tmp_path) and os.path.getsize(tmp_path) > 1024:
            print(f"🎬 ffmpeg重新封装完成（原{os.path.getsize(file_path)}字节 → 新{os.path.getsize(tmp_path)}字节）")
            shutil.move(tmp_path, file_path)
            return file_path
        if os.path.isfile(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        print(f"⚠️ ffmpeg重新封装失败，returncode={result.returncode}")
        if result.stderr:
            print(f"   ffmpeg错误：{result.stderr[-300:]}")
        return None
    except Exception as e:
        print(f"⚠️ ffmpeg重新封装异常：{e}")
        return None


async def upload_group_file(websocket, group_id: int, file_path: str, name: str) -> bool:
    """上传本地文件到QQ群文件（三种模式见 delivery_mode，NapCat需要 upload_file: True）。成功返回True。"""
    if not os.path.isfile(file_path):
        print(f"⚠️ 上传文件不存在: {file_path}")
        return False

    # 1. 把文件放到NapCat能读到的地方（共享目录里用纯英文随机名，避免中文/空格/特殊字符导致NapCat读不到；显示名用name）
    try:
        import random as _rnd
        _ext = os.path.splitext(name)[1] or os.path.splitext(file_path)[1] or ".bin"
        _tmp_name = f"upload_{int(time.time())}_{_rnd.randint(1000,9999)}{_ext}"
        file_param, mode = await asyncio.to_thread(put_file_where_napcat_reads, file_path, _tmp_name)
        print(f"📦 文件已就位: {file_param}（模式{mode}，显示名：{name}）")
    except Exception as e:
        print(f"❌ 文件就位失败，无法上传：{e}")
        return False

    # 2. 调用上传（NapCat 需要 upload_file: True 才会真正执行上传）
    resp = await api_call(websocket, "upload_group_file",
                          {"group_id": group_id, "file": file_param, "name": name,
                           "upload_file": True}, timeout=600)
    if resp.get("retcode") == 0:
        # 不立刻删源文件：等一段时间让NapCat把字节传完，再清理临时文件
        if mode != "native" and UPLOAD_CLEANUP:
            task = asyncio.create_task(_delayed_remove(file_param, mode))
            _bg_tasks.add(task)
            task.add_done_callback(_bg_tasks.discard)
        return True
    # 3. 失败：把完整响应打日志，方便排查（权限/路径/动作不支持等）
    print(f"⚠️ upload_group_file 失败（group={group_id}, file={file_param}, name={name}）")
    print(f"   原始响应: {json.dumps(resp, ensure_ascii=False) if resp else '无响应(可能超时或连接断开)'}")
    # 失败也清掉临时文件，防占磁盘
    if mode != "native":
        await asyncio.to_thread(remove_napcat_tmp, file_param, mode)
    return False


async def _delayed_remove(param: str, mode: str, delay_seconds: Optional[int] = None):
    """延迟删除NapCat的临时文件，避免打断后台上传"""
    if delay_seconds is None:
        delay_seconds = UPLOAD_CLEANUP_DELAY_SECONDS if UPLOAD_CLEANUP_DELAY_SECONDS else 300
    try:
        await asyncio.sleep(delay_seconds)
        await asyncio.to_thread(remove_napcat_tmp, param, mode)
    except Exception as e:
        print(f"⚠️ 延迟清理临时文件失败：{e}")


async def send_chat_file(websocket, is_group: bool, chat_id: int, file_path: str, name: str) -> bool:
    """以聊天文件消息发送文件（群聊/私聊都能用），消息里可直接点击下载。"""
    if not os.path.isfile(file_path):
        print(f"⚠️ 发送文件不存在: {file_path}")
        return False
    # 把文件放到NapCat能读到的地方；聊天文件消息的显示名=message里的name字段
    try:
        file_param, mode = await asyncio.to_thread(put_file_where_napcat_reads, file_path, name)
        print(f"📦 文件已就位: {file_param}（模式{mode}）")
    except Exception as e:
        print(f"❌ 文件就位失败，无法发送：{e}")
        return False
    # 用消息段数组形式发文件消息，NapCat会读取该路径
    if is_group:
        resp = await api_call(websocket, "send_group_msg",
                              {"group_id": chat_id,
                               "message": [{"type": "file", "data": {"file": file_param, "name": name}}]},
                              timeout=600)
    else:
        resp = await api_call(websocket, "send_private_msg",
                              {"user_id": chat_id,
                               "message": [{"type": "file", "data": {"file": file_param, "name": name}}]},
                              timeout=600)
    if resp.get("retcode") == 0:
        if mode != "native" and UPLOAD_CLEANUP:
            task = asyncio.create_task(_delayed_remove(file_param, mode))
            _bg_tasks.add(task)
            task.add_done_callback(_bg_tasks.discard)
        return True
    print(f"⚠️ 聊天文件消息发送失败：{json.dumps(resp, ensure_ascii=False) if resp else '无响应'}")
    # 失败也清掉临时文件，防占磁盘
    if mode != "native":
        await asyncio.to_thread(remove_napcat_tmp, file_param, mode)
    return False


async def handle_download_command(websocket, chat_id: int, aid: str, is_group: bool,
                                  chapter_index: int = DEFAULT_CHAPTER):
    """处理 下载本子<aid> [pN] 指令：后台 下载指定章节→打包加密→直接发文件。
    消息极简：收到任务一句 → 完成后只发文件本身（密码写在文件名里），不再刷屏。"""
    if not ENABLE_DOWNLOAD:
        print(f"📥 下载总开关已关闭，拒绝本子下载：{aid}")
        return
    async def notify(text: str):
        if is_group:
            await send_group_msg(websocket, chat_id, text)
        else:
            await send_private_msg(websocket, chat_id, text)

    task_key = f"{aid}:{chapter_index}"  # 同一本子不同章节可分开排队提示
    if task_key in _downloading_aids:
        await notify(f"{aid}正在下载中，请稍等～")
        return
    if _download_lock.locked():
        await notify("正在处理其他任务，稍后再试～")
        return

    _downloading_aids.add(task_key)
    img_suffix = IMG_SUFFIX  # 群聊/私聊统一png
    zip_path = zip_output_path(aid, img_suffix)
    file_name = display_file_name(aid)  # QQ上显示的文件名（带密码）
    print(f"📥 收到下载任务：{aid} p{chapter_index}（{'群' if is_group else '私聊'}{chat_id}）")

    async def worker():
        try:
            async with _download_lock:

                def fail_tip(path: str) -> str:
                    """发送失败提示：文件过大时明确告知换小包/jpg，而不是让人干等重试"""
                    try:
                        size_mb = os.path.getsize(path) / 1048576
                    except OSError:
                        size_mb = 0
                    if size_mb > MAX_SEND_MB:
                        return (f"{aid}发送失败：文件约{size_mb:.0f}MB\n"
                                f"文件已存服务端：{path}，可把 IMG_SUFFIX 改为 .jpg")
                    return f"{aid}发送失败，请稍后重试。文件已存服务端：{path}"

                async def do_send(path: str) -> bool:
                    """发送文件（聊天文件消息 或 群文件），成功返回True"""
                    if is_group and not SEND_FILE_AS_CHAT_MSG:
                        ok = await upload_group_file(websocket, chat_id, path, file_name)
                    else:
                        ok = await send_chat_file(websocket, is_group, chat_id, path, file_name)
                    if ok:
                        print(f"✅ {aid}已发送（{'群文件' if is_group and not SEND_FILE_AS_CHAT_MSG else '文件消息'}）")
                        # 发送成功：删本地缓存（图片目录+zip），容器临时文件由发送函数延时清理。
                        # 群文件上传后聊天里会出现文件卡片，不需要再补文字提示。
                        await asyncio.to_thread(cleanup_local_cache, aid, path)
                        return True
                    print(f"⚠️ {aid}发送失败：{path}")
                    return False

                # 异常残留的zip（上次发送失败留下的）直接重发；但超过大小上限就删掉重新下
                if os.path.isfile(zip_path) and os.path.getsize(zip_path) <= MAX_SEND_MB * 1024 * 1024:
                    print(f"📦 {aid}有残留zip，直接重发")
                    if not await do_send(zip_path):
                        await notify(fail_tip(zip_path))
                    return
                if os.path.isfile(zip_path):
                    print(f"🗑 {aid}残留zip超过{MAX_SEND_MB}MB，删除后重新下载小包")
                    try:
                        os.remove(zip_path)
                    except OSError:
                        pass

                if chapter_index > 1:
                    await notify(f"📥收到任务！开始下载{aid} 第{chapter_index}章")
                else:
                    await notify(f"📥收到任务！开始下载{aid}")
                # 1. 只下载用户选的章节（默认第1章）
                try:
                    folder, total_chapters = await asyncio.to_thread(download_jm_album, aid, chapter_index, img_suffix)
                    print(f"📥 {aid}（共{total_chapters}章）第{chapter_index}章下载完成：{folder}")
                except _NoSuchChapter as e:
                    await notify(f"本子{aid}只有{e.total_chapters}个章节，没有第{chapter_index}章")
                    return
                except Exception as e:
                    print(f"❌ {aid}下载失败：{e}")
                    await notify(f"{aid}下载失败：本子不存在或下载出错，请检查ID后重试")
                    return
                # 2. 打包（图片下载时已转png；有密码则7z AES加密）
                try:
                    new_zip, pages = await asyncio.to_thread(pack_album_to_zip, aid, folder, img_suffix)
                    print(f"📦 {aid}打包完成（{pages}张图）：{new_zip}")
                except Exception as e:
                    print(f"❌ {aid}打包失败：{e}")
                    await notify(f"{aid}打包失败，请稍后重试")
                    return
                # 3. 只发文件本身，不再发多余文字
                if not await do_send(new_zip):
                    await notify(fail_tip(new_zip))
        except Exception as e:
            print(f"❌ {aid}后台任务异常：{e}")
        finally:
            _downloading_aids.discard(task_key)

    task = asyncio.create_task(worker())
    _bg_tasks.add(task)  # 持有引用，任务完成后自动移除
    task.add_done_callback(_bg_tasks.discard)


# ===================== 汽水音乐自动下载：群/私聊收到 qishui.douyin.com 链接 → 下载MP3 → 发文件 =====================
_QISHUI_URL_RE = re.compile(r'https?://qishui\.douyin\.com/[A-Za-z0-9/._?=&%:\-]+')

# 汽水音乐VIP试听拦截：下载文件时长落在任一分段区间[秒]→疑似VIP试听（30秒或60秒试听），挂起待用户确认
_QISHUI_VIP_RANGES = [(29.0, 31.0), (59.0, 61.0)]
# 待确认表：chat_id -> {"path": 文件路径, "name": 发送名, "ts": 挂起时间戳}
_qishui_pending = {}
_QISHUI_PENDING_TTL = 60  # 60秒未回复自动清理文件（防止误触）

# ffmpeg可用性检测（模块级缓存，只检测一次）
_ffmpeg_checked = False
_ffmpeg_available = False


def _check_ffmpeg() -> bool:
    """检测系统是否有可用ffmpeg，结果缓存。有ffmpeg→转MP3；无ffmpeg→保留m4a原始格式。"""
    global _ffmpeg_checked, _ffmpeg_available
    if _ffmpeg_checked:
        return _ffmpeg_available
    _ffmpeg_checked = True
    # 用shutil.which搜索PATH，比subprocess调子进程更可靠（在asyncio.to_thread线程里不会有子进程问题）
    import shutil
    _ffmpeg_available = shutil.which("ffmpeg") is not None
    if _ffmpeg_available:
        print("🎵 检测到ffmpeg，汽水音乐将转码320k MP3")
    else:
        print("⚠️ 未检测到ffmpeg，汽水音乐保留原始m4a格式（不转MP3）")
    return _ffmpeg_available


def _probe_audio_duration(path: str):
    """用ffprobe查音频时长(秒)，失败返回None（复用视频识别的ffprobe方案）"""
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", path],
            capture_output=True, text=True, timeout=15)
        if probe.returncode != 0:
            return None
        info = json.loads(probe.stdout)
        d = float(info.get("format", {}).get("duration", 0))
        return d if d > 0 else None
    except Exception:
        return None


async def _qishui_pending_cleaner():
    """独立后台任务：每10秒主动清理过期的汽水VIP挂起文件（不等新消息触发）。
    超时未确认→删文件，避免占盘；也防误触。"""
    while True:
        try:
            _now_t = time.time()
            for _cid in [k for k, v in _qishui_pending.items() if _now_t - v.get("ts", 0) > _QISHUI_PENDING_TTL]:
                _v = _qishui_pending.pop(_cid, None)
                if _v:
                    try:
                        os.remove(_v["path"])
                        print(f"🎵 汽水试听未确认已过期，清理文件：{_v['path']}")
                    except Exception:
                        pass
        except Exception:
            pass
        await asyncio.sleep(10)


async def handle_qishui_download(websocket, chat_id, is_group, raw_link):
    """解析汽水音乐链接→下载→转码320k MP3→写ID3+封面→以聊天文件消息发出"""
    if not ENABLE_DOWNLOAD:
        print(f"📥 下载总开关已关闭，拒绝汽水音乐下载：{raw_link}")
        return
    async def notify(text):
        if is_group:
            await send_group_msg(websocket, chat_id, text)
        else:
            await send_private_msg(websocket, chat_id, text)

    print(f"🎵 收到汽水音乐链接：{raw_link}")
    await notify("🎵 检测到汽水音乐，正在下载...")

    def _do_work():
        """同步执行：解析→下载→转码→写标签，返回 (mp3_path, None) 或 (None, error_msg)"""
        song_name, artist, audio_url, cover_url, headers, err = qishui_parse(raw_link)
        if err:
            return None, err
        safe_name = re.sub(r'[\\/:*?"<>|]', "", song_name).strip() or "qishui_song"
        music_dir = os.path.join(DOWNLOAD_ROOT, "音乐")
        os.makedirs(music_dir, exist_ok=True)
        temp_path = os.path.join(music_dir, f"{safe_name}.tmp")
        mp3_path = os.path.join(music_dir, f"{safe_name}.mp3")
        # 已存在则直接复用（MP3或M4A任一存在即可）
        for ext in (".mp3", ".m4a"):
            exist_path = os.path.join(music_dir, f"{safe_name}{ext}")
            if os.path.isfile(exist_path) and os.path.getsize(exist_path) > 1024:
                print(f"🎵 {safe_name}{ext} 已存在，直接发送")
                return exist_path, None
        # 1. 下载原始音频
        try:
            res = requests.get(audio_url, headers=headers, timeout=90, stream=True)
            if res.status_code != 200:
                return None, f"音频下载失败，状态码：{res.status_code}"
            with open(temp_path, "wb") as f:
                for chunk in res.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        except Exception as e:
            return None, f"音频下载异常：{e}"
        # 2. 格式处理：有ffmpeg转320k MP3，无ffmpeg保留原始m4a
        use_ffmpeg = _check_ffmpeg()
        if use_ffmpeg:
            try:
                cmd = ["ffmpeg", "-y", "-i", temp_path, "-acodec", "libmp3lame",
                       "-b:a", "320k", "-id3v2_version", "3", mp3_path]
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
                if result.returncode != 0:
                    err_msg = result.stderr.decode("utf-8", errors="replace")[-300:] if result.stderr else "未知错误"
                    raise RuntimeError(f"ffmpeg返回码{result.returncode}：{err_msg}")
                if os.path.isfile(temp_path):
                    os.remove(temp_path)
                final_path = mp3_path
                print(f"🎵 ffmpeg转码完成：320k MP3")
            except Exception as e:
                print(f"⚠️ ffmpeg转码失败，保留原始m4a：{e}")
                final_path = os.path.join(music_dir, f"{safe_name}.m4a")
                try:
                    if os.path.isfile(temp_path):
                        os.rename(temp_path, final_path)
                except Exception:
                    final_path = temp_path
        else:
            final_path = os.path.join(music_dir, f"{safe_name}.m4a")
            try:
                if os.path.isfile(temp_path):
                    os.rename(temp_path, final_path)
            except Exception:
                final_path = temp_path

        if not os.path.isfile(final_path) or os.path.getsize(final_path) < 1024:
            return None, "音频文件不存在或过小"

        # 3. 写标签 + 封面（MP3用ID3，M4A用MP4原子；mutagen没装也不影响发送）
        try:
            ext = os.path.splitext(final_path)[1].lower()
            if ext == ".mp3":
                from mutagen.id3 import ID3, TIT2, TPE1, APIC
                tag = ID3()
                try:
                    tag.load(final_path)
                except Exception:
                    pass
                tag.delete()
                tag["TIT2"] = TIT2(encoding=3, text=song_name)
                tag["TPE1"] = TPE1(encoding=3, text=artist)
                if cover_url:
                    try:
                        img_res = requests.get(cover_url, timeout=15)
                        if img_res.status_code == 200 and len(img_res.content) > 100:
                            tag["APIC"] = APIC(encoding=0, mime="image/jpeg", type=3, desc="", data=img_res.content)
                    except Exception:
                        pass
                tag.save(final_path, v2_version=3)
            else:
                # M4A/MP4格式：用mutagen.mp4.MP4写原子标签（NAS无ffmpeg时也能带标题/歌手/封面）
                from mutagen.mp4 import MP4
                tag = MP4(final_path)
                tag["\xa9nam"] = [song_name]
                tag["\xa9ART"] = [artist]
                if cover_url:
                    try:
                        img_res = requests.get(cover_url, timeout=15)
                        if img_res.status_code == 200 and len(img_res.content) > 100:
                            tag["covr"] = [img_res.content]
                    except Exception:
                        pass
                tag.save()
            print(f"🎵 标签+封面写入完成：{song_name} - {artist}（{ext}格式）")
        except Exception as e:
            print(f"⚠️ 标签写入失败（不影响发送）：{e}")
        return final_path, None

    try:
        final_path, err = await asyncio.to_thread(_do_work)
    except Exception as e:
        err = f"下载异常：{e}"
        final_path = None

    if err or not final_path:
        await notify(f"❌ 汽水音乐下载失败：{err or '未知错误'}")
        return

    file_name = os.path.basename(final_path)
    # NTQQ群文件上传对长文件名/特殊字符敏感，清洗显示名（不改文件路径）
    upload_name = re.sub(r'[#\\/:*?"<>|]', '', file_name).strip()
    upload_name = re.sub(r'\s+', ' ', upload_name)
    if len(upload_name) > 50:
        _n, _e = os.path.splitext(upload_name)
        upload_name = _n[:45] + _e
    print(f"🎵 汽水音乐下载完成：{final_path}")
    # VIP试听拦截：时长落在29~45秒（疑似只能下30秒试听）→ 挂起待用户回复1确认，不直接发
    _dur = _probe_audio_duration(final_path)
    if _dur is not None and any(_lo <= _dur <= _hi for _lo, _hi in _QISHUI_VIP_RANGES):
        _qishui_pending[chat_id] = {"path": final_path, "name": upload_name, "ts": time.time()}
        print(f"🎵 疑似VIP歌曲（{_dur:.1f}秒），挂起待确认：{final_path}")
        await notify(f"🎵 疑似VIP歌曲，仅下载到{_dur:.0f}秒音频\n回复 1 强制发送试听音频；不回复则60秒后自动放弃。")
        return
    ok = await send_chat_file(websocket, is_group, chat_id, final_path, upload_name)
    if not ok:
        await notify(f"❌ 音乐发送失败，文件已存服务端：{final_path}")
    else:
        print(f"✅ 汽水音乐已发送")
        # 发送成功后删除原始文件，不占磁盘空间（不缓存，重复请求重新下载）
        try:
            if os.path.isfile(final_path):
                os.remove(final_path)
                print(f"🗑️ 汽水音乐已发送，删除原始文件：{final_path}")
        except Exception as e:
            print(f"⚠️ 删除汽水音乐文件失败：{e}")


# ===================== 抖音自动下载：群/私聊收到 douyin.com 链接 → 解析视频/图文 → 下载 → 发文件 =====================
# 排除qishui.douyin.com（汽水音乐已有单独处理，会先拦截）
# 短链 v.douyin.com/xxx/ 的ID后即停，避免把分享口令里的"8.71 04/"等尾巴吞进URL
_DOUYIN_URL_RE = re.compile(
    r'https?://'
    r'(?:'
    r'v\.douyin\.com/[A-Za-z0-9_-]+/?'
    r'|'
    r'(?!qishui\.)[a-zA-Z0-9_\-\.]*douyin\.com/[A-Za-z0-9/._?=&%:\-]+'
    r')'
)
_DOUYIN_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def _clean_douyin_url(raw_link):
    """清洗抖音分享文本里被正则多吞的尾巴，返回干净的URL。"""
    if not raw_link:
        return raw_link
    # 短链 v.douyin.com/xxx/  截断到ID，去掉尾部斜杠（https://v.douyin.com/XNG3v2JAmvk）
    m = re.match(r'(https?://v\.douyin\.com/[A-Za-z0-9_-]+)/?', raw_link)
    if m:
        return m.group(1)
    # 长链：去掉空格/换行后的尾巴（合法URL里不该有空格）
    return raw_link.split()[0]


def douyin_parse(raw_link):
    """用ttwid + 抖音web API解析作品，返回 (title, is_video, video_url, images_list, headers, error_msg)"""
    raw_link = _clean_douyin_url(raw_link)
    if not DOUYIN_TTWID:
        return None, None, None, None, None, (
            "抖音ttwid未配置！请在qqbot-config.yml的douyin_ttwid填入你的ttwid值。"
            "获取：浏览器登录抖音 → F12 → Application → Cookies → www.douyin.com → 复制ttwid的值"
        )
    s = requests.Session()
    s.headers.update({
        "User-Agent": _DOUYIN_UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    })
    s.cookies.set("ttwid", DOUYIN_TTWID, domain=".douyin.com")
    try:
        resp = s.get(raw_link, allow_redirects=True, timeout=20)
        final_url = str(resp.url)
    except Exception as e:
        return None, None, None, None, None, f"短链访问失败：{e}"
    if "/user/" in final_url:
        return None, None, None, None, None, "抖音主页链接不支持，请发单个作品链接"
    id_match = re.search(r'/(?:video|note|share)/(\d+)', final_url)
    if not id_match:
        return None, None, None, None, None, "无法提取作品ID"
    aweme_id = id_match.group(1)
    try:
        api_resp = s.get(
            f"https://www.douyin.com/aweme/v1/web/aweme/detail/?aweme_id={aweme_id}",
            headers={"Referer": final_url}, timeout=15)
        if api_resp.status_code != 200:
            return None, None, None, None, None, f"API请求失败，状态码：{api_resp.status_code}（ttwid可能已过期）"
        data = api_resp.json()
    except Exception as e:
        return None, None, None, None, None, f"API解析失败：{e}（ttwid可能已过期）"
    if data.get("status_code") != 0:
        return None, None, None, None, None, f"API返回错误：status_code={data.get('status_code')}（ttwid可能已过期）"
    item = data.get("aweme_detail")
    if not item:
        return None, None, None, None, None, "API返回中未找到作品数据"
    title = item.get("desc", "抖音作品")
    cookie_str = "; ".join([f"{c.name}={c.value}" for c in s.cookies])
    dy_headers = {"User-Agent": _DOUYIN_UA, "Referer": final_url, "Cookie": cookie_str}
    if "video" in item and item.get("video"):
        # 从bit_rate中选择最高清晰度（按height降序，相同height按bit_rate降序）
        video_data = item["video"]
        bit_rates = video_data.get("bit_rate", []) or []
        best_url = None
        best_info = ""
        if bit_rates:
            sorted_br = sorted(bit_rates, key=lambda x: (x.get("play_addr", {}).get("height", 0), x.get("bit_rate", 0)), reverse=True)
            for br in sorted_br:
                pa = br.get("play_addr", {})
                url_list = pa.get("url_list", [])
                if url_list:
                    best_url = url_list[0]
                    h = pa.get("height", 0)
                    w = pa.get("width", 0)
                    br_kbps = int(br.get("bit_rate", 0) / 1000)
                    best_info = f"{w}x{h} ({br_kbps}kbps)"
                    break
        # 回退到默认play_addr
        if not best_url:
            best_url = video_data["play_addr"]["url_list"][0]
            h = video_data["play_addr"].get("height", 0)
            w = video_data["play_addr"].get("width", 0)
            best_info = f"{w}x{h} (默认)"
        print(f"🎬 选择清晰度：{best_info}")
        return title, True, best_url, [], dy_headers, None
    elif "images" in item and item.get("images"):
        return None, None, None, None, None, "图文作品暂不支持，只支持抖音视频下载"
    else:
        return None, None, None, None, None, "无法识别作品类型（既不是视频也不是图文）"


# ============================================================
# 抖音 v2 解析器（Douyin_TikTok_Download_API，支持1080p）
# 优先用 v2，失败自动降级到 v1
# ============================================================
_DY_V2_COOKIE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "crawlers", "douyin", "web", "config.yaml")


def refresh_douyin_cookie_v2():
    """自动刷新 v2 解析器的 Cookie（ttwid + msToken + s_v_web_id），写入 config.yaml。
    成功返回 True，失败返回 False。"""
    try:
        import random as _rnd
        import string as _str

        # 1. ttwid
        ttwid = None
        try:
            _tt_data = json.dumps({
                "region": "cn", "aid": 1768, "needFid": False,
                "service": "www.ixigua.com",
                "migrate_info": {"ticket": "", "source": "node"},
                "cbUrlProtocol": "https", "union": True
            })
            _r = requests.post("https://ttwid.bytedance.com/ttwid/union/register/",
                               data=_tt_data, timeout=15,
                               headers={"Content-Type": "application/json"})
            ttwid = _r.cookies.get("ttwid")
        except Exception:
            pass

        # 2. msToken（失败用虚假的）
        msToken = None
        try:
            _mst_strdata = ("fWOdJTQR3/jwmZqBBsPO6tdNEc1jX7YTwPg0Z8CT+j3HScLFbj2Zm1XQ7"
                            "/lqgSutntVKLJWaY3Hc/+vc0h+So9N1t6EqiImu5jKyUa+S4NPy6cNP0x9CUQQ"
                            "gb4+RRihCgsn4QyV8jivEFOsj3N5zFQbzXRyOV+9aG5B5EAnwpn8C70llsWq0zJ"
                            "z1VjN6y2KZiBZRyonAHE8feSGpwMDeUTllvq6BG3AQZz7RrORLWNCLEoGzM6bMovY"
                            "VPRAJipuUML4Hq/568bNb5vqAo0eOFpvTZjQFgbB7f/CtAYYmnOYlvfrHKBKvb0TX"
                            "6AjYrw2qmNNEer2ADJosmT5kZeBsogDui8rNiI/OOdX9PVotmcSmHOLRfw1cYXTgwH"
                            "Xr6cJeJveuipgwtUj2FNT4YCdZfUGGyRDz5bR5bdBuYiSRteSX12EktobsKPksdhUPG"
                            "Gv99SI1QRVmR0ETdWqnKWOj/7ujFZsNnfCLxNfqxQYEZEp9/U01CHhWLVrdzlrJ1v+K"
                            "JH9EA4P1Wo5/2fuBFVdIz2upFqEQ11DJu8LSyD43qpTok+hFG3Moqrr81uPYiyPHnUvT"
                            "FgwA/TIE11mTc/pNvYIb8IdbE4UAlsR90eYvPkI+rK9KpYN/l0s9ti9sqTth12VAw8tz"
                            "CQvhKtxevJRQntU3STeZ3coz9Dg8qkvaSNFWuBDuyefZBGVSgILFdMy33//l/eTXhQpFr"
                            "Vc9OyxDNsG6cvdFwu7trkAENHU5eQEWkFSXBx9Ml54+fa3LvJBoacfPViyvzkJworlHcYY"
                            "TG392L4q6wuMSSpYUconb+0c5mwqnnLP6MvRdm/bBTaY2Q6RfJcCxyLW0xsJMO6fgLUEjA"
                            "g/dcqGxl6gDjUVRWbCcG1NAwPCfmYARTuXQYbFc8LO+r6WQTWikO9Q7Cgda78pwH07F8bg"
                            "J8zFBbWmyrghilNXENNQkyIzBqOQ1V3w0WXF9+Z3vG3aBKCjIENqAQM9qnC14WMrQkfCHo"
                            "sGbQyEH0n/5R2AaVTE/ye2oPQBWG1m0Gfcgs/96f6yYrsxbDcSnMvsA+okyd6GfWsdZYTIK"
                            "1E97PYHlncFeOjxySjPpfy6wJc4UlArJEBZYmgveo1SZAhmXl3pJY3yJa9CmYImWkhbpwsV"
                            "kSmG3g11JitJXTGLIfqKXSAhh+7jg4HTKe+5KNir8xmbBI/DF8O/+diFAlD+BQd3cV0G4mE"
                            "tCiPEhOvVLKV1pE+fv7nKJh0t38wNVdbs3qHtiQNN7JhY4uWZAosMuBXSjpEtoNUndI+o0cj"
                            "R8XJ8tSFnrAY8XihiRzLMfeisiZxWCvVwIP3kum9MSHXma75cdCQGFBfFRj0jPn1JildrTh"
                            "2vRgwG+KeDZ33BJ2VGw9PgRkztZ2l/W5d32jc7H91FftFFhwXil6sA23mr6nNp6CcrO7rObl"
                            "cm5SzXJ5MA601+WVic/g3p6A0lAnhjsm37qP+xGT+cbCFOfjexDYEhnqz0QZm94CCSnilQ9B"
                            "/HBLhWOddp9GK0SABIk5i3xAH701Xb4HCcgAulvfO5EK0RL2eN4fb+CccgZQeO1Zzo4qsMHc1"
                            "3UG0saMgBEH8SqYlHz2S0CVHuDY5j1MSV0nsShjM01vIynw6K0T8kmEyNjt1eRGlleJ5lvE8vo"
                            "nJv7rAeaVRZ06rlYaxrMT6cK3RSHd2liE50Z3ik3xezwWoaY6zBXvCzljyEmqjNFgAPU3gI+N1"
                            "vi0MsFmwAwFzYqqWdk3jwRoWLp//FnawQX0g5T64CnfAe/o2e/8o5/bvz83OsAAwZoR48GZzPu"
                            "7KCIN9q4GBjyrePNx5Csq2srblifmzSKwF5MP/RLYsk6mEE15jpCMKOVlHcu0zhJybNP3AKMVl"
                            "lF6pvn+HWvUnLXNkt0A6zsfvjAva/tbLQiiiYi6vtheasIyDz3HpODlI+BCkV6V8lkTt7m8J1Ic"
                            "gTfqjQBummyjYTSwsQji3DdNCnlKYd13ZQa545utqu837FFAzOZQhbnC3bKqeJqO2sE3m7WBUMb"
                            "RWLflPRqp/PsklN+9jBPADKxKPl8g6/NZVq8fB1w68D5EJlGExdDhglo4B0aihHhb1u3+zJ2Dqk"
                            "xkPCGBAZ2AcuFIDzD53yS4NssoWb4HJ7YyzPaJro+tgG9TshWRBtUw8Or3m0OtQtX+rboYn3+Gx"
                            "vD1O8vWInrg5qxnepelRcQzmnor4rHF6ZNhAJZAf18Rjncra00HPJBugY5rD+EwnN9+mGQo43b01"
                            "qBBRYEnxy9JJYuvXxNXxe47/MEPOw6qsxN+dmyIWZSuzkw8K+iBM/anE11yfU4qTFt0veCaVprK6"
                            "tXaFK0ZhGXDOYJd70sjIP4UrPhatp8hqIXSJ2cwi70B+TvlDk/o19CA3bH6YxrAAVeag1P9hmNlf"
                            "J7NxK3Jp7+Ny1Vd7JHWVF+R6rSJiXXPfsXi3ZEy0klJAjI51NrDAnzNtgIQf0V8OWeEVv7F8Rsm3"
                            "/GKnjdNOcDKymi9agZUgtctENWbCXGFnI40NHuVHtBRZeYAYtwfV7v6U0bP9s7uZGpkp+OETHMv3"
                            "AyV0MVbZwQvarnjmct4Z3Vma+DvT+Z4VlMVnkC2x2FLt26K3SIMz+KV2XLv5ocEdPFSn1vMR7zru"
                            "CWC8XqAG288biHo/soldmb/nlw8o8qlfZj4h296K3hfdFubGIUtqgsrZCrLCkkRC08Cv1ozEX/y6t"
                            "2YrQepwiNmwDVk5IufStVvJMj+y2r9TcYLv7UKWXx3P6aySvM2ZHPaZhv+6Z/A/jIMBSvOizn4qG1"
                            "1iK7Oo6JYhxCSMJZsetjsnL4ecSIAufEmoFlAScWBh6nFArRpVLvkAZ3tej7H2lWFRXIU7x7mdBfG"
                            "qU82PpM6znKMMZCpEsvHqpkSPSL+Kwz2z1f5wW7BKcKK4kNZ8iveg9VzY1NNjs91qU8DJpUnGyM04"
                            "C7KNMpeilEmoOxvyelMQdi85ndOVmigVKmy5JYlODNX744sHpeqmMEK/ux3xY5O406lm7dZlyGPSM"
                            "rFWbm4rzqvSEIskP43+9xVP8L84GeHE4RpOHg3qh/shx+/WnT1UhKuKpByHCpLoEo144udpzZswCY"
                            "SMp58uPrlwdVF31//AacTRk8dUP3tBlnSQPa1eTpXWFCn7vIiqOTXaRL//YQK+e7ssrgSUnwhuGKJ"
                            "8aqNDgdsL+haVZnV9g5Qrju643adyNixvYFEp0uxzOzVkekOMh2FYnFVIL2mJYGpZEXlAIC0zQbb54"
                            "rSP89j0G7soJ2HcOkD0NmMEWj/7hUdTuMin1lRNde/qmHjwhbhqL8Z9MEO/YG3iLMgFTgSNQQhyE8"
                            "AZAAKnehmzjORJfbK+qxyiJ07J843EDduzOoYt9p/YLqyTFmAgpdfK0uYrtAJ47cbl5WWhVXp5/XUx"
                            "wWdL7TvQB0Xh6ir1/XBRcsVSDrR7cPE221ThmW1EPzD+SPf2L2gS0WromZqj1PhLgk92YnnR9s7/n"
                            "LBXZHPKy+fDbJT16QqabFKqAl9G0blyfR5UGX2kN+iQp4VGXEoH5lXxNNTlgRskzrW7KliQXcac20o"
                            "imAHUE8Phf+rXXglpmSv4XN3eiwfXwvOaAMVjMRmRxsKitl5iZnwpcdbsC4jt16g2r/ihlKzLIYju+X"
                            "Zej4dNMlkftEidyNg24IVimJthXY1H15RZ8Hm7mAM/JZrsxiAVI0A49pWEiUk3cyZcBzq/vVEjHUy4"
                            "r6IZnKkRvLjqsvqWE95nAGMor+F0GLHWfBCVkuI51EIOknwSB1eTvLgwgRepV4pdy9cdp6iR8TZndP"
                            "VCikflXYVMlMEJ2bJ2c0Swiq57ORJW6vQwnkxtPudpFRc7tNNDzz4LKEznJxAwGi6pBR7/co2IUgRw1"
                            "ijLFTHWHQJOjgc7KaduHI0C6a+BJb4Y8IWuIk2u2qCMF1HNKFAUn/J1gTcqtIJcvK5uykpfJFCYc899"
                            "TmUc8LMKI9nu57m0S44Y2hPPYeW4XSakScsg8bJHMkcXk3Tbs9b4eqiD+kHUhTS2BGfsHadR3d5j8lN"
                            "hBPzA5e+mE==")
            _mst_payload = json.dumps({
                "magic": 538969122, "version": 1, "dataType": 8,
                "strData": _mst_strdata, "tspFromClient": int(time.time() * 1000),
            })
            _r2 = requests.post("https://mssdk.bytedance.com/web/report",
                                 data=_mst_payload, timeout=15,
                                 headers={"Content-Type": "application/json", "User-Agent": _DOUYIN_UA})
            msToken = _r2.cookies.get("msToken")
        except Exception:
            pass
        if not msToken:
            msToken = "".join(_rnd.choices(_str.ascii_letters + _str.digits, k=126)) + "=="

        # 3. s_v_web_id
        _base = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        _ms = int(round(time.time() * 1000))
        _b36 = ""
        while _ms > 0:
            _rem = _ms % 36
            _b36 = (str(_rem) if _rem < 10 else chr(ord("a") + _rem - 10)) + _b36
            _ms = int(_ms / 36)
        _o = [""] * 36
        _o[8] = _o[13] = _o[18] = _o[23] = "_"
        _o[14] = "4"
        for _i in range(36):
            if not _o[_i]:
                _n = int(_rnd.random() * len(_base))
                if _i == 19:
                    _n = 3 & _n | 8
                _o[_i] = _base[_n]
        s_v_web_id = "verify_" + _b36 + "_" + "".join(_o)

        # 4. 组装 Cookie
        cookie_parts = []
        if ttwid:
            cookie_parts.append(f"ttwid={ttwid}")
        cookie_parts.append(f"msToken={msToken}")
        cookie_parts.append(f"s_v_web_id={s_v_web_id}")
        cookie_str = "; ".join(cookie_parts)

        # 5. 写入 config.yaml（保留登录态字段，只更新 ttwid/msToken/s_v_web_id）
        if os.path.isfile(_DY_V2_COOKIE_PATH):
            with open(_DY_V2_COOKIE_PATH, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            existing_cookie = cfg.get("TokenManager", {}).get("douyin", {}).get("headers", {}).get("Cookie", "")
            # 登录态关键字段：如果现有Cookie包含这些字段，说明用户配了登录Cookie，需要保留
            _login_fields = ["sessionid", "sessionid_ss", "sid_guard", "sid_tt", "uid_tt", "uid_tt_ss",
                             "passport_csrf_token", "n_mh", "odin_tt", "sid_ucp_v1", "ssid_ucp_v1"]
            has_login = any(f in existing_cookie for f in _login_fields)
            if has_login and existing_cookie:
                # 合并：保留现有Cookie的所有字段，只替换 ttwid/msToken/s_v_web_id
                _parts = {}
                for _p in existing_cookie.split(";"):
                    _p = _p.strip()
                    if "=" in _p:
                        _k, _v = _p.split("=", 1)
                        _parts[_k.strip()] = _v.strip()
                if ttwid:
                    _parts["ttwid"] = ttwid
                _parts["msToken"] = msToken
                _parts["s_v_web_id"] = s_v_web_id
                cookie_str = "; ".join(f"{_k}={_v}" for _k, _v in _parts.items())
                print(f"🎬 [v2] Cookie 已刷新（保留登录态，更新ttwid/msToken）")
            else:
                print(f"🎬 [v2] Cookie 已自动刷新（ttwid={'有' if ttwid else '无'}, msToken={'真实' if msToken and len(msToken) in (120,128) else '虚假'}）")
            cfg["TokenManager"]["douyin"]["headers"]["Cookie"] = cookie_str
            with open(_DY_V2_COOKIE_PATH, "w", encoding="utf-8") as f:
                yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
            return True
        else:
            print(f"🎬 [v2] 配置文件不存在：{_DY_V2_COOKIE_PATH}")
            return False
    except Exception as e:
        print(f"🎬 [v2] Cookie 刷新失败：{e}")
        return False


def douyin_parse_v2(raw_link):
    """用 Douyin_TikTok_Download_API (HybridCrawler) 解析抖音作品，支持1080p。
    返回 (title, is_video, video_url, images_list, headers, error_msg)，失败时 error_msg 非空。"""
    raw_link = _clean_douyin_url(raw_link)
    try:
        from crawlers.hybrid.hybrid_crawler import HybridCrawler

        async def _parse():
            crawler = HybridCrawler()
            return await crawler.hybrid_parsing_single_video(raw_link, minimal=False)

        data = asyncio.run(_parse())
        if not data:
            return None, None, None, None, None, "v2解析返回空数据"

        title = data.get("desc", "抖音作品")

        # 图文作品（优先检查，因为图文作品的video字段可能也存在但无真实视频）
        if "images" in data and data.get("images"):
            images = []
            for img in data["images"]:
                url_list = img.get("url_list", [])
                if url_list:
                    images.append(url_list[0])
            if not images:
                return None, None, None, None, None, "v2解析未找到图片地址"
            dy_headers = {"User-Agent": _DOUYIN_UA, "Referer": "https://www.douyin.com/"}
            print(f"🎬 [v2] 图文作品，共{len(images)}张图片")
            return title, False, None, images, dy_headers, None

        # 视频作品
        if "video" in data and data.get("video"):
            video_data = data["video"]
            bit_rates = video_data.get("bit_rate", []) or []
            best_url = None
            best_info = ""
            # 按 bit_rate 降序选最高清晰度
            if bit_rates:
                sorted_br = sorted(bit_rates, key=lambda x: x.get("bit_rate", 0), reverse=True)
                for br in sorted_br:
                    pa = br.get("play_addr", {})
                    url_list = pa.get("url_list", [])
                    if url_list:
                        best_url = url_list[0]
                        h = pa.get("height", 0)
                        w = pa.get("width", 0)
                        br_kbps = int(br.get("bit_rate", 0) / 1000)
                        best_info = f"{w}x{h} ({br_kbps}kbps)"
                        break
            # 回退到默认 play_addr
            if not best_url:
                play_addr = video_data.get("play_addr", {})
                url_list = play_addr.get("url_list", [])
                if url_list:
                    best_url = url_list[0]
                    h = play_addr.get("height", 0)
                    w = play_addr.get("width", 0)
                    best_info = f"{w}x{h} (默认)"
            if not best_url:
                return None, None, None, None, None, "v2解析未找到视频地址"
            print(f"🎬 [v2] 选择清晰度：{best_info}")
            dy_headers = {"User-Agent": _DOUYIN_UA, "Referer": "https://www.douyin.com/"}
            return title, True, best_url, [], dy_headers, None

        else:
            return None, None, None, None, None, "v2解析无法识别作品类型"

    except Exception as e:
        return None, None, None, None, None, f"v2解析异常：{e}"



def douyin_parse_v3(raw_link):
    """v3：移动分享页直链方案——绕过aweme/detail API风控。
    移动UA+登录cookie访问iesdouyin分享页→提取item_list→playwm转play升1080p。
    返回 (title, is_video, video_url, images_list, headers, error_msg)"""
    raw_link = _clean_douyin_url(raw_link)
    # 从v2配置读取当前cookie（共享登录态）
    cookie_str = ""
    try:
        with open(_DY_V2_COOKIE_PATH, "r", encoding="utf-8") as _f:
            _cfg = yaml.safe_load(_f)
        cookie_str = _cfg.get("TokenManager", {}).get("douyin", {}).get("headers", {}).get("Cookie", "") or ""
    except Exception:
        pass
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 "
                      "Mobile/15E148 Safari/604.1",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://www.douyin.com/",
    })
    if cookie_str:
        for _part in cookie_str.split(";"):
            _part = _part.strip()
            if "=" in _part:
                _k, _v = _part.split("=", 1)
                try:
                    s.cookies.set(_k.strip(), _v.strip(), domain=".douyin.com")
                except Exception:
                    pass
    try:
        resp = s.get(raw_link, allow_redirects=True, timeout=20)
        final_url = str(resp.url)
    except Exception as e:
        return None, None, None, None, None, f"v3: 访问短链失败：{e}"
    id_match = re.search(r'/(?:video|note|share)/(\d+)', final_url)
    if not id_match:
        return None, None, None, None, None, "v3: 无法提取作品ID"
    aweme_id = id_match.group(1)
    # 移动分享页
    share_url = f"https://www.iesdouyin.com/share/video/{aweme_id}/"
    try:
        r2 = s.get(share_url, timeout=20)
        html = r2.text
    except Exception as e:
        return None, None, None, None, None, f"v3: 访问移动分享页失败：{e}"
    m = re.search(r'window\._ROUTER_DATA\s*=\s*(\{.*?\})\s*</script>', html, re.S)
    if not m:
        return None, None, None, None, None, "v3: 页面中未找到_ROUTER_DATA"
    try:
        data = json.loads(m.group(1))
    except Exception as e:
        return None, None, None, None, None, f"v3: JSON解析失败：{e}"

    def _find(obj, depth=0):
        if depth > 8:
            return None
        if isinstance(obj, dict):
            if "videoInfoRes" in obj and isinstance(obj["videoInfoRes"], dict):
                return obj["videoInfoRes"]
            for _v in obj.values():
                r = _find(_v, depth + 1)
                if r:
                    return r
        elif isinstance(obj, list):
            for _v in obj:
                r = _find(_v, depth + 1)
                if r:
                    return r
        return None

    vir = _find(data)
    if not vir:
        return None, None, None, None, None, "v3: 未找到videoInfoRes"
    items = vir.get("item_list") or []
    if not items:
        return None, None, None, None, None, "v3: item_list为空"
    item = items[0]
    title = item.get("desc", "抖音作品")
    dy_headers = {"User-Agent": _DOUYIN_UA, "Referer": "https://www.douyin.com/"}
    if cookie_str:
        dy_headers["Cookie"] = cookie_str

    # 图文作品
    images = item.get("images") or []
    if images:
        img_urls = []
        for img in images:
            url_list = img.get("url_list", []) or []
            if url_list:
                img_urls.append(url_list[-1])
        if not img_urls:
            return None, None, None, None, None, "v3: 图文未找到图片URL"
        print(f"🎬 [v3] 图文作品，共{len(img_urls)}张图片")
        return title, False, None, img_urls, dy_headers, None

    # 视频：playwm→play 无水印 + 升清晰度
    video = item.get("video") or {}
    play_addr = video.get("play_addr") or {}
    url_list = play_addr.get("url_list") or []
    if not url_list:
        return None, None, None, None, None, "v3: 未找到视频URL"
    best_url = url_list[0]
    h = video.get("height") or play_addr.get("height") or 0
    w = video.get("width") or play_addr.get("width") or 0
    new_url = best_url.replace("/playwm/", "/play/")
    if "ratio=" in new_url:
        target = "1080p" if h and h >= 1080 else (f"{h}p" if h else "720p")
        new_url = re.sub(r'ratio=\d+p', f'ratio={target}', new_url)
    print(f"🎬 [v3] 视频：{w}x{h}（playwm→play 无水印）")
    return title, True, new_url, [], dy_headers, None


async def handle_douyin_download(websocket, chat_id, is_group, raw_link):
    """解析抖音链接→下载视频/图文→发文件"""
    if not ENABLE_DOWNLOAD:
        print(f"📥 下载总开关已关闭，拒绝抖音下载：{raw_link}")
        return
    async def notify(text):
        if is_group:
            await send_group_msg(websocket, chat_id, text)
        else:
            await send_private_msg(websocket, chat_id, text)

    print(f"🎬 收到抖音链接：{raw_link}")
    await notify("🎬 检测到抖音链接，正在解析...")

    def _do_work():
        # 只走v3（移动分享页直链），不调用被风控的aweme/detail API，避免IP被重点关注
        title, is_video, video_url, images, headers, err = douyin_parse_v3(raw_link)
        if err:
            print(f"🎬 v3解析失败：{err}")
        if err:
            return None, err
        # 去掉特殊字符和换行符，按字节截断到200字节以内（Linux文件名最大255字节，中文占3字节）
        safe_title = re.sub(r'[\\/:*?"<>|\n\r\t]', "", title).strip()
        safe_title = safe_title.encode('utf-8')[:200].decode('utf-8', errors='ignore') or "douyin_post"
        dy_dir = os.path.join(DOWNLOAD_ROOT, "抖音")
        os.makedirs(dy_dir, exist_ok=True)

        if is_video:
            save_path = os.path.join(dy_dir, f"{safe_title}.mp4")
            if os.path.isfile(save_path) and os.path.getsize(save_path) > 1024:
                print(f"🎬 {safe_title}.mp4 已存在，直接发送")
                return save_path, None
            print(f"🎬 正在下载抖音视频：{title}")
            try:
                res = requests.get(video_url, headers=headers, timeout=120, stream=True)
                if res.status_code != 200:
                    return None, f"视频下载失败，状态码：{res.status_code}"
                with open(save_path, "wb") as f:
                    for chunk in res.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            except Exception as e:
                return None, f"视频下载异常：{e}"
            if not os.path.isfile(save_path) or os.path.getsize(save_path) < 1024:
                return None, "视频文件不存在或过小"
            return save_path, None
        else:
            zip_path = os.path.join(dy_dir, f"{safe_title}.zip")
            if os.path.isfile(zip_path) and os.path.getsize(zip_path) > 1024:
                print(f"🎬 {safe_title}.zip 已存在，直接发送")
                return zip_path, None
            print(f"🎬 抖音图文作品，共{len(images)}张图片，开始下载打包")
            import zipfile, shutil
            temp_dir = os.path.join(dy_dir, f"_tmp_{safe_title}")
            os.makedirs(temp_dir, exist_ok=True)
            try:
                for idx, img_url in enumerate(images, 1):
                    try:
                        img_res = requests.get(img_url, headers=headers, timeout=35, stream=True)
                        if img_res.status_code == 200:
                            # 先下载到临时文件，再用Pillow统一转成png（抖音图片多为webp，png兼容性更好）
                            tmp_path = os.path.join(temp_dir, f"{idx:02d}.tmp")
                            with open(tmp_path, "wb") as f:
                                for chunk in img_res.iter_content(chunk_size=512 * 1024):
                                    if chunk:
                                        f.write(chunk)
                            try:
                                from PIL import Image
                                img = Image.open(tmp_path)
                                # 处理透明通道：webp可能有alpha通道，转png时保留
                                if img.mode in ("RGBA", "LA", "P"):
                                    img = img.convert("RGBA")
                                else:
                                    img = img.convert("RGB")
                                png_path = os.path.join(temp_dir, f"{idx:02d}.png")
                                img.save(png_path, "PNG")
                                img.close()
                                os.remove(tmp_path)
                            except Exception as e:
                                # 转换失败则保留原始文件（改回原始扩展名）
                                ct = img_res.headers.get("Content-Type", "")
                                ext = ".jpg"
                                if "png" in ct: ext = ".png"
                                elif "webp" in ct: ext = ".webp"
                                os.rename(tmp_path, os.path.join(temp_dir, f"{idx:02d}{ext}"))
                                print(f"⚠️ 第{idx}张图片转png失败，保留原格式：{e}")
                    except Exception as e:
                        print(f"⚠️ 第{idx}张图片下载失败：{e}")
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for fname in sorted(os.listdir(temp_dir)):
                        fpath = os.path.join(temp_dir, fname)
                        if os.path.isfile(fpath):
                            zf.write(fpath, fname)
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)
            if not os.path.isfile(zip_path) or os.path.getsize(zip_path) < 1024:
                return None, "图文打包失败"
            return zip_path, None

    try:
        final_path, err = await asyncio.to_thread(_do_work)
    except Exception as e:
        err = f"下载异常：{e}"
        final_path = None

    if err or not final_path:
        await notify(f"❌ 抖音下载失败：{err or '未知错误'}")
        return

    file_name = os.path.basename(final_path)
    # NTQQ群文件上传对长文件名/特殊字符敏感，清洗显示名（不改文件路径）
    upload_name = re.sub(r'[#\\/:*?"<>|]', '', file_name).strip()
    upload_name = re.sub(r'\s+', ' ', upload_name)
    if len(upload_name) > 50:
        _n, _e = os.path.splitext(upload_name)
        upload_name = _n[:45] + _e
    print(f"🎬 抖音下载完成：{final_path}")
    # v2下载的1080p视频文件头/元数据可能异常，导致NTQQ报rich media transfer failed，用ffmpeg重新封装修复
    _repacked = await asyncio.to_thread(_repack_video_with_ffmpeg, final_path)
    if _repacked:
        final_path = _repacked
    ok = await send_chat_file(websocket, is_group, chat_id, final_path, upload_name)
    if not ok:
        await notify(f"❌ 抖音文件发送失败，文件已存服务端：{final_path}")
    else:
        print(f"✅ 抖音视频已发送")
        # 抖音视频文件大，发送成功后立即删除原始文件，不占磁盘空间（不缓存，重复请求重新下载）
        try:
            if os.path.isfile(final_path):
                os.remove(final_path)
                print(f"🗑️ 抖音视频已发送，删除原始文件：{final_path}")
        except Exception as e:
            print(f"⚠️ 删除抖音视频文件失败：{e}")


# ===================== DSML工具调用解析（部分模型会输出DSML而不是tool_calls） =====================
_DSML_INVOKE_RE = re.compile(r'<invoke\s+name="([^"]+)"[^>]*>(.*?)</invoke>', re.S)
_DSML_PARAM_RE = re.compile(r'<parameter\s+name="([^"]+)"[^>]*>(.*?)</parameter>', re.S)


def parse_dsml_calls(text: str) -> list:
    """解析DSML格式，返回 [{"name": ..., "arguments": {参数名: 值}}]，支持多条调用"""
    calls = []
    for m in _DSML_INVOKE_RE.finditer(text):
        name = m.group(1).strip()
        inner = m.group(2)
        args = {}
        for pm in _DSML_PARAM_RE.finditer(inner):
            key = pm.group(1).strip()
            val = pm.group(2).strip()
            if val.startswith("<![CDATA[") and val.endswith("]]>"):
                val = val[9:-3]
            args[key] = html.unescape(val)
        if name and args:
            calls.append({"name": name, "arguments": args})
    return calls


def _strip_dsml_tags(text: str) -> str:
    """清洗模型残留输出的DSML/工具调用标签（兼容全角｜｜和半角||竖线），返回纯文本。"""
    if not text:
        return ""
    # 成对的 invoke 块，连同内部参数一起删（DSML全角/半角变体 + 标准invoke）
    text = re.sub(r'<[｜|]*DSML[｜|]*invoke[^>]*>.*?</[｜|]*DSML[｜|]*invoke>', '', text, flags=re.S)
    text = _DSML_INVOKE_RE.sub('', text)
    # 残留的 tool_calls / parameter 等尖括号标签
    text = re.sub(r'</?[｜|]*DSML[｜|]*[^>]*>', '', text)
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


# ===================== 永久记忆存档（memory.json） =====================
class MemoryStore:
    """持久化记忆：chat_context(各群最近N轮上下文) + members(成员档案/印象值) + global_notes(全局备忘)。
    原子写（先写.tmp再os.replace），防止写坏存档；用户也可直接用记事本编辑memory.json。"""

    def __init__(self, path: str):
        self.path = path
        self.data = {"chat_context": {}, "members": {}, "global_notes": []}
        self.load()

    def load(self):
        try:
            if os.path.isfile(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    loaded = json.load(f) or {}
                if isinstance(loaded.get("chat_context"), dict):
                    self.data["chat_context"] = loaded["chat_context"]
                if isinstance(loaded.get("members"), dict):
                    self.data["members"] = loaded["members"]
                if isinstance(loaded.get("global_notes"), list):
                    self.data["global_notes"] = loaded["global_notes"]
                print(f"🧠 永久记忆已加载：{len(self.data['members'])}个成员档案，"
                      f"{len(self.data['global_notes'])}条备忘，{len(self.data['chat_context'])}个会话上下文")
        except Exception as e:
            print(f"⚠️ 记忆文件读取失败，将新建存档：{e}")

    def save(self):
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
        except Exception as e:
            print(f"⚠️ 记忆存档写入失败：{e}")

    # ---------- 对话上下文 ----------
    def load_all_context(self) -> dict:
        """启动时把存档里的各会话上下文全部取出（key转回str即可，group_history用int/str都能索引）"""
        return self.data.get("chat_context", {}) or {}

    def save_context(self, chat_key, history: list, max_turns: int):
        """保存某个会话最近 max_turns 轮（1轮=user+assistant两条）"""
        limit = max(1, int(max_turns)) * 2
        self.data.setdefault("chat_context", {})[str(chat_key)] = list(history)[-limit:]
        self.save()

    # ---------- 成员档案 / 印象值 ----------
    def _ensure_note_lists(self, m: dict) -> None:
        """把旧的字符串note字段转成列表结构（兼容历史数据）。"""
        if "public_notes" not in m:
            old = m.get("public_note") or m.get("note") or ""
            m["public_notes"] = [old[:60]] if old else []
        if "private_notes" not in m:
            old = m.get("private_note") or ""
            m["private_notes"] = [old[:80]] if old else []

    def _append_note(self, notes: list, text: str, max_items: int = 5, max_len: int = 60) -> bool:
        """追加一条note（去重、限条数），返回是否新增。"""
        t = text.strip()[:max_len]
        if not t:
            return False
        # 去重：已包含相同或高度相似的就不加
        for n in notes:
            if t in n or n in t:
                return False
        notes.append(t)
        while len(notes) > max_items:
            notes.pop(0)  # 删最老的
        return True

    def update_member(self, qq, favor_delta: int, nickname: str = "", note: str = "") -> dict:
        key = str(qq)
        members = self.data.setdefault("members", {})
        m = members.setdefault(key, {"nickname": "", "favor": 50, "note": "", "public_note": "", "private_note": "", "public_notes": [], "private_notes": []})
        self._ensure_note_lists(m)
        try:
            delta = int(favor_delta)
        except Exception:
            delta = 0
        delta = max(-1, min(1, delta))
        m["favor"] = max(0, min(100, int(m.get("favor", 50)) + delta))
        if nickname and str(nickname).strip():
            m["nickname"] = str(nickname).strip()[:20]
        if note and str(note).strip():
            self._append_note(m["public_notes"], note, max_items=5, max_len=60)
            # 同步旧字符串字段兼容
            m["note"] = "；".join(m["public_notes"])
            m["public_note"] = m["note"]
        self.save()
        return m

    def update_member_private(self, qq, private_note: str) -> dict:
        """私聊私密记忆：只在私聊场景注入，群聊不可见。列表式追加。"""
        key = str(qq)
        members = self.data.setdefault("members", {})
        m = members.setdefault(key, {"nickname": "", "favor": 50, "note": "", "public_note": "", "private_note": "", "public_notes": [], "private_notes": []})
        self._ensure_note_lists(m)
        if private_note and str(private_note).strip():
            self._append_note(m["private_notes"], private_note, max_items=5, max_len=80)
            m["private_note"] = "；".join(m["private_notes"])
        self.save()
        return m

    def forget_member_info(self, qq, keyword: str) -> int:
        """删掉成员public_notes/private_notes里包含关键词的记录，返回删除条数。"""
        key = str(qq)
        m = self.data.get("members", {}).get(key)
        if not m:
            return 0
        self._ensure_note_lists(m)
        kw = keyword.strip()
        if not kw:
            return 0
        before_pub = len(m["public_notes"])
        before_pri = len(m["private_notes"])
        m["public_notes"] = [n for n in m["public_notes"] if kw not in n]
        m["private_notes"] = [n for n in m["private_notes"] if kw not in n]
        removed = (before_pub - len(m["public_notes"])) + (before_pri - len(m["private_notes"]))
        if removed:
            m["note"] = "；".join(m["public_notes"])
            m["public_note"] = m["note"]
            m["private_note"] = "；".join(m["private_notes"])
            self.save()
        return removed

    def add_note(self, note: str):
        note = (note or "").strip()
        if not note:
            return
        notes = self.data.setdefault("global_notes", [])
        notes.append(note[:200])
        if len(notes) > 30:  # 最多保留30条，防止无限膨胀烧token
            del notes[:-30]
        self.save()

    def delete_note(self, keyword: str) -> int:
        """根据关键词删除已完成/过时的全局备忘；返回删除条数。"""
        notes = self.data.get("global_notes", [])
        if not notes:
            return 0
        kw = (keyword or "").strip()
        if not kw:
            return 0
        before = len(notes)
        self.data["global_notes"] = [n for n in notes if kw not in n]
        removed = before - len(self.data["global_notes"])
        if removed:
            self.save()
        return removed


def _impression_level(impression: int) -> str:
    """印象值数值转文字描述，让AI理解自己当前对该成员的整体印象"""
    if impression <= 20: return "印象很差"
    if impression <= 40: return "印象较差"
    if impression <= 60: return "印象一般"
    if impression <= 80: return "印象较好"
    return "印象很好"


def _build_memory_block(member_key, is_private: bool = False) -> str:
    """生成注入system提示词的记忆块。
    member_key：群聊=g{群号}_{QQ}，私聊=p{QQ}。
    is_private=False（群聊）：只注入公开层（public_note）——不知道私聊私事。
    is_private=True（私聊）：注入公开层+私密层（private_note）——完整人格。"""
    if not ENABLE_MEMORY or memory_store is None:
        return ""
    if not MEMORY_INJECT_PROMPT:
        return ""
    parts = []
    notes = memory_store.data.get("global_notes", [])
    if notes:
        parts.append("【群内长期备忘】\n" + "\n".join(f"- {n}" for n in notes[-15:]))
    members_map = memory_store.data.get("members", {})
    m = members_map.get(member_key)
    if m is None:
        _uid_fb = member_key.rsplit("_", 1)[-1] if "_" in member_key else member_key
        if _uid_fb != member_key:
            m = members_map.get(_uid_fb)
    if m:
        _qq_show = member_key.rsplit("_", 1)[-1] if "_" in member_key else member_key
        line = f"【当前成员】QQ号{_qq_show}"
        if m.get("nickname"):
            line += f"，昵称{m['nickname']}"
        parts.append(line)
        # 公开层：列表式拼接（群+私聊都可见）
        pub_list = m.get("public_notes") or ([m["public_note"]] if m.get("public_note") else [])
        if pub_list:
            parts.append("长期喜好/习惯/口头禅：\n" + "\n".join(f"- {n}" for n in pub_list))
        # 私密层：只私聊可见
        if is_private:
            pri_list = m.get("private_notes") or ([m["private_note"]] if m.get("private_note") else [])
            if pri_list:
                parts.append("【私聊私密记忆】（仅私聊可见，不要在群聊提起）\n" + "\n".join(f"- {n}" for n in pri_list))
    return ("\n\n" + "\n\n".join(parts)) if parts else ""


def _memory_tool_defs(is_group: bool = True) -> list:
    """AI可自主调用的记忆工具（写操作）。仅在ENABLE_MEMORY时下发。
    is_group=True（群聊）：只下发公开层工具。is_group=False（私聊）：额外下发私密记忆工具。"""
    tools = []

    # update_member：只记成员长期喜好/风格/习惯/口头禅/称呼
    update_props = {
        "qq": {"type": "string", "description": "成员QQ号，即消息前缀[QQ号:xxx]里的数字"},
        "nickname": {"type": "string", "description": "该成员的昵称/称呼，不知道或无需更新就留空字符串"},
        "note": {
            "type": "string",
            "description": (
                "该成员长期稳定的信息，比如：爱好（如编程/打游戏/看动漫）、职业、常用口头禅、说话风格、"
                "固定称呼、性格特点。一句话概括。会自动追加到档案里，最多保留5条；新记忆会顶掉最老的一条，重要的别乱加。"
                "不要记临时事件、当天话题、发的表情。"
            )
        }
    }
    required = ["qq", "note"]
    if ENABLE_FAVOR:
        update_props["favor_delta"] = {
            "type": "integer", "enum": [-1, 0, 1],
            "description": "印象值微调：愉快/被帮忙→1，被冒犯/不愉快→-1，一般→0"
        }
        required.append("favor_delta")
    tools.append({
        "type": "function",
        "function": {
            "name": "update_member",
            "description": (
                "记录某个群成员的长期信息（喜好、职业、风格、习惯、口头禅、固定称呼、性格特点）。"
                "当用户透露自己的爱好/职业/习惯/口头禅/称呼等长期稳定信息时，必须立刻调用本工具记录下来，"
                "不要犹豫或跳过。只记长期稳定的信息，不要记临时事件、当天话题、发的表情。"
                "绝不可以在回复里提到这个调用。"
            ),
            "parameters": {"type": "object", "properties": update_props, "required": required}
        }
    })

    # remember_note：记长期有用的群信息/自己要记住的事
    tools.append({
        "type": "function",
        "function": {
            "name": "remember_note",
            "description": (
                "记录一条长期有用的群内信息（约定、群规、重要事实、以后对话还要用到的事）或你自己需要记住的事。"
                "不要记临时话题/一次性事件。重启后依然保留。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "note": {"type": "string", "description": "要长期记住的内容，一句话概括"}
                },
                "required": ["note"]
            }
        }
    })

    # delete_note：删除已完成/过时的备忘
    tools.append({
        "type": "function",
        "function": {
            "name": "delete_note",
            "description": (
                "删除已经完成或过时的全局备忘（比如之前约定的事已经办完、旧信息过期了）。"
                "传入能唯一标识那条备忘的关键词，会删除所有包含该关键词的备忘。不要删还在生效的信息。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "要删除的备忘里的关键词（能唯一标识那条备忘）"}
                },
                "required": ["keyword"]
            }
        }
    })

    # update_private_info：只私聊下发，记私密信息（群聊不可见）
    if not is_group:
        tools.append({
            "type": "function",
            "function": {
                "name": "update_private_info",
                "description": (
                    "记录私聊中用户透露的私密/私人信息（感情、私事、隐私、只有你俩知道的事）。"
                    "这些信息只在私聊场景记住，绝不会在群聊里提起。当用户在私聊里聊到私人事情时调用。"
                    "一句话概括，会自动追加，最多保留5条；新记忆会顶掉最老的一条，重要的别乱加。绝不可以在回复里提到这个调用。"
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "info": {"type": "string", "description": "私密信息，一句话概括"}
                    },
                    "required": ["info"]
                }
            }
        })

    # replace_member_info：替换过时记忆（删旧+加新一步完成）
    tools.append({
        "type": "function",
        "function": {
            "name": "replace_member_info",
            "description": (
                "替换成员档案里过时的记忆：先删掉一条旧的，再加上一条新的。"
                "当记忆满了（5条）需要更新、或对方情况变了（换了爱好/换了称呼/之前记错了）时调用。"
                "keyword填旧记忆里的关键词，new_note填新的记忆内容。不要删还在生效的信息。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "qq": {"type": "string", "description": "成员QQ号"},
                    "keyword": {"type": "string", "description": "要删除的那条旧记忆里的关键词"},
                    "new_note": {"type": "string", "description": "新的记忆内容，一句话概括"}
                },
                "required": ["qq", "keyword", "new_note"]
            }
        }
    })

    return tools


_MEMORY_TOOL_NAMES = ("update_member", "remember_note", "delete_note", "update_private_info", "replace_member_info")


def _execute_memory_tool(tool_name: str, arguments: dict, member_key: str = "", is_private: bool = False) -> str:
    """执行记忆写操作，返回给AI的确认文本（tool角色消息）。
    member_key：群聊=g{群号}_{QQ}，私聊=p{QQ}。is_private：是否私聊场景。"""
    if memory_store is None:
        return "记忆功能未开启。"
    try:
        if tool_name == "update_private_info":
            if not is_private:
                return "此工具仅私聊可用。"
            info = str(arguments.get("info", "")).strip()
            if not info:
                return "update_private_info失败：info不能为空。"
            _key = member_key or str(arguments.get("qq", ""))
            memory_store.update_member_private(_key, info)
            print(f"🔒 记录私聊私密记忆：{info}")
            return "已记住这条私密信息（仅私聊可见）。请正常给出你的回复。"
        if tool_name == "replace_member_info":
            qq = str(arguments.get("qq", "")).strip()
            if not qq.isdigit():
                return "replace_member_info失败：qq必须是数字QQ号。"
            kw = str(arguments.get("keyword", "")).strip()
            new_note = str(arguments.get("new_note", "")).strip()
            if not kw:
                return "replace_member_info失败：keyword不能为空。"
            _key = member_key if member_key else qq
            removed = memory_store.forget_member_info(_key, kw)
            added = False
            if new_note:
                memory_store.update_member(_key, 0, "", new_note)
                added = True
            _r = f"已删除{removed}条旧记忆" if removed else "没找到要删的旧记忆"
            if added:
                _r += "，并加入了新记忆"
            print(f"🧠 替换成员记忆：{_r}（关键词：{kw}）")
            return f"{_r}。请正常给出你的回复。"
        if tool_name == "update_member":
            qq = str(arguments.get("qq", "")).strip()
            if not qq.isdigit():
                return "update_member失败：qq必须是数字QQ号。"
            _delta = int(arguments.get("favor_delta", 0) or 0) if ENABLE_FAVOR else 0
            _key = member_key if member_key else qq
            m = memory_store.update_member(
                _key, _delta,
                str(arguments.get("nickname", "")), str(arguments.get("note", ""))
            )
            _extra = f"，印象值→{m['favor']}" if ENABLE_FAVOR else ""
            print(f"🧠 更新成员{qq}长期信息{_extra}"
                  f"{'，备注：'+m['note'] if m.get('note') else ''}")
            return "已更新该成员的长期信息。请正常给出你的回复。"
        if tool_name == "remember_note":
            note = str(arguments.get("note", ""))
            memory_store.add_note(note)
            print(f"🧠 新增长期备忘：{note}")
            return "已记住该信息，会在以后的对话中保留。请正常给出你的回复。"
        if tool_name == "delete_note":
            kw = str(arguments.get("keyword", "")).strip()
            removed = memory_store.delete_note(kw)
            if removed > 0:
                print(f"🧠 删除了{removed}条过期备忘（关键词：{kw}）")
                return f"已删除{removed}条相关的过期备忘。请正常给出你的回复。"
            return f"没有找到包含「{kw}」的备忘，未删除。请正常给出你的回复。"
    except Exception as e:
        return f"记忆写入异常：{e}，请正常给出你的回复。"
    return "未知记忆工具。"


# ===================== AI表情包（自主选图，图片消息发送） =====================
def scan_emoji_dir():
    """扫描表情包目录，建立 {表情名(文件名去扩展名): 绝对路径} 映射。启动时和配置加载后调用。"""
    EMOJI_MAP.clear()
    if not ENABLE_EMOJI or not EMOJI_DIR or not os.path.isdir(EMOJI_DIR):
        return
    for fname in os.listdir(EMOJI_DIR):
        ext = os.path.splitext(fname)[1].lower()
        if ext in _EMOJI_EXTS:
            name = os.path.splitext(fname)[0].strip()
            if name:
                EMOJI_MAP[name] = os.path.join(EMOJI_DIR, fname)
    if EMOJI_MAP:
        print(f"😀 已加载{len(EMOJI_MAP)}个表情包：{ '、'.join(list(EMOJI_MAP.keys())[:12]) }"
              f"{'…' if len(EMOJI_MAP) > 12 else ''}")


def _emoji_tool_def():
    """动态生成 send_emoji 工具定义（enum 用当前扫描到的表情名，AI只能从已有表情里选）。"""
    names = list(EMOJI_MAP.keys())
    return {
        "type": "function",
        "function": {
            "name": "send_emoji",
            "description": (
                "当对话氛围合适时，发送一个表情包图片来辅助表达你的情绪/反应（相当于你真的发了张图）。"
                f"name 必须严格等于下列表情包名称之一：{'、'.join(names)}。"
                "要根据当前话题和情绪挑最贴切的；纯查资料、严肃问题、没有合适表情时就不要调用。一次最多发一个。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "enum": names, "description": "表情包名称，必须是上面列出的名称之一"}
                },
                "required": ["name"]
            }
        }
    }


def _pick_emoji(name: str) -> str:
    """根据AI给的名称匹配表情路径，支持精确匹配和去标点模糊匹配；匹配不到返回空串。"""
    if not name:
        return ""
    name = str(name).strip()
    if name in EMOJI_MAP:
        return EMOJI_MAP[name]
    # 容错：去掉感叹号/问号/空格/标点后再匹配（AI可能返回"你走！！"而文件名是"你走！！！"）
    def _norm(s):
        return re.sub(r'[\s!！?？.。~～]+', '', s)
    target = _norm(name)
    for k, v in EMOJI_MAP.items():
        if _norm(k) == target:
            return v
    # 包含匹配兜底
    for k, v in EMOJI_MAP.items():
        if target and (target in _norm(k) or _norm(k) in target):
            return v
    return ""


async def send_emoji_image(websocket, chat_id: int, is_group: bool, emoji_name: str):
    """把选中的表情包以图片消息(CQ:image base64)直接发到群/私聊，不走群文件。"""
    path = _pick_emoji(emoji_name)
    if not path:
        print(f"⚠️ AI选的表情包「{emoji_name}」不存在，已跳过")
        return
    try:
        import base64
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
        ext = os.path.splitext(path)[1].lower().lstrip(".")
        mime = "jpeg" if ext in ("jpg", "jpeg") else ("png" if ext == "png" else ("gif" if ext == "gif" else "webp"))
        cq = f"[CQ:image,file=base64://{b64},subType=0]"
        if is_group:
            resp = await api_call(websocket, "send_group_msg",
                                  {"group_id": chat_id, "message": cq}, timeout=20)
        else:
            resp = await api_call(websocket, "send_private_msg",
                                  {"user_id": chat_id, "message": cq}, timeout=20)
        if resp.get("retcode") == 0:
            print(f"😀 已发送表情包：{os.path.basename(path)}")
        else:
            print(f"⚠️ 表情包发送失败：{resp.get('msg') or resp}")
    except Exception as e:
        print(f"⚠️ 表情包发送异常：{e}")


def call_deepseek(prompt: str, history: list, user_id: int, pending_emojis: list = None,
                  is_group: bool = False, obj_id: int = 0):
    """调用DeepSeek API，AI自动判断是否需要联网搜索/发表情（function calling）。
    pending_emojis: 传入一个list，AI选中的表情包名会append进去，由主循环负责发图。
    is_group/obj_id：群聊=True+群号，私聊=False+QQ号；用于群/私聊记忆场景隔离。"""
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    # 场景化记忆key：群聊=g{群号}_{QQ}，私聊=p{QQ}——私聊记的事不串到群里
    member_key = f"g{obj_id}_{user_id}" if is_group else f"p{user_id}"
    # 系统提示词放在最前面（追加永久记忆块：当前发言人档案+全局备忘；私聊加私密层）
    messages = [{"role": "system", "content": SYSTEM_PROMPT + REPLY_STYLE_PROMPT + _build_memory_block(member_key, is_private=not is_group)}]
    # 加上历史对话
    messages.extend(history)
    # 加上当前用户消息，前面带QQ号标识
    _now_str = _format_now()
    messages.append({"role": "user", "content": f"[{_now_str}][QQ号:{user_id}] {prompt}"})

    # 定义搜索工具，让AI自己判断是否调用、调用几次、用什么关键词
    # 只有开启联网搜索时才下发搜索工具；记忆工具按 ENABLE_MEMORY 独立下发
    tools = []
    if ENABLE_WEB_SEARCH:
        tools.extend([
        {
            "type": "function",
            "function": {
                "name": "web_search",
                "description": "联网搜索实时信息，用于回答需要最新信息、新闻、天气、股价、比赛结果、具体数据、不了解的事物等问题。闲聊和常识问题不需要调用。如果需要更全面准确的信息，可以多次调用本工具，每次用不同的搜索关键词或角度。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "搜索关键词，要简洁准确"
                        }
                    },
                    "required": ["query"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "github_search",
                "description": "在GitHub上搜索开源项目、代码仓库、插件、源码。当用户要找某个具体开源项目/仓库/repo/插件/源码，或给出英文项目名、仓库名时，优先用本工具（比通用网页搜索精准得多）。query直接用项目英文原名，不要加无关中文修饰词。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "GitHub搜索关键词，直接用项目英文原名"
                        }
                    },
                    "required": ["query"]
                }
            }
        }
        ])
    if ENABLE_MEMORY:
        tools.extend(_memory_tool_defs(is_group))
    if ENABLE_EMOJI and EMOJI_MAP:
        tools.append(_emoji_tool_def())

    data = _build_chat_data(DEEPSEEK_MODEL, messages, ENABLE_THINKING_CHAT)
    if tools:
        data["tools"] = tools

    # 第一次调用：AI判断是否需要搜索（网络异常直接兜底，避免异常冒泡把整条连接打断）
    try:
        res_json = _post_json("https://api.deepseek.com/v1/chat/completions", headers, data, 30, "对话模型")
    except Exception as e:
        print(f"❌ {e}")
        return "（我这边网络卡了一下，再说一遍？）"

    _choices = res_json.get("choices") or []
    if not _choices:
        return f"AI请求出错：{res_json.get('error','未知错误')}"

    message = _choices[0]["message"]
    reply_text = message.get("content", "").strip()
    _log_reasoning(message, "对话-首轮")

    # 检查AI是否调用了搜索工具（标准tool_calls格式）
    tool_calls = list(message.get("tool_calls", []))

    # DSML兜底：部分模型输出DSML标签而不是tool_calls
    if (ENABLE_WEB_SEARCH or ENABLE_MEMORY) and reply_text and "<invoke" in reply_text:
        print("检测到DSML格式工具调用")
        dsml_calls = parse_dsml_calls(reply_text)
        if dsml_calls:
            for i, dc in enumerate(dsml_calls):
                tool_calls.append({
                    "id": f"dsml_{i}",
                    "type": "function",
                    "function": {"name": dc["name"], "arguments": json.dumps(dc["arguments"], ensure_ascii=False)}
                })
            # 去掉DSML标签，只留正文（通常为空）
            reply_text = _DSML_INVOKE_RE.sub("", reply_text).strip()
            reply_text = re.sub(r"<[^>]+>", "", reply_text).strip()

    # 挑出可执行的工具调用：搜索类（联网开启时）+ 记忆类（记忆开启时）
    _runnable_names = set()
    if ENABLE_WEB_SEARCH:
        _runnable_names.update(("web_search", "github_search"))
    if ENABLE_MEMORY:
        _runnable_names.update(_MEMORY_TOOL_NAMES)
    if ENABLE_EMOJI and EMOJI_MAP:
        _runnable_names.add("send_emoji")
    runnable_calls = [
        tc for tc in tool_calls
        if tc.get("function", {}).get("name") in _runnable_names
    ]

    if runnable_calls:
        # 把带tool_calls的assistant消息加入历史（过滤掉不执行的调用，只保留实际执行的）
        filtered_msg = dict(message)
        filtered_msg["tool_calls"] = runnable_calls
        messages.append(filtered_msg)

        for tool_call in runnable_calls:
            _tool_name = tool_call.get("function", {}).get("name")

            # 记忆类工具：写存档，回填确认文本
            if _tool_name in _MEMORY_TOOL_NAMES:
                try:
                    _mem_args = json.loads(tool_call["function"]["arguments"])
                except Exception:
                    _mem_args = {}
                _mem_result = _execute_memory_tool(_tool_name, _mem_args if isinstance(_mem_args, dict) else {}, member_key, not is_group)
                messages.append({"role": "tool", "tool_call_id": tool_call.get("id", ""),
                                 "content": _mem_result})
                continue

            # 表情包工具：把选中的表情名记到pending_emojis（主循环拿到ws后发图），回填确认文本
            if _tool_name == "send_emoji":
                try:
                    _emo_name = json.loads(tool_call["function"]["arguments"]).get("name", "")
                except Exception:
                    _emo_name = ""
                _emo_name = str(_emo_name or "").strip()
                if pending_emojis is not None and _emo_name and not pending_emojis:
                    pending_emojis.append(_emo_name)  # 一轮最多发一个表情
                print(f"😀 AI选择表情包：{_emo_name}")
                messages.append({"role": "tool", "tool_call_id": tool_call.get("id", ""),
                                 "content": "表情包已选定，会随你的文字回复一起发出，请正常给出文字回复，不要在文字里描述该图片。"})
                continue

            try:
                search_query = json.loads(tool_call["function"]["arguments"]).get("query", "")
            except Exception:
                search_query = ""
            search_query = (search_query or "").strip()

            if _tool_name == "github_search":
                # GitHub仓库搜索：直接用项目原名，不做中文无意义词过滤
                if not search_query:
                    messages.append({"role": "tool", "tool_call_id": tool_call.get("id", ""),
                                     "content": "未提供GitHub搜索关键词。"})
                    continue
                print(f"🐙 AI决定GitHub搜索：{search_query}")
                search_result = github_search(search_query, SEARCH_RESULT_NUM)
                print("="*60)
                print(f"📊 GitHub搜索关键词：{search_query}")
                print(search_result)
                print("="*60)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.get("id", ""),
                    "content": search_result
                })
                continue

            # 过滤无意义搜索词（AI有时返回"无""没有"或过短内容，避免乱搜出无关结果）
            if (not search_query or len(search_query) < 2
                    or search_query.lower() in ("无", "没有", "无。", "none", "null", "n/a", "不知道", "暂无", "无内容")):
                print(f"⚠️ AI生成的搜索词「{search_query}」无意义，跳过本次搜索")
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.get("id", ""),
                    "content": "本次无需联网搜索（搜索词无意义），请直接根据已有知识和用户提供的内容回答。"
                })
                continue
            print(f"🔍 AI决定联网搜索：{search_query}")

            # 搜索1遍，用AI自己生成的关键词，不浪费次数
            print(f"🔍 正在联网搜索：{search_query}")
            search_result = web_search(search_query, SEARCH_RESULT_NUM)
            print("="*60)
            print(f"📊 搜索关键词：{search_query}")
            print(f"📊 完整搜索结果如下：")
            print(search_result)
            print("="*60)

            # 把搜索结果作为tool角色的回复加入消息历史
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.get("id", ""),
                "content": search_result
            })

        # 第二次调用：AI结合搜索结果回答（同样兜底网络异常）
        data2 = _build_chat_data(DEEPSEEK_MODEL, messages, ENABLE_THINKING_CHAT)
        try:
            res_json2 = _post_json("https://api.deepseek.com/v1/chat/completions", headers, data2, 30, "对话模型")
            _choices2 = res_json2.get("choices") or []
            if _choices2:
                msg2 = _choices2[0]["message"]
                reply_text = msg2.get("content", "").strip()
                _log_reasoning(msg2, "对话-搜索后")
            else:
                reply_text = f"AI请求出错：{res_json2.get('error','未知错误')}"
        except Exception as e:
            print(f"❌ {e}")
            reply_text = reply_text or "（我这边网络卡了一下，再说一遍？）"
    elif tool_calls:
        # AI调了工具但没调可执行工具（比如联网被关闭），保留第一轮回答
        print("⚠️ AI发起了工具调用但没有可执行的工具，跳过搜索")
        if not reply_text:
            reply_text = "（抱歉，我这次没处理好，请再问一次）"

    # 兜底清洗：删除任何残留的DSML/工具调用标签，防止泄漏到群里
    reply_text = _strip_dsml_tags(reply_text)
    if not reply_text:
        reply_text = "（我这边没组织好语言，再问我一次试试）"
    return reply_text


def query_deepseek_balance():
    """查询DeepSeek API余额，返回(余额字符串, 是否可用, 错误信息)"""
    try:
        resp = requests.get(
            "https://api.deepseek.com/user/balance",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}"},
            timeout=15
        )
        data = resp.json()
        infos = data.get("balance_infos")
        if not infos:
            return None, False, f"查询失败：{data.get('error', data)}"
        # 优先取人民币(CNY)余额，没有就取第一个
        info = next((x for x in infos if x.get("currency") == "CNY"), infos[0])
        return info.get("total_balance"), bool(data.get("is_available")), None
    except Exception as e:
        return None, False, f"查询异常：{e}"


def get_pricing_period():
    """判断当前北京时间是高峰还是空闲时段（DeepSeek峰谷定价）。
    工作日(周一至周五) 9:00-12:00、14:00-18:00 为高峰，其余（含周末全天）为空闲。
    返回 'peak'（高峰）或 'offpeak'（空闲），显示名由配置 PEAK_PERIOD_NAME/OFFPEAK_PERIOD_NAME 决定。"""
    from datetime import timezone, timedelta
    bj_now = datetime.now(timezone(timedelta(hours=8)))
    if bj_now.weekday() >= 5:  # 周六周日全天空闲
        return "offpeak"
    h = bj_now.hour
    return "peak" if (9 <= h < 12) or (14 <= h < 18) else "offpeak"


def call_deepseek_vision(prompt: str, history: list, user_id: int, image_data_urls: list,
                         pending_emojis: list = None, is_group: bool = False, obj_id: int = 0):
    """调用DeepSeek视觉模型：文字+多张base64图片一次性投给AI，并支持博查联网搜索（function calling）。
    图片不逐张串行识别，多张图在一个请求里投完，需要搜索时由视觉模型自己发起。
    pending_emojis: 传入list，AI选中的表情包名append进去，由主循环发图。
    is_group/obj_id：群聊=True+群号，私聊=False+QQ号；用于群/私聊记忆场景隔离。"""
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    # 场景化记忆key：群聊=g{群号}_{QQ}，私聊=p{QQ}——私聊记的事不串到群里
    member_key = f"g{obj_id}_{user_id}" if is_group else f"p{user_id}"
    messages = [{"role": "system", "content": SYSTEM_PROMPT + REPLY_STYLE_PROMPT + _build_memory_block(member_key, is_private=not is_group)}]
    # 历史只保留纯文本（图片base64太大不入历史）
    messages.extend(history)
    # 当前用户消息：文字 + 多张图片（一次性投完，不逐张识别）
    _now_str = _format_now()
    content = [{"type": "text", "text": f"[{_now_str}][QQ号:{user_id}] {prompt}"}]
    for data_url in image_data_urls:
        content.append({"type": "image_url", "image_url": {"url": data_url}})
    messages.append({"role": "user", "content": content})

    tools = []
    if ENABLE_WEB_SEARCH:
        tools.extend([
        {
            "type": "function",
            "function": {
                "name": "web_search",
                "description": "联网搜索实时信息，用于回答需要最新信息、新闻、天气、股价、比赛结果、具体数据、不了解的事物等问题。闲聊和常识问题、以及图片里已经能看清的内容不需要调用。如果需要更全面准确的信息，可以多次调用本工具，每次用不同的搜索关键词或角度。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "搜索关键词，要简洁准确"
                        }
                    },
                    "required": ["query"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "github_search",
                "description": "在GitHub上搜索开源项目、代码仓库、插件、源码。当用户要找某个具体开源项目/仓库/repo/插件/源码，或给出英文项目名、仓库名（包括图片里出现的项目名）时，优先用本工具（比通用网页搜索精准得多）。query直接用项目英文原名，不要加无关中文修饰词。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "GitHub搜索关键词，直接用项目英文原名"
                        }
                    },
                    "required": ["query"]
                }
            }
        }
        ])
    if ENABLE_MEMORY:
        tools.extend(_memory_tool_defs(is_group))
    if ENABLE_EMOJI and EMOJI_MAP:
        tools.append(_emoji_tool_def())

    # 开关打开时图片识别用语言模型（V4.1原生多模态），关闭时用单独的视觉模型（原逻辑）
    _vision_model = DEEPSEEK_MODEL if USE_CHAT_MODEL_FOR_VISION else DEEPSEEK_VISION_MODEL
    _vision_thinking = ENABLE_THINKING_CHAT if USE_CHAT_MODEL_FOR_VISION else ENABLE_THINKING_VISION

    data = _build_chat_data(_vision_model, messages, _vision_thinking)
    if tools:
        data["tools"] = tools

    # 第一次调用：视觉模型看图 + 判断是否需要搜索（网络异常兜底，避免打断整条连接）
    try:
        res_json = _post_json("https://api.deepseek.com/v1/chat/completions", headers, data, 90, "视觉模型")
    except Exception as e:
        print(f"❌ {e}")
        return "（图片我看到了，但我这边网络卡了一下，稍后再试？）"

    _choices = res_json.get("choices") or []
    if not _choices:
        return f"AI请求出错：{res_json.get('error','未知错误')}"

    message = _choices[0]["message"]
    reply_text = (message.get("content") or "").strip()
    _log_reasoning(message, "视觉-首轮")
    tool_calls = list(message.get("tool_calls", []))

    # DSML兜底：部分模型输出DSML标签而不是tool_calls
    if (ENABLE_WEB_SEARCH or ENABLE_MEMORY) and reply_text and "<invoke" in reply_text:
        print("[视觉]检测到DSML格式工具调用")
        dsml_calls = parse_dsml_calls(reply_text)
        if dsml_calls:
            for i, dc in enumerate(dsml_calls):
                tool_calls.append({
                    "id": f"vdsml_{i}",
                    "type": "function",
                    "function": {"name": dc["name"], "arguments": json.dumps(dc["arguments"], ensure_ascii=False)}
                })
            reply_text = _DSML_INVOKE_RE.sub("", reply_text).strip()
            reply_text = re.sub(r"<[^>]+>", "", reply_text).strip()

    # 可执行工具：搜索类（联网开启时）+ 记忆类（记忆开启时）
    _runnable_names = set()
    if ENABLE_WEB_SEARCH:
        _runnable_names.update(("web_search", "github_search"))
    if ENABLE_MEMORY:
        _runnable_names.update(_MEMORY_TOOL_NAMES)
    if ENABLE_EMOJI and EMOJI_MAP:
        _runnable_names.add("send_emoji")
    runnable_calls = [
        tc for tc in tool_calls
        if tc.get("function", {}).get("name") in _runnable_names
    ]

    if runnable_calls:
        filtered_msg = dict(message)
        filtered_msg["tool_calls"] = runnable_calls
        messages.append(filtered_msg)

        for tool_call in runnable_calls:
            _tool_name = tool_call.get("function", {}).get("name")

            # 记忆类工具：写存档，回填确认文本
            if _tool_name in _MEMORY_TOOL_NAMES:
                try:
                    _mem_args = json.loads(tool_call["function"]["arguments"])
                except Exception:
                    _mem_args = {}
                _mem_result = _execute_memory_tool(_tool_name, _mem_args if isinstance(_mem_args, dict) else {}, member_key, not is_group)
                messages.append({"role": "tool", "tool_call_id": tool_call.get("id", ""),
                                 "content": _mem_result})
                continue

            # 表情包工具：把选中的表情名记到pending_emojis（主循环拿到ws后发图），回填确认文本
            if _tool_name == "send_emoji":
                try:
                    _emo_name = json.loads(tool_call["function"]["arguments"]).get("name", "")
                except Exception:
                    _emo_name = ""
                _emo_name = str(_emo_name or "").strip()
                if pending_emojis is not None and _emo_name and not pending_emojis:
                    pending_emojis.append(_emo_name)  # 一轮最多发一个表情
                print(f"😀 AI选择表情包：{_emo_name}")
                messages.append({"role": "tool", "tool_call_id": tool_call.get("id", ""),
                                 "content": "表情包已选定，会随你的文字回复一起发出，请正常给出文字回复，不要在文字里描述该图片。"})
                continue

            try:
                search_query = json.loads(tool_call["function"]["arguments"]).get("query", "")
            except Exception:
                search_query = ""
            search_query = (search_query or "").strip()

            if _tool_name == "github_search":
                # GitHub仓库搜索：直接用项目原名，不做中文无意义词过滤
                if not search_query:
                    messages.append({"role": "tool", "tool_call_id": tool_call.get("id", ""),
                                     "content": "未提供GitHub搜索关键词。"})
                    continue
                print(f"🐙 [视觉]AI决定GitHub搜索：{search_query}")
                search_result = github_search(search_query, SEARCH_RESULT_NUM)
                print("="*60)
                print(f"📊 [视觉]GitHub搜索关键词：{search_query}")
                print(search_result)
                print("="*60)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.get("id", ""),
                    "content": search_result
                })
                continue

            if (not search_query or len(search_query) < 2
                    or search_query.lower() in ("无", "没有", "无。", "none", "null", "n/a", "不知道", "暂无", "无内容")):
                print(f"⚠️ [视觉]搜索词「{search_query}」无意义，跳过本次搜索")
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.get("id", ""),
                    "content": "本次无需联网搜索（搜索词无意义），请直接根据图片内容和已有知识回答。"
                })
                continue
            print(f"🔍 [视觉]AI决定联网搜索：{search_query}")
            search_result = web_search(search_query, SEARCH_RESULT_NUM)
            print("="*60)
            print(f"📊 [视觉]搜索关键词：{search_query}")
            print(search_result)
            print("="*60)
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.get("id", ""),
                "content": search_result
            })

        # 第二次调用：结合图片+搜索结果回答；不再下发tools，并明确要求直接作答，避免再次输出工具标签
        messages.append({"role": "system", "content": "搜索结果已在上方，请直接用中文、结合图片内容回答用户，不要再调用任何工具，也不要输出任何工具调用标签或代码。"})
        data2 = _build_chat_data(_vision_model, messages, _vision_thinking)
        try:
            res_json2 = _post_json("https://api.deepseek.com/v1/chat/completions", headers, data2, 90, "视觉模型")
            _choices2 = res_json2.get("choices") or []
            if _choices2:
                msg2 = _choices2[0]["message"]
                reply_text = (msg2.get("content") or "").strip()
                _log_reasoning(msg2, "视觉-搜索后")
            else:
                reply_text = f"AI请求出错：{res_json2.get('error','未知错误')}"
        except Exception as e:
            print(f"❌ {e}")
            reply_text = reply_text or "（我这边网络卡了一下，稍后再试？）"
    elif tool_calls:
        print("⚠️ [视觉]发起了工具调用但没有可执行的web_search，跳过搜索")
        if not reply_text:
            reply_text = "（抱歉，我这次没处理好，请再问一次）"

    # 兜底清洗：删除任何残留的DSML/工具调用标签，防止泄漏到群里
    reply_text = _strip_dsml_tags(reply_text)
    if not reply_text:
        reply_text = "（我这边没组织好语言，再问我一次试试）"
    return reply_text


def spawn_link_download(websocket, chat_id, is_group, url, kind):
    """抖音/汽水链接下载统一走后台任务：同链接去重 + 每会话冷却，避免阻塞主循环被刷爆"""
    key = (kind, url)
    if key in _download_tasks:
        print(f"⏳ 该链接正在下载中，忽略重复请求：{url[:60]}")
        return
    now = asyncio.get_running_loop().time()
    if now - _last_link_download.get(chat_id, 0.0) < LINK_DOWNLOAD_COOLDOWN:
        print(f"⏳ {chat_id} 链接下载冷却中（{LINK_DOWNLOAD_COOLDOWN}s），忽略：{url[:60]}")
        return
    _last_link_download[chat_id] = now
    coro = (handle_qishui_download(websocket, chat_id, is_group, url) if kind == "qishui"
            else handle_douyin_download(websocket, chat_id, is_group, url))
    task = asyncio.create_task(coro)
    _download_tasks[key] = task
    _bg_tasks.add(task)

    def _done(t):
        _bg_tasks.discard(t)
        _download_tasks.pop(key, None)

    task.add_done_callback(_done)



# ===================== 消息合并（防抖）：同一会话连续消息合并成一次AI回复 =====================
async def get_reply_message(ws, message_id):
    """调用OneBot API根据消息ID获取被回复的消息内容"""
    try:
        payload = {
            "action": "get_msg",
            "params": {"message_id": message_id},
            "echo": f"get_msg_{message_id}"
        }
        await ws.send(json.dumps(payload))
        # 简单等待返回（这里用同步方式拿不到，先返回None避免报错，后面再改异步）
        return None
    except Exception as e:
        print(f"获取回复消息失败：{e}")
        return None


async def _do_ai_reply(ws, chat_id, user_id, is_group, user_text, display_text, reply_text=""):
    """单次AI回复完整流程：取图片/视频/语音缓存→组装prompt→调模型→更新上下文→分段发送→表情包。
    由主循环（未开启合并）或合并worker调用；异常只影响本条消息，不冒泡断WS。
    注意：冷却防刷屏由调用方处理（未合并路径在主循环、合并路径靠窗口+条数上限）。"""
    # 取出该用户缓存的所有图片（含当前消息的，取完即清空；自动过滤过期）
    cached_data_urls = _cache_take_images(chat_id, user_id)
    # 取出该用户缓存的视频抽帧（取完即清空）
    video_frames, video_transcript = _cache_take_video(chat_id, user_id)
    if video_frames:
        cached_data_urls.extend(video_frames)
        print(f"📹 连同视频抽帧{len(video_frames)}帧一并投给视觉模型")
    if video_transcript:
        user_text += f"\n\n【视频语音转文字内容】：{video_transcript}"
    # 取出该用户缓存的语音转写（取完即清空），@时把语音内容一并投给语言模型
    voice_transcript = _cache_take_record(chat_id, user_id)
    if voice_transcript:
        user_text += f"\n\n【语音转文字内容】：{voice_transcript}"

    # 把被回复消息的内容加到用户问题里
    if reply_text:
        user_text += reply_text

    if not user_text:
        # 只@没有文字时，传简单标识，AI自己知道怎么回；私聊空消息单独处理
        user_text = "（群友@了你）" if is_group else "（收到一条空消息）"

    # 上下文管理（场景隔离：群聊上下文=g_{群号}，私聊上下文=p_{QQ号}，互不串）
    mem_key = f"g_{chat_id}" if is_group else f"p_{chat_id}"
    his = group_history[mem_key]
    # 私聊单向可见：把群聊最近几条作为背景拼到私聊上下文前面（群聊看不到私聊内容）
    if not is_group and ENABLE_MEMORY:
        try:
            for _g in ALLOWED_GROUPS:
                _gkey = f"g_{_g}"
                if _gkey in group_history and group_history[_gkey]:
                    _g_hist = group_history[_gkey][-6:]
                    _g_lines = []
                    for _gm in _g_hist:
                        _role = "群成员" if _gm.get("role") == "user" else "你在群里"
                        _g_lines.append(f"{_role}：{_gm.get('content','')[:150]}")
                    if _g_lines:
                        his = [{"role": "system", "content": "【群里最近聊的（你能看到群聊，但群聊不知道私聊内容）】\n" + "\n".join(_g_lines)}] + his
                    break
        except Exception:
            pass
    # AI选中的表情包会收集到这里，文字回复后以图片消息发出
    pending_emojis = []
    # 群聊旁听缓存：把"没@你"的闲聊一并作为上下文投给AI（取走即清空，不写进历史、不重复塞）
    _ambient_block = ""
    if is_group and ENABLE_AMBIENT_CACHE:
        _ambient_block = _ambient_take_text(chat_id)
        if _ambient_block:
            print(f"👂 附带群聊背景 {_ambient_block.count('[QQ号:')} 条（未@机器人的消息）")
    _prompt_for_ai = (user_text + _ambient_block) if _ambient_block else user_text
    # 有图：文字+多张图一次性投视觉模型（视觉模型自带博查搜索，不逐张串行识别）；无图：走对话模型
    # 整段兜底：AI调用异常只影响这一条消息，不再冒泡出main导致WS断开、后台任务全被取消
    try:
        if cached_data_urls:
            print(f"🖼️  连同{len(cached_data_urls)}张图片一次性投给视觉模型（带联网搜索）")
            reply = await asyncio.to_thread(call_deepseek_vision, _prompt_for_ai, his, user_id, cached_data_urls, pending_emojis, is_group, chat_id)
        else:
            reply = await asyncio.to_thread(call_deepseek, _prompt_for_ai, his, user_id, pending_emojis, is_group, chat_id)
    except Exception as _e:
        print(f"❌ AI调用异常（本条消息跳过，连接保持）：{_e}")
        return
    # 更新上下文
    his.append({"role": "user", "content": user_text})
    his.append({"role": "assistant", "content": reply})
    # 控制上下文长度
    if len(his) > MAX_CONTEXT_LEN * 2:
        his.pop(0)
        his.pop(0)
    group_history[mem_key] = his
    # 持久化最近 MEMORY_CONTEXT_TURNS 轮到 memory.json（重启不丢；群聊/私聊分key隔离）
    if ENABLE_MEMORY and memory_store is not None:
        try:
            await asyncio.to_thread(
                memory_store.save_context, mem_key, his, MEMORY_CONTEXT_TURNS)
        except Exception as _e:
            print(f"⚠️ 上下文存档失败：{_e}")

    # 回复消息（按段落拆成多条发送，降低人机感）
    _parts = await send_reply_multi(ws, is_group, chat_id, reply)
    _where = f"群{chat_id}" if is_group else "私聊"
    chat_print(f"{_where} | 用户{user_id}：{display_text}\n AI回复（{len(_parts)}条）："
               + "\n---\n".join(_parts))

    # AI选中了表情包：文字回复后以图片消息直接发出（不走群文件）
    for _emo in pending_emojis:
        await asyncio.sleep(0.8)  # 与文字回复稍微错开，更像真人
        await send_emoji_image(ws, chat_id, is_group, _emo)


async def _merge_worker(ws, chat_id, is_group):
    """消息合并防抖worker：每会话一个。
    窗口逻辑：条数未到上限且距最后一条未到等待时长 → 继续等；条数到上限 → 立即处理。
    处理完若又有新消息（用户还在发）则继续下一轮，直到缓冲清空。"""
    try:
        while True:
            buf = _merge_buf.get(chat_id, [])
            if not buf:
                break
            while len(buf) < MERGE_MAX_COUNT and (time.time() - _merge_last_ts.get(chat_id, 0)) < MERGE_WAIT_SECONDS:
                await asyncio.sleep(0.5)
                buf = _merge_buf.get(chat_id, [])
            msgs = _merge_buf.pop(chat_id, [])
            if not msgs:
                break
            # 多条合并成一条用户输入（让AI知道这些是连续消息，一并回复）
            if len(msgs) == 1:
                user_text = msgs[0]["user_text"]
                display_text = msgs[0]["display_text"]
                reply_text = msgs[0].get("reply_text", "")
            else:
                # 检查是不是同一个人发的
                user_ids = set(_m["user_id"] for _m in msgs)
                if len(user_ids) == 1:
                    # 同一个人连续发的
                    _lines = []
                    for _i, _m in enumerate(msgs, 1):
                        _t = (_m["user_text"] or "").strip() or "（空消息）"
                        _lines.append(f"第{_i}条：{_t}")
                    user_text = f"（你连续发送了{len(msgs)}条消息，这是一次连续发送，请一并回复）\n" + "\n".join(_lines)
                    display_text = " | ".join(_m["display_text"] for _m in msgs)
                    reply_text = "\n".join(_m.get("reply_text", "") for _m in msgs if _m.get("reply_text"))
                else:
                    # 不同人连续发的：按昵称区分
                    _lines = []
                    for _m in msgs:
                        _t = (_m["user_text"] or "").strip() or "（空消息）"
                        _lines.append(f"{_m['nickname']}：{_t}")
                    user_text = f"（群里连续{len(msgs)}条消息，分别来自不同的人，请一并回复）\n" + "\n".join(_lines)
                    display_text = " | ".join(f"{_m['nickname']}：{_m['display_text']}" for _m in msgs)
                    reply_text = "\n".join(_m.get("reply_text", "") for _m in msgs if _m.get("reply_text"))
            print(f"🧩 合并窗口结束，{len(msgs)}条消息一起回复（{'群' if is_group else '私聊'}{chat_id}）")
            try:
                await _do_ai_reply(ws, chat_id, msgs[-1]["user_id"], is_group, user_text, display_text, reply_text)
            except Exception as _e:
                print(f"❌ 合并回复异常：{_e}")
            if not _merge_buf.get(chat_id):
                break
    finally:
        _merge_workers.pop(chat_id, None)


# 说明：max_size 放宽到 64MB。默认 1MB 时，发 base64 表情/大图后 NapCat 回传的事件会超限触发 1009 断连
async def main():
    async with websockets.connect(ONEBOT_WS, max_size=64 * 1024 * 1024) as ws:
        print("✅ 成功连接 Lagrange.OneBot！等待群消息...")
        print(f"📋 已启用群白名单，只回复群: {ALLOWED_GROUPS}")
        print(f"👤 已启用私聊白名单，只回复QQ: {ALLOWED_USERS}")
        print("🎭 已加载系统提示词，机器人身份已设定")
        # 从永久记忆恢复各会话的历史上下文（重启不丢对话）
        if ENABLE_MEMORY and memory_store is not None:
            _saved_ctx = memory_store.load_all_context()
            # 旧key迁移：裸数字群号→g_群号，裸数字QQ号→p_QQ号（群聊/私聊上下文隔离）
            for _old in list(_saved_ctx.keys()):
                if not (_old.startswith("g_") or _old.startswith("p_")):
                    try:
                        _num = int(_old)
                    except Exception:
                        continue
                    if _num in ALLOWED_GROUPS:
                        _saved_ctx[f"g_{_num}"] = _saved_ctx.pop(_old)
                    elif _num in ALLOWED_USERS:
                        _saved_ctx[f"p_{_num}"] = _saved_ctx.pop(_old)
            _restored = 0
            for _k, _v in _saved_ctx.items():
                if isinstance(_v, list) and _v:
                    group_history[_k] = _v
                    _restored += 1
            print(f"🧠 永久记忆已开启：恢复{_restored}个会话的最近{MEMORY_CONTEXT_TURNS}轮上下文，AI可自主更新成员印象值/备忘")
        else:
            print("🧠 永久记忆已关闭")
        print(f"🧠 对话模型思考模式：{'开启(' + REASONING_EFFORT + ')' if ENABLE_THINKING_CHAT else '关闭'}")
        if USE_CHAT_MODEL_FOR_VISION:
            print(f"🧠 图片识别：使用语言模型 {DEEPSEEK_MODEL}（思考模式{'开启' if ENABLE_THINKING_CHAT else '关闭'}）")
        else:
            print(f"🧠 视觉模型思考模式：{'开启(' + REASONING_EFFORT + ')' if ENABLE_THINKING_VISION else '关闭'}（模型：{DEEPSEEK_VISION_MODEL}）")
        print("📥 本子下载已开启：群/私聊直接发「下载本子<ID>」或「下载<ID>」(默认第1章)，加 pN 选章节，完成自动清理缓存")
        if ENABLE_MERGE:
            print(f"🧩 消息合并已开启：连续消息{MERGE_WAIT_SECONDS}秒内无新消息或攒够{MERGE_MAX_COUNT}条→合并成一次AI回复（防多条回复混在一起）")
        else:
            print("🧩 消息合并已关闭：每条消息立即单独回复")
        # 不再自动刷新v2 cookie（避免触发aweme/detail API风控）
        # v3直接读config.yaml里的登录cookie即可
        if ENABLE_WEB_SEARCH and BOCHA_API_KEY and len(BOCHA_API_KEY.strip()) > 10:
            print("🌐 博查AI联网搜索已开启")
        elif ENABLE_WEB_SEARCH:
            print("⚠️  未配置博查API_KEY，联网搜索未启用")
        else:
            print("🚫 联网搜索已关闭（不会给AI下发搜索工具）")

        # 发送上线消息到所有允许的群（开关控制）
        if ENABLE_NOTIFY:
            for gid in ALLOWED_GROUPS:
                try:
                    await api_call(ws, "send_group_msg", {"group_id": gid, "message": "服务启动"}, timeout=5)
                except:
                    pass
            print("📢 已发送上线通知")
        else:
            print("上线通知已关闭")

        # 启动清理：只删自己遗留的待传临时文件（只跑一次，重连不重复）
        if delivery_mode() != "native":
            try:
                await asyncio.to_thread(docker_clear_container_data)
            except Exception as e:
                print(f"⚠️ 启动清理异常（可忽略）：{e}")

        # 启动时清理遗留待传文件（容器/shared目录，占盘元凶之一），失败不影响启动
        if delivery_mode() != "native":
            try:
                await asyncio.to_thread(docker_clear_container_data)
            except Exception as e:
                print(f"⚠️ 启动清理临时文件异常（可忽略）：{e}")

        # ===== 单连接分发器：所有收到的帧只有两个去处 =====
        #   - 带echo的API响应 -> 交给对应等待方（不丢失、不错配）
        #   - 其余事件（消息/通知/心跳）-> 放进事件队列，由主循环逐个处理
        event_queue = asyncio.Queue()
        disconnected = False

        async def dispatcher():
            nonlocal disconnected
            while True:
                try:
                    raw = await ws.recv()
                except Exception as e:
                    print(f"❌ WebSocket连接断开：{e}")
                    disconnected = True
                    # 让所有等待中的API调用立刻结束，避免挂死
                    for fut in list(_pending_echo.values()):
                        if not fut.done():
                            fut.set_exception(ConnectionError("WebSocket连接断开"))
                    _pending_echo.clear()
                    await event_queue.put(None)  # 哨兵：通知主循环退出
                    return
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                echo = data.get("echo")
                if echo is not None:
                    fut = _pending_echo.pop(echo, None)
                    if fut is not None and not fut.done():
                        fut.set_result(data)
                else:
                    await event_queue.put(data)

        # 汽水VIP挂起文件后台清理（常驻，每10秒主动检查过期）
        _qs_cleaner = asyncio.create_task(_qishui_pending_cleaner())
        _bg_tasks.add(_qs_cleaner)
        _qs_cleaner.add_done_callback(_bg_tasks.discard)

        dispatcher_task = asyncio.create_task(dispatcher())

        try:
            while True:
                data = await event_queue.get()
                if data is None:  # 连接已断开
                    break
                # 只处理消息事件
                if data.get("post_type") != "message":
                    continue

                message_type = data.get("message_type")
                user_id = data.get("user_id")
                nickname = data.get("sender", {}).get("nickname", str(user_id))
                message = data.get("message", "") or ""
                raw_msg = data.get("raw_message", "") or ""
                self_id = data.get("self_id")  # 机器人小号QQ
                if user_id is None or self_id is None:   # 畸形事件直接跳过，避免KeyError拖断连接
                    continue
                # 兼容消息段数组格式：统一转成CQ码字符串再处理
                cq_msg = message_to_cq_str(message)

                # 根据消息类型分别处理
                if message_type == "group":
                    group_id = data["group_id"]
                    # 群白名单过滤
                    if group_id not in ALLOWED_GROUPS:
                        continue
                    chat_id = group_id
                    is_group = True
                elif message_type == "private":
                    # 私聊白名单过滤
                    if user_id not in ALLOWED_USERS:
                        continue
                    chat_id = user_id
                    is_group = False
                else:
                    continue

                # ===== 汽水音乐VIP试听确认：用户回复"1"→发送挂起的试听音频文件（过期清理由后台任务负责） =====
                _pend = _qishui_pending.get(chat_id)
                if _pend and re.sub(r"\[CQ:[^]]*\]", "", cq_msg).strip() == "1":
                    _qishui_pending.pop(chat_id, None)
                    print(f"🎵 用户确认，发送试听文件：{_pend['path']}")
                    _ok = await send_chat_file(ws, is_group, chat_id, _pend["path"], _pend["name"])
                    if _ok:
                        print(f"✅ 汽水音乐试听已发送")
                        try:
                            os.remove(_pend["path"])
                            print(f"🗑️ 汽水音乐试听已发送，删除文件")
                        except Exception:
                            pass
                    else:
                        if is_group:
                            await send_group_msg(ws, chat_id, f"❌ 音乐发送失败，文件已存服务端：{_pend['path']}")
                        else:
                            await send_private_msg(ws, chat_id, f"❌ 音乐发送失败，文件已存服务端：{_pend['path']}")
                    continue

                # ===== 汽水音乐链接自动下载（后台任务，不阻塞主循环；同一链接去重+冷却） =====
                qs_match = _QISHUI_URL_RE.search(cq_msg)
                if qs_match:
                    spawn_link_download(ws, chat_id, is_group, qs_match.group(0), "qishui")
                    continue

                # ===== 抖音链接自动下载（后台任务；汽水音乐已先排除） =====
                dy_match = _DOUYIN_URL_RE.search(cq_msg)
                if dy_match:
                    spawn_link_download(ws, chat_id, is_group, dy_match.group(0), "douyin")
                    continue

                # ===== 本子下载指令拦截（不需要@）：群/私聊直接发「下载本子123」或「下载123 pN」 =====
                _album_text = re.sub(r"\[CQ:[^]]*\]", "", cq_msg).strip()
                _album_cmd = _ALBUM_CMD_RE.match(_album_text)
                if _album_cmd:
                    aid = _album_cmd.group(1).lstrip("0") or "0"
                    chapter = int(_album_cmd.group(2)) if _album_cmd.group(2) else DEFAULT_CHAPTER
                    if aid == "0":
                        _tip = "格式：下载本子123 或 下载123，可选章节：下载本子123 p2"
                        if is_group:
                            await send_group_msg(ws, chat_id, _tip)
                        else:
                            await send_private_msg(ws, chat_id, _tip)
                    else:
                        await handle_download_command(ws, chat_id, aid, is_group, chapter)
                    continue

                # ===== 图片提前提取+缓存（在@判断之前，手机端可先不@只发图）=====
                image_urls = extract_images_from_message(cq_msg)
                # 视频提取（群成员发的视频消息）
                video_urls = extract_videos_from_message(cq_msg)
                # CQ:file 文件形式发的视频（私聊常见，无 url= 只有 file_id），调 get_file API 补链接
                if not video_urls:
                    for fm in re.finditer(r'\[CQ:file,([^\]]+)\]', cq_msg):
                        fparams = fm.group(1)
                        print("🔍 [debug] fparams完整内容:")
                        print("  " + fparams)
                        # 已经有 url 的跳过
                        if re.search(r'url=', fparams):
                            continue
                        fname_m = re.search(r'file=([^,\]]+)', fparams)
                        if not fname_m:
                            print("🔍 [debug] 没匹配到file=")
                            continue
                        fname = fname_m.group(1).replace('&amp;', '&').strip()
                        print("🔍 [debug] fname=" + fname)
                        _video_suffix_re = r'\.(?:mp4|mov|avi|mkv|webm|flv|m4v)$'
                        _suffix_ok = bool(re.search(_video_suffix_re, fname, re.I))
                        print("🔍 [debug] 后缀匹配=" + str(_suffix_ok))
                        if not _suffix_ok:
                            continue
                        fid_m = re.search(r'file_id=([^,\]]+)', fparams)
                        if not fid_m:
                            print("🔍 [debug] 没匹配到file_id=")
                            continue
                        fid = fid_m.group(1).replace('&amp;', '&').strip()
                        print("🔍 [debug] fid长度=" + str(len(fid)))
                        try:
                            fdata = await api_call(ws, "get_file", {"file_id": fid}, timeout=10)
                            print("🔍 [debug] get_file返回keys=" + str(list(fdata.keys())))
                            finfo = (fdata.get("data", {}) or {})
                            print("🔍 [debug] data=" + str(finfo)[:200])
                            furl = finfo.get("url") or finfo.get("file") or ""
                            print("🔍 [debug] furl=" + str(furl)[:150])
                            # 接受http URL 或 容器内本地路径（共享napcat-qqdata卷后可直接访问）
                            if furl and (str(furl).startswith("http") or str(furl).startswith("/app/") or os.path.isfile(str(furl))):
                                video_urls.append(str(furl))
                                print(f"🎬 通过get_file API获取到文件视频（{fname}）")
                            else:
                                print("🔍 [debug] furl不是http也不是本地路径，跳过")
                        except Exception as fe:
                            print(f"⚠️ get_file获取视频链接失败：{fe}")
                # 语音提取（群成员发的语音消息，QQ语音silk格式需NapCat转码再转写）
                record_items = extract_records_from_message(cq_msg)
                # 回复消息里的图片/文本也要提取
                reply_match = re.search(r'\[CQ:reply,id=(\d+)', cq_msg)
                reply_text = ""
                if reply_match:
                    reply_id = int(reply_match.group(1))
                    print(f"🔗 检测到回复消息，ID: {reply_id}，正在获取被回复消息...")
                    reply_data = await get_reply_message(ws, reply_id)
                    if reply_data:
                        reply_raw = reply_data.get("raw_message", reply_data.get("message", ""))
                        reply_sender = reply_data.get("sender", {})
                        reply_nickname = reply_sender.get("nickname", "未知")
                        reply_text = f"\n\n【被回复的消息内容】（来自{reply_nickname}）：{reply_raw}"
                        print(f"📨 被回复消息：{reply_raw[:100]}")
                        reply_cq = message_to_cq_str(reply_raw) if not isinstance(reply_raw, str) else reply_raw
                        reply_images = extract_images_from_message(reply_cq)
                        if reply_images:
                            print(f"🖼️  被回复消息中包含{len(reply_images)}张图片")
                            image_urls.extend(reply_images)
                        # 被回复消息里的视频：先从url=提取，没有再调OneBot API补
                        reply_videos = extract_videos_from_message(reply_cq)
                        if not reply_videos:
                            # CQ:video 没url=，提取file字段调API获取
                            m = re.search(r'\[CQ:video,[^\]]*file=([^,\]]+)', reply_cq)
                            if m:
                                vfile = m.group(1).replace('&amp;', '&')
                                try:
                                    resp = await ws.send_json({"action": "get_video", "params": {"file": vfile}}, no_resp=False)
                                    vdata = json.loads(resp) if isinstance(resp, str) else resp
                                    vurl = (vdata.get("data", {}) or {}).get("url") or (vdata.get("data", {}) or {}).get("video_url", "")
                                    if vurl:
                                        reply_videos.append(vurl)
                                        print(f"🎬 通过API获取到被回复视频URL")
                                except Exception as ve:
                                    print(f"⚠️ 获取被回复视频URL失败：{ve}")
                        if reply_videos:
                            print(f"🎬 被回复消息中包含{len(reply_videos)}个视频")
                            video_urls.extend(reply_videos)
                        # 被回复消息里的语音
                        reply_records = extract_records_from_message(reply_cq)
                        if reply_records:
                            print(f"🎙️ 被回复消息中包含{len(reply_records)}条语音")
                            record_items.extend(reply_records)

                # 去掉所有CQ码后的纯文字（@判断和纯图片判断都要用）
                _plain_text = re.sub(r"\[CQ:[^]]*\]", "", cq_msg).strip()

                # 群消息先判断是否@了机器人——必须放在"纯图片静默缓存"之前，
                # 否则"@了它但只发图/只引用图片"（正文为空）会被静默吞掉、永远不回复
                if is_group:
                    at_flag = (f"[CQ:at,qq={self_id}]" in cq_msg     # CQ码at自己
                               or f"@{self_id}" in raw_msg)           # 纯文本at自己QQ号
                    # 注意：不能用 _plain_text.startswith("@") 判断，因为NapCat把@别人传成纯文本"@昵称"会误判
                else:
                    at_flag = True    # 私聊一律视为在跟它说话

                # 下载图片转base64并加入该用户缓存（每张60秒过期，每人最多10张）
                if image_urls:
                    print(f"🖼️  检测到{len(image_urls)}张图片，下载并缓存...")
                    for img_url in image_urls:
                        data_url, dl_err = await asyncio.to_thread(download_image_as_data_url, img_url)
                        if data_url:
                            cnt, rejected = _cache_add_image(chat_id, user_id, data_url)
                            if rejected:
                                print(f"⚠️ 图片缓存已满（上限{_IMAGE_CACHE_MAX}张），拒绝新图片")
                            else:
                                print(f"📦 图片已缓存（该用户当前{cnt}/{_IMAGE_CACHE_MAX}张，{_IMAGE_CACHE_TTL}秒有效）")
                        else:
                            print(f"⚠️ 图片下载失败：{dl_err}")

                # 视频：下载→查时长(>10分钟拒)→抽帧缓存（每人只存一个）
                if ENABLE_VIDEO and video_urls:
                    print(f"📹 检测到{len(video_urls)}个视频，下载并抽帧...")
                    for vurl in video_urls:
                        frames, dur, transcript, verr = await asyncio.to_thread(_download_and_extract_frames, vurl)
                        if verr:
                            print(f"⚠️ 视频处理失败：{verr}")
                        else:
                            _cache_add_video(chat_id, user_id, frames, dur, transcript)

                # 语音：NapCat get_record转wav→下载→Whisper转写→缓存（每人只存一条，@时把内容投给AI）
                if ENABLE_VOICE and record_items:
                    print(f"🎙️ 检测到{len(record_items)}条语音，转写中...")
                    for rec in record_items:
                        voice_url = rec.get("url", "")
                        if not voice_url:
                            # 没带url就调NapCat get_record（out_format=wav）拿转换后的语音链接
                            try:
                                resp = await api_call(ws, "get_record",
                                                      {"file": rec["file"], "out_format": "wav"}, timeout=10)
                                vdata = resp.get("data")
                                if isinstance(vdata, dict):
                                    voice_url = vdata.get("url") or vdata.get("file") or ""
                                else:
                                    voice_url = vdata or ""
                            except Exception as ve:
                                print(f"⚠️ 获取语音链接失败：{ve}")
                        if not voice_url or (not str(voice_url).startswith("http") and not os.path.isfile(str(voice_url))):
                            print(f"⚠️ 语音{rec['file']}无法获取可下载的链接，跳过转写")
                            continue
                        transcript, verr = await asyncio.to_thread(_download_and_transcribe_audio, str(voice_url))
                        if verr:
                            print(f"⚠️ 语音转写失败：{verr}")
                        elif transcript:
                            _cache_add_record(chat_id, user_id, transcript)
                        else:
                            print(f"⚠️ 语音转写结果为空")

                # 纯图片且没在叫它：静默缓存，不调AI、不发提示（等它被@/用户发文字时再连图一起处理）
                if _is_pure_image_silent(image_urls, _plain_text, is_group, at_flag):
                    continue

                # 群消息：没@就只记旁听缓存；@了才取正文继续回复。私聊不需要@
                if is_group:
                    if not at_flag:
                        # 没@机器人：记进旁听缓存（最多10条/每条100字，满了丢最旧），等被@时一起当上下文
                        if user_id != self_id:
                            _amb_text = extract_user_text(cq_msg, self_id, raw_msg)
                            if _amb_text:
                                _ambient_add(chat_id, user_id, _amb_text)
                        continue
                    # 提取去掉@后的纯文字
                    user_text = extract_user_text(cq_msg, self_id, raw_msg)
                else:
                    # 私聊直接用原始消息
                    user_text = raw_msg.strip()
                    if not user_text:
                        user_text = extract_user_text(cq_msg, self_id)

                # 控制器指令过滤：只有控制器白名单用户发的指令AI才跳过，非白名单正常回复（避免其他人发"重启服务"没人理）
                if (CONTROLLER_ALLOWED_USERS and user_id in CONTROLLER_ALLOWED_USERS
                        and is_controller_command(user_text)):
                    print(f"🎮 检测到控制器指令，AI跳过回复: {user_text}")
                    continue

                # 指令列表：@DeepSeek 所有指令 / 指令列表 → 直接列出所有可用指令，不调API
                _help_text = user_text.strip().lower()
                if _help_text in ("所有指令", "指令列表", "功能列表", "帮助", "help"):
                    _help_msg = (
                        "📋 可用指令列表：\n"
                        "━━━━━━━━━━━━━\n"
                        "📥 下载本子：下载本子350234加 p？ 选章节\n"
                        "🎬 抖音视频：抖音分享链接\n"
                        "🎵 汽水音乐：汽水音乐链接\n"
                        "💰 查询余额：token\n"
                        "🧠 查看永久记忆：查看记忆\n"
                        "🔄 远程控制（仅白名单）：\n"
                        "   • 关闭服务\n"
                        "   • 重启服务\n"
                        "   • 重启服务 ai\n"
                        "   • 重启服务 napcat"
                    )
                    if is_group:
                        await send_group_msg(ws, chat_id, _help_msg)
                    else:
                        await send_private_msg(ws, chat_id, _help_msg)
                    chat_print(f"{'群' if is_group else '私聊'}{chat_id} | 用户{user_id}：{user_text}\n AI回复：[指令列表]")
                    continue

                # 配置查询：@DeepSeek 配置 / 当前配置 → 直接显示当前配置，不调API
                _config_text = user_text.strip().lower()
                if _config_text in ("配置", "当前配置", "查看配置", "config", "配置信息"):
                    _thinking_status = "开" if ENABLE_THINKING_CHAT else "关"
                    _config_msg = (
                        "当前配置\n"
                        "———————————————\n"
                        f"语言模型：{DEEPSEEK_MODEL}\n"
                        f"视觉模型：{DEEPSEEK_VISION_MODEL}\n"
                        f"模型思考及强度：{_thinking_status}/{REASONING_EFFORT}\n"
                        f"上下轮记忆：{MAX_CONTEXT_LEN}轮\n"
                        f"系统人设：{'加载' if SYSTEM_PROMPT else '未加载'}\n"
                        f"搜索引擎：{'开' if ENABLE_WEB_SEARCH else '关'}\n"
                        f"下载器：{'开' if ENABLE_DOWNLOAD else '关'}"
                    )
                    if is_group:
                        await send_group_msg(ws, chat_id, _config_msg)
                    else:
                        await send_private_msg(ws, chat_id, _config_msg)
                    chat_print(f"{'群' if is_group else '私聊'}{chat_id} | 用户{user_id}：{user_text}\n AI回复：[当前配置]")
                    continue

                # Token余额查询：@DeepSeek token / 余额 / 额度 → 查询API余额+当前峰谷时段，不调对话API
                _token_text = user_text.strip().lower()
                if _token_text in ("token", "余额", "额度", "查询余额", "剩余额度"):
                    balance, available, bal_err = await asyncio.to_thread(query_deepseek_balance)
                    period_key = get_pricing_period()
                    period_name = PEAK_PERIOD_NAME if period_key == "peak" else OFFPEAK_PERIOD_NAME
                    if bal_err:
                        _token_msg = f"剩余额度查询失败\n{bal_err}"
                    else:
                        _token_msg = (
                            f"剩余额度：{balance}元\n"
                            f"当前时段：{period_name}"
                        )
                    if is_group:
                        await send_group_msg(ws, chat_id, _token_msg)
                    else:
                        await send_private_msg(ws, chat_id, _token_msg)
                    chat_print(f"{'群' if is_group else '私聊'}{chat_id} | 用户{user_id}：{user_text}\n AI回复：[Token余额]")
                    continue

                # 查看记忆：@DeepSeek 查看记忆/记忆 → 显示自己的档案+全局备忘+存档状态，不调API
                _mem_cmd = user_text.strip().lower()
                if _mem_cmd in ("查看记忆", "记忆", "我的记忆", "memory"):
                    if not ENABLE_MEMORY or memory_store is None:
                        _mem_view = "永久记忆未开启。"
                    else:
                        lines = ["===== 永久记忆存档 ====="]
                        m = memory_store.data.get("members", {}).get(str(user_id))
                        if m:
                            # 印象值不外显（用户要求：不要把印象值往群里说），这里只展示昵称/印象
                            if m.get("nickname"):
                                lines.append(f"昵称：{m['nickname']}")
                            if m.get("note"):
                                lines.append(f"印象：{m['note']}")
                            if not m.get("nickname") and not m.get("note"):
                                lines.append("你的档案还没有内容。")
                        else:
                            lines.append("你还没有档案（对话后会自动建立）")
                        notes = memory_store.data.get("global_notes", [])
                        lines.append(f"—— 全局备忘（共{len(notes)}条）——")
                        if notes:
                            lines.extend(f"· {n}" for n in notes[-10:])
                        else:
                            lines.append("（暂无）")
                        ctx = memory_store.data.get("chat_context", {})
                        lines.append(f"—— 已存档{len(ctx)}个会话，每个保留{MEMORY_CONTEXT_TURNS}轮 ——")
                        _mem_view = "\n".join(lines)
                    if is_group:
                        await send_group_msg(ws, chat_id, _mem_view)
                    else:
                        await send_private_msg(ws, chat_id, _mem_view)
                    chat_print(f"{'群' if is_group else '私聊'}{chat_id} | 用户{user_id}：{user_text}\n AI回复：[查看记忆]")
                    continue

                # 保存用户文本用于聊天记录
                display_text = user_text if user_text else "[无文字]"
                if image_urls:
                    img_tag = f"[图片x{len(image_urls)}]"
                    display_text = (display_text + " " + img_tag) if display_text != "[无文字]" else img_tag
                if video_urls:
                    vid_tag = f"[视频x{len(video_urls)}]"
                    display_text = (display_text + " " + vid_tag) if display_text != "[无视频]" else vid_tag
                if record_items:
                    rec_tag = f"[语音x{len(record_items)}]"
                    display_text = (display_text + " " + rec_tag) if display_text != "[无文字]" else rec_tag
                if reply_match:
                    display_text = "[回复] " + display_text

                # ===== 消息合并（防抖）：同一会话连续消息合并成一次AI回复 =====
                if ENABLE_MERGE:
                    _merge_buf.setdefault(chat_id, []).append({
                        "user_text": user_text,
                        "display_text": display_text,
                        "reply_text": reply_text,
                        "user_id": user_id,
                        "nickname": nickname,
                        "ts": time.time(),
                    })
                    _merge_last_ts[chat_id] = time.time()
                    if chat_id not in _merge_workers or _merge_workers[chat_id].done():
                        print(f"🧩 消息合并：{'群' if is_group else '私聊'}{chat_id} 进入{MERGE_WAIT_SECONDS}秒合并窗口（攒够{MERGE_MAX_COUNT}条立即处理）")
                        _merge_workers[chat_id] = asyncio.create_task(_merge_worker(ws, chat_id, is_group))
                    continue
                # 未开启消息合并：直接按原逻辑回复（保留冷却防刷屏）
                now = asyncio.get_running_loop().time()
                if now - last_reply_time[chat_id] < COOLDOWN:
                    continue
                last_reply_time[chat_id] = now
                await _do_ai_reply(ws, chat_id, user_id, is_group, user_text, display_text, reply_text)
                continue
        finally:
            if not dispatcher_task.done():
                dispatcher_task.cancel()


# ===================== 配置外置：从 qqbot-config.yml 加载（优先级：配置文件 > 环境变量 > 内置默认） =====================
def _load_config_file():
    """读取 qqbot-config.yml（或 qqaibot-config.yml，或用 QQ2_CONFIG 指定路径）覆盖默认配置。
    所有可自定义项都在配置文件里；找不到文件则继续用内置默认+环境变量。"""
    import yaml
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.environ.get("QQ2_CONFIG"),
        os.path.join(script_dir, "qqbot-config.yml"),
        os.path.join(script_dir, "qqaibot-config.yml"),
    ]
    path = next((p for p in candidates if p and os.path.isfile(p)), None)
    if not path:
        print("📄 未找到 qqbot-config.yml，使用内置默认值（如需自定义请放一个同名配置文件）")
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        print(f"⚠️ 读取配置文件失败（{path}），继续用内置默认：{e}")
        return

    def s(key, cur):
        v = cfg.get(key)
        return v if isinstance(v, str) else cur
    def i(key, cur):
        v = cfg.get(key)
        try:
            return int(v)
        except Exception:
            return cur
    def f(key, cur):
        v = cfg.get(key)
        try:
            return float(v)
        except Exception:
            return cur
    def b(key, cur):
        v = cfg.get(key)
        return bool(v) if isinstance(v, bool) else cur
    def lst(key, cur):
        v = cfg.get(key)
        if isinstance(v, list):
            out = []
            for x in v:
                try:
                    out.append(int(x))
                except Exception:
                    pass
            return out if out else cur
        return cur

    global ONEBOT_WS, DEEPSEEK_API_KEY, DEEPSEEK_MODEL, DEEPSEEK_VISION_MODEL, BOCHA_API_KEY
    global MAX_CONTEXT_LEN, COOLDOWN, ALLOWED_GROUPS, ALLOWED_USERS
    global ENABLE_WEB_SEARCH, ENABLE_NOTIFY, SEARCH_RESULT_NUM, SYSTEM_PROMPT
    global DOWNLOAD_ROOT, _CONVERT_DIR, IMG_SUFFIX, ZIP_PASSWORD, MAX_SEND_MB, DEFAULT_CHAPTER, DOUYIN_TTWID
    global SEND_FILE_AS_CHAT_MSG, UPLOAD_CLEANUP, UPLOAD_CLEANUP_DELAY_SECONDS
    global NAPCAT_CONTAINER, NAPCAT_DATA_DIR, NAPCAT_HOST_DIR, NAPCAT_VIEW_DIR
    global CONTROLLER_ALLOWED_USERS
    global ENABLE_THINKING_CHAT, ENABLE_THINKING_VISION, REASONING_EFFORT
    global ENABLE_DOWNLOAD, ENABLE_SEND_DELAY, SEND_DELAY_MIN, SEND_DELAY_MAX
    global ENABLE_SPLIT_REPLY, SPLIT_DELAY_MIN, SPLIT_DELAY_MAX, SPLIT_MAX_PARTS, SPLIT_PART_MAX_LEN
    global USE_CHAT_MODEL_FOR_VISION
    global PEAK_PERIOD_NAME, OFFPEAK_PERIOD_NAME
    global ENABLE_MEMORY, MEMORY_CONTEXT_TURNS, MEMORY_FILE, memory_store
    global MEMORY_INJECT_PROMPT
    global ENABLE_FAVOR, ENABLE_AMBIENT_CACHE, AMBIENT_MAX_MSGS, AMBIENT_MSG_MAX_LEN
    global ENABLE_EMOJI, EMOJI_DIR, EMOJI_MAP
    global ENABLE_VIDEO, ENABLE_VOICE
    global ENABLE_MERGE, MERGE_WAIT_SECONDS, MERGE_MAX_COUNT
    global ENABLE_TTS, TTS_ENGINE, TTS_VOICE, VOLC_API_KEY, VOLC_VOICE

    # onebot_ws：环境变量优先（Docker compose 里用它指向 napcat:6700，yml里可保持127.0.0.1给本机用）
    ONEBOT_WS = os.environ.get("QQ2_ONEBOT_WS") or s("onebot_ws", ONEBOT_WS)
    DEEPSEEK_API_KEY = s("deepseek_api_key", DEEPSEEK_API_KEY)
    DEEPSEEK_MODEL = s("deepseek_model", DEEPSEEK_MODEL)
    DEEPSEEK_VISION_MODEL = s("deepseek_vision_model", DEEPSEEK_VISION_MODEL)
    BOCHA_API_KEY = s("bocha_api_key", BOCHA_API_KEY)
    MAX_CONTEXT_LEN = i("max_context_len", MAX_CONTEXT_LEN)
    COOLDOWN = f("cooldown", COOLDOWN)
    ALLOWED_GROUPS = lst("allowed_groups", ALLOWED_GROUPS)
    ALLOWED_USERS = lst("allowed_users", ALLOWED_USERS)
    # 控制器白名单：从 controller.allowed_users 加载（只有这些人发控制指令AI才跳过）
    _ctrl = cfg.get("controller") or {}
    if isinstance(_ctrl, dict) and isinstance(_ctrl.get("allowed_users"), list):
        CONTROLLER_ALLOWED_USERS = [int(x) for x in _ctrl["allowed_users"] if str(x).strip().isdigit()]
    ENABLE_WEB_SEARCH = b("enable_web_search", ENABLE_WEB_SEARCH)
    ENABLE_NOTIFY = b("enable_notify", ENABLE_NOTIFY)
    SEARCH_RESULT_NUM = i("search_result_num", SEARCH_RESULT_NUM)
    # 思考模式开关与强度（DeepSeek V4：thinking.type=enabled/disabled + reasoning_effort=low/high/max）
    ENABLE_THINKING_CHAT = b("enable_thinking_chat", ENABLE_THINKING_CHAT)
    ENABLE_THINKING_VISION = b("enable_thinking_vision", ENABLE_THINKING_VISION)
    REASONING_EFFORT = s("reasoning_effort", REASONING_EFFORT)
    if REASONING_EFFORT and REASONING_EFFORT not in ("low", "high", "max"):
        REASONING_EFFORT = "high"
    # 下载总开关 + 发送消息随机延迟（防检测）
    ENABLE_DOWNLOAD = b("enable_download", ENABLE_DOWNLOAD)
    ENABLE_SEND_DELAY = b("enable_send_delay", ENABLE_SEND_DELAY)
    SEND_DELAY_MIN = i("send_delay_min", SEND_DELAY_MIN)
    SEND_DELAY_MAX = i("send_delay_max", SEND_DELAY_MAX)
    if SEND_DELAY_MIN < 0: SEND_DELAY_MIN = 0
    if SEND_DELAY_MAX < SEND_DELAY_MIN: SEND_DELAY_MAX = SEND_DELAY_MIN
    # 分段回复：按段落把回复拆成多条发（降低人机感）
    ENABLE_SPLIT_REPLY = b("enable_split_reply", ENABLE_SPLIT_REPLY)
    SPLIT_DELAY_MIN = f("split_delay_min", SPLIT_DELAY_MIN)
    SPLIT_DELAY_MAX = f("split_delay_max", SPLIT_DELAY_MAX)
    SPLIT_MAX_PARTS = i("split_max_parts", SPLIT_MAX_PARTS)
    SPLIT_PART_MAX_LEN = i("split_part_max_len", SPLIT_PART_MAX_LEN)
    if SPLIT_DELAY_MIN < 0: SPLIT_DELAY_MIN = 0
    if SPLIT_DELAY_MAX < SPLIT_DELAY_MIN: SPLIT_DELAY_MAX = SPLIT_DELAY_MIN
    if SPLIT_MAX_PARTS < 1: SPLIT_MAX_PARTS = 1
    if SPLIT_PART_MAX_LEN < 20: SPLIT_PART_MAX_LEN = 20
    # true=图片识别用语言模型（V4.1原生多模态），false=用单独的视觉模型（原逻辑）
    USE_CHAT_MODEL_FOR_VISION = b("use_chat_model_for_vision", USE_CHAT_MODEL_FOR_VISION)
    # 峰谷时段自定义显示名（如高峰="梁文峰"、空闲="梁文谷"；留空则用默认"高峰"/"空闲"）
    _peak_name = s("peak_period_name", PEAK_PERIOD_NAME)
    _offpeak_name = s("offpeak_period_name", OFFPEAK_PERIOD_NAME)
    if _peak_name: PEAK_PERIOD_NAME = _peak_name
    if _offpeak_name: OFFPEAK_PERIOD_NAME = _offpeak_name

    # 永久记忆存档：开关 + 上下文持久化轮数 + memory.json路径（环境变量 > 配置项 > 默认脚本目录/memory/）
    ENABLE_MEMORY = b("enable_memory", ENABLE_MEMORY)
    MEMORY_CONTEXT_TURNS = i("memory_context_turns", MEMORY_CONTEXT_TURNS)
    if MEMORY_CONTEXT_TURNS < 1: MEMORY_CONTEXT_TURNS = 1
    _mem_file = os.environ.get("QQ2_MEMORY_FILE") or s("memory_file", "")
    if not _mem_file:
        _mem_file = os.path.join(script_dir, "memory", "memory.json")
    MEMORY_FILE = _mem_file
    if ENABLE_MEMORY:
        try:
            memory_store = MemoryStore(MEMORY_FILE)
        except Exception as e:
            print(f"⚠️ 永久记忆初始化失败，本次运行不使用记忆：{e}")
            memory_store = None

    # 记忆注入system提示词开关（false=只记录存档+恢复上下文，完全不往人格提示词里塞成员档案/印象值/备忘，避免记忆把AI性格带偏）
    MEMORY_INJECT_PROMPT = b("memory_inject_prompt", MEMORY_INJECT_PROMPT)

    # 印象值开关（false=不注入印象值、也不允许AI改印象值）
    ENABLE_FAVOR = b("enable_favor", ENABLE_FAVOR)
    # 群聊旁听缓存：没@机器人的群消息也存起来当上下文
    ENABLE_AMBIENT_CACHE = b("enable_ambient_cache", ENABLE_AMBIENT_CACHE)
    AMBIENT_MAX_MSGS = i("ambient_max_msgs", AMBIENT_MAX_MSGS)
    AMBIENT_MSG_MAX_LEN = i("ambient_msg_max_len", AMBIENT_MSG_MAX_LEN)
    if AMBIENT_MAX_MSGS < 1: AMBIENT_MAX_MSGS = 1
    if AMBIENT_MSG_MAX_LEN < 10: AMBIENT_MSG_MAX_LEN = 10

    # AI表情包：开关 + 目录（环境变量 > 配置项 > 默认脚本目录/表情包，其次emoji）
    ENABLE_EMOJI = b("enable_emoji", ENABLE_EMOJI)
    _emo_dir = os.environ.get("QQ2_EMOJI_DIR") or s("emoji_dir", "")
    if not _emo_dir:
        _cand = os.path.join(script_dir, "表情包")
        _emo_dir = _cand if os.path.isdir(_cand) else os.path.join(script_dir, "emoji")
    EMOJI_DIR = _emo_dir
    if ENABLE_EMOJI:
        scan_emoji_dir()
        if not EMOJI_MAP:
            print(f"⚠️ 表情包已开启但目录无图片：{EMOJI_DIR}")

    # 视频识别开关
    ENABLE_VIDEO = b("enable_video", ENABLE_VIDEO)
    if ENABLE_VIDEO:
        print(f"📹 视频识别已开启：群里发视频自动下载→场景检测抽帧(≤10分钟)→@时投视觉模型")
    # 语音转文字开关
    ENABLE_VOICE = b("enable_voice", ENABLE_VOICE)
    # AI回复转语音开关（edge-tts）+ 音色
    ENABLE_TTS = b("enable_tts", ENABLE_TTS)
    _tts_voice = s("tts_voice", "")
    if _tts_voice:
        TTS_VOICE = _tts_voice
    # TTS引擎选择（edge/volc）+ 火山引擎配置
    _tts_engine = s("tts_engine", "")
    if _tts_engine:
        TTS_ENGINE = _tts_engine
    VOLC_API_KEY = s("volc_api_key", VOLC_API_KEY)
    VOLC_VOICE = s("volc_voice", VOLC_VOICE)
    if ENABLE_TTS:
        if TTS_ENGINE == "volc" and VOLC_API_KEY:
            print(f"🎙️ AI回复转语音已开启：火山引擎 音色={VOLC_VOICE}")
        else:
            print(f"🎙️ AI回复转语音已开启：edge-tts 音色={TTS_VOICE}")
    MERGE_MAX_COUNT = i("merge_max_count", MERGE_MAX_COUNT)
    if MERGE_WAIT_SECONDS < 1: MERGE_WAIT_SECONDS = 1
    if MERGE_MAX_COUNT < 1: MERGE_MAX_COUNT = 1
    if ENABLE_VOICE:
        print(f"🎙️ 语音转文字已开启：群里发语音自动转写→@时把语音内容投给语言模型")

    SYSTEM_PROMPT = s("system_prompt", SYSTEM_PROMPT)
    DEFAULT_CHAPTER = i("default_chapter", DEFAULT_CHAPTER)

    # 下载/打包相关（download_root 同样环境变量优先，Docker里由compose指到/data）
    root = os.environ.get("QQ2_DOWNLOAD_ROOT")
    if not root:
        root = cfg.get("download_root")
    if isinstance(root, str) and root.strip():
        r = root.strip()
        if not os.path.isabs(r):
            r = os.path.join(script_dir, r)
        DOWNLOAD_ROOT = r
        _CONVERT_DIR = os.path.join(DOWNLOAD_ROOT, "_convert")
    IMG_SUFFIX = s("img_suffix", IMG_SUFFIX)
    ZIP_PASSWORD = s("zip_password", ZIP_PASSWORD)
    MAX_SEND_MB = i("max_send_mb", MAX_SEND_MB)
    DOUYIN_TTWID = s("douyin_ttwid", DOUYIN_TTWID)
    SEND_FILE_AS_CHAT_MSG = b("send_file_as_chat_msg", SEND_FILE_AS_CHAT_MSG)
    UPLOAD_CLEANUP = b("upload_cleanup", UPLOAD_CLEANUP)
    UPLOAD_CLEANUP_DELAY_SECONDS = i("upload_cleanup_delay_seconds", UPLOAD_CLEANUP_DELAY_SECONDS)

    # NapCat投递相关（docker-cp / NAS共享目录 / 本机原生）——部署相关，环境变量优先（compose显式指定）
    NAPCAT_CONTAINER = os.environ.get("QQ2_NAPCAT_CONTAINER", s("napcat_container", NAPCAT_CONTAINER))
    NAPCAT_DATA_DIR = os.environ.get("QQ2_NAPCAT_DATA_DIR", s("napcat_data_dir", NAPCAT_DATA_DIR))
    NAPCAT_HOST_DIR = os.environ.get("QQ2_NAPCAT_HOST_DIR", s("napcat_host_dir", NAPCAT_HOST_DIR))
    NAPCAT_VIEW_DIR = os.environ.get("QQ2_NAPCAT_VIEW_DIR", s("napcat_view_dir", NAPCAT_VIEW_DIR))

    print(f"📄 已从配置文件加载设置：{path}")


def _validate_required_config():
    """关键配置缺失检查：无内置兜底，必须从配置文件获取。
    无论配置文件是否存在/解析是否失败，启动前都执行（避免带着 None 跑第一条消息就崩）。"""
    _missing = []
    if not DEEPSEEK_API_KEY: _missing.append("deepseek_api_key")
    if not ONEBOT_WS: _missing.append("onebot_ws")
    if not SYSTEM_PROMPT: _missing.append("system_prompt")
    if not ALLOWED_GROUPS: _missing.append("allowed_groups")
    if not ALLOWED_USERS: print("⚠️ 未配置 allowed_users（私聊白名单为空，私聊将全部不回复）")
    if _missing:
        print(f"❌ 配置缺少关键项：{', '.join(_missing)}")
        print("❌ 请检查 qqbot-config.yml（路径/格式/必填项）后重新启动")
        sys.exit(1)


_load_config_file()
_validate_required_config()

# 思考模式默认值兜底（配置文件没写时默认关闭，避免变慢/费token）
if ENABLE_THINKING_CHAT is None:
    ENABLE_THINKING_CHAT = False
if ENABLE_THINKING_VISION is None:
    ENABLE_THINKING_VISION = False
if not REASONING_EFFORT:
    REASONING_EFFORT = "high"


if __name__ == "__main__":
    import time as _time
    try:
        while True:
            try:
                asyncio.run(main())
            except KeyboardInterrupt:
                print("\n👋 机器人已退出")
                break
            except Exception as e:
                print(f"❌ 连接异常：{e}")
            # 连接断开/失败后自动重连（NapCat还没起或中途断线都适用）
            print("5 秒后重新连接…")
            _time.sleep(5)
    finally:
        try:
            _kzt_file.close()
            _lt_file.close()
        except:
            pass
        sys.stdout.write("日志已保存：QQKZT.txt（控制台）、QQLT.txt（聊天记录）\n")
