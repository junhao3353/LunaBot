import os
import sys
import json
import uuid
import base64
import tempfile

import requests

# 安全提示：不要把 API Key 写进代码/提交到仓库。
# 优先读环境变量 VOLC_API_KEY，其次读同目录的 volc_api_key.txt（该文件已被 .gitignore 忽略）
api_key = (os.environ.get("VOLC_API_KEY") or "").strip()
if not api_key:
    _key_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "volc_api_key.txt")
    if os.path.isfile(_key_file):
        with open(_key_file, encoding="utf-8") as f:
            api_key = f.read().strip()
if not api_key:
    print("未找到火山引擎 API Key：请设置环境变量 VOLC_API_KEY，或创建 volc_api_key.txt 后重试")
    sys.exit(1)

url = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
speaker = "ICL_uranus_zh_female_jinglingxiangdao_tob"

headers = {
    "X-Api-Key": api_key,
    "X-Api-Resource-Id": "seed-tts-2.0",
    "X-Api-Request-Id": str(uuid.uuid4()),
    "Content-Type": "application/json",
    "Connection": "keep-alive"
}

payload = {
    "req_params": {
        "text": "你好，我是你的AI助手，很高兴见到你！",
        "speaker": speaker,
        "audio_params": {
            "format": "mp3",
            "sample_rate": 24000
        }
    }
}

resp = requests.post(url, headers=headers, json=payload, timeout=30, stream=True)
audio_chunks = []
for line in resp.iter_lines(decode_unicode=True):
    if not line:
        continue
    try:
        chunk = json.loads(line)
        if chunk.get("code") not in (0, 20000000):
            print(f"错误: code={chunk.get('code')} msg={chunk.get('message')}")
            sys.exit(1)
        b64_data = chunk.get("data", "")
        if b64_data:
            audio_chunks.append(base64.b64decode(b64_data))
    except json.JSONDecodeError:
        continue

total_size = sum(len(c) for c in audio_chunks)
print(f"音频块数量: {len(audio_chunks)}")
print(f"总音频大小: {total_size} 字节")

if audio_chunks:
    out_path = os.path.join(tempfile.gettempdir(), "test_volc_tts.mp3")
    with open(out_path, "wb") as f:
        for c in audio_chunks:
            f.write(c)
    print(f"✅ 成功！文件：{out_path}")
else:
    print("❌ 没有音频数据")
