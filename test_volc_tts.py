import requests, uuid, base64, json

url = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
api_key = "REDACTED"
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
            exit(1)
        b64_data = chunk.get("data", "")
        if b64_data:
            audio_chunks.append(base64.b64decode(b64_data))
    except json.JSONDecodeError:
        continue

total_size = sum(len(c) for c in audio_chunks)
print(f"音频块数量: {len(audio_chunks)}")
print(f"总音频大小: {total_size} 字节")

if audio_chunks:
    with open("/tmp/test_volc_tts.mp3", "wb") as f:
        for c in audio_chunks:
            f.write(c)
    print("✅ 成功！")
else:
    print("❌ 没有音频数据")
