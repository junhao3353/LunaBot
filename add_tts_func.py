import os

path = r"D:\py\XM\QQbot\QQ_AI_Bot.py"
with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

# 找 send_reply_multi 的位置
func_start = -1
for i, line in enumerate(lines):
    if line.startswith("async def send_reply_multi"):
        func_start = i
        break

print(f"send_reply_multi 在第 {func_start + 1} 行")

tts_func = '''async def _tts_and_send(websocket, is_group: bool, chat_id: int, text: str) -> bool:
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


'''

# 在 send_reply_multi 前面插入
new_lines = lines[:func_start] + [tts_func] + lines[func_start:]

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)

print(f"✅ 已添加 _tts_and_send 函数")
print(f"总行数: {len(new_lines)}")
