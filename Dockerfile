# QQ2 机器人（NAS/Docker 版）—— 国内源可直接构建
# 构建：docker compose -p qqbot build
# 基础镜像默认走国内镜像站 docker.1ms.run（和你的NapCat同一个源，Docker Hub在本机不通也能拉）；
# 海外环境想用官方源可加 --build-arg PY_BASE=python:3.11-slim
ARG PY_BASE=docker.1ms.run/library/python:3.11-slim
FROM ${PY_BASE}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Shanghai \
    PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn

# apt/pip 切国内镜像（阿里云deb + 清华pip），装 p7zip(加密zip) 和 tzdata
RUN sed -i 's@deb.debian.org@mirrors.aliyun.com@g' /etc/apt/sources.list.d/debian.sources /etc/apt/sources.list 2>/dev/null; \
    apt-get update \
    && apt-get install -y --no-install-recommends p7zip-full tzdata ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo "$TZ" > /etc/timezone

WORKDIR /app

COPY requirements.txt ./
# 先装CPU版torch（避免拉GPU版几个GB的CUDA包）
RUN pip install --no-cache-dir torch torchaudio --index-url https://download.pytorch.org/whl/cpu
# 再装其他依赖（抖音解析已移到独立容器，本镜像不再需要 playwright/crawlers）
RUN pip install --no-cache-dir -r requirements.txt

COPY QQ_AI_Bot.py ./
COPY douyin_dtk.py ./

# 默认按“NAS共享目录”模式运行（与NapCat容器共享 ./transfer），compose会用env覆盖
# 注意：access_token 这里只放占位符，真实token由 docker-compose 的 .env（ONEBOT_TOKEN）注入，避免泄露
ENV QQ2_ONEBOT_WS="ws://napcat:6700?access_token=CHANGE_ME" \
    QQ2_NAPCAT_CONTAINER="" \
    QQ2_NAPCAT_HOST_DIR="/deliver" \
    QQ2_NAPCAT_VIEW_DIR="/app/data" \
    QQ2_DOWNLOAD_ROOT="/data/downloads" \
    QQ2_LOG_DIR="/data/logs" \
    QQ2_IMG_SUFFIX=".png"

VOLUME ["/data"]

CMD ["python", "-u", "QQ_AI_Bot.py"]
