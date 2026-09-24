# rpg-mcp-server 对话式适配网关 —— 可直接部署的公网 streamable_http 镜像
#
# 为什么不用纯 node 基础镜像：网关本身是 Python 标准库实现（零三方依赖），
# 只有上游 rpg-mcp-server 需要 node。上游在**构建期**装好并钉版本，
# 运行期不再碰 npm registry —— 避免线上冷启动时 npx 拉包超时。
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PORT=8000 \
    MCP_PATH=/mcp \
    RPG_REQ_TIMEOUT=90 \
    RPG_SERVER_CMD=/usr/local/bin/rpg-mcp-server

# nodejs 18（Debian 自带，满足 rpg-mcp-server engines >=18）
RUN apt-get update \
 && apt-get install -y --no-install-recommends nodejs npm ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# 钉死版本，从随包携带的 tarball 安装（构建期完全不碰 npm registry）
# 注意：不要在这里执行 `rpg-mcp-server --help` 之类的“自检”——该程序不处理 argv，
# 一执行就进入 stdio 阻塞读，会把镜像构建卡死。只检查文件在且可执行。
COPY rpg-mcp-server-1.3.2.tgz /tmp/rpg-mcp-server.tgz
RUN npm install -g /tmp/rpg-mcp-server.tgz --no-audit --no-fund \
 && test -x /usr/local/bin/rpg-mcp-server \
 && rm -f /tmp/rpg-mcp-server.tgz \
 && node --version

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 8000
# 平台会探测 /healthz；网关对 /healthz 直接 200，不需要触发上游
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python3 -c "import os,urllib.request;urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PORT','8000')+'/healthz',timeout=3)" || exit 1

CMD ["python3", "/app/server.py"]
