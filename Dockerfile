FROM ghcr.io/astral-sh/uv:0.11.28 AS uv
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

RUN groupadd --system bilimcp \
    && useradd --system --gid bilimcp --home-dir /app bilimcp
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY --from=uv /uv /uvx /bin/
COPY pyproject.toml uv.lock README.md LICENSE NOTICE ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
COPY docs ./docs
RUN uv sync --locked --no-dev \
    && mkdir -p /credentials /data \
    && chown -R bilimcp:bilimcp /app /credentials /data \
    && chmod 700 /credentials /data
USER bilimcp

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3).read()"]
CMD ["bilibili-connect"]
