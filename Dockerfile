FROM docker.io/library/debian:trixie-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

COPY --from=ghcr.io/astral-sh/uv:0.12.22 /uv /uvx /bin/

RUN apt-get update && apt-get install -y --no-install-recommends \
  ffmpeg \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ ./src/
RUN uv sync --frozen --no-dev --compile-bytecode

CMD ["/app/.venv/bin/trivial-transcoder"]
