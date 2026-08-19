# syntax=docker/dockerfile:1

# ---------- builder: dựng .venv bằng uv ----------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev

# ---------- runtime: image sạch, không có uv ----------
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

# non-root user
RUN groupadd -r app && useradd -r -g app app

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY finnhubproducer/ ./finnhubproducer/

RUN chown -R app:app /app
USER app

WORKDIR /app/finnhubproducer/src

CMD ["python", "producer.py"]
