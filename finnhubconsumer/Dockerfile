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

# ---------- runtime ----------
FROM python:3.12-slim-bookworm

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY finnhubconsumer/ ./finnhubconsumer/

# Dùng chung đúng file schema với producer, không nhân bản .avsc trong repo.
# Encode và decode lệch schema là lỗi im lặng: decode ra số rác chứ không báo lỗi.
COPY finnhubproducer/src/schemas/trades.avsc ./finnhubconsumer/src/schemas/trades.avsc

WORKDIR /app/finnhubconsumer/src

CMD ["python", "consumer.py"]
