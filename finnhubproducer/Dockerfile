# syntax=docker/dockerfile:1

# ---------- builder: dựng .venv bằng uv ----------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Cài dependencies trước (tận dụng Docker layer cache).
# Chỉ copy lockfile => layer này chỉ rebuild khi dependencies đổi, không phải khi sửa code.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev

# ---------- runtime: image sạch, không có uv ----------
FROM python:3.12-slim-bookworm

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy code. Build context là thư mục gốc của repo.
COPY finnhubproducer/ ./finnhubproducer/

# Đứng ngay tại src để đường dẫn tương đối 'schemas/trades.avsc' trong config.py
# hoạt động y hệt lúc chạy local. Container và máy bạn cùng một hành vi.
WORKDIR /app/finnhubproducer/src

CMD ["python", "producer.py"]
