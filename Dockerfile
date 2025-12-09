FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENV=.venv \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

FROM base AS builder
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY flows/ ./flows/
COPY utils/ ./utils/
COPY main.py ./
RUN uv sync --frozen --no-dev


FROM base AS dev
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY . .
CMD ["uv", "run", "python", "main.py"]


FROM base AS prod
COPY --from=builder /app /app
CMD ["uv", "run", "python", "main.py"]
