FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml uv.lock ./
COPY flows/ ./flows/
COPY utils/ ./utils/
COPY main.py ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Run the flow server
CMD ["uv", "run", "python", "main.py"]

