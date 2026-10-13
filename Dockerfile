# syntax=docker/dockerfile:1.7
# ---- builder: resolve and install dependencies with uv -----------------------
FROM python:3.12-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Third-party dependencies first for layer caching.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Then the project itself.
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# ---- runtime: minimal image, non-root user ----------------------------------
FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    DATA_ROOT=/app/data \
    LOG_LEVEL=INFO \
    LOG_FORMAT=json \
    ACTOR=SYSTEM

RUN groupadd --system pipeline && useradd --system --gid pipeline --home /app pipeline

WORKDIR /app
COPY --from=builder --chown=pipeline:pipeline /app /app
COPY --chown=pipeline:pipeline docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh && mkdir -p /app/data && chown pipeline:pipeline /app/data

USER pipeline
VOLUME ["/app/data"]

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["txn-pipeline", "demo"]
