FROM python:3.14-slim AS builder

# uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_PREFERENCE=only-system

WORKDIR /app
RUN uv venv

COPY app/ ./app/
COPY src/ ./src/
COPY pyproject.toml ./
RUN uv sync --extra app
RUN uv pip list

FROM python:3.14-slim AS final

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appuser /app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/.venv/lib/python3.14/site-packages" \
    PYTHONUNBUFFERED=1

USER appuser

EXPOSE 5000

CMD ["hypercorn", "app/main:app", "-c", "app/hypercorn_config.py"]