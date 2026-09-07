FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /srv
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project

COPY asr_server.py ./
RUN uv sync --no-dev

ENV PATH="/srv/.venv/bin:$PATH"

EXPOSE 2700

CMD ["python", "asr_server.py"]
