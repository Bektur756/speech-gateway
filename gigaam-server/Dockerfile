FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# ffmpeg: GigaAM's transcribe() shells out to it to decode the input wav.
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project

COPY asr_server.py ./
RUN uv sync --no-dev

ENV PATH="/srv/.venv/bin:$PATH"
# HF_HOME should be volume-mounted (see docker-compose.yml) so the ~800MB
# model download survives container recreation instead of re-fetching it
# every rebuild.
ENV HF_HOME=/root/.cache/huggingface

EXPOSE 2700

CMD ["python", "asr_server.py"]
