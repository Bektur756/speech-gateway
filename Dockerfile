# ==============================================================================
# Stage 1: Build Vue 3 + Pinia + Tailwind CSS frontend
# ==============================================================================
FROM node:20-alpine AS frontend-builder

WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci || npm install

COPY frontend/ ./
RUN npm run build

# ==============================================================================
# Stage 2: Python Speech Gateway service
# ==============================================================================
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /srv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app ./app
# Copy the compiled Vue frontend from the builder stage
COPY --from=frontend-builder /build/app/static ./app/static
RUN uv sync --frozen --no-dev

ENV PATH="/srv/.venv/bin:$PATH"

# AudioSocket ports (Asterisk connects directly, not via nginx) + HTTP/WS API
EXPOSE 9098 9099 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

