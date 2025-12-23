FROM astral/uv:python3.11-trixie-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 999 nonroot \
    && useradd --system --gid 999 --uid 999 --create-home nonroot

# Копируем файлы конфигурации
COPY pyproject.toml .
COPY uv.lock .

# Синхронизация зависимостей через uv
ENV UV_NO_DEV=1
ENV UV_TOOL_BIN_DIR=/usr/local/bin
ENV UV_SYSTEM_PYTHON=1
ENV UV_LINK_MODE=copy

RUN uv sync --locked

# Копируем код приложения
COPY dashboard ./dashboard/
COPY data ./data/
COPY config.py .
COPY logger.py .

ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

USER nonroot

CMD ["uv", "run", "streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
