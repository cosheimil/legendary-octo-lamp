# 🛍️ Fashion E-commerce Sales Analytics Platform

Проектная работа по дисциплине «Технология обработки больших данных»

## 📋 Описание проекта

Распределенная система обработки данных e-commerce продаж модной одежды с использованием:

- **Dask** - распределенная обработка больших данных
- **Prefect** - оркестрация ETL пайплайнов
- **PostgreSQL** - хранилище аналитических данных
- **MinIO** - объектное хранилище (S3-совместимое)
- **Streamlit** - интерактивная визуализация данных

## 🚀 Быстрый старт

### 1. Запуск инфраструктуры

```bash
# Запустите все сервисы
docker-compose up -d

# Проверьте статус
docker-compose ps
```

### 2. Запуск ETL Pipeline

```bash
# Выполнение ETL процесса
docker-compose exec prefect-server uv run /app/flows/etl_flow.py
```

### 3. Обучение ML модели

```bash
# Запуск ML Pipeline
docker-compose exec prefect-server uv run /app/dask_jobs/model_training.py
```

### 4. Просмотр дашборда

Откройте http://localhost:8501

## 📊 Доступ к сервисам

| Сервис              | URL                   | Логин/Пароль            |
| ------------------- | --------------------- | ----------------------- |
| Streamlit Dashboard | http://localhost:8501 | -                       |
| Prefect UI          | http://localhost:4200 | -                       |
| MinIO Console       | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL          | localhost:5432        | admin / admin           |

## 🎯 Соответствие требованиям

✅ Использование Big Data технологии (Dask)  
✅ Система хранения (PostgreSQL + MinIO)  
✅ ETL-процесс (Prefect Flow)  
✅ Визуализация результатов (Streamlit)  
✅ Документация архитектуры  
✅ Запуск через docker-compose  
✅ Минимум 3 задачи (extract, transform, load)

Готово к запуску! 🚀

## .env

```
# PostgreSQL Configuration
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=sales_db
POSTGRES_USER=...
POSTGRES_PASSWORD=...

# MinIO Configuration
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=...
MINIO_SECRET_KEY=...
MINIO_SECURE=False

# Prefect Configuration
PREFECT_API_URL=http://prefect-server:4200/api
PREFECT_HOME=/root/.prefect

# Dask Configuration
DASK_SCHEDULER_HOST=dask-scheduler
DASK_SCHEDULER_PORT=8786
DASK_N_WORKERS=2
DASK_THREADS_PER_WORKER=2
DASK_MEMORY_LIMIT=2GB

# Application Configuration
LOG_LEVEL=INFO
DATA_PATH=/app/data
TEMP_PATH=/tmp

# Feature Flags
ENABLE_LOGGING=True
ENABLE_MINIO=True
ENABLE_ML=True
```
