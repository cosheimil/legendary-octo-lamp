"""
Configuration module for loading environment variables and application settings.

This module handles all configuration management using python-dotenv,
providing centralized access to all environment variables used throughout the application.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
ENV_FILE = Path(__file__).parent / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    print("⚠️ .env file not found, using system environment variables")

# ============================================================================
# PostgreSQL Configuration
# ============================================================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sales_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")

# Construct database URL
DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ============================================================================
# MinIO Configuration
# ============================================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"
MINIO_BUCKET_RAW = "raw-data"
MINIO_BUCKET_PROCESSED = os.getenv("MINIO_BUCKET_PROCESSED", "processed-data")
MINIO_BUCKET_ML = "ml-models"

# ============================================================================
# Prefect Configuration
# ============================================================================
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
PREFECT_HOME = os.getenv("PREFECT_HOME", "/root/.prefect")

# ============================================================================
# Dask Configuration
# ============================================================================
DASK_SCHEDULER_HOST = os.getenv("DASK_SCHEDULER_HOST", "localhost")
DASK_SCHEDULER_PORT = int(os.getenv("DASK_SCHEDULER_PORT", "8786"))
DASK_N_WORKERS = int(os.getenv("DASK_N_WORKERS", "2"))
DASK_THREADS_PER_WORKER = int(os.getenv("DASK_THREADS_PER_WORKER", "2"))
DASK_MEMORY_LIMIT = os.getenv("DASK_MEMORY_LIMIT", "2GB")

# Dask scheduler address
DASK_SCHEDULER = f"tcp://{DASK_SCHEDULER_HOST}:{DASK_SCHEDULER_PORT}"

# ============================================================================
# Application Configuration
# ============================================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DATA_PATH = os.getenv("DATA_PATH", "/app/data")
TEMP_PATH = os.getenv("TEMP_PATH", "/tmp")

# ============================================================================
# Feature Flags
# ============================================================================
ENABLE_LOGGING = os.getenv("ENABLE_LOGGING", "True").lower() == "true"
ENABLE_MINIO = os.getenv("ENABLE_MINIO", "True").lower() == "true"
ENABLE_ML = os.getenv("ENABLE_ML", "True").lower() == "true"

# ============================================================================
# File Paths
# ============================================================================
AMAZON_SALES_FILE = os.path.join(DATA_PATH, "Amazon Sale Report.csv")
INTERNATIONAL_SALES_FILE = os.path.join(DATA_PATH, "International sale Report.csv")
SALE_REPORT_FILE = os.path.join(DATA_PATH, "Sale Report.csv")

# ============================================================================
# Logging Configuration
# ============================================================================
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s - %(message)s"
LOG_DATE_FORMAT = "%H:%M:%S"


def get_config_summary():
    """Return a summary of all configuration settings (without sensitive data)."""
    return f"""
    ╔══════════════════════════════════════════════════╗
    ║       APPLICATION CONFIGURATION SUMMARY          ║
    ╚══════════════════════════════════════════════════╝
    
    PostgreSQL:
      - Host: {POSTGRES_HOST}
      - Port: {POSTGRES_PORT}
      - Database: {POSTGRES_DB}
    
    MinIO:
      - Endpoint: {MINIO_ENDPOINT}
      - Secure: {MINIO_SECURE}
      - Buckets: {MINIO_BUCKET_RAW}, {MINIO_BUCKET_PROCESSED}, {MINIO_BUCKET_ML}
    
    Dask:
      - Scheduler: {DASK_SCHEDULER}
      - Workers: {DASK_N_WORKERS}
      - Threads/Worker: {DASK_THREADS_PER_WORKER}
      - Memory Limit: {DASK_MEMORY_LIMIT}
    
    Application:
      - Log Level: {LOG_LEVEL}
      - Data Path: {DATA_PATH}
      - Enable ML: {ENABLE_ML}
    """


if __name__ == "__main__":
    print(get_config_summary())
