"""
Logging configuration module.

This module provides centralized logging setup for the entire application,
with support for both console and file logging with different levels and formats.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from config import LOG_LEVEL, LOG_FORMAT, LOG_DATE_FORMAT, TEMP_PATH

# Create logs directory if it doesn't exist
LOGS_DIR = Path(TEMP_PATH) / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Logger Configuration
# ============================================================================


def setup_logging(name: str, level: str | None = None) -> logging.Logger:
    """
    Setup and configure logger for a specific module.

    Args:
        name (str): Logger name (typically __name__)
        level (str, optional): Logging level. Defaults to LOG_LEVEL from config.

    Returns:
        logging.Logger: Configured logger instance

    Example:
        >>> logger = setup_logging(__name__)
        >>> logger.info("Application started")
    """
    if level is None:
        level = LOG_LEVEL

    # Create logger instance
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Prevent duplicate handlers if logger already configured
    if logger.hasHandlers():
        return logger

    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Console Handler (INFO and above)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler (DEBUG and above)
    log_file = LOGS_DIR / f"{name.replace('.', '_')}.log"
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Error File Handler (ERRORS only)
    error_log_file = LOGS_DIR / f"{name.replace('.', '_')}_errors.log"
    error_handler = logging.handlers.RotatingFileHandler(
        error_log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    return logger


# ============================================================================
# Global Loggers
# ============================================================================

# Main application logger
app_logger = setup_logging("app")

# ETL pipeline logger
etl_logger = setup_logging("etl")

# ML pipeline logger
ml_logger = setup_logging("ml")

# Database logger
db_logger = setup_logging("database")

# MinIO logger
minio_logger = setup_logging("minio")

# Dask logger
dask_logger = setup_logging("dask")


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger for the specified name.

    Args:
        name (str): Logger name (typically __name__)

    Returns:
        logging.Logger: Logger instance

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.debug("Debug information")
    """
    return setup_logging(name)


if __name__ == "__main__":
    # Test logging configuration
    app_logger.info("✓ Logging system initialized successfully")
    app_logger.debug("Debug message example")
    app_logger.warning("Warning message example")
    app_logger.error("Error message example (check error logs)")
    print(f"\n📝 Logs directory: {LOGS_DIR}")
