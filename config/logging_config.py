"""
Structured logging configuration using Loguru / Python standard logging.
Provides colorized output for development and structured JSON formatting for production.
"""
import sys
from loguru import logger
from config.settings import settings


def setup_logger(service_name: str = "datapulse"):
    """
    Configure loguru logger with timestamps, contextual service tags,
    and appropriate log level based on environment settings.
    """
    logger.remove()  # Remove default handler

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        f"<cyan>{service_name}</cyan> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )

    logger.add(
        sys.stdout,
        format=log_format,
        level=settings.LOG_LEVEL.upper(),
        colorize=True,
        enqueue=True
    )

    return logger.bind(service=service_name)
