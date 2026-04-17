"""
etl/logger.py
─────────────
Shared logger used by all ETL modules.
Uses loguru for structured, coloured console output + rotating file logs.
"""

import os
import sys
from loguru import logger
from config.settings import LOG_LEVEL, LOG_DIR

os.makedirs(LOG_DIR, exist_ok=True)

# Remove the default handler and reconfigure
logger.remove()

# Console — coloured, human-readable
logger.add(
    sys.stdout,
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> — <level>{message}</level>",
    colorize=True,
)

# File — JSON-style, rotates at 10 MB, keeps 7 days
logger.add(
    os.path.join(LOG_DIR, "etl_{time:YYYY-MM-DD}.log"),
    level="DEBUG",
    rotation="10 MB",
    retention="7 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{line} — {message}",
    enqueue=True,
)
