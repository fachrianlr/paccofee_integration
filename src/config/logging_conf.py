import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from src.config.common import PARENT_FOLDER

log_dir = Path(PARENT_FOLDER) / "logs"

# Create the logs directory if it doesn't exist
try:
    log_dir.mkdir(parents=True, exist_ok=True)
except OSError as e:
    print(f"Error creating log directory: {e}")
    # Handle the error appropriately (e.g., exit or use a default log location)

# Define the logging format
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"

# Create a logger for this module
logger = logging.getLogger(__name__)

# Make log level configurable
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
try:
    logger.setLevel(getattr(logging, log_level))
except AttributeError:
    print(f"Invalid log level: {log_level}. Using INFO.")
    logger.setLevel()

# File Handler: Create a handler that rotates log files daily
log_file = log_dir / "app.log"
file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1)
file_handler.suffix = "%Y-%m-%d"  # Log files will be named app.log.YYYY-MM-DD

# Console Handler: Set up a handler to log to the console (stdout)
console_handler = logging.StreamHandler()

# Create a formatter and associate it with both handlers
formatter = logging.Formatter(log_format)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)
