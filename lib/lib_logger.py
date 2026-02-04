"""
Logging Utility for Media Renamer
---------------------------------

This module provides a structured logging setup used throughout the
Media Renamer application. It configures a consistent logging format,
output handlers, and log levels to ensure clear, readable diagnostic
information during file processing.

The logger is designed to:
- Produce uniform log messages across all modules
- Include log levels, and contextual prefixes
- Support both console and file output (depending on configuration)
- Provide lightweight, dependency-free logging suitable for large
  batch operations
- Integrate cleanly with the renaming pipeline for progress tracking,
  conflict reporting, and error diagnostics

The logging configuration emphasizes clarity and reliability, enabling
the application to trace renaming operations, metadata extraction,
conflict resolution, and unexpected failures in a reproducible manner.
"""

import logging
import os
from datetime import datetime

TODAY = datetime.now().strftime("%Y%m%d")
FOLDER = "log"


class MaxLevelFilter(logging.Filter):
    """Filters log records to only allow those at or below a specified level."""

    def __init__(self, max_level):
        super().__init__()
        self.max_level = max_level

    def filter(self, record):
        return record.levelno <= self.max_level


def setup_logging(app_name: str) -> logging.Logger:
    """Sets up logging with different handlers and formatters."""
    log_file = f"{app_name}-{TODAY}.log"
    os.makedirs(FOLDER, exist_ok=True)
    log_path = os.path.join(FOLDER, log_file)

    # Root cleaning (Prevents Windows locking issues)
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
        try:
            h.close()
        except:
            ...

    if os.path.exists(log_path):
        try:
            os.remove(log_path)
        except Exception:
            ...

    # --- FORMATTERS ---
    info_formatter = logging.Formatter("%(message)s")
    error_formatter = logging.Formatter("%(message)s - Line: %(lineno)d")
    debug_formatter = logging.Formatter(
        "%(asctime)s - :%(lineno)d - %(funcName)s - %(message)s"
    )

    # --- HANDLER: INFO (Screen + File) ---
    info_console = logging.StreamHandler()
    info_console.setLevel(logging.INFO)
    info_console.setFormatter(info_formatter)
    info_console.addFilter(MaxLevelFilter(logging.WARNING))

    info_file = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    info_file.setLevel(logging.INFO)
    info_file.setFormatter(info_formatter)
    info_file.addFilter(MaxLevelFilter(logging.WARNING))

    # --- HANDLER: ERROR (Screen + File) ---
    error_console = logging.StreamHandler()
    error_console.setLevel(logging.ERROR)
    error_console.setFormatter(error_formatter)

    error_file = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    error_file.setLevel(logging.ERROR)
    error_file.setFormatter(error_formatter)

    # --- HANDLER: DEBUG (Only File) ---
    debug_file = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    debug_file.setLevel(logging.DEBUG)
    debug_file.setFormatter(debug_formatter)

    logger = logging.getLogger(app_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Add handlers to root logger
    if not logger.handlers:
        logger.addHandler(info_console)
        logger.addHandler(info_file)
        logger.addHandler(error_console)
        logger.addHandler(error_file)
        # logger.addHandler(debug_file)

    return logger


# Test
if __name__ == "__main__":
    logger = setup_logging("lib_logger")
    logger.debug("This is DEBUG log (file only, detailed format)")
    logger.info("This is INFO log (screen + file, simple format)")
    logger.error("This is ERROR log (screen + file, detailed format)")
