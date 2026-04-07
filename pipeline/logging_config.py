"""
Logging configuration for production-grade observability.
"""

import logging
import sys
from pathlib import Path

# Create logs directory
Path("logs").mkdir(exist_ok=True)


def setup_logging(
    level: str = "INFO",
    log_to_file: bool = True,
    log_file: str = "logs/pipeline.log",
):
    """Configure logging for the pipeline."""
    
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_to_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    return root_logger


# Default setup
setup_logging()