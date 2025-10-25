"""
KIMBALL Logging System

This module provides centralized logging for the entire KIMBALL platform.
Supports structured logging with different levels and outputs.
"""

import logging
import sys
from datetime import datetime
from typing import Optional
from pathlib import Path

class Logger:
    """Centralized logging system for KIMBALL."""
    
    def __init__(self, name: str = "kimball", level: str = "INFO"):
        """Initialize logger."""
        self.name = name
        self.level = getattr(logging, level.upper(), logging.INFO)
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logger with handlers."""
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.level)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler
        log_file = Path("logs") / f"{self.name}.log"
        log_file.parent.mkdir(exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(self.level)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self.logger.info(message, extra=kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self.logger.warning(message, extra=kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self.logger.error(message, extra=kwargs)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self.logger.debug(message, extra=kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        self.logger.critical(message, extra=kwargs)
    
    def log_phase_start(self, phase: str, **kwargs) -> None:
        """Log phase start."""
        self.info(f"Starting {phase} phase", phase=phase, **kwargs)
    
    def log_phase_complete(self, phase: str, **kwargs) -> None:
        """Log phase completion."""
        self.info(f"Completed {phase} phase", phase=phase, **kwargs)
    
    def log_api_call(self, endpoint: str, method: str, **kwargs) -> None:
        """Log API call."""
        self.info(f"API call: {method} {endpoint}", endpoint=endpoint, method=method, **kwargs)
