"""
KIMBALL Logging System

This module provides centralized logging for the entire KIMBALL platform.
Logs are stored in ClickHouse tables in the logs schema instead of files.
"""

import logging
import sys
from datetime import datetime
from typing import Optional, Any
from pathlib import Path

from .config import Config

# Import ClickHouse log handler
from .clickhouse_logger import ClickHouseLogHandler


class Logger:
    """Centralized logging system for KIMBALL with ClickHouse storage."""
    
    def __init__(self, name: str = "kimball", level: str = "INFO", db_manager: Optional[Any] = None):
        """Initialize logger.
        
        Args:
            name: Logger name
            level: Log level
            db_manager: Optional DatabaseManager instance (to avoid circular dependency)
        """
        self.name = name
        self.config = Config()
        # Lazy import to avoid circular dependency
        if db_manager is None:
            from .database import DatabaseManager
            self.db_manager = DatabaseManager()
        else:
            self.db_manager = db_manager
        
        # Get log level from config if available
        config_level = self.config.get('logging.level', level)
        self.level = getattr(logging, config_level.upper(), logging.INFO)
        
        # DO NOT ensure logs tables during __init__ to avoid startup initialization
        # Tables should be created via the initialization endpoint
        # Only check if they exist, don't create them
        
        self.logger = self._setup_logger()
        
    def _ensure_logs_table(self):
        """Ensure logs schema and all log tables exist.
        
        NOTE: Uses TableInitializer with use_logger=False to avoid recursion.
        """
        try:
            from .table_initializer import TableInitializer
            # CRITICAL: Don't use logger here to avoid recursion loop!
            # TableInitializer -> DatabaseManager -> Logger would create infinite recursion
            initializer = TableInitializer(use_logger=False)
            
            # Create logs schema if needed
            initializer.create_schema("logs")
            
            # Create all log tables
            log_tables = [
                "logs.application",
                "logs.acquire",
                "logs.discover",
                "logs.model",
                "logs.transform",
                "logs.pipeline",
                "logs.administration"
            ]
            
            for table_name in log_tables:
                try:
                    initializer.create_table(table_name)
                except Exception as e:
                    # If table creation fails, continue (might fail later on write)
                    print(f"Warning: Could not ensure {table_name} table exists: {e}", file=sys.stderr)
        except Exception as e:
            # If table creation fails, we'll still try to log (might fail later)
            print(f"Warning: Could not ensure logs tables exist: {e}", file=sys.stderr)
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logger with ClickHouse handler."""
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Console handler (keep for development/debugging)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.level)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # ClickHouse handler (replaces file handler)
        try:
            clickhouse_handler = ClickHouseLogHandler(self.db_manager)
            clickhouse_handler.setLevel(self.level)
            logger.addHandler(clickhouse_handler)
        except Exception as e:
            # If ClickHouse handler fails, continue with console only
            print(f"Warning: Could not set up ClickHouse log handler: {e}", file=sys.stderr)
        
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
