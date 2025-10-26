# Enhanced Logging Configuration for KIMBALL FastAPI Server

import logging
import sys
from datetime import datetime
from typing import Dict, Any
import json

class EnhancedLogger:
    """Enhanced logger with structured logging for FastAPI operations."""
    
    def __init__(self, name: str = "kimball"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Create console handler with colored output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Create file handler for persistent logging
        file_handler = logging.FileHandler('kimball_server.log', mode='a')
        file_handler.setLevel(logging.DEBUG)
        
        # Create formatters
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_handler.setFormatter(console_formatter)
        file_handler.setFormatter(file_formatter)
        
        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
    
    def info(self, message: str, extra_data: Dict[str, Any] = None):
        """Log info message with optional extra data."""
        if extra_data:
            message = f"{message} | Data: {json.dumps(extra_data, default=str)}"
        self.logger.info(message)
    
    def error(self, message: str, extra_data: Dict[str, Any] = None):
        """Log error message with optional extra data."""
        if extra_data:
            message = f"{message} | Data: {json.dumps(extra_data, default=str)}"
        self.logger.error(message)
    
    def warning(self, message: str, extra_data: Dict[str, Any] = None):
        """Log warning message with optional extra data."""
        if extra_data:
            message = f"{message} | Data: {json.dumps(extra_data, default=str)}"
        self.logger.warning(message)
    
    def debug(self, message: str, extra_data: Dict[str, Any] = None):
        """Log debug message with optional extra data."""
        if extra_data:
            message = f"{message} | Data: {json.dumps(extra_data, default=str)}"
        self.logger.debug(message)
    
    def log_api_call(self, endpoint: str, method: str, request_data: Dict[str, Any] = None):
        """Log API call with structured data."""
        self.info(
            f"API Call: {method} {endpoint}",
            {
                "endpoint": endpoint,
                "method": method,
                "request_data": request_data,
                "timestamp": datetime.now().isoformat()
            }
        )
    
    def log_data_extraction(self, source_id: str, extraction_type: str, record_count: int, target_table: str):
        """Log data extraction operation."""
        self.info(
            f"Data Extraction: {extraction_type} from {source_id}",
            {
                "source_id": source_id,
                "extraction_type": extraction_type,
                "record_count": record_count,
                "target_table": target_table,
                "timestamp": datetime.now().isoformat()
            }
        )
    
    def log_connection_test(self, source_id: str, success: bool, error_message: str = None):
        """Log connection test results."""
        level = "info" if success else "error"
        message = f"Connection Test: {source_id} - {'SUCCESS' if success else 'FAILED'}"
        
        extra_data = {
            "source_id": source_id,
            "success": success,
            "timestamp": datetime.now().isoformat()
        }
        
        if error_message:
            extra_data["error"] = error_message
        
        if success:
            self.info(message, extra_data)
        else:
            self.error(message, extra_data)
    
    def log_validation_result(self, table_name: str, expected_count: int, actual_count: int, validation_passed: bool):
        """Log data validation results."""
        self.info(
            f"Data Validation: {table_name} - {'PASSED' if validation_passed else 'FAILED'}",
            {
                "table_name": table_name,
                "expected_count": expected_count,
                "actual_count": actual_count,
                "validation_passed": validation_passed,
                "timestamp": datetime.now().isoformat()
            }
        )

# Global logger instance
logger = EnhancedLogger("kimball_server")
