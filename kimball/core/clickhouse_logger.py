"""
KIMBALL ClickHouse Logger

This module provides logging functionality that writes to ClickHouse tables
instead of log files. Logs are stored in the logs schema.
"""

import logging
import sys
import traceback
from datetime import datetime
from typing import Optional, Dict, Any, TYPE_CHECKING
import json

from .config import Config

if TYPE_CHECKING:
    from .database import DatabaseManager


class ClickHouseLogHandler(logging.Handler):
    """Custom logging handler that writes logs to ClickHouse, routing by phase."""
    
    # Map phases to their corresponding log tables
    PHASE_TABLE_MAP = {
        "Acquire": "logs.acquire",
        "Discover": "logs.discover",
        "Model": "logs.model",
        "Transform": "logs.transform",
        "Pipeline": "logs.pipeline",
        "Administration": "logs.administration"
    }
    DEFAULT_TABLE = "logs.application"
    
    def __init__(self, db_manager: Any):
        """Initialize ClickHouse log handler."""
        super().__init__()
        self.db_manager = db_manager
        self.buffer = {}  # Buffer by table name
        self.buffer_size = 10  # Buffer logs before batch insert
    
    def _get_table_name(self, phase: Optional[str] = None) -> str:
        """Determine the correct log table based on phase."""
        if phase and phase in self.PHASE_TABLE_MAP:
            return self.PHASE_TABLE_MAP[phase]
        return self.DEFAULT_TABLE
        
    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to ClickHouse, routing to appropriate table by phase."""
        try:
            # Determine target table based on phase
            phase = getattr(record, 'phase', None)
            table_name = self._get_table_name(phase)
            
            # Format the log entry
            log_entry = self._format_log_record(record)
            
            # Add to buffer for appropriate table
            if table_name not in self.buffer:
                self.buffer[table_name] = []
            self.buffer[table_name].append(log_entry)
            
            # Flush buffer when it reaches threshold for this table
            if len(self.buffer[table_name]) >= self.buffer_size:
                self._flush_table(table_name)
                
        except Exception as e:
            # Fallback to stderr if ClickHouse logging fails
            print(f"Error writing to ClickHouse logs: {e}", file=sys.stderr)
    
    def _format_log_record(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Format a log record for ClickHouse insertion."""
        # Extract metadata from record
        metadata_dict = {}
        phase = getattr(record, 'phase', '')
        
        if hasattr(record, 'phase'):
            metadata_dict['phase'] = record.phase
        if hasattr(record, 'endpoint'):
            metadata_dict['endpoint'] = record.endpoint
        if hasattr(record, 'method'):
            metadata_dict['method'] = record.method
        if hasattr(record, 'request_data'):
            metadata_dict['request_data'] = record.request_data
        
        # Format message
        message = record.getMessage()
        
        # Extract error information if present
        error_type = None
        error_traceback = None
        if record.exc_info:
            error_type = record.exc_info[0].__name__ if record.exc_info[0] else None
            error_traceback = ''.join(traceback.format_exception(*record.exc_info))
        
        # Base log entry fields
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created),
            'logger_name': record.name,
            'log_level': record.levelname,
            'message': message,
            'module': record.module,
            'function': record.funcName,
            'line_number': record.lineno,
            'phase': phase,
            'endpoint': getattr(record, 'endpoint', ''),
            'method': getattr(record, 'method', ''),
            'request_data': json.dumps(getattr(record, 'request_data', {})) if hasattr(record, 'request_data') and record.request_data else '',
            'error_type': error_type or '',
            'error_traceback': error_traceback or '',
            'metadata': json.dumps(metadata_dict) if metadata_dict else ''
        }
        
        # Add phase-specific fields based on phase
        table_name = self._get_table_name(phase)
        if table_name == "logs.acquire":
            log_entry['source_id'] = getattr(record, 'source_id', '')
            log_entry['transformation_id'] = getattr(record, 'transformation_id', 0)
        elif table_name == "logs.discover":
            log_entry['schema_name'] = getattr(record, 'schema_name', '')
            log_entry['table_name'] = getattr(record, 'table_name', '')
        elif table_name == "logs.model":
            log_entry['schema_name'] = getattr(record, 'schema_name', '')
            log_entry['table_name'] = getattr(record, 'table_name', '')
            log_entry['analysis_type'] = getattr(record, 'analysis_type', '')
        elif table_name == "logs.transform":
            log_entry['transformation_id'] = getattr(record, 'transformation_id', 0)
            log_entry['transformation_stage'] = getattr(record, 'transformation_stage', '')
            log_entry['schema_name'] = getattr(record, 'schema_name', '')
            log_entry['table_name'] = getattr(record, 'table_name', '')
        elif table_name == "logs.pipeline":
            log_entry['pipeline_id'] = getattr(record, 'pipeline_id', '')
            log_entry['execution_id'] = getattr(record, 'execution_id', '')
            log_entry['schedule_type'] = getattr(record, 'schedule_type', '')
        elif table_name == "logs.administration":
            log_entry['operation_type'] = getattr(record, 'operation_type', '')
            log_entry['resource_type'] = getattr(record, 'resource_type', '')
        
        return log_entry
    
    def _get_column_names(self, table_name: str) -> list:
        """Get column names for a specific log table."""
        # Base columns common to all log tables
        base_columns = [
            'timestamp', 'logger_name', 'log_level', 'message', 'module',
            'function', 'line_number', 'phase', 'endpoint', 'method',
            'request_data', 'error_type', 'error_traceback', 'metadata'
        ]
        
        # Phase-specific columns
        phase_columns = {
            "logs.acquire": ['source_id', 'transformation_id'],
            "logs.discover": ['schema_name', 'table_name'],
            "logs.model": ['schema_name', 'table_name', 'analysis_type'],
            "logs.transform": ['transformation_id', 'transformation_stage', 'schema_name', 'table_name'],
            "logs.pipeline": ['pipeline_id', 'execution_id', 'schedule_type'],
            "logs.administration": ['operation_type', 'resource_type']
        }
        
        if table_name in phase_columns:
            return base_columns + phase_columns[table_name]
        return base_columns  # logs.application
    
    def _flush_table(self, table_name: str) -> None:
        """Flush buffered logs for a specific table."""
        if table_name not in self.buffer or not self.buffer[table_name]:
            return
        
        try:
            conn = self.db_manager.get_connection("clickhouse")
            column_names = self._get_column_names(table_name)
            
            # Prepare data for insertion
            insert_data = []
            for entry in self.buffer[table_name]:
                row = []
                for col in column_names:
                    row.append(entry.get(col, '' if col != 'transformation_id' else 0))
                insert_data.append(row)
            
            # Batch insert
            conn.insert(table_name, insert_data, column_names=column_names)
            
            # Clear buffer for this table
            self.buffer[table_name].clear()
            
        except Exception as e:
            # If ClickHouse insert fails, log to stderr and clear buffer
            print(f"Error inserting logs to {table_name}: {e}", file=sys.stderr)
            self.buffer[table_name].clear()
    
    def _flush_buffer(self) -> None:
        """Flush all buffered logs to ClickHouse."""
        if not self.buffer:
            return
        
        # Flush each table's buffer
        for table_name in list(self.buffer.keys()):
            self._flush_table(table_name)
    
    def flush(self) -> None:
        """Flush any buffered logs."""
        self._flush_buffer()
    
    def close(self) -> None:
        """Close handler and flush remaining logs."""
        self._flush_buffer()
        super().close()


class ClickHouseLogger:
    """Logger that writes to ClickHouse tables instead of files."""
    
    def __init__(self, name: str = "kimball", level: str = "INFO", db_manager: Optional[Any] = None):
        """Initialize ClickHouse logger.
        
        Args:
            name: Logger name
            level: Log level
            db_manager: Optional DatabaseManager instance
        """
        self.name = name
        self.level = getattr(logging, level.upper(), logging.INFO)
        # Lazy import to avoid circular dependency
        if db_manager is None:
            from .database import DatabaseManager
            self.db_manager: Any = DatabaseManager()
        else:
            self.db_manager: Any = db_manager
        self.config = Config()
        
        # Get log level from config if available
        config_level = self.config.get('logging.level', level)
        self.level = getattr(logging, config_level.upper(), logging.INFO)
        
        # Ensure logs schema and table exist
        self._ensure_logs_table()
        
        # Set up logger
        self.logger = self._setup_logger()
    
    def _ensure_logs_table(self):
        """Ensure logs schema and table exist."""
        try:
            from .table_initializer import TableInitializer
            initializer = TableInitializer()
            # Try to create the schema first
            # Process-specific log tables are created by initialization endpoint
            # This is now handled in logger._ensure_logs_table() which creates all process tables
            pass
        except Exception as e:
            # If table creation fails, we'll still try to log (might fail later)
            print(f"Warning: Could not ensure log tables exist: {e}")
    
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
        
        # ClickHouse handler
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
        request_data = kwargs.pop('request_data', {})
        self.info(
            f"API call: {method} {endpoint}",
            endpoint=endpoint,
            method=method,
            request_data=request_data,
            **kwargs
        )
    
    def flush(self) -> None:
        """Flush all handlers."""
        for handler in self.logger.handlers:
            if hasattr(handler, 'flush'):
                handler.flush()

