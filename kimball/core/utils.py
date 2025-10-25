"""
KIMBALL Core Utilities

This module provides common utility functions used across the KIMBALL platform:
- Data validation and formatting
- File operations and I/O
- String manipulation and parsing
- Date/time utilities
- Performance monitoring
"""

import os
import json
import uuid
import hashlib
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from .logger import Logger

class Utils:
    """Utility class with common functions for KIMBALL platform."""
    
    def __init__(self):
        """Initialize utilities."""
        self.logger = Logger("utils")
    
    @staticmethod
    def generate_id(prefix: str = "kimball") -> str:
        """Generate a unique ID with timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4().hex[:8])
        return f"{prefix}_{timestamp}_{unique_id}"
    
    @staticmethod
    def generate_hash(data: str) -> str:
        """Generate SHA-256 hash of data."""
        return hashlib.sha256(data.encode()).hexdigest()
    
    @staticmethod
    def validate_json(data: str) -> bool:
        """Validate JSON string."""
        try:
            json.loads(data)
            return True
        except json.JSONDecodeError:
            return False
    
    @staticmethod
    def safe_json_loads(data: str, default: Any = None) -> Any:
        """Safely load JSON with default fallback."""
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return default
    
    @staticmethod
    def format_bytes(bytes_value: int) -> str:
        """Format bytes into human-readable string."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"
    
    @staticmethod
    def format_duration(seconds: float) -> str:
        """Format duration in seconds to human-readable string."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"
    
    @staticmethod
    def clean_string(text: str) -> str:
        """Clean string by removing special characters and normalizing."""
        if not isinstance(text, str):
            return str(text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        # Replace multiple spaces with single space
        text = ' '.join(text.split())
        
        # Remove null characters
        text = text.replace('\x00', '')
        
        return text
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email address format."""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
    
    @staticmethod
    def validate_url(url: str) -> bool:
        """Validate URL format."""
        import re
        pattern = r'^https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:\#(?:[\w.])*)?)?$'
        return re.match(pattern, url) is not None
    
    @staticmethod
    def truncate_string(text: str, max_length: int = 100, suffix: str = "...") -> str:
        """Truncate string to maximum length."""
        if len(text) <= max_length:
            return text
        return text[:max_length - len(suffix)] + suffix
    
    @staticmethod
    def get_file_extension(filename: str) -> str:
        """Get file extension from filename."""
        return os.path.splitext(filename)[1].lower()
    
    @staticmethod
    def is_safe_filename(filename: str) -> bool:
        """Check if filename is safe (no path traversal)."""
        # Remove any path components
        filename = os.path.basename(filename)
        
        # Check for dangerous characters
        dangerous_chars = ['..', '/', '\\', ':', '*', '?', '"', '<', '>', '|']
        return not any(char in filename for char in dangerous_chars)
    
    @staticmethod
    def create_directory(path: str) -> bool:
        """Create directory if it doesn't exist."""
        try:
            os.makedirs(path, exist_ok=True)
            return True
        except Exception:
            return False
    
    @staticmethod
    def get_file_size(filepath: str) -> int:
        """Get file size in bytes."""
        try:
            return os.path.getsize(filepath)
        except OSError:
            return 0
    
    @staticmethod
    def file_exists(filepath: str) -> bool:
        """Check if file exists."""
        return os.path.isfile(filepath)
    
    @staticmethod
    def directory_exists(dirpath: str) -> bool:
        """Check if directory exists."""
        return os.path.isdir(dirpath)
    
    @staticmethod
    def list_files(directory: str, pattern: str = "*") -> List[str]:
        """List files in directory matching pattern."""
        import glob
        try:
            return glob.glob(os.path.join(directory, pattern))
        except Exception:
            return []
    
    @staticmethod
    def read_file_safely(filepath: str, encoding: str = 'utf-8') -> Optional[str]:
        """Safely read file content."""
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                return f.read()
        except Exception:
            return None
    
    @staticmethod
    def write_file_safely(filepath: str, content: str, encoding: str = 'utf-8') -> bool:
        """Safely write content to file."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w', encoding=encoding) as f:
                f.write(content)
            return True
        except Exception:
            return False
    
    @staticmethod
    def parse_datetime(date_string: str, format: str = None) -> Optional[datetime]:
        """Parse datetime string with various formats."""
        if format:
            try:
                return datetime.strptime(date_string, format)
            except ValueError:
                return None
        
        # Try common formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%SZ'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue
        
        return None
    
    @staticmethod
    def format_datetime(dt: datetime, format: str = '%Y-%m-%d %H:%M:%S') -> str:
        """Format datetime to string."""
        return dt.strftime(format)
    
    @staticmethod
    def get_timestamp() -> str:
        """Get current timestamp as ISO string."""
        return datetime.now().isoformat()
    
    @staticmethod
    def calculate_percentage(part: float, total: float) -> float:
        """Calculate percentage."""
        if total == 0:
            return 0.0
        return (part / total) * 100
    
    @staticmethod
    def calculate_ratio(value1: float, value2: float) -> float:
        """Calculate ratio between two values."""
        if value2 == 0:
            return 0.0
        return value1 / value2
    
    @staticmethod
    def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame by cleaning data types and values."""
        try:
            # Create a copy to avoid modifying original
            normalized_df = df.copy()
            
            # Convert object columns to string and clean
            for col in normalized_df.select_dtypes(include=['object']).columns:
                normalized_df[col] = normalized_df[col].astype(str)
                normalized_df[col] = normalized_df[col].apply(Utils.clean_string)
            
            # Handle datetime columns
            for col in normalized_df.select_dtypes(include=['datetime64']).columns:
                normalized_df[col] = pd.to_datetime(normalized_df[col], errors='coerce')
            
            # Handle numeric columns
            for col in normalized_df.select_dtypes(include=['object']).columns:
                # Try to convert to numeric
                try:
                    normalized_df[col] = pd.to_numeric(normalized_df[col], errors='ignore')
                except:
                    pass
            
            return normalized_df
            
        except Exception as e:
            # Return original DataFrame if normalization fails
            return df
    
    @staticmethod
    def detect_data_types(df: pd.DataFrame) -> Dict[str, str]:
        """Detect optimal data types for DataFrame columns."""
        try:
            type_mapping = {}
            
            for col in df.columns:
                # Check if column is numeric
                if pd.api.types.is_numeric_dtype(df[col]):
                    if df[col].dtype in ['int64', 'int32']:
                        type_mapping[col] = 'integer'
                    elif df[col].dtype in ['float64', 'float32']:
                        type_mapping[col] = 'float'
                    else:
                        type_mapping[col] = 'numeric'
                
                # Check if column is datetime
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    type_mapping[col] = 'datetime'
                
                # Check if column is boolean
                elif pd.api.types.is_bool_dtype(df[col]):
                    type_mapping[col] = 'boolean'
                
                # Check if column is categorical
                elif df[col].nunique() < len(df) * 0.5:
                    type_mapping[col] = 'categorical'
                
                # Default to string
                else:
                    type_mapping[col] = 'string'
            
            return type_mapping
            
        except Exception:
            return {col: 'string' for col in df.columns}
    
    @staticmethod
    def measure_performance(func):
        """Decorator to measure function performance."""
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger = Logger("performance")
            logger.info(f"Function {func.__name__} executed in {Utils.format_duration(duration)}")
            
            return result
        return wrapper
    
    @staticmethod
    def validate_dataframe_schema(df: pd.DataFrame, expected_schema: Dict[str, str]) -> Dict[str, Any]:
        """Validate DataFrame against expected schema."""
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        try:
            # Check if all expected columns exist
            missing_columns = set(expected_schema.keys()) - set(df.columns)
            if missing_columns:
                validation_result["valid"] = False
                validation_result["errors"].append(f"Missing columns: {missing_columns}")
            
            # Check data types
            for col, expected_type in expected_schema.items():
                if col in df.columns:
                    actual_type = str(df[col].dtype)
                    if expected_type not in actual_type:
                        validation_result["warnings"].append(
                            f"Column {col}: expected {expected_type}, found {actual_type}"
                        )
            
            return validation_result
            
        except Exception as e:
            validation_result["valid"] = False
            validation_result["errors"].append(f"Validation error: {str(e)}")
            return validation_result
