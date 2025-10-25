"""
KIMBALL Configuration Management

This module handles configuration for the entire KIMBALL platform.
Supports multiple environments and configuration sources.
"""

import os
import json
from typing import Dict, Any, Optional
from pathlib import Path

class Config:
    """Configuration manager for KIMBALL platform."""
    
    def __init__(self, config_file: str = "config.json"):
        """Initialize configuration."""
        self.config_file = config_file
        self.config = self._load_config()
        self.environment = os.getenv("KIMBALL_ENV", "development")
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            else:
                return self._get_default_config()
        except Exception as e:
            print(f"Error loading config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "clickhouse": {
                "host": "localhost",
                "port": 8123,
                "user": "default",
                "password": "",
                "database": "kimball"
            },
            "api": {
                "host": "0.0.0.0",
                "port": 8000,
                "debug": True
            },
            "logging": {
                "level": "INFO",
                "file": "kimball.log"
            },
            "phases": {
                "acquire": {
                    "enabled": True,
                    "max_connections": 10
                },
                "discover": {
                    "enabled": True,
                    "analysis_timeout": 300
                },
                "model": {
                    "enabled": True,
                    "max_hierarchies": 100
                },
                "build": {
                    "enabled": True,
                    "dag_timeout": 3600
                }
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save(self) -> bool:
        """Save configuration to file."""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving config: {e}")
            return False
