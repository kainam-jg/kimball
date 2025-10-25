"""
KIMBALL FastAPI Backend

This module provides the FastAPI backend services:
- REST API endpoints for all KIMBALL phases
- Authentication and authorization
- Request/response models
- Error handling and validation

Organized by phase: acquire, discover, model, build
"""

from .main import app
from .acquire_routes import acquire_router
from .discover_routes import discover_router
from .model_routes import model_router
from .build_routes import build_router

__all__ = [
    'app',
    'acquire_router',
    'discover_router', 
    'model_router',
    'build_router'
]
