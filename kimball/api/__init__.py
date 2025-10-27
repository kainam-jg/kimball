"""
KIMBALL FastAPI Backend

This module provides the FastAPI backend services:
- REST API endpoints for all KIMBALL phases
- Authentication and authorization
- Request/response models
- Error handling and validation

Organized by phase: acquire, discover, transform, model
"""

from .main import app
from .acquire_routes import acquire_router
from .discover_routes import discover_router
from .transform_routes import transform_router
from .model_routes import model_router

__all__ = [
    'app',
    'acquire_router',
    'discover_router', 
    'transform_router',
    'model_router'
]
