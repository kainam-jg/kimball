"""
KIMBALL FastAPI Main Application

This is the main FastAPI application that orchestrates all KIMBALL services.
It provides a unified API for the four phases: Acquire, Discover, Transform, Model.

Current Active Phases:
- Acquire: Data source management and data extraction
- Discover: Metadata analysis and intelligent type inference
- Transform: ELT orchestration with ClickHouse UDFs
- Model: ERD generation and dimensional hierarchy analysis
- Access: Query engine for gold schema dimensional model
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
from typing import Dict, Any

# Import active API routers
from .acquire_routes import acquire_router
from .discover_routes import discover_router
from .transform_routes import transform_router
from .model_routes import model_router
from .demo_routes import demo_router
from .access_routes import access_router
from .pipeline_routes import pipeline_router
from .admin_routes import admin_router

# Future phases (commented out for systematic testing)
# from .build_routes import build_router

# Core configuration and logging
from ..core.config import Config
from ..core.logger import Logger

# Initialize configuration and logging
config = Config()
logger = Logger()

# Global log pruner instance (for startup/shutdown lifecycle)
_log_pruner_instance = None

# Create FastAPI application
app = FastAPI(
    title="KIMBALL API",
    description="Kinetic Intelligent Model Builder with Augmented Learning and Loading",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://kimballapp.netlify.app",  # Netlify frontend
        "http://localhost:3000",           # Local development
        "http://localhost:8000",           # API docs
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Include active API routers
# Each router handles a specific phase of the KIMBALL pipeline
app.include_router(acquire_router, tags=["Acquire"])
app.include_router(discover_router, tags=["Discover"])
app.include_router(model_router, tags=["Model"])
app.include_router(transform_router, tags=["Transform"])
app.include_router(demo_router, tags=["Demo"])
app.include_router(access_router, tags=["Access"])
app.include_router(pipeline_router, tags=["Pipeline"])
app.include_router(admin_router, tags=["Administration"])

# Future phases (commented out for systematic testing)
# app.include_router(build_router, tags=["Build"])

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "KIMBALL API - Kinetic Intelligent Model Builder",
        "version": "2.0.0",
        "phases": ["Acquire", "Discover", "Model", "Transform", "Demo", "Access", "Pipeline", "Administration"],  # Currently active phases
        "docs": "/docs",
        "redoc": "/redoc"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "KIMBALL API"}

# Startup event to initialize background services
@app.on_event("startup")
async def startup_event():
    """Initialize background services on startup."""
    global _log_pruner_instance
    from ..core.log_pruner import LogPruner
    
    # Start log pruning service if enabled
    _log_pruner_instance = LogPruner()
    if _log_pruner_instance.is_enabled():
        _log_pruner_instance.start()
        logger.info("Log pruning service started on application startup")

# Shutdown event to cleanup background services
@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup background services on shutdown."""
    global _log_pruner_instance
    
    # Stop log pruning service
    if _log_pruner_instance and _log_pruner_instance.running:
        await _log_pruner_instance.stop_async()
        logger.info("Log pruning service stopped on application shutdown")

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error", "detail": str(exc)}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
