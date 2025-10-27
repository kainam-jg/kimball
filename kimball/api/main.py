"""
KIMBALL FastAPI Main Application

This is the main FastAPI application that orchestrates all KIMBALL services.
It provides a unified API for the five phases: Acquire, Discover, Transformation, Model, Build.

Current Active Phases:
- Acquire: Data source management and data extraction
- Discover: Metadata analysis and intelligent type inference
- Transformation: ELT orchestration with ClickHouse UDFs
- Model: ERD generation and dimensional hierarchy analysis

Future Phases:
- Build: Pipeline generation and orchestration
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
from typing import Dict, Any

# Import active API routers
from .acquire_routes import acquire_router
from .discover_routes import discover_router
from .transformation_routes import transformation_router
from .model_routes import model_router

# Future phases (commented out for systematic testing)
# from .build_routes import build_router

# Core configuration and logging
from ..core.config import Config
from ..core.logger import Logger

# Initialize configuration and logging
config = Config()
logger = Logger()

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
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include active API routers
# Each router handles a specific phase of the KIMBALL pipeline
app.include_router(acquire_router, tags=["Acquire"])
app.include_router(discover_router, tags=["Discover"])
app.include_router(transformation_router, tags=["Transformation"])
app.include_router(model_router, tags=["Model"])

# Future phases (commented out for systematic testing)
# app.include_router(build_router, tags=["Build"])

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "KIMBALL API - Kinetic Intelligent Model Builder",
        "version": "2.0.0",
        "phases": ["Acquire", "Discover", "Transformation", "Model"],  # Currently active phases
        "docs": "/docs",
        "redoc": "/redoc"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "KIMBALL API"}

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
