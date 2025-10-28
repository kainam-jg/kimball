"""
KIMBALL Platform Setup

This module provides setup configuration for the KIMBALL platform.
"""

from setuptools import setup, find_packages
import os

# Read README file
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="kimball",
    version="2.0.0",
    author="KIMBALL Development Team",
    author_email="dev@kimball.ai",
    description="Kinetic Intelligent Model Builder with Augmented Learning and Loading",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/kainam-jg/kimball",
    project_urls={
        "Bug Reports": "https://github.com/kainam-jg/kimball/issues",
        "Source": "https://github.com/kainam-jg/kimball",
        "Documentation": "https://kimball.readthedocs.io/",
    },
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.5.0",
        ],
        "docs": [
            "sphinx>=7.0.0",
            "sphinx-rtd-theme>=1.3.0",
            "myst-parser>=2.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "kimball=kimball.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "kimball": [
            "templates/*.html",
            "static/*.css",
            "static/*.js",
        ],
    },
    keywords=[
        "data-warehouse",
        "etl",
        "data-pipeline",
        "metadata-discovery",
        "data-quality",
        "star-schema",
        "olap",
        "clickhouse",
        "fastapi",
    ],
    license="MIT",
    zip_safe=False,
)
