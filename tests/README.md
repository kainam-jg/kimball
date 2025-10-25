# KIMBALL Test Suite

This directory contains all test modules for the KIMBALL platform.

## Test Structure

```
tests/
├── __init__.py              # Test package initialization
├── conftest.py              # Pytest configuration and fixtures
├── test_suite.py            # Comprehensive test runner
├── test_server.py           # FastAPI server testing
├── test_streamlit.py        # Streamlit frontend testing
├── test_data_sources.py     # PostgreSQL and S3 connection testing
├── test_s3_connection.py     # S3 connection and API testing
├── test_acquire.py           # Acquire phase testing
├── test_discover.py         # Discover phase testing
├── run_data_sources_test.py # Quick data sources test runner
├── run_s3_test.py           # Quick S3 test runner
└── data/                    # Test data directory
    └── README.md            # Test data documentation
```

## Running Tests

### Run All Tests
```bash
# From project root
python run_tests.py

# Or directly
python tests/test_suite.py
```

### Run Individual Tests
```bash
# Data sources test (PostgreSQL + S3)
python tests/test_data_sources.py

# Quick data sources test runner
python tests/run_data_sources_test.py

# S3 connection test
python tests/test_s3_connection.py

# Quick S3 test runner
python tests/run_s3_test.py

# FastAPI server test
python tests/test_server.py

# Streamlit frontend test
python tests/test_streamlit.py

# Acquire phase test
python tests/test_acquire.py

# Discover phase test
python tests/test_discover.py
```

### Using Pytest
```bash
# Run all tests
pytest tests/

# Run specific test
pytest tests/test_s3_connection.py

# Run with verbose output
pytest tests/ -v
```

## Test Categories

### 1. Infrastructure Tests
- **test_server.py**: FastAPI server health, endpoints, documentation
- **test_streamlit.py**: Streamlit frontend accessibility and functionality

### 2. Data Source Tests
- **test_data_sources.py**: PostgreSQL and S3 connections, schema discovery, data extraction, API endpoints
- **test_s3_connection.py**: S3 connection, file listing, data extraction, API endpoints
- **test_acquire.py**: Multi-source data acquisition, configuration loading

### 3. Phase Tests
- **test_discover.py**: Metadata analysis, schema discovery, quality assessment

## Test Data

The `tests/data/` directory contains test data files for various scenarios:
- Sample CSV files
- Sample JSON files  
- Sample Excel files
- Mock data for testing

## Configuration

Tests use the same `config.json` file as the main application, but with test-specific credentials and settings.

## Best Practices

1. **Always use `/tests` directory**: All test files must be in this directory
2. **Proper path handling**: Use `os.path.dirname(os.path.dirname(os.path.abspath(__file__)))` for project root
3. **Comprehensive testing**: Test both direct functionality and API endpoints
4. **Error handling**: Gracefully handle missing dependencies or services
5. **Documentation**: Include clear test descriptions and expected outcomes

## Troubleshooting

### Common Issues
1. **Import errors**: Ensure project root is in Python path
2. **Missing dependencies**: Install required packages with `pip install -r requirements.txt`
3. **Server not running**: Start FastAPI server with `start_server.bat` before API tests
4. **Configuration errors**: Check `config.json` for correct credentials

### Getting Help
1. Check test output for specific error messages
2. Verify server is running for API tests
3. Check configuration files for correct settings
4. Review test logs for detailed error information
