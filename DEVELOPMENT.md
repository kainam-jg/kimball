# KIMBALL v2.0 Development Guide

## 🚀 Quick Start

### Development Mode (Recommended)
```bash
# Start FastAPI server with hot reloading
start_server_dev.bat

# Start Streamlit frontend
cd frontend
start_streamlit.bat
```

### Production Mode
```bash
# Start FastAPI server optimized for production
start_server_prod.bat
```

## 🔧 Development Features

### Hot Reloading
- **FastAPI Server**: Automatically restarts when you save Python files
- **File Watching**: Monitors `kimball/` and `frontend/` directories
- **Debug Mode**: Detailed error messages and stack traces
- **Access Logs**: Track all API requests and responses

### Development URLs
- **FastAPI Server**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **Alternative Docs**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health
- **Streamlit Frontend**: http://localhost:8501

## 📁 File Structure

```
kimball/
├── start_server.bat          # Basic server startup
├── start_server_dev.bat      # Development mode (hot reloading)
├── start_server_prod.bat     # Production mode (optimized)
├── stop_server.bat           # Stop server
├── test_server.py            # Test server functionality
├── frontend/
│   ├── start_streamlit.bat   # Start Streamlit frontend
│   ├── stop_streamlit.bat    # Stop Streamlit frontend
│   └── test_streamlit.py     # Test Streamlit functionality
└── requirements.txt          # Python dependencies
```

## 🛠️ Development Workflow

### 1. Start Development Environment
```bash
# Terminal 1: Start FastAPI backend
start_server_dev.bat

# Terminal 2: Start Streamlit frontend
cd frontend
start_streamlit.bat
```

### 2. Make Changes
- Edit Python files in `kimball/` directory
- Server automatically restarts on file changes
- Frontend automatically reloads on file changes
- Check console for error messages

### 3. Test Changes
```bash
# Test FastAPI server
python test_server.py

# Test Streamlit frontend
cd frontend
python test_streamlit.py
```

### 4. Stop Services
```bash
# Stop FastAPI server
stop_server.bat

# Stop Streamlit frontend
cd frontend
stop_streamlit.bat
```

## 🔍 Debugging

### FastAPI Debugging
- **Debug Mode**: Enabled in development mode
- **Detailed Logs**: Check console output for errors
- **API Testing**: Use http://localhost:8000/docs for interactive testing
- **Health Check**: http://localhost:8000/health

### Streamlit Debugging
- **Error Messages**: Displayed in browser console
- **File Reloading**: Automatic on file save
- **Debug Mode**: Built-in Streamlit debugging

## 📊 Monitoring

### Development Monitoring
- **File Changes**: Server restarts automatically
- **Error Tracking**: Detailed stack traces in console
- **API Requests**: Access logs in development mode
- **Performance**: Debug-level logging

### Production Monitoring
- **Stability**: No hot reloading for stability
- **Performance**: Optimized with multiple workers
- **Logging**: Production-level logging
- **Error Handling**: Optimized error responses

## 🚀 Deployment

### Development Deployment
```bash
# Use development mode for local testing
start_server_dev.bat
```

### Production Deployment
```bash
# Use production mode for deployment
start_server_prod.bat
```

## 🔧 Configuration

### Environment Variables
- **Database**: Configure in `config.json` (not tracked in git)
- **Logging**: Configured in `kimball/core/logger.py`
- **API**: Configured in `kimball/api/main.py`

### Dependencies
- **Installation**: Automatic via `requirements.txt`
- **Virtual Environment**: Created automatically
- **Updates**: Run `pip install -r requirements.txt` to update

## 📝 Best Practices

### Development
1. **Use Development Mode**: Always use `start_server_dev.bat` for development
2. **Test Frequently**: Run test scripts after changes
3. **Check Logs**: Monitor console output for errors
4. **API Testing**: Use the interactive docs at `/docs`

### Code Changes
1. **Hot Reloading**: Changes are automatically picked up
2. **Error Handling**: Check console for detailed error messages
3. **Testing**: Use the provided test scripts
4. **Documentation**: Update this guide as needed

## 🆘 Troubleshooting

### Common Issues
1. **Port Conflicts**: Ensure ports 8000 and 8501 are available
2. **Dependencies**: Run `pip install -r requirements.txt`
3. **Virtual Environment**: Ensure venv is activated
4. **File Permissions**: Check file access permissions

### Getting Help
1. **Check Logs**: Review console output for errors
2. **Test Scripts**: Run test scripts to verify functionality
3. **API Docs**: Use interactive documentation at `/docs`
4. **Health Checks**: Verify services are running properly
