"""
KIMBALL Streamlit Frontend

This is a testing frontend for the KIMBALL platform using Streamlit.
It provides a simple interface to test all KIMBALL APIs and functionality.
"""

import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime
import time

# Configure page
st.set_page_config(
    page_title="KIMBALL Platform",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = "http://localhost:8000"
API_ENDPOINTS = {
    "acquire": f"{API_BASE_URL}/api/v1/acquire",
    "discover": f"{API_BASE_URL}/api/v1/discover",
    "model": f"{API_BASE_URL}/api/v1/model",
    "build": f"{API_BASE_URL}/api/v1/build"
}

def main():
    """Main Streamlit application."""
    
    # Header
    st.title("🔍 KIMBALL Platform")
    st.markdown("### Kinetic Intelligent Model Builder with Augmented Learning and Loading")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    phase = st.sidebar.selectbox(
        "Select Phase",
        ["Acquire", "Discover", "Model", "Build", "Status"]
    )
    
    # Main content based on selected phase
    if phase == "Acquire":
        acquire_phase()
    elif phase == "Discover":
        discover_phase()
    elif phase == "Model":
        model_phase()
    elif phase == "Build":
        build_phase()
    elif phase == "Status":
        status_page()

def acquire_phase():
    """Acquire phase interface."""
    st.header("📥 Acquire Phase")
    st.write("Connect to data sources, extract data, and load into the bronze layer.")
    
    # Acquire phase tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Data Sources", "Discover", "Connections", "Extraction", "Loading"])
    
    with tab1:
        st.subheader("Data Source Management")
        
        # List existing data sources
        if st.button("🔄 Refresh Data Sources", key="refresh_sources"):
            try:
                response = requests.get(f"{API_ENDPOINTS['acquire']}/datasources")
                if response.status_code == 200:
                    data = response.json()
                    st.session_state.data_sources = data.get("data_sources", {})
                    st.success(f"Found {data.get('count', 0)} data sources")
                else:
                    st.error(f"Failed to load data sources: {response.text}")
            except Exception as e:
                st.error(f"Error: {str(e)}")
        
        # Display data sources
        if "data_sources" in st.session_state:
            st.subheader("Configured Data Sources")
            
            for source_id, config in st.session_state.data_sources.items():
                with st.expander(f"{source_id} ({config.get('type', 'unknown')}) - {'✅ Enabled' if config.get('enabled') else '❌ Disabled'}"):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.write(f"**Type:** {config.get('type', 'N/A')}")
                        st.write(f"**Description:** {config.get('description', 'No description')}")
                        
                        # Show connection details (masked for security)
                        if config.get('type') == 'postgres':
                            st.write(f"**Host:** {config.get('host', 'N/A')}")
                            st.write(f"**Database:** {config.get('database', 'N/A')}")
                            st.write(f"**Schema:** {config.get('schema', 'N/A')}")
                        elif config.get('type') == 's3':
                            st.write(f"**Bucket:** {config.get('bucket', 'N/A')}")
                            st.write(f"**Region:** {config.get('region', 'N/A')}")
                            st.write(f"**Prefix:** {config.get('prefix', 'N/A')}")
                    
                    with col2:
                        # Test connection
                        if st.button(f"Test Connection", key=f"test_{source_id}"):
                            with st.spinner("Testing connection..."):
                                try:
                                    response = requests.get(f"{API_ENDPOINTS['acquire']}/test/{source_id}")
                                    if response.status_code == 200:
                                        result = response.json()
                                        if result.get("connection_test") == "success":
                                            st.success("✅ Connection successful")
                                        else:
                                            st.error("❌ Connection failed")
                                    else:
                                        st.error(f"❌ Test failed: {response.text}")
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
                        
                        # Edit source
                        if st.button(f"✏️ Edit", key=f"edit_{source_id}"):
                            st.session_state.edit_source_id = source_id
                            st.session_state.edit_source_config = config
                            st.rerun()
                        
                        # Delete source
                        if st.button(f"🗑️ Delete", key=f"delete_{source_id}", type="secondary"):
                            try:
                                response = requests.delete(f"{API_ENDPOINTS['acquire']}/datasources/{source_id}")
                                if response.status_code == 200:
                                    st.success("✅ Data source deleted")
                                    # Refresh the list
                                    if "data_sources" in st.session_state:
                                        del st.session_state.data_sources[source_id]
                                    st.rerun()
                                else:
                                    st.error(f"❌ Delete failed: {response.text}")
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
        
        # Add new data source
        st.subheader("Add New Data Source")
        
        # Initialize session state for form
        if 'source_type' not in st.session_state:
            st.session_state.source_type = "postgres"
        
        # Source type selection (outside form for dynamic updates)
        col1, col2 = st.columns([1, 2])
        with col1:
            source_type = st.selectbox(
                "Source Type", 
                ["postgres", "s3", "api"],
                key="source_type_selector",
                on_change=lambda: st.session_state.update(source_type=st.session_state.source_type_selector)
            )
            st.session_state.source_type = source_type
        
        with col2:
            st.info(f"📋 **Configuring {source_type.upper()} data source**")
        
        # Dynamic form based on source type
        with st.form("add_data_source", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                source_id = st.text_input("Source ID", placeholder="e.g., my_postgres_db")
                enabled = st.checkbox("Enabled", value=True)
                description = st.text_area("Description", placeholder="Brief description of this data source")
            
            with col2:
                if st.session_state.source_type == "postgres":
                    host = st.text_input("Host", placeholder="localhost")
                    port = st.number_input("Port", value=5432, min_value=1, max_value=65535)
                    database = st.text_input("Database", placeholder="mydb")
                    user = st.text_input("Username", placeholder="user")
                    password = st.text_input("Password", type="password")
                    schema = st.text_input("Schema", placeholder="public")
                    
                    config = {
                        "host": host,
                        "port": port,
                        "database": database,
                        "user": user,
                        "password": password,
                        "schema_name": schema
                    }
                
                elif st.session_state.source_type == "s3":
                    bucket = st.text_input("Bucket Name", placeholder="my-bucket")
                    region = st.text_input("Region", placeholder="us-east-1")
                    access_key = st.text_input("Access Key", type="password")
                    secret_key = st.text_input("Secret Key", type="password")
                    prefix = st.text_input("Prefix", placeholder="data/")
                    
                    config = {
                        "bucket": bucket,
                        "region": region,
                        "access_key": access_key,
                        "secret_key": secret_key,
                        "prefix": prefix
                    }
                
                elif st.session_state.source_type == "api":
                    base_url = st.text_input("Base URL", placeholder="https://api.example.com")
                    auth_type = st.selectbox("Auth Type", ["none", "bearer", "basic"])
                    token = st.text_input("Token/Key", type="password") if auth_type != "none" else ""
                    
                    config = {
                        "base_url": base_url,
                        "auth_type": auth_type
                    }
                    
                    if token:
                        config["token"] = token
            
            submitted = st.form_submit_button("Add Data Source", type="primary")
            
            if submitted:
                if not source_id or not st.session_state.source_type:
                    st.error("Please fill in Source ID and select Source Type")
                else:
                    try:
                        payload = {
                            "source_id": source_id,
                            "source_type": st.session_state.source_type,
                            "config": config,
                            "enabled": enabled,
                            "description": description
                        }
                        
                        response = requests.post(f"{API_ENDPOINTS['acquire']}/datasources", json=payload)
                        
                        if response.status_code == 200:
                            st.success("✅ Data source created successfully!")
                            # Clear form and refresh
                            st.rerun()
                        else:
                            st.error(f"❌ Failed to create data source: {response.text}")
                            
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
        
        # Edit existing data source
        if 'edit_source_id' in st.session_state and st.session_state.edit_source_id:
            st.subheader(f"Edit Data Source: {st.session_state.edit_source_id}")
            
            edit_config = st.session_state.edit_source_config
            
            with st.form("edit_data_source", clear_on_submit=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    edit_source_id = st.text_input("Source ID", value=st.session_state.edit_source_id, disabled=True)
                    edit_source_type = st.selectbox(
                        "Source Type", 
                        ["postgres", "s3", "api"],
                        index=["postgres", "s3", "api"].index(edit_config.get("type", "postgres"))
                    )
                    edit_enabled = st.checkbox("Enabled", value=edit_config.get("enabled", True))
                    edit_description = st.text_area("Description", value=edit_config.get("description", ""))
                
                with col2:
                    if edit_source_type == "postgres":
                        edit_host = st.text_input("Host", value=edit_config.get("host", ""))
                        edit_port = st.number_input("Port", value=edit_config.get("port", 5432), min_value=1, max_value=65535)
                        edit_database = st.text_input("Database", value=edit_config.get("database", ""))
                        edit_user = st.text_input("Username", value=edit_config.get("user", ""))
                        edit_password = st.text_input("Password", type="password", value=edit_config.get("password", ""))
                        edit_schema = st.text_input("Schema", value=edit_config.get("schema", ""))
                        
                        edit_config_data = {
                            "host": edit_host,
                            "port": edit_port,
                            "database": edit_database,
                            "user": edit_user,
                            "password": edit_password,
                            "schema_name": edit_schema
                        }
                    
                    elif edit_source_type == "s3":
                        edit_bucket = st.text_input("Bucket Name", value=edit_config.get("bucket", ""))
                        edit_region = st.text_input("Region", value=edit_config.get("region", ""))
                        edit_access_key = st.text_input("Access Key", type="password", value=edit_config.get("access_key", ""))
                        edit_secret_key = st.text_input("Secret Key", type="password", value=edit_config.get("secret_key", ""))
                        edit_prefix = st.text_input("Prefix", value=edit_config.get("prefix", ""))
                        
                        edit_config_data = {
                            "bucket": edit_bucket,
                            "region": edit_region,
                            "access_key": edit_access_key,
                            "secret_key": edit_secret_key,
                            "prefix": edit_prefix
                        }
                    
                    elif edit_source_type == "api":
                        edit_base_url = st.text_input("Base URL", value=edit_config.get("base_url", ""))
                        edit_auth_type = st.selectbox("Auth Type", ["none", "bearer", "basic"], 
                                                   index=["none", "bearer", "basic"].index(edit_config.get("auth_type", "none")))
                        edit_token = st.text_input("Token/Key", type="password", value=edit_config.get("token", ""))
                        
                        edit_config_data = {
                            "base_url": edit_base_url,
                            "auth_type": edit_auth_type
                        }
                        
                        if edit_token:
                            edit_config_data["token"] = edit_token
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    edit_submitted = st.form_submit_button("Update Data Source", type="primary")
                with col2:
                    if st.form_submit_button("Cancel"):
                        if 'edit_source_id' in st.session_state:
                            del st.session_state.edit_source_id
                        if 'edit_source_config' in st.session_state:
                            del st.session_state.edit_source_config
                        st.rerun()
                
                if edit_submitted:
                    try:
                        payload = {
                            "config": edit_config_data,
                            "enabled": edit_enabled,
                            "description": edit_description
                        }
                        
                        response = requests.put(f"{API_ENDPOINTS['acquire']}/datasources/{edit_source_id}", json=payload)
                        
                        if response.status_code == 200:
                            st.success("✅ Data source updated successfully!")
                            # Clear edit state and refresh
                            if 'edit_source_id' in st.session_state:
                                del st.session_state.edit_source_id
                            if 'edit_source_config' in st.session_state:
                                del st.session_state.edit_source_config
                            st.rerun()
                        else:
                            st.error(f"❌ Failed to update data source: {response.text}")
                            
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
    
    with tab2:
        st.subheader("Data Discovery")
        st.write("Discover objects in S3 buckets and tables in databases.")
        
        # Discovery type selection
        discovery_type = st.selectbox(
            "Discovery Type",
            ["S3 Objects", "Database Tables", "SQL Query"]
        )
        
        if discovery_type == "S3 Objects":
            st.subheader("🔍 S3 Object Discovery")
            
            with st.form("s3_discovery"):
                bucket = st.text_input("S3 Bucket", value="kimball-data")
                prefix = st.text_input("Prefix (optional)", value="vehicle_sales_data/")
                max_keys = st.number_input("Max Objects (leave empty for no limit)", value=None, min_value=1, max_value=10000, help="Leave empty to get all objects")
                search_subdirectories = st.checkbox("Search Subdirectories", value=True, help="If unchecked, only search at the current directory level")
                
                if st.form_submit_button("🔍 Discover Objects"):
                    try:
                        payload = {
                            "bucket": bucket,
                            "prefix": prefix,
                            "search_subdirectories": search_subdirectories
                        }
                        
                        # Only add max_keys if it has a value
                        if max_keys:
                            payload["max_keys"] = max_keys
                        
                        response = requests.post(f"{API_ENDPOINTS['acquire']}/discover/s3-objects", json=payload)
                        
                        if response.status_code == 200:
                            data = response.json()
                            st.success(f"✅ Found {data['count']} objects")
                            
                            # Display objects in a table
                            if data['objects']:
                                objects_df = pd.DataFrame(data['objects'])
                                st.dataframe(objects_df, use_container_width=True)
                                
                                # Store objects for extraction
                                st.session_state.s3_objects = data['objects']
                            else:
                                st.info("No objects found")
                        else:
                            st.error(f"❌ Discovery failed: {response.text}")
                            
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
        
        elif discovery_type == "Database Tables":
            st.subheader("🔍 Database Table Discovery")
            
            if "data_sources" in st.session_state and st.session_state.data_sources:
                # Find database sources
                db_sources = {k: v for k, v in st.session_state.data_sources.items() 
                             if v.get('type') in ['postgres', 'mysql', 'clickhouse']}
                
                if db_sources:
                    selected_db = st.selectbox("Select Database Source", list(db_sources.keys()), key="db_discovery_selector")
                    
                    with st.form("db_discovery"):
                        schema = st.text_input("Schema (optional)", value="vehicles")
                        table_pattern = st.text_input("Table Pattern (optional)", placeholder="sales%")
                        
                        if st.form_submit_button("🔍 Discover Tables"):
                            try:
                                payload = {
                                    "schema_name": schema if schema else None,
                                    "table_pattern": table_pattern if table_pattern else None
                                }
                                
                                response = requests.post(f"{API_ENDPOINTS['acquire']}/discover/database-tables/{selected_db}", json=payload)
                                
                                if response.status_code == 200:
                                    data = response.json()
                                    st.success(f"✅ Found {data['count']} tables")
                                    
                                    # Display tables
                                    if data['tables']:
                                        tables_df = pd.DataFrame(data['tables'])
                                        st.dataframe(tables_df, use_container_width=True)
                                        
                                        # Store tables for extraction
                                        st.session_state.db_tables = data['tables']
                                    else:
                                        st.info("No tables found")
                                else:
                                    st.error(f"❌ Discovery failed: {response.text}")
                                    
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
                else:
                    st.info("No database sources available. Add a database source first.")
            else:
                st.info("No data sources available. Add a data source first.")
        
        elif discovery_type == "SQL Query":
            st.subheader("🔍 SQL Query Execution")
            
            if "data_sources" in st.session_state and st.session_state.data_sources:
                # Find database sources
                db_sources = {k: v for k, v in st.session_state.data_sources.items() 
                             if v.get('type') in ['postgres', 'mysql', 'clickhouse']}
                
                if db_sources:
                    selected_db = st.selectbox("Select Database Source", list(db_sources.keys()), key="sql_query_selector")
                    
                    with st.form("sql_query"):
                        query = st.text_area("SQL Query", value="SELECT * FROM vehicles.sales LIMIT 10", height=100)
                        limit = st.number_input("Limit (optional)", value=10, min_value=1, max_value=1000)
                        
                        if st.form_submit_button("🔍 Execute Query"):
                            try:
                                payload = {
                                    "query": query,
                                    "limit": limit
                                }
                                
                                response = requests.post(f"{API_ENDPOINTS['acquire']}/execute/sql-query/{selected_db}", json=payload)
                                
                                if response.status_code == 200:
                                    data = response.json()
                                    st.success(f"✅ Query executed successfully")
                                    st.write(f"**Query:** {data['query']}")
                                    st.write(f"**Rows returned:** {data['row_count']}")
                                    
                                    # Display results
                                    if data['result']:
                                        results_df = pd.DataFrame(data['result'])
                                        st.dataframe(results_df, use_container_width=True)
                                        
                                        # Store results for extraction
                                        st.session_state.query_results = data['result']
                                    else:
                                        st.info("No results returned")
                                else:
                                    st.error(f"❌ Query execution failed: {response.text}")
                                    
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
                else:
                    st.info("No database sources available. Add a database source first.")
            else:
                st.info("No data sources available. Add a data source first.")

    with tab3:
        st.subheader("Connection Management")
        
        if "data_sources" in st.session_state and st.session_state.data_sources:
            selected_source = st.selectbox(
                "Select Data Source",
                list(st.session_state.data_sources.keys())
            )
            
            if selected_source:
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🔌 Connect", key="connect_source"):
                        with st.spinner("Connecting..."):
                            try:
                                response = requests.post(f"{API_ENDPOINTS['acquire']}/connect/{selected_source}")
                                if response.status_code == 200:
                                    result = response.json()
                                    st.success("✅ Connected successfully")
                                    st.json(result)
                                else:
                                    st.error(f"❌ Connection failed: {response.text}")
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
                
                with col2:
                    if st.button("🔌 Disconnect", key="disconnect_source"):
                        with st.spinner("Disconnecting..."):
                            try:
                                response = requests.post(f"{API_ENDPOINTS['acquire']}/disconnect/{selected_source}")
                                if response.status_code == 200:
                                    result = response.json()
                                    st.success("✅ Disconnected successfully")
                                    st.json(result)
                                else:
                                    st.error(f"❌ Disconnect failed: {response.text}")
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
        else:
            st.info("No data sources available. Add a data source first.")
    
    with tab3:
        st.subheader("Data Extraction")
        
        if "data_sources" in st.session_state and st.session_state.data_sources:
            selected_source = st.selectbox(
                "Select Source for Extraction",
                list(st.session_state.data_sources.keys()),
                key="extract_source"
            )
            
            if selected_source:
                source_config = st.session_state.data_sources[selected_source]
                
                if source_config.get("type") == "postgres":
                    st.write("**PostgreSQL Extraction**")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        table_name = st.text_input("Table Name", placeholder="users")
                        schema_name = st.text_input("Schema", value=source_config.get("schema", "public"))
                    with col2:
                        batch_size = st.number_input("Batch Size", value=1000, min_value=1, max_value=10000)
                        limit = st.number_input("Record Limit (0 = all)", value=0, min_value=0)
                    
                    if st.button("📥 Extract Data", key="extract_postgres"):
                        with st.spinner("Extracting data..."):
                            try:
                                payload = {
                                    "extraction_config": {
                                        "table_name": table_name,
                                        "schema_name": schema_name,
                                        "limit": limit if limit > 0 else None
                                    },
                                    "batch_size": batch_size
                                }
                                
                                response = requests.post(f"{API_ENDPOINTS['acquire']}/extract/{selected_source}", json=payload)
                                
                                if response.status_code == 200:
                                    result = response.json()
                                    st.success("✅ Data extracted successfully!")
                                    
                                    # Display extraction results
                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.metric("Records Extracted", result.get("record_count", 0))
                                    with col2:
                                        st.metric("Columns", len(result.get("columns", [])))
                                    with col3:
                                        st.metric("Extraction ID", result.get("extraction_id", "N/A"))
                                    
                                    # Store extraction ID for loading
                                    st.session_state.extraction_id = result.get("extraction_id")
                                    
                                    # Show sample data
                                    if result.get("sample_data"):
                                        st.subheader("Sample Data")
                                        sample_df = pd.DataFrame(result["sample_data"])
                                        st.dataframe(sample_df.head(10))
                                
                                else:
                                    st.error(f"❌ Extraction failed: {response.text}")
                                    
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
                
                elif source_config.get("type") == "s3":
                    st.write("**S3 Extraction**")
                    
                    # List files in S3
                    if st.button("📁 List Files", key="list_s3_files"):
                        with st.spinner("Listing files..."):
                            try:
                                response = requests.get(f"{API_ENDPOINTS['acquire']}/sources/{selected_source}/files")
                                if response.status_code == 200:
                                    files_data = response.json()
                                    files = files_data.get("files", [])
                                    
                                    if files:
                                        st.success(f"Found {len(files)} files")
                                        
                                        # Show files in a selectbox
                                        selected_file = st.selectbox("Select File", files)
                                        
                                        if selected_file and st.button("📥 Extract File", key="extract_s3_file"):
                                            with st.spinner("Extracting file..."):
                                                try:
                                                    payload = {
                                                        "file_pattern": selected_file,
                                                        "extraction_config": {}
                                                    }
                                                    
                                                    response = requests.post(f"{API_ENDPOINTS['acquire']}/extract-file/{selected_source}", json=payload)
                                                    
                                                    if response.status_code == 200:
                                                        result = response.json()
                                                        st.success("✅ File extracted successfully!")
                                                        
                                                        # Display extraction results
                                                        col1, col2, col3 = st.columns(3)
                                                        with col1:
                                                            st.metric("Records Extracted", result.get("record_count", 0))
                                                        with col2:
                                                            st.metric("Columns", len(result.get("columns", [])))
                                                        with col3:
                                                            st.metric("File", selected_file)
                                                        
                                                        # Store extraction ID for loading
                                                        st.session_state.extraction_id = result.get("extraction_id")
                                                        
                                                        # Show sample data
                                                        if result.get("sample_data"):
                                                            st.subheader("Sample Data")
                                                            sample_df = pd.DataFrame(result["sample_data"])
                                                            st.dataframe(sample_df.head(10))
                                                    
                                                    else:
                                                        st.error(f"❌ File extraction failed: {response.text}")
                                                        
                                                except Exception as e:
                                                    st.error(f"❌ Error: {str(e)}")
                                    else:
                                        st.info("No files found in S3 bucket")
                                
                                else:
                                    st.error(f"❌ Failed to list files: {response.text}")
                                    
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
        else:
            st.info("No data sources available. Add a data source first.")
    
    with tab4:
        st.subheader("Data Extraction & Loading")
        st.write("Extract data from discovered sources and load to bronze layer.")
        
        # Check for available data sources
        if "data_sources" in st.session_state and st.session_state.data_sources:
            # Source selection
            selected_source = st.selectbox(
                "Select Data Source",
                list(st.session_state.data_sources.keys()),
                key="extract_source_selector"
            )
            
            if selected_source:
                source_config = st.session_state.data_sources[selected_source]
                st.write(f"**Source Type:** {source_config.get('type', 'unknown')}")
                
                # Extraction type selection
                extraction_type = st.selectbox(
                    "Extraction Type",
                    ["S3 Objects", "SQL Query", "Table Data"],
                    key="extraction_type_selector"
                )
                
                target_table = st.text_input("Target Table Name", placeholder="bronze_sales_data")
                
                if extraction_type == "S3 Objects":
                    st.subheader("📁 S3 Object Extraction")
                    
                    if "s3_objects" in st.session_state and st.session_state.s3_objects:
                        st.write(f"**Available Objects:** {len(st.session_state.s3_objects)}")
                        
                        # Object selection
                        selected_objects = st.multiselect(
                            "Select Objects to Extract",
                            [obj['key'] for obj in st.session_state.s3_objects],
                            default=[obj['key'] for obj in st.session_state.s3_objects[:3]]  # Select first 3 by default
                        )
                        
                        if selected_objects and target_table:
                            if st.button("📤 Extract & Load to Bronze", key="extract_s3"):
                                with st.spinner("Extracting and loading data..."):
                                    try:
                                        payload = {
                                            "source_id": selected_source,
                                            "table_name": target_table,
                                            "extraction_type": "s3_objects",
                                            "extraction_config": {
                                                "objects": selected_objects
                                            },
                                            "target_table": target_table
                                        }
                                        
                                        response = requests.post(f"{API_ENDPOINTS['acquire']}/extract-data", json=payload)
                                        
                                        if response.status_code == 200:
                                            result = response.json()
                                            st.success("✅ Data extracted and loaded successfully!")
                                            
                                            # Display results
                                            col1, col2, col3 = st.columns(3)
                                            with col1:
                                                st.metric("Records Extracted", result.get("records_extracted", 0))
                                            with col2:
                                                st.metric("Records Loaded", result.get("records_loaded", 0))
                                            with col3:
                                                st.metric("Target Table", result.get("target_table", "N/A"))
                                            
                                            st.write(f"**Load ID:** {result.get('load_id', 'N/A')}")
                                        else:
                                            st.error(f"❌ Extraction failed: {response.text}")
                                            
                                    except Exception as e:
                                        st.error(f"❌ Error: {str(e)}")
                    else:
                        st.info("No S3 objects available. Use the Discover tab to find S3 objects first.")
                
                elif extraction_type == "SQL Query":
                    st.subheader("🔍 SQL Query Extraction")
                    
                    query = st.text_area("SQL Query", value="SELECT * FROM vehicles.sales LIMIT 100", height=100)
                    
                    if query and target_table:
                        if st.button("📤 Extract & Load to Bronze", key="extract_sql"):
                            with st.spinner("Executing query and loading data..."):
                                try:
                                    payload = {
                                        "source_id": selected_source,
                                        "table_name": target_table,
                                        "extraction_type": "sql_query",
                                        "extraction_config": {
                                            "query": query
                                        },
                                        "target_table": target_table
                                    }
                                    
                                    response = requests.post(f"{API_ENDPOINTS['acquire']}/extract-data", json=payload)
                                    
                                    if response.status_code == 200:
                                        result = response.json()
                                        st.success("✅ Data extracted and loaded successfully!")
                                        
                                        # Display results
                                        col1, col2, col3 = st.columns(3)
                                        with col1:
                                            st.metric("Records Extracted", result.get("records_extracted", 0))
                                        with col2:
                                            st.metric("Records Loaded", result.get("records_loaded", 0))
                                        with col3:
                                            st.metric("Target Table", result.get("target_table", "N/A"))
                                        
                                        st.write(f"**Load ID:** {result.get('load_id', 'N/A')}")
                                    else:
                                        st.error(f"❌ Extraction failed: {response.text}")
                                        
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
                
                elif extraction_type == "Table Data":
                    st.subheader("📊 Table Data Extraction")
                    
                    if "db_tables" in st.session_state and st.session_state.db_tables:
                        selected_table = st.selectbox(
                            "Select Table",
                            [table['name'] for table in st.session_state.db_tables],
                            key="table_selector"
                        )
                        
                        if selected_table and target_table:
                            if st.button("📤 Extract & Load to Bronze", key="extract_table"):
                                with st.spinner("Extracting table data and loading..."):
                                    try:
                                        payload = {
                                            "source_id": selected_source,
                                            "table_name": target_table,
                                            "extraction_type": "table_data",
                                            "extraction_config": {
                                                "table_name": selected_table
                                            },
                                            "target_table": target_table
                                        }
                                        
                                        response = requests.post(f"{API_ENDPOINTS['acquire']}/extract-data", json=payload)
                                        
                                        if response.status_code == 200:
                                            result = response.json()
                                            st.success("✅ Data extracted and loaded successfully!")
                                            
                                            # Display results
                                            col1, col2, col3 = st.columns(3)
                                            with col1:
                                                st.metric("Records Extracted", result.get("records_extracted", 0))
                                            with col2:
                                                st.metric("Records Loaded", result.get("records_loaded", 0))
                                            with col3:
                                                st.metric("Target Table", result.get("target_table", "N/A"))
                                            
                                            st.write(f"**Load ID:** {result.get('load_id', 'N/A')}")
                                        else:
                                            st.error(f"❌ Extraction failed: {response.text}")
                                            
                                    except Exception as e:
                                        st.error(f"❌ Error: {str(e)}")
                    else:
                        st.info("No database tables available. Use the Discover tab to find tables first.")
        else:
            st.info("No data sources available. Add a data source first.")
        
        # Full pipeline option
        st.subheader("Full Pipeline")
        
        if "data_sources" in st.session_state and st.session_state.data_sources:
            pipeline_source = st.selectbox(
                "Select Source for Full Pipeline",
                list(st.session_state.data_sources.keys()),
                key="pipeline_source"
            )
            
            if pipeline_source and st.button("🚀 Run Full Pipeline", key="run_pipeline"):
                with st.spinner("Running full pipeline (connect → extract → load)..."):
                    try:
                        response = requests.post(f"{API_ENDPOINTS['acquire']}/full-pipeline/{pipeline_source}")
                        
                        if response.status_code == 200:
                            result = response.json()
                            st.success("✅ Full pipeline completed successfully!")
                            
                            # Display pipeline results
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Records Processed", result.get("records_processed", 0))
                            with col2:
                                st.metric("Pipeline Status", result.get("status", "N/A"))
                            with col3:
                                st.metric("Target Table", result.get("target_table", "N/A"))
                        
                        else:
                            st.error(f"❌ Pipeline failed: {response.text}")
                            
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")

def discover_phase():
    """Discover phase interface."""
    st.header("🔍 Discover Phase")
    st.write("Analyze your bronze schema and discover metadata, relationships, and data quality issues.")
    
    # Discover phase tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Schema Analysis", "Quality Assessment", "Relationships", "Catalogs"])
    
    with tab1:
        st.subheader("Schema Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            schema_name = st.text_input("Schema Name", value="bronze")
            include_quality = st.checkbox("Include Quality Analysis", value=True)
            include_relationships = st.checkbox("Include Relationship Analysis", value=True)
            include_hierarchies = st.checkbox("Include Hierarchy Analysis", value=True)
        
        with col2:
            if st.button("Start Analysis", type="primary"):
                with st.spinner("Analyzing schema..."):
                    try:
                        # Call discover API
                        response = requests.post(
                            f"{API_ENDPOINTS['discover']}/analyze",
                            json={
                                "schema_name": schema_name,
                                "include_quality": include_quality,
                                "include_relationships": include_relationships,
                                "include_hierarchies": include_hierarchies
                            }
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            st.success("Analysis completed successfully!")
                            
                            # Display results
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Tables", result["total_tables"])
                            with col2:
                                st.metric("Total Columns", result["total_columns"])
                            with col3:
                                st.metric("Fact Columns", result["fact_columns"])
                            with col4:
                                st.metric("Dimension Columns", result["dimension_columns"])
                            
                            # Store catalog ID in session state
                            st.session_state.catalog_id = result["catalog_id"]
                            
                        else:
                            st.error(f"Analysis failed: {response.text}")
                            
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    with tab2:
        st.subheader("Data Quality Assessment")
        
        if "catalog_id" in st.session_state:
            if st.button("Assess Quality"):
                with st.spinner("Assessing data quality..."):
                    try:
                        response = requests.post(
                            f"{API_ENDPOINTS['discover']}/quality",
                            params={"catalog_id": st.session_state.catalog_id}
                        )
                        
                        if response.status_code == 200:
                            quality_report = response.json()
                            
                            # Display quality metrics
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Overall Score", f"{quality_report['overall_score']:.2f}")
                            with col2:
                                st.metric("High Quality Tables", quality_report["high_quality_tables"])
                            with col3:
                                st.metric("Low Quality Tables", quality_report["low_quality_tables"])
                            
                            # Display issues
                            if quality_report["issues"]:
                                st.subheader("Quality Issues")
                                issues_df = pd.DataFrame(quality_report["issues"])
                                st.dataframe(issues_df)
                            
                            # Display recommendations
                            if quality_report["recommendations"]:
                                st.subheader("Recommendations")
                                for rec in quality_report["recommendations"]:
                                    with st.expander(f"{rec['title']} (Priority: {rec['priority']})"):
                                        st.write(rec["description"])
                                        st.write("Actions:")
                                        for action in rec["actions"]:
                                            st.write(f"• {action}")
                        
                        else:
                            st.error(f"Quality assessment failed: {response.text}")
                            
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        else:
            st.info("Please run schema analysis first to get a catalog ID.")
    
    with tab3:
        st.subheader("Relationship Discovery")
        
        if "catalog_id" in st.session_state:
            if st.button("Find Relationships"):
                with st.spinner("Discovering relationships..."):
                    try:
                        response = requests.post(
                            f"{API_ENDPOINTS['discover']}/relationships",
                            params={"catalog_id": st.session_state.catalog_id}
                        )
                        
                        if response.status_code == 200:
                            rel_report = response.json()
                            
                            # Display relationship metrics
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Relationships", rel_report["total_relationships"])
                            with col2:
                                st.metric("High Confidence Joins", rel_report["high_confidence_joins"])
                            with col3:
                                st.metric("Primary Key Candidates", rel_report["primary_key_candidates"])
                            
                            # Display relationships
                            if rel_report["relationships"]:
                                st.subheader("Discovered Relationships")
                                rel_df = pd.DataFrame(rel_report["relationships"])
                                st.dataframe(rel_df)
                        
                        else:
                            st.error(f"Relationship discovery failed: {response.text}")
                            
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        else:
            st.info("Please run schema analysis first to get a catalog ID.")
    
    with tab4:
        st.subheader("Available Catalogs")
        
        if st.button("Refresh Catalogs"):
            try:
                response = requests.get(f"{API_ENDPOINTS['discover']}/catalogs")
                
                if response.status_code == 200:
                    catalogs_data = response.json()
                    
                    if catalogs_data["catalogs"]:
                        st.subheader("Available Catalogs")
                        catalogs_df = pd.DataFrame(catalogs_data["catalogs"])
                        st.dataframe(catalogs_df)
                        
                        # Allow selection of catalog
                        selected_catalog = st.selectbox(
                            "Select Catalog",
                            [cat["catalog_id"] for cat in catalogs_data["catalogs"]],
                            key="catalog_selector"
                        )
                        
                        if st.button("Load Catalog"):
                            st.session_state.catalog_id = selected_catalog
                            st.success(f"Loaded catalog: {selected_catalog}")
                    else:
                        st.info("No catalogs available. Run schema analysis first.")
                
                else:
                    st.error(f"Failed to load catalogs: {response.text}")
                    
            except Exception as e:
                st.error(f"Error: {str(e)}")

def model_phase():
    """Model phase interface."""
    st.header("🏗️ Model Phase")
    st.write("Design your data warehouse model with ERDs, hierarchies, and star schemas.")
    
    st.info("Model phase functionality coming soon...")
    
    # Placeholder for model phase functionality
    st.subheader("ERD Generation")
    st.write("Generate and edit Entity Relationship Diagrams")
    
    st.subheader("Hierarchy Modeling")
    st.write("Discover and model dimensional hierarchies")
    
    st.subheader("Star Schema Design")
    st.write("Design fact and dimension tables for your data warehouse")

def build_phase():
    """Build phase interface."""
    st.header("🚀 Build Phase")
    st.write("Generate production data pipelines and DAGs.")
    
    st.info("Build phase functionality coming soon...")
    
    # Placeholder for build phase functionality
    st.subheader("DAG Generation")
    st.write("Generate Apache Airflow DAGs for data pipelines")
    
    st.subheader("SQL Generation")
    st.write("Generate transformation SQL for your data warehouse")
    
    st.subheader("Pipeline Orchestration")
    st.write("Schedule and monitor your data pipelines")

def status_page():
    """Status and monitoring page."""
    st.header("📊 System Status")
    st.write("Monitor the health and status of the KIMBALL platform.")
    
    # API Health Check
    st.subheader("API Health Check")
    
    if st.button("Check API Status"):
        try:
            response = requests.get(f"{API_BASE_URL}/health")
            
            if response.status_code == 200:
                st.success("✅ API is healthy")
                health_data = response.json()
                st.json(health_data)
            else:
                st.error("❌ API is not responding")
                
        except Exception as e:
            st.error(f"❌ API connection failed: {str(e)}")
    
    # System Information
    st.subheader("System Information")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Current Time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        st.metric("API Base URL", API_BASE_URL)
    
    with col2:
        st.metric("Available Phases", "4")
        st.metric("API Endpoints", "12")

if __name__ == "__main__":
    main()
