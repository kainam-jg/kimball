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
    "discover": f"{API_BASE_URL}/api/v1/discover",
    "model": f"{API_BASE_URL}/api/v1/model",
    "build": f"{API_BASE_URL}/api/v1/build"
}

def main():
    """Main Streamlit application."""
    
    # Header
    st.title("🔍 KIMBALL Platform")
    st.subtitle("Kinetic Intelligent Model Builder with Augmented Learning and Loading")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    phase = st.sidebar.selectbox(
        "Select Phase",
        ["Discover", "Model", "Build", "Status"]
    )
    
    # Main content based on selected phase
    if phase == "Discover":
        discover_phase()
    elif phase == "Model":
        model_phase()
    elif phase == "Build":
        build_phase()
    elif phase == "Status":
        status_page()

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
                            [cat["catalog_id"] for cat in catalogs_data["catalogs"]]
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
