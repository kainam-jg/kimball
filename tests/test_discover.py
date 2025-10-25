"""
Tests for KIMBALL Discover Phase

This module contains tests for the discover phase functionality.
"""

import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime

from kimball.discover.metadata_analyzer import MetadataAnalyzer
from kimball.discover.catalog_builder import CatalogBuilder
from kimball.discover.quality_assessor import QualityAssessor
from kimball.discover.relationship_finder import RelationshipFinder

class TestMetadataAnalyzer:
    """Test cases for MetadataAnalyzer."""
    
    def test_initialization(self):
        """Test MetadataAnalyzer initialization."""
        analyzer = MetadataAnalyzer()
        assert analyzer is not None
        assert analyzer.catalog == {}
        assert analyzer.fact_columns == []
        assert analyzer.dimension_columns == []
    
    @patch('kimball.discover.metadata_analyzer.get_clickhouse_connection')
    def test_connect_success(self, mock_conn):
        """Test successful connection."""
        mock_conn.return_value.connect.return_value = True
        
        analyzer = MetadataAnalyzer()
        result = analyzer.connect()
        
        assert result is True
        mock_conn.return_value.connect.assert_called_once()
    
    @patch('kimball.discover.metadata_analyzer.get_clickhouse_connection')
    def test_connect_failure(self, mock_conn):
        """Test connection failure."""
        mock_conn.return_value.connect.return_value = False
        
        analyzer = MetadataAnalyzer()
        result = analyzer.connect()
        
        assert result is False
    
    def test_classify_column_fact(self):
        """Test column classification as fact."""
        analyzer = MetadataAnalyzer()
        
        # High cardinality numeric column should be fact
        classification = analyzer._classify_column(
            "sales_amount", "Float64", 1000, [100.0, 200.0, 300.0]
        )
        assert classification == "fact"
    
    def test_classify_column_dimension(self):
        """Test column classification as dimension."""
        analyzer = MetadataAnalyzer()
        
        # String column should be dimension
        classification = analyzer._classify_column(
            "product_name", "String", 50, ["Product A", "Product B"]
        )
        assert classification == "dimension"
    
    def test_calculate_data_quality_score(self):
        """Test data quality score calculation."""
        analyzer = MetadataAnalyzer()
        
        # Test with good data
        score = analyzer._calculate_data_quality_score(0, 1000)
        assert score > 0.5
        
        # Test with poor data
        score = analyzer._calculate_data_quality_score(500, 10)
        assert score < 0.5

class TestCatalogBuilder:
    """Test cases for CatalogBuilder."""
    
    def test_initialization(self):
        """Test CatalogBuilder initialization."""
        builder = CatalogBuilder()
        assert builder is not None
    
    def test_generate_summary(self):
        """Test catalog summary generation."""
        builder = CatalogBuilder()
        
        # Mock catalog data
        catalog = {
            "schema_name": "test_schema",
            "analysis_timestamp": datetime.now().isoformat(),
            "total_tables": 2,
            "tables": {
                "table1": {
                    "row_count": 100,
                    "column_count": 5,
                    "fact_columns": ["amount"],
                    "dimension_columns": ["name", "category"],
                    "columns": [
                        {"name": "amount", "classification": "fact", "data_quality_score": 0.8},
                        {"name": "name", "classification": "dimension", "data_quality_score": 0.9}
                    ]
                }
            }
        }
        
        summary = builder.generate_summary(catalog)
        
        assert summary["schema_name"] == "test_schema"
        assert summary["total_tables"] == 2
        assert len(summary["table_summaries"]) == 1
        assert len(summary["fact_columns"]) == 1
        assert len(summary["dimension_columns"]) == 1

class TestQualityAssessor:
    """Test cases for QualityAssessor."""
    
    def test_initialization(self):
        """Test QualityAssessor initialization."""
        assessor = QualityAssessor()
        assert assessor is not None
    
    def test_assess_catalog_quality(self):
        """Test catalog quality assessment."""
        assessor = QualityAssessor()
        
        # Mock catalog data
        catalog = {
            "tables": {
                "table1": {
                    "columns": [
                        {
                            "name": "col1",
                            "data_quality_score": 0.9,
                            "null_percentage": 5.0,
                            "cardinality": 1000,
                            "classification": "fact"
                        }
                    ]
                }
            }
        }
        
        quality_report = assessor.assess_catalog_quality(catalog)
        
        assert "overall_score" in quality_report
        assert "high_quality_tables" in quality_report
        assert "issues" in quality_report
        assert "recommendations" in quality_report

class TestRelationshipFinder:
    """Test cases for RelationshipFinder."""
    
    def test_initialization(self):
        """Test RelationshipFinder initialization."""
        finder = RelationshipFinder()
        assert finder is not None
    
    def test_are_types_compatible(self):
        """Test type compatibility checking."""
        finder = RelationshipFinder()
        
        # Compatible types
        assert finder._are_types_compatible("String", "String") is True
        assert finder._are_types_compatible("Int32", "Int64") is True
        
        # Incompatible types
        assert finder._are_types_compatible("String", "Int32") is False
    
    def test_calculate_join_confidence(self):
        """Test join confidence calculation."""
        finder = RelationshipFinder()
        
        col1 = {
            "type": "String",
            "cardinality": 1000,
            "is_pk_candidate": True
        }
        col2 = {
            "type": "String", 
            "cardinality": 1000,
            "is_pk_candidate": True
        }
        
        confidence = finder._calculate_join_confidence(col1, col2)
        assert 0.0 <= confidence <= 1.0
        assert confidence > 0.5  # Should be high confidence for matching PKs

if __name__ == "__main__":
    pytest.main([__file__])
