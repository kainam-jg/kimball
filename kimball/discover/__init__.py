"""
KIMBALL Discover Phase

This module handles metadata discovery and catalog generation:
- Table and column analysis
- Data quality assessment
- Primary key identification
- Join relationship discovery
- Fact vs dimension classification

Builds upon existing metadata analysis code to create comprehensive catalogs.
"""

from .metadata_analyzer import MetadataAnalyzer
from .catalog_builder import CatalogBuilder
from .quality_assessor import QualityAssessor
from .relationship_finder import RelationshipFinder

__all__ = [
    'MetadataAnalyzer',
    'CatalogBuilder', 
    'QualityAssessor',
    'RelationshipFinder'
]
