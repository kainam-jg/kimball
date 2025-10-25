"""
KIMBALL ERD Generator

This module provides ERD generation functionality for the Model phase:
- Entity Relationship Diagram generation
- Interactive ERD editing
- Relationship visualization
- Schema validation
"""

from typing import Dict, List, Any, Optional
import json
import uuid
from datetime import datetime

from ..core.logger import Logger

class ERDGenerator:
    """
    ERD generator for creating and editing Entity Relationship Diagrams.
    """
    
    def __init__(self):
        """Initialize the ERD generator."""
        self.logger = Logger("erd_generator")
        self.erds = {}
    
    def generate_erd(self, catalog_id: str, include_relationships: bool = True, include_attributes: bool = True) -> Dict[str, Any]:
        """
        Generate ERD from catalog metadata.
        
        Args:
            catalog_id (str): ID of the catalog to generate ERD from
            include_relationships (bool): Whether to include relationships
            include_attributes (bool): Whether to include attributes
            
        Returns:
            Dict[str, Any]: Generated ERD data
        """
        try:
            self.logger.info(f"Generating ERD for catalog: {catalog_id}")
            
            # Generate ERD ID
            erd_id = f"erd_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
            
            # Load catalog data (in production, this would load from storage)
            catalog_data = self._load_catalog_data(catalog_id)
            
            if not catalog_data:
                raise ValueError(f"Catalog not found: {catalog_id}")
            
            # Generate entities
            entities = self._generate_entities(catalog_data, include_attributes)
            
            # Generate relationships
            relationships = []
            if include_relationships:
                relationships = self._generate_relationships(catalog_data)
            
            # Create ERD data structure
            erd_data = {
                "erd_id": erd_id,
                "catalog_id": catalog_id,
                "entities": entities,
                "relationships": relationships,
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "total_entities": len(entities),
                    "total_relationships": len(relationships),
                    "include_attributes": include_attributes,
                    "include_relationships": include_relationships
                }
            }
            
            # Store ERD
            self.erds[erd_id] = erd_data
            
            self.logger.info(f"ERD generated successfully: {erd_id}")
            return erd_data
            
        except Exception as e:
            self.logger.error(f"Error generating ERD: {str(e)}")
            raise
    
    def _load_catalog_data(self, catalog_id: str) -> Optional[Dict[str, Any]]:
        """Load catalog data by ID."""
        try:
            # In production, this would load from database or file storage
            # For now, return mock data
            return {
                "schema_name": "bronze",
                "tables": {
                    "table1": {
                        "columns": [
                            {"name": "id", "type": "String", "is_primary_key_candidate": True},
                            {"name": "name", "type": "String", "is_primary_key_candidate": False}
                        ]
                    }
                }
            }
        except Exception as e:
            self.logger.error(f"Error loading catalog data: {str(e)}")
            return None
    
    def _generate_entities(self, catalog_data: Dict[str, Any], include_attributes: bool) -> List[Dict[str, Any]]:
        """Generate entities from catalog data."""
        try:
            entities = []
            
            for table_name, table_data in catalog_data.get("tables", {}).items():
                entity = {
                    "id": f"entity_{table_name}",
                    "name": table_name,
                    "type": "table",
                    "attributes": [],
                    "primary_keys": [],
                    "position": {"x": 0, "y": 0}  # Default position
                }
                
                if include_attributes:
                    for column in table_data.get("columns", []):
                        attribute = {
                            "name": column.get("name", ""),
                            "type": column.get("type", ""),
                            "is_primary_key": column.get("is_primary_key_candidate", False),
                            "nullable": "Nullable" in column.get("type", ""),
                            "cardinality": column.get("cardinality", 0)
                        }
                        entity["attributes"].append(attribute)
                        
                        if attribute["is_primary_key"]:
                            entity["primary_keys"].append(attribute["name"])
                
                entities.append(entity)
            
            return entities
            
        except Exception as e:
            self.logger.error(f"Error generating entities: {str(e)}")
            return []
    
    def _generate_relationships(self, catalog_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate relationships from catalog data."""
        try:
            relationships = []
            
            # This is a simplified relationship generation
            # In production, this would use the relationship finder
            tables = list(catalog_data.get("tables", {}).keys())
            
            for i, table1 in enumerate(tables):
                for table2 in tables[i+1:]:
                    # Check for potential relationships
                    relationship = self._find_potential_relationship(table1, table2, catalog_data)
                    if relationship:
                        relationships.append(relationship)
            
            return relationships
            
        except Exception as e:
            self.logger.error(f"Error generating relationships: {str(e)}")
            return []
    
    def _find_potential_relationship(self, table1: str, table2: str, catalog_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find potential relationship between two tables."""
        try:
            # Simple relationship detection based on column names
            table1_data = catalog_data.get("tables", {}).get(table1, {})
            table2_data = catalog_data.get("tables", {}).get(table2, {})
            
            table1_columns = [col.get("name", "").lower() for col in table1_data.get("columns", [])]
            table2_columns = [col.get("name", "").lower() for col in table2_data.get("columns", [])]
            
            # Look for common column names
            common_columns = set(table1_columns) & set(table2_columns)
            
            if common_columns:
                return {
                    "id": f"rel_{table1}_{table2}",
                    "from_entity": f"entity_{table1}",
                    "to_entity": f"entity_{table2}",
                    "type": "one_to_many",
                    "columns": list(common_columns),
                    "confidence": 0.7
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding relationship: {str(e)}")
            return None
    
    def edit_erd(self, erd_id: str, edits: Dict[str, Any]) -> Dict[str, Any]:
        """
        Edit an existing ERD.
        
        Args:
            erd_id (str): ID of the ERD to edit
            edits (Dict[str, Any]): Edit operations
            
        Returns:
            Dict[str, Any]: Updated ERD data
        """
        try:
            if erd_id not in self.erds:
                raise ValueError(f"ERD not found: {erd_id}")
            
            erd_data = self.erds[erd_id]
            
            # Apply edits
            if "entities" in edits:
                erd_data["entities"] = edits["entities"]
            
            if "relationships" in edits:
                erd_data["relationships"] = edits["relationships"]
            
            # Update metadata
            erd_data["metadata"]["last_modified"] = datetime.now().isoformat()
            erd_data["metadata"]["edit_count"] = erd_data["metadata"].get("edit_count", 0) + 1
            
            self.logger.info(f"ERD edited successfully: {erd_id}")
            return erd_data
            
        except Exception as e:
            self.logger.error(f"Error editing ERD: {str(e)}")
            raise
    
    def validate_erd(self, erd_id: str) -> Dict[str, Any]:
        """
        Validate ERD for consistency and completeness.
        
        Args:
            erd_id (str): ID of the ERD to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        try:
            if erd_id not in self.erds:
                raise ValueError(f"ERD not found: {erd_id}")
            
            erd_data = self.erds[erd_id]
            validation_result = {
                "valid": True,
                "errors": [],
                "warnings": [],
                "suggestions": []
            }
            
            # Validate entities
            entity_ids = {entity["id"] for entity in erd_data["entities"]}
            
            # Validate relationships
            for relationship in erd_data["relationships"]:
                from_entity = relationship.get("from_entity")
                to_entity = relationship.get("to_entity")
                
                if from_entity not in entity_ids:
                    validation_result["errors"].append(f"Relationship references non-existent entity: {from_entity}")
                    validation_result["valid"] = False
                
                if to_entity not in entity_ids:
                    validation_result["errors"].append(f"Relationship references non-existent entity: {to_entity}")
                    validation_result["valid"] = False
            
            # Check for orphaned entities
            referenced_entities = set()
            for relationship in erd_data["relationships"]:
                referenced_entities.add(relationship.get("from_entity"))
                referenced_entities.add(relationship.get("to_entity"))
            
            orphaned_entities = entity_ids - referenced_entities
            if orphaned_entities:
                validation_result["warnings"].append(f"Orphaned entities found: {orphaned_entities}")
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Error validating ERD: {str(e)}")
            raise
    
    def export_erd(self, erd_id: str, format: str = "json") -> str:
        """
        Export ERD in specified format.
        
        Args:
            erd_id (str): ID of the ERD to export
            format (str): Export format (json, mermaid, dot)
            
        Returns:
            str: Exported ERD content
        """
        try:
            if erd_id not in self.erds:
                raise ValueError(f"ERD not found: {erd_id}")
            
            erd_data = self.erds[erd_id]
            
            if format.lower() == "json":
                return json.dumps(erd_data, indent=2, default=str)
            elif format.lower() == "mermaid":
                return self._export_to_mermaid(erd_data)
            elif format.lower() == "dot":
                return self._export_to_dot(erd_data)
            else:
                raise ValueError(f"Unsupported export format: {format}")
                
        except Exception as e:
            self.logger.error(f"Error exporting ERD: {str(e)}")
            raise
    
    def _export_to_mermaid(self, erd_data: Dict[str, Any]) -> str:
        """Export ERD to Mermaid format."""
        try:
            mermaid_lines = ["erDiagram"]
            
            # Add entities
            for entity in erd_data["entities"]:
                mermaid_lines.append(f"    {entity['name']} {{")
                for attr in entity["attributes"]:
                    mermaid_lines.append(f"        {attr['type']} {attr['name']}")
                mermaid_lines.append("    }")
            
            # Add relationships
            for relationship in erd_data["relationships"]:
                from_entity = relationship["from_entity"].replace("entity_", "")
                to_entity = relationship["to_entity"].replace("entity_", "")
                mermaid_lines.append(f"    {from_entity} ||--o{{ {to_entity} : \"{relationship.get('type', 'relates')}\"")
            
            return "\n".join(mermaid_lines)
            
        except Exception as e:
            self.logger.error(f"Error exporting to Mermaid: {str(e)}")
            return ""
    
    def _export_to_dot(self, erd_data: Dict[str, Any]) -> str:
        """Export ERD to DOT format."""
        try:
            dot_lines = ["digraph ERD {"]
            dot_lines.append("    rankdir=LR;")
            dot_lines.append("    node [shape=record];")
            
            # Add entities
            for entity in erd_data["entities"]:
                entity_name = entity["name"]
                attributes = "|".join([attr["name"] for attr in entity["attributes"]])
                dot_lines.append('    {} [label="{{{}|{}}}"];'.format(entity_name, entity_name, attributes))
            
            # Add relationships
            for relationship in erd_data["relationships"]:
                from_entity = relationship["from_entity"].replace("entity_", "")
                to_entity = relationship["to_entity"].replace("entity_", "")
                dot_lines.append(f"    {from_entity} -> {to_entity};")
            
            dot_lines.append("}")
            return "\n".join(dot_lines)
            
        except Exception as e:
            self.logger.error(f"Error exporting to DOT: {str(e)}")
            return ""
    
    def get_erd(self, erd_id: str) -> Optional[Dict[str, Any]]:
        """Get ERD by ID."""
        return self.erds.get(erd_id)
    
    def list_erds(self) -> List[Dict[str, Any]]:
        """List all ERDs."""
        return list(self.erds.values())
