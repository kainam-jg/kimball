"""
Hierarchy Validation Tool

This tool allows users to validate, modify, and manage the discovered
hierarchical relationships in the bronze schema.
"""

import json
import os
from typing import Dict, List, Any
from datetime import datetime


class HierarchyValidator:
    """
    Tool for validating and modifying hierarchical relationships.
    """
    
    def __init__(self, hierarchies_file: str = "hierarchies.json"):
        """Initialize with hierarchies JSON file."""
        self.hierarchies_file = hierarchies_file
        self.hierarchies_data = self.load_hierarchies()
    
    def load_hierarchies(self) -> Dict:
        """Load hierarchies from JSON file."""
        try:
            with open(self.hierarchies_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: {self.hierarchies_file} not found.")
            return {}
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON in {self.hierarchies_file}")
            return {}
    
    def save_hierarchies(self) -> bool:
        """Save hierarchies to JSON file."""
        try:
            with open(self.hierarchies_file, 'w') as f:
                json.dump(self.hierarchies_data, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving hierarchies: {str(e)}")
            return False
    
    def display_hierarchy_summary(self) -> None:
        """Display a summary of all hierarchies."""
        print("=" * 80)
        print("HIERARCHY VALIDATION SUMMARY")
        print("=" * 80)
        
        metadata = self.hierarchies_data.get('metadata', {})
        print(f"Total Hierarchies: {metadata.get('total_hierarchies', 0)}")
        print(f"Total Relationships: {metadata.get('total_relationships', 0)}")
        print(f"Confidence Threshold: {metadata.get('confidence_threshold', 0.7)}")
        
        validation_status = self.hierarchies_data.get('validation_status', {})
        print(f"Validated Hierarchies: {validation_status.get('validated_hierarchies', 0)}")
        print(f"Pending Validation: {validation_status.get('pending_validation', 0)}")
        print(f"Validation Progress: {validation_status.get('validation_progress', '0%')}")
        
        print("\nHIERARCHY LIST:")
        print("-" * 80)
        
        for i, hierarchy in enumerate(self.hierarchies_data.get('hierarchies', []), 1):
            root = hierarchy.get('root', {})
            children = hierarchy.get('children', [])
            validated = hierarchy.get('validated', False)
            status = "✓ VALIDATED" if validated else "⏳ PENDING"
            
            print(f"{i:2d}. {hierarchy.get('name', 'Unknown')}")
            print(f"    Root: {root.get('table', 'Unknown')}.{root.get('column', 'Unknown')}")
            print(f"    Children: {len(children)}")
            print(f"    Status: {status}")
            if hierarchy.get('notes'):
                print(f"    Notes: {hierarchy.get('notes')}")
            print()
    
    def display_hierarchy_details(self, hierarchy_id: str) -> None:
        """Display detailed information about a specific hierarchy."""
        hierarchy = self.find_hierarchy_by_id(hierarchy_id)
        if not hierarchy:
            print(f"Hierarchy {hierarchy_id} not found.")
            return
        
        print("=" * 80)
        print(f"HIERARCHY DETAILS: {hierarchy.get('name', 'Unknown')}")
        print("=" * 80)
        
        root = hierarchy.get('root', {})
        print(f"Root: {root.get('table', 'Unknown')}.{root.get('column', 'Unknown')}")
        print(f"Cardinality: {root.get('cardinality', 0):,}")
        print(f"Confidence: {root.get('confidence', 0):.2f}")
        print(f"Level: {root.get('level', 1)}")
        print(f"Validated: {'Yes' if hierarchy.get('validated', False) else 'No'}")
        
        if hierarchy.get('notes'):
            print(f"Notes: {hierarchy.get('notes')}")
        
        print(f"\nChildren ({len(hierarchy.get('children', []))}):")
        print("-" * 80)
        
        for i, child in enumerate(hierarchy.get('children', []), 1):
            print(f"{i:2d}. {child.get('table', 'Unknown')}.{child.get('column', 'Unknown')}")
            print(f"    Cardinality: {child.get('cardinality', 0):,}")
            print(f"    Confidence: {child.get('confidence', 0):.2f}")
            print(f"    Level: {child.get('level', 2)}")
            print(f"    Type: {child.get('relationship_type', 'unknown')}")
            print(f"    Validated: {'Yes' if child.get('validated', False) else 'No'}")
            if child.get('notes'):
                print(f"    Notes: {child.get('notes')}")
            print()
        
        siblings = hierarchy.get('siblings', [])
        if siblings:
            print(f"Siblings ({len(siblings)}):")
            print("-" * 80)
            for i, sibling in enumerate(siblings, 1):
                print(f"{i:2d}. {sibling.get('table', 'Unknown')}.{sibling.get('column', 'Unknown')}")
                print(f"    Confidence: {sibling.get('confidence', 0):.2f}")
                print(f"    Validated: {'Yes' if sibling.get('validated', False) else 'No'}")
                if sibling.get('notes'):
                    print(f"    Notes: {sibling.get('notes')}")
                print()
    
    def find_hierarchy_by_id(self, hierarchy_id: str) -> Dict:
        """Find a hierarchy by its ID."""
        for hierarchy in self.hierarchies_data.get('hierarchies', []):
            if hierarchy.get('id') == hierarchy_id:
                return hierarchy
        return {}
    
    def validate_hierarchy(self, hierarchy_id: str, validated: bool = True) -> bool:
        """Mark a hierarchy as validated or not."""
        hierarchy = self.find_hierarchy_by_id(hierarchy_id)
        if not hierarchy:
            print(f"Hierarchy {hierarchy_id} not found.")
            return False
        
        hierarchy['validated'] = validated
        hierarchy['validation_date'] = datetime.now().isoformat()
        
        # Update validation status
        self.update_validation_status()
        
        print(f"Hierarchy {hierarchy_id} marked as {'validated' if validated else 'not validated'}.")
        return True
    
    def validate_child(self, hierarchy_id: str, child_index: int, validated: bool = True) -> bool:
        """Mark a child relationship as validated or not."""
        hierarchy = self.find_hierarchy_by_id(hierarchy_id)
        if not hierarchy:
            print(f"Hierarchy {hierarchy_id} not found.")
            return False
        
        children = hierarchy.get('children', [])
        if child_index < 0 or child_index >= len(children):
            print(f"Child index {child_index} out of range.")
            return False
        
        children[child_index]['validated'] = validated
        children[child_index]['validation_date'] = datetime.now().isoformat()
        
        print(f"Child {child_index} in hierarchy {hierarchy_id} marked as {'validated' if validated else 'not validated'}.")
        return True
    
    def add_note(self, hierarchy_id: str, note: str, child_index: int = None) -> bool:
        """Add a note to a hierarchy or child."""
        hierarchy = self.find_hierarchy_by_id(hierarchy_id)
        if not hierarchy:
            print(f"Hierarchy {hierarchy_id} not found.")
            return False
        
        if child_index is not None:
            children = hierarchy.get('children', [])
            if child_index < 0 or child_index >= len(children):
                print(f"Child index {child_index} out of range.")
                return False
            children[child_index]['notes'] = note
            print(f"Note added to child {child_index} in hierarchy {hierarchy_id}.")
        else:
            hierarchy['notes'] = note
            print(f"Note added to hierarchy {hierarchy_id}.")
        
        return True
    
    def update_confidence(self, hierarchy_id: str, child_index: int, new_confidence: float) -> bool:
        """Update confidence score for a child relationship."""
        hierarchy = self.find_hierarchy_by_id(hierarchy_id)
        if not hierarchy:
            print(f"Hierarchy {hierarchy_id} not found.")
            return False
        
        children = hierarchy.get('children', [])
        if child_index < 0 or child_index >= len(children):
            print(f"Child index {child_index} out of range.")
            return False
        
        if not 0.0 <= new_confidence <= 1.0:
            print("Confidence must be between 0.0 and 1.0.")
            return False
        
        children[child_index]['confidence'] = new_confidence
        children[child_index]['modified_date'] = datetime.now().isoformat()
        
        print(f"Confidence updated for child {child_index} in hierarchy {hierarchy_id}.")
        return True
    
    def update_validation_status(self) -> None:
        """Update the overall validation status."""
        hierarchies = self.hierarchies_data.get('hierarchies', [])
        total_hierarchies = len(hierarchies)
        validated_hierarchies = sum(1 for h in hierarchies if h.get('validated', False))
        pending_validation = total_hierarchies - validated_hierarchies
        validation_progress = f"{(validated_hierarchies / total_hierarchies * 100):.1f}%" if total_hierarchies > 0 else "0%"
        
        self.hierarchies_data['validation_status'] = {
            'total_hierarchies': total_hierarchies,
            'validated_hierarchies': validated_hierarchies,
            'pending_validation': pending_validation,
            'validation_progress': validation_progress
        }
    
    def export_validated_hierarchies(self, output_file: str = "validated_hierarchies.json") -> bool:
        """Export only validated hierarchies to a separate file."""
        validated_hierarchies = [
            h for h in self.hierarchies_data.get('hierarchies', [])
            if h.get('validated', False)
        ]
        
        export_data = {
            'metadata': self.hierarchies_data.get('metadata', {}),
            'validated_hierarchies': validated_hierarchies,
            'export_date': datetime.now().isoformat(),
            'total_validated': len(validated_hierarchies)
        }
        
        try:
            with open(output_file, 'w') as f:
                json.dump(export_data, f, indent=2)
            print(f"Validated hierarchies exported to {output_file}")
            return True
        except Exception as e:
            print(f"Error exporting validated hierarchies: {str(e)}")
            return False
    
    def interactive_validation(self) -> None:
        """Interactive validation mode."""
        print("=" * 80)
        print("INTERACTIVE HIERARCHY VALIDATION")
        print("=" * 80)
        
        while True:
            print("\nOptions:")
            print("1. Display hierarchy summary")
            print("2. View hierarchy details")
            print("3. Validate hierarchy")
            print("4. Add note to hierarchy")
            print("5. Update child confidence")
            print("6. Export validated hierarchies")
            print("7. Save and exit")
            print("8. Exit without saving")
            
            choice = input("\nEnter your choice (1-8): ").strip()
            
            if choice == '1':
                self.display_hierarchy_summary()
            elif choice == '2':
                hierarchy_id = input("Enter hierarchy ID: ").strip()
                self.display_hierarchy_details(hierarchy_id)
            elif choice == '3':
                hierarchy_id = input("Enter hierarchy ID: ").strip()
                validated = input("Mark as validated? (y/n): ").strip().lower() == 'y'
                self.validate_hierarchy(hierarchy_id, validated)
            elif choice == '4':
                hierarchy_id = input("Enter hierarchy ID: ").strip()
                child_index = input("Enter child index (or press Enter for hierarchy note): ").strip()
                note = input("Enter note: ").strip()
                
                if child_index:
                    try:
                        child_index = int(child_index)
                        self.add_note(hierarchy_id, note, child_index)
                    except ValueError:
                        print("Invalid child index.")
                else:
                    self.add_note(hierarchy_id, note)
            elif choice == '5':
                hierarchy_id = input("Enter hierarchy ID: ").strip()
                child_index = input("Enter child index: ").strip()
                new_confidence = input("Enter new confidence (0.0-1.0): ").strip()
                
                try:
                    child_index = int(child_index)
                    new_confidence = float(new_confidence)
                    self.update_confidence(hierarchy_id, child_index, new_confidence)
                except ValueError:
                    print("Invalid input.")
            elif choice == '6':
                output_file = input("Enter output filename (or press Enter for default): ").strip()
                if not output_file:
                    output_file = "validated_hierarchies.json"
                self.export_validated_hierarchies(output_file)
            elif choice == '7':
                if self.save_hierarchies():
                    print("Changes saved successfully.")
                break
            elif choice == '8':
                print("Exiting without saving changes.")
                break
            else:
                print("Invalid choice. Please try again.")


def main():
    """Main function to run hierarchy validation."""
    validator = HierarchyValidator()
    
    if not validator.hierarchies_data:
        print("No hierarchies data loaded. Exiting.")
        return
    
    print("KIMBALL Hierarchy Validation Tool")
    print("=" * 40)
    
    # Display initial summary
    validator.display_hierarchy_summary()
    
    # Start interactive validation
    validator.interactive_validation()


if __name__ == "__main__":
    main()
