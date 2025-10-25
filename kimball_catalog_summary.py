"""
Summary Report Generator for Metadata Catalog

This script generates a comprehensive summary report from the metadata catalog JSON file.
"""

import json
import pandas as pd
from typing import Dict, Any, List


def load_catalog(filename: str = "metadata_catalog.json") -> Dict[str, Any]:
    """Load the metadata catalog from JSON file."""
    with open(filename, 'r') as f:
        return json.load(f)


def generate_summary_report(catalog: Dict[str, Any]) -> None:
    """Generate a comprehensive summary report."""
    
    print("=" * 80)
    print("BRONZE SCHEMA METADATA CATALOG SUMMARY REPORT")
    print("=" * 80)
    
    # Schema Overview
    print(f"\nSCHEMA OVERVIEW:")
    print(f"  Schema Name: {catalog['schema_name']}")
    print(f"  Analysis Time: {catalog['analysis_timestamp']}")
    print(f"  Total Tables: {catalog['total_tables']}")
    
    schema_summary = catalog.get('schema_summary', {})
    print(f"  Total Columns: {schema_summary.get('total_columns', 0)}")
    print(f"  Fact Columns: {schema_summary.get('total_fact_columns', 0)}")
    print(f"  Dimension Columns: {schema_summary.get('total_dimension_columns', 0)}")
    print(f"  Fact/Dimension Ratio: {schema_summary.get('fact_dimension_ratio', 0):.2f}")
    
    # Table Analysis
    print(f"\nTABLE ANALYSIS:")
    print("-" * 80)
    
    table_summaries = []
    for table_name, table_data in catalog['tables'].items():
        if 'error' in table_data:
            continue
            
        summary = {
            'table_name': table_name,
            'row_count': table_data.get('row_count', 0),
            'column_count': table_data.get('column_count', 0),
            'fact_columns': len(table_data.get('fact_columns', [])),
            'dimension_columns': len(table_data.get('dimension_columns', [])),
            'data_quality': table_data.get('summary', {}).get('avg_data_quality', 0),
            'primary_key_candidates': table_data.get('summary', {}).get('primary_key_candidates', 0)
        }
        table_summaries.append(summary)
    
    # Sort by row count
    table_summaries.sort(key=lambda x: x['row_count'], reverse=True)
    
    print(f"{'Table Name':<25} {'Rows':<12} {'Cols':<6} {'Facts':<6} {'Dims':<6} {'Quality':<8} {'PK Cand':<8}")
    print("-" * 80)
    
    for summary in table_summaries:
        print(f"{summary['table_name']:<25} {summary['row_count']:<12,} {summary['column_count']:<6} "
              f"{summary['fact_columns']:<6} {summary['dimension_columns']:<6} "
              f"{summary['data_quality']:<8.2f} {summary['primary_key_candidates']:<8}")
    
    # Fact vs Dimension Analysis
    print(f"\nFACT vs DIMENSION ANALYSIS:")
    print("-" * 80)
    
    all_fact_columns = []
    all_dimension_columns = []
    
    for table_name, table_data in catalog['tables'].items():
        if 'error' in table_data:
            continue
            
        for column in table_data.get('columns', []):
            col_info = {
                'table': table_name,
                'column': column['name'],
                'type': column['type'],
                'cardinality': column['cardinality'],
                'null_percentage': column['null_percentage'],
                'quality_score': column['data_quality_score']
            }
            
            if column['classification'] == 'fact':
                all_fact_columns.append(col_info)
            else:
                all_dimension_columns.append(col_info)
    
    print(f"\nFACT COLUMNS ({len(all_fact_columns)} total):")
    print("-" * 40)
    for col in sorted(all_fact_columns, key=lambda x: x['quality_score'], reverse=True)[:10]:
        print(f"  {col['table']}.{col['column']:<30} {col['type']:<20} Quality: {col['quality_score']:.2f}")
    
    print(f"\nDIMENSION COLUMNS ({len(all_dimension_columns)} total):")
    print("-" * 40)
    for col in sorted(all_dimension_columns, key=lambda x: x['quality_score'], reverse=True)[:10]:
        print(f"  {col['table']}.{col['column']:<30} {col['type']:<20} Quality: {col['quality_score']:.2f}")
    
    # Data Quality Analysis
    print(f"\nDATA QUALITY ANALYSIS:")
    print("-" * 80)
    
    high_quality_tables = [s for s in table_summaries if s['data_quality'] > 0.8]
    medium_quality_tables = [s for s in table_summaries if 0.5 <= s['data_quality'] <= 0.8]
    low_quality_tables = [s for s in table_summaries if s['data_quality'] < 0.5]
    
    print(f"  High Quality Tables (>0.8): {len(high_quality_tables)}")
    print(f"  Medium Quality Tables (0.5-0.8): {len(medium_quality_tables)}")
    print(f"  Low Quality Tables (<0.5): {len(low_quality_tables)}")
    
    # Column Type Distribution
    print(f"\nCOLUMN TYPE DISTRIBUTION:")
    print("-" * 80)
    
    type_counts = {}
    for table_data in catalog['tables'].values():
        if 'error' in table_data:
            continue
        for column in table_data.get('columns', []):
            col_type = column['type']
            type_counts[col_type] = type_counts.get(col_type, 0) + 1
    
    for col_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {col_type:<30} {count:>4} columns")
    
    # Recommendations
    print(f"\nRECOMMENDATIONS:")
    print("-" * 80)
    
    print("1. FACT COLUMNS (Measures for Analysis):")
    fact_cols = [col for col in all_fact_columns if col['quality_score'] > 0.7]
    for col in fact_cols[:5]:
        print(f"   - {col['table']}.{col['column']} ({col['type']}) - Quality: {col['quality_score']:.2f}")
    
    print("\n2. DIMENSION COLUMNS (Grouping Attributes):")
    dim_cols = [col for col in all_dimension_columns if col['quality_score'] > 0.7]
    for col in dim_cols[:5]:
        print(f"   - {col['table']}.{col['column']} ({col['type']}) - Quality: {col['quality_score']:.2f}")
    
    print("\n3. DATA QUALITY ISSUES:")
    low_quality_cols = [col for col in all_fact_columns + all_dimension_columns if col['quality_score'] < 0.5]
    for col in low_quality_cols[:5]:
        print(f"   - {col['table']}.{col['column']} - Quality: {col['quality_score']:.2f} (Nulls: {col['null_percentage']:.1f}%)")


def main():
    """Main function to generate the summary report."""
    try:
        catalog = load_catalog()
        generate_summary_report(catalog)
    except FileNotFoundError:
        print("Error: metadata_catalog.json not found. Please run metadata_catalog.py first.")
    except Exception as e:
        print(f"Error generating report: {str(e)}")


if __name__ == "__main__":
    main()
