"""
Script to extract DDL statements for all tables in the metadata schema
and save them to the /sql directory.

This script connects directly to ClickHouse without using Logger to avoid circular dependencies.
"""

import os
import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Direct ClickHouse connection without using our modules
import clickhouse_connect


def get_clickhouse_config():
    """Load ClickHouse config from config.json"""
    config_file = Path(__file__).parent.parent / "config.json"
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        ch_config = config.get('clickhouse', {})
        print(f"Loaded config: host={ch_config.get('host')}, port={ch_config.get('port')}")
        return ch_config
    except Exception as e:
        print(f"Error loading config: {e}")
        return {}


def extract_table_ddl(client, table_name: str) -> str:
    """Extract DDL for a specific table."""
    try:
        query = f"SHOW CREATE TABLE {table_name}"
        result = client.command(query)
        
        if result:
            return result
        else:
            print(f"Warning: No DDL found for table {table_name}")
            return None
    except Exception as e:
        print(f"Error extracting DDL for {table_name}: {e}")
        return None


def get_all_metadata_tables(client) -> list:
    """Get list of all tables in the metadata schema."""
    tables_found = []
    
    # Method 1: Query system.tables for tables with 'metadata.' prefix
    try:
        query = """
        SELECT name 
        FROM system.tables 
        WHERE database = 'default' 
        AND name LIKE 'metadata.%'
        ORDER BY name
        """
        result = client.query(query)
        if result.result_rows:
            for row in result.result_rows:
                table_name = row[0]
                if table_name not in tables_found:
                    tables_found.append(table_name)
            print(f"Found {len(tables_found)} table(s) using system.tables query")
    except Exception as e:
        print(f"Method 1 failed: {e}")
    
    # Method 2: Test known metadata tables
    if not tables_found:
        print("Testing known metadata table names...")
        known_tables = [
            "metadata.acquire",
            "metadata.transformation0",
            "metadata.transformation1",
            "metadata.transformation2",
            "metadata.transformation3",
            "metadata.transformation4",
            "metadata.definitions",
            "metadata.discover",
            "metadata.erd",
            "metadata.hierarchies",
            "metadata.dimensional_model"
        ]
        
        for table_name in known_tables:
            try:
                # Try to query the table to see if it exists
                test_query = f"SELECT 1 FROM {table_name} LIMIT 1"
                client.command(test_query)
                tables_found.append(table_name)
                print(f"  ✓ Found: {table_name}")
            except:
                # Table doesn't exist, skip it
                pass
    
    return sorted(tables_found)


def save_ddl_to_file(ddl: str, table_name: str, sql_dir: Path) -> bool:
    """Save DDL to a file in the sql directory."""
    try:
        # Convert table name to filename (e.g., metadata.acquire -> metadata.acquire.sql)
        filename = f"{table_name}.sql"
        filepath = sql_dir / filename
        
        # Write DDL to file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- DDL for {table_name}\n")
            f.write(f"-- Extracted from ClickHouse\n")
            f.write(f"-- \n")
            f.write(f"-- IMPORTANT: This file is auto-generated. Manual edits may be overwritten.\n")
            f.write(f"-- \n\n")
            f.write(ddl)
            f.write("\n")
        
        print(f"  ✓ Saved: {filename}")
        return True
    except Exception as e:
        print(f"  ✗ Error saving DDL for {table_name}: {e}")
        return False


def main():
    """Main function to extract all metadata table DDLs."""
    print("=" * 80)
    print("KIMBALL Metadata DDL Extractor")
    print("=" * 80)
    print()
    
    # Load config and connect to ClickHouse
    print("Connecting to ClickHouse...")
    ch_config = get_clickhouse_config()
    
    if not ch_config:
        print("  ✗ Failed to load ClickHouse configuration")
        return
    
    try:
        host = ch_config.get("host", "localhost")
        port = ch_config.get("port", 8123)
        username = ch_config.get("username", "default")
        password = ch_config.get("password", "")
        database = ch_config.get("database", "default")
        
        print(f"  Attempting connection to {host}:{port}...")
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )
        # Test connection
        client.command("SELECT 1")
        print(f"  ✓ Connected to {host}:{port}")
    except Exception as e:
        print(f"  ✗ Failed to connect to ClickHouse: {e}")
        print(f"  Make sure ClickHouse is running and accessible at {host}:{port}")
        return
    
    print()
    
    # Get SQL directory path
    sql_dir = Path(__file__).parent.parent / "sql"
    sql_dir.mkdir(exist_ok=True)
    print(f"SQL directory: {sql_dir}")
    print()
    
    # Get all metadata tables
    print("Querying ClickHouse for metadata schema tables...")
    tables = get_all_metadata_tables(client)
    
    if not tables:
        print("No metadata tables found. Make sure:")
        print("  1. ClickHouse is running and accessible")
        print("  2. Metadata tables have been created")
        print("  3. Connection configuration is correct")
        return
    
    print()
    print(f"Found {len(tables)} metadata table(s):")
    for table in tables:
        print(f"  - {table}")
    print()
    
    # Extract and save DDL for each table
    print("Extracting DDLs...")
    print()
    
    success_count = 0
    failed_count = 0
    
    for table_name in tables:
        print(f"Processing {table_name}...", end=" ")
        
        ddl = extract_table_ddl(client, table_name)
        
        if ddl:
            if save_ddl_to_file(ddl, table_name, sql_dir):
                success_count += 1
            else:
                failed_count += 1
        else:
            failed_count += 1
            print("FAILED - No DDL extracted")
    
    print()
    print("=" * 80)
    print(f"Summary:")
    print(f"  Total tables: {len(tables)}")
    print(f"  Successfully extracted: {success_count}")
    print(f"  Failed: {failed_count}")
    print("=" * 80)
    
    if success_count > 0:
        print()
        print(f"✓ DDL files saved to: {sql_dir}")
        print()
        print("Next steps:")
        print("  1. Review the generated DDL files")
        print("  2. Update setup/initialization endpoints to use these DDLs")
        print("  3. Commit the DDL files to version control")


if __name__ == "__main__":
    main()
