#!/usr/bin/env python3
"""
Database Verification Script
Run: python verify_database.py
"""

from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config import DATABASE_PATH
from src.database_handler import list_tables, run_sql_query


def print_header(text):
    """Print formatted header"""
    print("\n" + "=" * 70)
    print(f" {text}")
    print("=" * 70)


def verify_database():
    """Main verification function"""

    if not DATABASE_PATH.exists():
        print("Database not found. Run main.py first!")
        return False

    print_header("DATABASE VERIFICATION & EXAMPLE QUERIES")

    try:
        # Show available tables
        tables = list_tables()
        if not tables:
            print("No tables found in database")
            return False

        print("\nAvailable Tables:")
        for table in tables:
            count_result = run_sql_query(f"SELECT COUNT(*) as count FROM {table}")
            if count_result is not None:
                count = count_result.iloc[0]['count']
                print(f"    {table} ({count} rows)")

        # Check each table
        for table in tables:
            verify_table(table)

        # Run example queries
        run_example_queries(tables)

        print_header(" DATABASE VERIFICATION COMPLETE")
        print("\n All checks passed! Your database is ready for analysis.")
        return True

    except Exception as e:
        print(f" Verification failed: {e}")
        return False


def verify_table(table_name):
    """Verify individual table structure and data"""
    print_header(f"VERIFYING TABLE: {table_name}")

    try:
        # Get table schema
        schema = run_sql_query(f"PRAGMA table_info({table_name})")
        if schema is not None:
            print(f"\n Table Schema:")
            print(schema.to_string(index=False))

        # Get sample data
        sample_data = run_sql_query(f"SELECT * FROM {table_name} LIMIT 3")
        if sample_data is not None:
            print(f"\n Sample Data (first 3 rows):")
            print(sample_data.to_string(index=False))

        # Basic statistics
        stats = run_sql_query(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(*) - COUNT(*) as null_count,  
                MIN(rowid) as first_id,
                MAX(rowid) as last_id
            FROM {table_name}
        """)
        if stats is not None:
            print(f"\n Basic Statistics:")
            print(stats.to_string(index=False))

    except Exception as e:
        print(f"  Could not verify table {table_name}: {e}")


def run_example_queries(tables):
    """Run helpful example queries"""
    if not tables:
        return

    # Use the first table for examples
    main_table = tables[0]

    print_header("EXAMPLE ANALYSIS QUERIES")

    try:
        # Query 1: Basic overview
        print("\n  DATASET OVERVIEW")
        overview = run_sql_query(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT rowid) as unique_rows,
                (SELECT COUNT(*) FROM pragma_table_info('{main_table}')) as column_count
            FROM {main_table}
        """)
        if overview is not None:
            print(overview.to_string(index=False))

        # Query 2: Column types sample
        print("\n  COLUMN SAMPLES")
        columns_query = run_sql_query(f"""
            SELECT * FROM {main_table} LIMIT 1
        """)
        if columns_query is not None:
            print("First row sample:")
            print(columns_query.to_string(index=False))

        # Query 3: Numeric column statistics (if any numeric columns)
        numeric_cols = get_numeric_columns(main_table)
        if numeric_cols:
            print(f"\n  NUMERIC COLUMN STATISTICS")
            for col in numeric_cols[:3]:  # Show first 3 numeric columns
                stats = run_sql_query(f"""
                    SELECT 
                        '{col}' as column_name,
                        ROUND(AVG({col}), 2) as average,
                        ROUND(MIN({col}), 2) as minimum,
                        ROUND(MAX({col}), 2) as maximum,
                        COUNT({col}) as non_null_count
                    FROM {main_table}
                """)
                if stats is not None:
                    print(stats.to_string(index=False))

        # Query 4: Missing values analysis
        print(f"\n  DATA QUALITY CHECK")
        quality = run_sql_query(f"""
            SELECT 
                COUNT(*) as total_rows,
                {', '.join([f'SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as {col}_nulls' for col in get_all_columns(main_table)[:5]])}
            FROM {main_table}
        """)
        if quality is not None:
            print("Missing values per column (first 5 columns):")
            print(quality.to_string(index=False))

    except Exception as e:
        print(f"  Example queries failed: {e}")


def get_numeric_columns(table_name):
    """Get list of numeric columns in a table"""
    try:
        # Sample data to infer types
        sample = run_sql_query(f"SELECT * FROM {table_name} LIMIT 1")
        if sample is not None:
            numeric_cols = []
            for col in sample.columns:
                # Simple heuristic: if first value looks numeric
                first_val = sample.iloc[0][col]
                if first_val is not None and isinstance(first_val, (int, float)):
                    numeric_cols.append(col)
            return numeric_cols
    except:
        pass
    return []


def get_all_columns(table_name):
    """Get all column names for a table"""
    try:
        schema = run_sql_query(f"PRAGMA table_info({table_name})")
        if schema is not None:
            return schema['name'].tolist()
    except:
        pass
    return []


def main():
    """Main execution function"""
    print(" Starting Database Verification...")

    success = verify_database()

    if success:
        print(f"""
        NEXT STEPS:
        1. Explore your data with: python verify_database.py
        2. Check visualizations in: outputs/plots/
        3. View analysis reports in: outputs/reports/
        4. Query the database directly: sqlite3 data/pipeline.db

        💡 TIP: Use tools like DB Browser for SQLite for visual database exploration!
        """)
    else:
        print("\n Verification failed. Please run main.py first!")
        sys.exit(1)


if __name__ == "__main__":
    main()